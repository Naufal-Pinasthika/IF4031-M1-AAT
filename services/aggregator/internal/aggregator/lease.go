package aggregator

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/correlation"
	metric "github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/metrics"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/store"
)

type requestGuardKey struct{}
type requestGuard func(context.Context) error
type pollIdentityKey struct{}

// Every HTTP round trip (including BMKG's second endpoint) revalidates the
// current epoch. This reduces stale requests; arbitrary pauses after this
// check still require upstream-side fencing for strict exactly-once HTTP.
type leaseTransport struct{ next http.RoundTripper }

func (t leaseTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if err := r.Context().Err(); err != nil {
		return nil, err
	}
	if guard, ok := r.Context().Value(requestGuardKey{}).(requestGuard); ok {
		if err := guard(r.Context()); err != nil {
			return nil, err
		}
	}
	// A renewal may have canceled the request while validation was in flight.
	if err := r.Context().Err(); err != nil {
		return nil, err
	}
	if attempt, ok := r.Context().Value(pollIdentityKey{}).(store.PollAttempt); ok {
		r = r.Clone(r.Context())
		r.Header.Set("X-Poll-ID", strconv.FormatInt(attempt.ID, 10))
		r.Header.Set("X-Poll-Holder", attempt.Holder)
		r.Header.Set("X-Poll-Epoch", strconv.FormatInt(attempt.Token, 10))
	}
	return t.next.RoundTrip(r)
}

func pollCorrelationID(a store.PollAttempt) string {
	return "poll-" + a.Source + "-" + strconv.FormatInt(a.ID, 10)
}

// Disable implicit GET replay on reused connections and redirects for source
// requests. Any retry must be a new recorded cycle, not hidden in Transport.
// These clients poll low-rate upstreams; this does NOT affect client read load.
func sourceHTTPClient(timeout time.Duration) *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DisableKeepAlives = true
	transport.MaxConnsPerHost = 2
	transport.ForceAttemptHTTP2 = false
	transport.Protocols = new(http.Protocols)
	transport.Protocols.SetHTTP1(true)
	return &http.Client{Timeout: timeout, Transport: leaseTransport{next: transport},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}

func (a *Aggregator) leaseContext(ctx context.Context, source string, token int64) (context.Context, func()) {
	cycle, cancel := context.WithCancelCause(ctx)
	guard := requestGuard(func(requestCtx context.Context) error {
		return a.coordinator.ValidateLease(requestCtx, source, a.config.InstanceID, token)
	})
	cycle = context.WithValue(cycle, requestGuardKey{}, guard)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(a.config.LeaseTTL / 3)
		defer ticker.Stop()
		for {
			select {
			case <-cycle.Done():
				return
			case <-ticker.C:
				if err := a.coordinator.RenewLease(cycle, source, a.config.InstanceID, token, a.config.LeaseTTL); err != nil {
					metric.Default.Leader(source, false)
					a.logger.Warn("source_lease_lost", "source", source, "holder_id", a.config.InstanceID, "fencing_token", token, "error", err)
					cancel(err)
					return
				}
			}
		}
	}()
	return cycle, func() { cancel(context.Canceled); <-done }
}

func (a *Aggregator) runSource(ctx context.Context, source string, interval time.Duration, poll func(context.Context, int64) error) {
	defer metric.Default.Leader(source, false)
	var ownedToken int64
	defer func() {
		if ownedToken == 0 {
			return
		}
		// Shutdown has canceled ctx; use a new bounded context, and preserve
		// the row/token so an old owner cannot create an ABA epoch reset.
		releaseCtx, cancel := context.WithTimeout(context.Background(), a.config.CoordinationTimeout)
		defer cancel()
		if err := a.coordinator.ReleaseLease(releaseCtx, source, a.config.InstanceID, ownedToken); err != nil {
			a.logger.Warn("source_lease_release_failed", "source", source, "error", err)
		}
	}()
	for ctx.Err() == nil {
		delay := interval
		lease, leader, err := a.coordinator.AcquireOrRenewLease(ctx, source, a.config.InstanceID, a.config.LeaseTTL)
		metric.Default.Leader(source, leader && err == nil)
		if err != nil {
			a.recordPollFailure(source, err)
		} else if !leader {
			ownedToken = 0
			a.status.Standby(source)
		} else {
			ownedToken = lease.Token
			a.logger.Info("source_lease_acquired", "source", source, "holder_id", a.config.InstanceID, "fencing_token", lease.Token)
			ran, retryAfter, pollErr := a.pollCycle(ctx, source, interval, lease.Token, poll)
			if retryAfter > 0 {
				delay = retryAfter
			} else {
				delay = time.Millisecond
			}
			if ran && pollErr == nil {
				a.status.Success(source, time.Now())
				metric.Default.Poll(source, "success")
			} else if pollErr != nil && ctx.Err() == nil {
				metric.Default.Leader(source, false)
				a.recordPollFailure(source, pollErr)
			}
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (a *Aggregator) pollCycle(ctx context.Context, source string, interval time.Duration, token int64, poll func(context.Context, int64) error) (bool, time.Duration, error) {
	started := time.Now()
	// A failed/ambiguous begin or finish can leave an in-flight row. Retire
	// the epoch, so a later acquisition can reconcile it rather than keep
	// renewing an abandoned same-epoch cycle forever.
	retire := func(cause error) error {
		cleanup, cancel := context.WithTimeout(context.Background(), a.config.CoordinationTimeout)
		defer cancel()
		return errors.Join(cause, a.coordinator.ReleaseLease(cleanup, source, a.config.InstanceID, token))
	}
	attempt, run, err := a.coordinator.BeginPoll(ctx, source, a.config.InstanceID, token, interval)
	if err != nil {
		return false, interval, retire(err)
	}
	if !run {
		return false, attempt.RetryAfter, nil
	}
	id := pollCorrelationID(attempt)
	a.logger.Info("source_poll_started", "source", source, "poll_id", attempt.ID, "holder_id", attempt.Holder, "fencing_token", token, "correlation_id", id)
	cycle, stop := a.leaseContext(ctx, source, token)
	cycle = correlation.WithID(cycle, id)
	cycle = context.WithValue(cycle, pollIdentityKey{}, attempt)
	cycle = context.WithValue(cycle, requestGuardKey{}, requestGuard(func(requestCtx context.Context) error {
		return a.coordinator.ValidatePoll(requestCtx, source, a.config.InstanceID, token, attempt.ID)
	}))
	err = poll(cycle, token)
	if err == nil && cycle.Err() != nil {
		err = context.Cause(cycle)
	}
	stop()
	finishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), a.config.CoordinationTimeout)
	defer cancel()
	finishErr := a.coordinator.FinishPoll(finishCtx, attempt, interval, err)
	if finishErr != nil {
		err = errors.Join(err, retire(finishErr))
	}
	result := "completed"
	if err != nil {
		result = "uncertain"
	}
	a.logger.Info("source_poll_finished", "source", source, "poll_id", attempt.ID, "holder_id", attempt.Holder, "fencing_token", token, "result", result, "correlation_id", id, "error", err)
	delay := max(time.Millisecond, interval-time.Since(started))
	if err != nil {
		delay = interval
	}
	return true, delay, err
}

// Projection is independent of polling leadership: only already committed
// intents are replayed. Crashes before the delivery acknowledgement replay the
// same version, without admitting stale inputs or changing their identity.
func (a *Aggregator) RunDeliveries(ctx context.Context) {
	var workers sync.WaitGroup
	targets := []string{"broker", "canonical"} // canonical drains pending pre-v3 intents
	for _, unit := range a.config.StorageUnits {
		targets = append(targets, "canonical:"+unit.Name)
	}
	for _, target := range targets {
		workers.Go(func() {
			idleDelay := 50 * time.Millisecond
			for ctx.Err() == nil {
				worked, err := a.coordinator.DeliverNext(ctx, "", target, func(deliveryCtx context.Context, item store.AcceptedEvent) error {
					deliveryCtx = correlation.WithID(deliveryCtx, item.CorrelationID)
					return a.deliverAccepted(deliveryCtx, item)
				})
				if err != nil {
					a.logger.Warn("ingestion_delivery_failed", "target", target, "error", err)
				}
				if !worked || err != nil {
					timer := time.NewTimer(idleDelay)
					select {
					case <-ctx.Done():
						timer.Stop()
						return
					case <-timer.C:
					}
					idleDelay = min(500*time.Millisecond, idleDelay*2)
				} else {
					idleDelay = 50 * time.Millisecond
				}
			}
		})
	}
	workers.Wait()
}

// Each target runs independently and its acknowledgement is durable. A failed
// replica neither delays critical broker delivery nor republishes a broker
// projection that was already acknowledged. Ambiguous acks remain at-least-once.
func (a *Aggregator) deliverAccepted(ctx context.Context, item store.AcceptedEvent) error {
	canonical := item.Event
	switch item.Target {
	case "broker":
		return a.publish(ctx, item.Event, item.CorrelationID, item.Version)
	case "canonical":
		return a.store.UpsertAccepted(ctx, &canonical, item.Version)
	default:
		return a.store.UpsertAcceptedTarget(ctx, &canonical, item.Version, item.Target)
	}
}
