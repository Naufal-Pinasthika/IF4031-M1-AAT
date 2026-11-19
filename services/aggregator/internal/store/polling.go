package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

var ErrPollBusy = errors.New("source has an unfinished poll in this epoch")
var ErrPollSuperseded = errors.New("source poll is no longer current")

type PollAttempt struct {
	RetryAfter time.Duration   `json:"-"`
	ID         int64           `json:"poll_id"`
	Source     string          `json:"source"`
	Holder     string          `json:"holder_id"`
	Token      int64           `json:"fencing_token"`
	PreviousID *int64          `json:"previous_poll_id,omitempty"`
	Status     string          `json:"status"`
	StartedAt  time.Time       `json:"started_at"`
	FinishedAt *time.Time      `json:"finished_at"`
	LastError  string          `json:"last_error,omitempty"`
	Checkpoint json.RawMessage `json:"checkpoint"`
}

// All poll-state changes lock the same row as admission and cursor advancement.
func lockPollLease(ctx context.Context, tx pgx.Tx, source, holder string, token int64) (time.Time, error) {
	var now time.Time
	err := tx.QueryRow(ctx, `UPDATE source_leases SET updated_at=clock_timestamp()
 WHERE source=$1 AND holder_id=$2 AND fencing_token=$3 AND lease_until>clock_timestamp()
 RETURNING clock_timestamp()`, source, holder, token).Scan(&now)
	if errors.Is(err, pgx.ErrNoRows) {
		err = ErrLeaseLost
	}
	return now, err
}

// BeginPoll serializes the logical polling cadence across pod starts, scaling
// and ownership changes. A completed cycle's cooldown is not reset by takeover.
// An abandoned cycle becomes explicitly uncertain and incurs one shared retry
// interval before replay; it is NEVER treated as a successfully consumed page.
func (c *Coordinator) BeginPoll(ctx context.Context, source, holder string, token int64, interval time.Duration) (PollAttempt, bool, error) {
	if interval <= 0 || token <= 0 {
		return PollAttempt{}, false, fmt.Errorf("invalid polling bounds")
	}
	op, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	tx, err := c.pool.Begin(op)
	if err != nil {
		return PollAttempt{}, false, err
	}
	defer tx.Rollback(op)
	now, err := lockPollLease(op, tx, source, holder, token)
	if err != nil {
		return PollAttempt{}, false, err
	}
	var id, epoch int64
	var owner, status string
	var due time.Time
	err = tx.QueryRow(op, `SELECT p.poll_id,p.holder_id,p.fencing_token,p.status,h.next_poll_at
 FROM source_poll_heads h JOIN poll_attempts p USING(poll_id) WHERE h.source=$1 FOR UPDATE OF h`, source).Scan(&id, &owner, &epoch, &status, &due)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return PollAttempt{}, false, err
	}
	var previous *int64
	if err == nil {
		previous = &id
		if status == "in_flight" {
			if owner == holder && epoch == token {
				return PollAttempt{}, false, ErrPollBusy
			}
			_, err = tx.Exec(op, `UPDATE poll_attempts SET status='uncertain',finished_at=clock_timestamp(),last_error='owner_epoch_changed' WHERE poll_id=$1`, id)
			if err == nil {
				_, err = tx.Exec(op, `UPDATE source_poll_heads SET next_poll_at=GREATEST(next_poll_at,clock_timestamp()+($2*interval '1 millisecond')) WHERE source=$1`, source, interval.Milliseconds())
			}
			if err != nil {
				return PollAttempt{}, false, err
			}
			return PollAttempt{RetryAfter: max(interval, due.Sub(now))}, false, tx.Commit(op)
		}
		if now.Before(due) {
			return PollAttempt{RetryAfter: due.Sub(now)}, false, tx.Commit(op)
		}
	}
	attempt := PollAttempt{Source: source, Holder: holder, Token: token, PreviousID: previous, Status: "in_flight"}
	err = tx.QueryRow(op, `INSERT INTO poll_attempts(source,holder_id,fencing_token,previous_poll_id,status,checkpoint)
 VALUES($1,$2,$3,$4,'in_flight',COALESCE((SELECT jsonb_build_object('occurred_at',occurred_at,'opaque_cursor',opaque_cursor)
 FROM source_cursors WHERE source=$1),'{}'::jsonb)) RETURNING poll_id,started_at,checkpoint`, source, holder, token, previous).Scan(&attempt.ID, &attempt.StartedAt, &attempt.Checkpoint)
	if err != nil {
		return PollAttempt{}, false, err
	}
	var attempts int64
	err = tx.QueryRow(op, `INSERT INTO source_poll_heads(source,poll_id,next_poll_at) VALUES($1,$2,clock_timestamp())
 ON CONFLICT(source) DO UPDATE SET poll_id=EXCLUDED.poll_id,next_poll_at=EXCLUDED.next_poll_at,
 attempt_count=source_poll_heads.attempt_count+1 RETURNING attempt_count`, source, attempt.ID).Scan(&attempts)
	if err != nil {
		return PollAttempt{}, false, err
	}
	// Metadata only; retain at least the newest 10k attempts/source, prune
	// occasionally by sequence rather than a possibly shifted wall clock.
	if attempts%128 == 0 {
		_, err = tx.Exec(op, `DELETE FROM poll_attempts WHERE source=$1 AND status<>'in_flight'
 AND poll_id < (SELECT poll_id FROM poll_attempts WHERE source=$1 ORDER BY poll_id DESC OFFSET 9999 LIMIT 1)`, source)
		if err != nil {
			return PollAttempt{}, false, err
		}
	}
	if err = tx.Commit(op); err != nil {
		return PollAttempt{}, false, err
	}
	return attempt, true, nil
}

func (c *Coordinator) ValidatePoll(ctx context.Context, source, holder string, token, id int64) error {
	op, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	var valid bool
	err := c.pool.QueryRow(op, `SELECT EXISTS(SELECT 1 FROM source_leases l
 JOIN source_poll_heads h USING(source) JOIN poll_attempts p USING(poll_id)
 WHERE l.source=$1 AND l.holder_id=$2 AND l.fencing_token=$3 AND l.lease_until>clock_timestamp()
 AND p.poll_id=$4 AND p.holder_id=$2 AND p.fencing_token=$3 AND p.status='in_flight')`, source, holder, token, id).Scan(&valid)
	if err != nil {
		return err
	}
	if !valid {
		return ErrPollSuperseded
	}
	return nil
}

func (c *Coordinator) FinishPoll(ctx context.Context, attempt PollAttempt, interval time.Duration, pollErr error) error {
	if interval <= 0 {
		return fmt.Errorf("invalid polling interval")
	}
	op, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	tx, err := c.pool.Begin(op)
	if err != nil {
		return err
	}
	defer tx.Rollback(op)
	if _, err = lockPollLease(op, tx, attempt.Source, attempt.Holder, attempt.Token); err != nil {
		return err
	}
	status, message := "completed", ""
	if pollErr != nil {
		status = "uncertain"
		runes := []rune(strings.ToValidUTF8(pollErr.Error(), "�"))
		if len(runes) > 1024 {
			runes = runes[:1024]
		}
		message = string(runes)
	}
	result, err := tx.Exec(op, `UPDATE poll_attempts p SET status=$5,finished_at=clock_timestamp(),last_error=$6
 WHERE p.source=$1 AND p.holder_id=$2 AND p.fencing_token=$3 AND p.poll_id=$4 AND p.status='in_flight'
 AND EXISTS(SELECT 1 FROM source_poll_heads h WHERE h.source=$1 AND h.poll_id=p.poll_id)`, attempt.Source, attempt.Holder, attempt.Token, attempt.ID, status, message)
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		return ErrPollSuperseded
	}
	_, err = tx.Exec(op, `UPDATE source_poll_heads h SET next_poll_at=CASE WHEN $3 THEN
 clock_timestamp()+($2*interval '1 millisecond') ELSE GREATEST(clock_timestamp(),p.started_at+($2*interval '1 millisecond')) END
 FROM poll_attempts p WHERE h.source=$1 AND p.poll_id=h.poll_id`, attempt.Source, interval.Milliseconds(), pollErr != nil)
	if err != nil {
		return err
	}
	return tx.Commit(op)
}

// Operational metadata only, not the specification's canonical Audit API.
func (c *Coordinator) ListPollAttempts(ctx context.Context, source string, limit int) ([]PollAttempt, error) {
	if limit < 1 || limit > 1000 {
		return nil, fmt.Errorf("poll limit must be between 1 and 1000")
	}
	op, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	rows, err := c.pool.Query(op, `SELECT poll_id,source,holder_id,fencing_token,previous_poll_id,status,started_at,finished_at,last_error,checkpoint
 FROM poll_attempts WHERE ($1='' OR source=$1) ORDER BY poll_id DESC LIMIT $2`, source, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]PollAttempt, 0, limit)
	for rows.Next() {
		var item PollAttempt
		if err := rows.Scan(&item.ID, &item.Source, &item.Holder, &item.Token, &item.PreviousID, &item.Status, &item.StartedAt, &item.FinishedAt, &item.LastError, &item.Checkpoint); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}
