package store

import (
	"context"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/notifier-consumer/internal/event"
)

type Backend interface {
	Deliver(string, event.Envelope) (bool, error)
	ListNotifications(context.Context, int, string) (NotificationPage, error)
	ConfirmNotification(context.Context, string) (Notification, error)
	Stats() (Stats, error)
	Ping(context.Context) error
	Close() error
}
