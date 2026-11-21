package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/notifier-consumer/internal/event"
	"go.etcd.io/bbolt"
	"strings"
	"time"
)

var ErrNotificationAbsent = errors.New("notification absent")

// ConfirmNotification is an explicit measurement operation, not the delivery
// path. bbolt exposes the row only after the synchronous write transaction has
// completed. The observer timestamps the RESPONSE using its own monotonic clock,
// not the source clock or this host's created_at.
func (s *Store) ConfirmNotification(ctx context.Context, id string) (Notification, error) {
	var out Notification
	if len(id) < 1 || len(id) > 128 || strings.ContainsAny(id, "\x00\n\r") {
		return out, ErrInvalidPage
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return out, err
	}
	found := false
	err := s.db.View(func(tx *bbolt.Tx) error {
		prefix := []byte(event.UpsertedType + ":" + id + ":")
		cursor := tx.Bucket(notificationsBucket).Cursor()
		for key, value := cursor.Seek(prefix); key != nil && bytes.HasPrefix(key, prefix); key, value = cursor.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			var n Notification
			if err := json.Unmarshal(value, &n); err != nil {
				return err
			}
			if n.HazardID == id {
				out = n
				found = true
				return nil
			}
		}
		return nil
	})
	if err == nil && !found {
		err = ErrNotificationAbsent
	}
	return out, err
}
