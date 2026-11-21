package store

import (
	"context"
	"encoding/json"
	"time"

	"go.etcd.io/bbolt"
)

// ImportBolt opens the retained legacy file read-only and streams bounded
// transactions. Stop the old consumer first. Both identity sets are imported;
// retries resume safely after interruption without changing delivery times.
func (s *Postgres) ImportBolt(ctx context.Context, path string) error {
	db, err := bbolt.Open(path, 0o600, &bbolt.Options{ReadOnly: true, Timeout: time.Second})
	if err != nil {
		return err
	}
	defer db.Close()
	return db.View(func(old *bbolt.Tx) error {
		for _, name := range [][]byte{notificationsBucket, processedBucket} {
			bucket := old.Bucket(name)
			if bucket == nil {
				continue
			}
			cursor := bucket.Cursor()
			key, value := cursor.First()
			for key != nil {
				if err := ctx.Err(); err != nil {
					return err
				}
				tx, err := s.pool.Begin(ctx)
				if err != nil {
					return err
				}
				for batch := 0; key != nil && batch < 100; batch++ {
					if string(name) == string(notificationsBucket) {
						var n Notification
						err = json.Unmarshal(value, &n)
						if err == nil {
							_, err = insertNotification(ctx, tx, string(key), n)
						}
					} else {
						var count int
						err = tx.QueryRow(ctx, `WITH added AS (INSERT INTO notifier_processed VALUES($1) ON CONFLICT DO NOTHING RETURNING 1)
 SELECT count(*) FROM added`, string(key)).Scan(&count)
						if err == nil && count > 0 {
							_, err = tx.Exec(ctx, `UPDATE notifier_counts SET processed=processed+1 WHERE singleton`)
						}
					}
					if err != nil {
						tx.Rollback(ctx)
						return err
					}
					key, value = cursor.Next()
				}
				if err := tx.Commit(ctx); err != nil {
					return err
				}
			}
		}
		return nil
	})
}
