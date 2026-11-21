package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/notifier-consumer/internal/store"
)

func main() {
	if len(os.Args) != 2 || os.Getenv("NOTIFIER_DATABASE_URL") == "" {
		log.Fatal("usage: NOTIFIER_DATABASE_URL=... import-bbolt /data/notifier.db (old writer must be stopped)")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	s, err := store.OpenPostgres(ctx, os.Getenv("NOTIFIER_DATABASE_URL"))
	if err != nil {
		log.Fatal(err)
	}
	defer s.Close()
	if err := s.ImportBolt(ctx, os.Args[1]); err != nil {
		log.Fatal(err)
	}
	stats, err := s.Stats()
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("legacy import complete: notifications=%d processed=%d", stats.NotificationCount, stats.ProcessedCount)
}
