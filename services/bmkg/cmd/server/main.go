package main

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/bmkg/internal/data"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/bmkg/internal/handler"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/bmkg/internal/middleware"
)

func main()  {
	// init generator data 
	// configure `x * time.Second' to set the event more faster/slower
	const eventInterval = 10 * time.Second		
	go data.StartGenerator(context.Background(), 10*time.Second)

	// init chi router
	r := chi.NewRouter()
	
	// init logger
	r.Use(middleware.RequestLogger("bmkg"))

	// define endpoints
	r.Get("/health", handler.GetHealth)
	r.Group(func(protected chi.Router) {
		// validate auth key & apply delay
		protected.Use(middleware.BMKGAuth)
		protected.Use(middleware.ApplyDelay)

		protected.Get("/seismic-events", handler.GetSesimicEvents)
		protected.Get("/tsunami-warnings", handler.GetTsunamiWarnings)
	})

	// listen to port 8081
	log.Println("BMKG server running at :8081")
	err := http.ListenAndServe(":8081", r)

	if err != nil {
		log.Fatal(err)
	}


}