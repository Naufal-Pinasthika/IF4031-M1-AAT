package main

import (
	"log"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/bmkg/internal/middleware"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/pvmbg/internal/handler"
)

func main()  {
	// init chi router
	r := chi.NewRouter()

	// validate auth key & apply delay
	r.Use(middleware.BMKGAuth)
	r.Use(middleware.ApplyDelay)
	
	// define endpoints
	r.Get("/health", handler.GetHealth)
	r.Get("/seismic-events", handler.GetSesimicEvents)
	r.Get("/tsunami-warnings", handler.GetTsunamiWarnings)

	// listen to port 8081
	log.Println("BMKG server running at :8081")
	err := http.ListenAndServe(":8081", r)

	if err != nil {
		log.Fatal(err)
	}


}