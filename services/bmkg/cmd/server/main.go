package main

import (
	"log"
	"net/http"

	"github.com/go-chi/chi/v5"
)

func main()  {
	// init chi router
	r := chi.NewRouter()

	// GET /health 
	r.Get("/health", func (w http.ResponseWriter, r *http.Request)  {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status":"ok"}`))
	})

	// listen to port 8081
	log.Println("BMKG server running at :8081")
	err := http.ListenAndServe(":8081", r)

	if err != nil {
		log.Fatal(err)
	}


}