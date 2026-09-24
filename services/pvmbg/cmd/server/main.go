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

	// listen to port 8082
	log.Println("PVMBG server running at :8082")
	err := http.ListenAndServe(":8082", r)

	if err != nil {
		log.Fatal(err)
	}


}