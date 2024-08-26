package main

import (
	"errors"
	"github.com/gorilla/mux"
	"go-huge-json-streaming-parser/internal/handlers"
	"log"
	"net/http"
	"time"
)

func main() {
	// create http router
	router := mux.NewRouter()
	router.HandleFunc("/data", handlers.UploadData).Methods("POST")

	srv := &http.Server{
		ReadHeaderTimeout: 1 * time.Minute,
		Addr:              ":8080",
		Handler:           router,
	}

	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("HTTP server ListenAndServe Error: %v", err)
	}
}
