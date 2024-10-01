package handlers

import (
	"encoding/json"
	"log"
	"net/http"

	"go-huge-json-streaming-parser/internal/processors"
)

// UploadData reads data from Huge JSON / file and processing, using streaming with low memory
func UploadData(w http.ResponseWriter, r *http.Request) {
	log.Println("uploading data")

	dataChan := make(chan any)
	errChan := make(chan error)
	doneChan := make(chan struct{})

	go func() {
		err := processors.ReadData(r.Context(), r.Body, dataChan)
		if err != nil {
			errChan <- err
		} else {
			doneChan <- struct{}{}
		}
	}()
	dataCounter := 0
	for {
		select {
		case <-r.Context().Done():
			log.Printf("request cancelled")
			return
		case <-doneChan:
			log.Printf("finished reading data")
			RespondOK(map[string]int{"total_data": dataCounter}, w, r)
			return
		case err := <-errChan:
			log.Printf("error while parsing data json: %+v", err)
			BadRequest("invalid-json", err, w, r)
			return
		case data := <-dataChan:
			dataCounter++
			log.Printf("[%d] received data: %+v", dataCounter, data)
		}
	}
}

func RespondOK(data any, w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(data)
}

func BadRequest(slug string, err error, w http.ResponseWriter, r *http.Request) {
	httpRespondWithError(err, slug, w, r, "Bad request", http.StatusBadRequest)
}

func httpRespondWithError(err error, slug string, w http.ResponseWriter, _ *http.Request, msg string, status int) {
	log.Printf("error: %s, slug: %s, msg: %s", err, slug, msg)

	resp := ErrorResponse{slug, status}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(resp)
}

type ErrorResponse struct {
	Slug       string `json:"slug"`
	httpStatus int
}
