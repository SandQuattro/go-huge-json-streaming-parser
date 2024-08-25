package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gorilla/mux"
	"go-huge-json-streaming-parser/internal/errs"
	"io"
	"log"
	"net/http"
	"time"
)

func main() {
	// create http router
	router := mux.NewRouter()
	router.HandleFunc("/data", UploadData).Methods("POST")

	srv := &http.Server{
		ReadHeaderTimeout: 1 * time.Minute,
		Addr:              ":8080",
		Handler:           router,
	}

	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("HTTP server ListenAndServe Error: %v", err)
	}
}

// UploadData reads data from Huge JSON / file and processing, using streaming with low memory
func UploadData(w http.ResponseWriter, r *http.Request) {
	log.Println("uploading data")

	dataChan := make(chan any)
	errChan := make(chan error)
	doneChan := make(chan struct{})

	go func() {
		err := readData(r.Context(), r.Body, dataChan)
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

// readData reads data from provided reader and sends them to dataChan.
func readData(ctx context.Context, r io.Reader, dataChan chan any) error {
	decoder := json.NewDecoder(r)

	// Read opening delimiter
	t, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("failed to read opening delimiter: %w", err)
	}

	// Make sure opening delimiter is `{`
	if t != json.Delim('{') {
		return fmt.Errorf("expected {, got %v", t)
	}

	for decoder.More() {
		// Check if context is cancelled.
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// Read the data ID.
		t, err := decoder.Token()
		if err != nil {
			return fmt.Errorf("failed to read data ID: %w", err)
		}
		// Make sure data ID is a string.
		_, ok := t.(string)
		if !ok {
			return fmt.Errorf("expected string, got %v", t)
		}

		// Read the data and send it to the channel.
		var data any
		if err := decoder.Decode(&data); err != nil {
			return fmt.Errorf("failed to decode json data: %w", err)
		}

		dataChan <- data
	}

	return nil
}

func RespondOK(data any, w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(data)
}

func InternalError(slug string, err error, w http.ResponseWriter, r *http.Request) {
	httpRespondWithError(err, slug, w, r, "Internal server error", http.StatusInternalServerError)
}

func Unauthorised(slug string, err error, w http.ResponseWriter, r *http.Request) {
	httpRespondWithError(err, slug, w, r, "Unauthorised", http.StatusUnauthorized)
}

func BadRequest(slug string, err error, w http.ResponseWriter, r *http.Request) {
	httpRespondWithError(err, slug, w, r, "Bad request", http.StatusBadRequest)
}

func NotFound(slug string, err error, w http.ResponseWriter, r *http.Request) {
	httpRespondWithError(err, slug, w, r, "Not found", http.StatusBadRequest)
}

func RespondWithError(err error, w http.ResponseWriter, r *http.Request) {
	var slugError errs.SlugError
	ok := errors.As(err, &slugError)
	if !ok {
		InternalError("internal-server-error", err, w, r)
		return
	}

	switch slugError.ErrorType() {
	case errs.ErrorTypeAuthorization:
		Unauthorised(slugError.Slug(), slugError, w, r)
	case errs.ErrorTypeIncorrectInput:
		BadRequest(slugError.Slug(), slugError, w, r)
	case errs.ErrorTypeNotFound:
		NotFound(slugError.Slug(), slugError, w, r)
	default:
		InternalError(slugError.Slug(), slugError, w, r)
	}
}

func httpRespondWithError(err error, slug string, w http.ResponseWriter, r *http.Request, msg string, status int) {
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

func (e ErrorResponse) Render(w http.ResponseWriter, _ *http.Request) error {
	w.WriteHeader(e.httpStatus)
	return nil
}
