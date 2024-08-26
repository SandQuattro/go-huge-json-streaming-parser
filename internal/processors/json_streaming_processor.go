package processors

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
)

// ReadData reads data from provided reader and sends them to dataChan.
func ReadData(ctx context.Context, r io.Reader, dataChan chan any) error {
	decoder := json.NewDecoder(r)

	// Read opening delimiter
	t, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("failed to read opening delimiter: %w", err)
	}

	// Make sure opening delimiter is `{`
	if t != json.Delim('{') && t != json.Delim('[') {
		return fmt.Errorf("expected { or [, got %v", t)
	}

	for decoder.More() {
		// Check if context is cancelled.
		if ctx.Err() != nil {
			return ctx.Err()
		}

		if t != json.Delim('[') {
			// Read the data ID.
			t, err = decoder.Token()
			if err != nil {
				return fmt.Errorf("failed to read data ID: %w", err)
			}

			// Make sure data ID is a string.
			_, ok := t.(string)
			if !ok {
				return fmt.Errorf("expected string, got %v", t)
			}
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
