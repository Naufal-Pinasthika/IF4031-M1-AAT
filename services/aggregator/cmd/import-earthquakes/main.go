package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/model"
)

// p1 consumes only canonical JSONL never guesses assistant dataset columns or source identities
func main() {
	endpoint := flag.String("url", "http://localhost:8083", "Aggregator base URL")
	file := flag.String("file", "", "canonical HazardEvent JSONL file")
	flag.Parse()
	key := os.Getenv("AGGREGATOR_API_KEY")
	if *file == "" || key == "" {
		fmt.Fprintln(os.Stderr, "-file and AGGREGATOR_API_KEY are required")
		os.Exit(2)
	}
	input, err := os.Open(*file)
	if err != nil {
		fmt.Fprintln(os.Stderr, "cannot open input file")
		os.Exit(1)
	}
	defer input.Close()
	accepted, failed, err := importJSONL(context.Background(), input, *endpoint, key)
	fmt.Printf("accepted=%d failed=%d\n", accepted, failed)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// rerunning a partially imported file is safe because identities are deterministic
func importJSONL(ctx context.Context, input io.Reader, endpoint, key string) (int, int, error) {
	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 64<<10), 2<<20)
	client := &http.Client{Timeout: 30 * time.Second}
	batch := make([]model.HazardEvent, 0, 100)
	accepted, failed, line := 0, 0, 0
	send := func() error {
		data, err := json.Marshal(batch)
		if err != nil {
			return fmt.Errorf("cannot encode canonical batch")
		}
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint+"/internal/import/events", bytes.NewReader(data))
		if err != nil {
			return fmt.Errorf("invalid endpoint")
		}
		request.Header.Set("X-Aggregator-Key", key)
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("X-Correlation-ID", fmt.Sprintf("p1-import-%d", line))
		response, err := client.Do(request)
		if err != nil {
			failed += len(batch)
			return fmt.Errorf("import request failed; rerun the file to retry")
		}
		defer response.Body.Close()
		var result struct {
			Accepted int `json:"accepted"`
			Failed   int `json:"failed"`
		}
		if json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&result) != nil || result.Accepted+result.Failed != len(batch) {
			failed += len(batch)
			return fmt.Errorf("invalid import response (HTTP %d)", response.StatusCode)
		}
		accepted += result.Accepted
		failed += result.Failed
		if response.StatusCode != 200 || result.Failed > 0 {
			return fmt.Errorf("batch partly failed; rerun the file to retry")
		}
		batch = batch[:0]
		return nil
	}
	for scanner.Scan() {
		line++
		if len(bytes.TrimSpace(scanner.Bytes())) == 0 {
			continue
		}
		var event model.HazardEvent
		if json.Unmarshal(scanner.Bytes(), &event) != nil {
			return accepted, failed + 1, fmt.Errorf("invalid canonical JSON at line %d", line)
		}
		batch = append(batch, event)
		if len(batch) == 100 {
			if err := send(); err != nil {
				return accepted, failed, err
			}
		}
	}
	if scanner.Err() != nil {
		return accepted, failed + 1, fmt.Errorf("cannot read canonical JSONL (maximum line 2 MiB)")
	}
	if len(batch) > 0 {
		if err := send(); err != nil {
			return accepted, failed, err
		}
	}
	return accepted, failed, nil
}
