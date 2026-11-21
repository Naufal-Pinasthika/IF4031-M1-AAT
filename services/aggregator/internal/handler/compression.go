package handler

import (
	"bytes"
	"compress/gzip"
	"net/http"
	"strconv"
	"strings"
	"sync"
)

// BestSpeed trades a bounded amount of CPU for fewer wire bytes. Compress only
// negotiated, complete JSON responses. Never change the JSON or client checks.
var gzipWriters = sync.Pool{New: func() any { w, _ := gzip.NewWriterLevel(nil, gzip.BestSpeed); return w }}

// A gzip encoder has substantially more state than a socket write. Bound its
// concurrency separately from the response budget under the 512Mi pod limit.
// Exhaustion preserves the original uncompressed representation, not an error.
var compressionSlots = make(chan struct{}, 16)

func acceptsGzip(header string) bool {
	wildcard := false
	for _, part := range strings.Split(header, ",") {
		fields := strings.Split(strings.TrimSpace(part), ";")
		name := strings.ToLower(strings.TrimSpace(fields[0]))
		q := 1.0
		for _, param := range fields[1:] {
			k, v, ok := strings.Cut(strings.TrimSpace(param), "=")
			if ok && strings.EqualFold(k, "q") {
				n, e := strconv.ParseFloat(v, 64)
				if e != nil || n < 0 || n > 1 {
					q = 0
				} else {
					q = n
				}
			}
		}
		if name == "gzip" {
			return q > 0
		}
		if name == "*" {
			wildcard = q > 0
		}
	}
	return wildcard
}

func compressedPage(w http.ResponseWriter, r *http.Request, body []byte) bool {
	w.Header().Add("Vary", "Accept-Encoding")
	if r == nil || len(body) < 1024 || !acceptsGzip(r.Header.Get("Accept-Encoding")) {
		return false
	}
	select {
	case compressionSlots <- struct{}{}:
		defer func() { <-compressionSlots }()
	default:
		return false
	}
	var output bytes.Buffer
	writer := gzipWriters.Get().(*gzip.Writer)
	writer.Reset(&output)
	_, err := writer.Write(body)
	closeErr := writer.Close()
	writer.Reset(nil)
	gzipWriters.Put(writer)
	if err != nil || closeErr != nil || output.Len() >= len(body) {
		return false
	}
	w.Header().Set("Content-Encoding", "gzip")
	w.Header().Set("Content-Length", strconv.Itoa(output.Len()))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(output.Bytes())
	return true
}
