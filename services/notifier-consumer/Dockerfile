FROM golang:1.25-alpine AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/notifier-consumer ./cmd/server
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/import-bbolt ./cmd/import-bbolt

FROM alpine:3.20
RUN adduser -D -H appuser && mkdir -p /data && chown appuser:appuser /data
WORKDIR /app
COPY --from=builder /out/notifier-consumer /app/notifier-consumer
COPY --from=builder /out/import-bbolt /app/import-bbolt
USER appuser
EXPOSE 8086
ENTRYPOINT ["/app/notifier-consumer"]
