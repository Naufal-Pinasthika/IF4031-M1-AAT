FROM golang:1.25-alpine AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/notifier-consumer ./cmd/server

FROM alpine:3.20
RUN adduser -D -H appuser && mkdir -p /data && chown appuser:appuser /data
WORKDIR /app
COPY --from=builder /out/notifier-consumer /app/notifier-consumer
USER appuser
EXPOSE 8086
ENTRYPOINT ["/app/notifier-consumer"]
