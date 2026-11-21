FROM golang:1.25-alpine AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/dashboard-consumer ./cmd/server

FROM alpine:3.20
RUN adduser -D -H appuser && mkdir -p /data && chown appuser:appuser /data
WORKDIR /app
COPY --from=builder /out/dashboard-consumer /app/dashboard-consumer
USER appuser
EXPOSE 8085
ENTRYPOINT ["/app/dashboard-consumer"]
