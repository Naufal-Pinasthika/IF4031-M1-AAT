FROM golang:1.25-alpine AS builder

WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/aggregator ./cmd/server

FROM alpine:3.20
RUN adduser -D -H appuser
WORKDIR /app
COPY --from=builder /out/aggregator /app/aggregator
USER appuser
EXPOSE 8083
ENTRYPOINT ["/app/aggregator"]
