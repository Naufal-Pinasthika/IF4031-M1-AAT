FROM golang:1.23-alpine AS builder

WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/client-api ./cmd/server

FROM alpine:3.20
RUN adduser -D -H appuser
WORKDIR /app
COPY --from=builder /out/client-api /app/client-api
USER appuser
EXPOSE 8080
ENTRYPOINT ["/app/client-api"]
