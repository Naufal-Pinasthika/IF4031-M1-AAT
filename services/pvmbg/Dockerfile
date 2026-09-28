FROM golang:1.23-alpine AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/pvmbg ./cmd/server

FROM alpine:3.20

RUN adduser -D -H appuser
WORKDIR /app

COPY --from=builder /out/pvmbg /app/pvmbg

USER appuser
EXPOSE 8082
ENTRYPOINT ["/app/pvmbg"]
