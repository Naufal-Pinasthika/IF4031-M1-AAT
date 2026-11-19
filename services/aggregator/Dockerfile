FROM golang:1.25-alpine AS builder

WORKDIR /src
COPY go.mod ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
ARG BUILD_VERSION=development
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w -X main.buildVersion=${BUILD_VERSION}" -o /out/aggregator ./cmd/server

FROM alpine:3.20
RUN adduser -D -H appuser
WORKDIR /app
COPY --from=builder /out/aggregator /app/aggregator
USER appuser
EXPOSE 8083
ENTRYPOINT ["/app/aggregator"]
