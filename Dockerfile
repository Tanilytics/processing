FROM golang:1.26-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY cmd/ ./cmd/
COPY internal/ ./internal/

RUN CGO_ENABLED=0 GOOS=linux go build \
  -ldflags="-s -w" \
  -o bin/processor \
  ./cmd/processor

FROM alpine:3.21.6

RUN adduser -D -g '' appuser

WORKDIR /app

COPY --from=builder /app/bin/processor/ /bin/processor

RUN chown -R appuser:appuser /app

USER appuser

EXPOSE 3000

HEALTHCHECK --interval=10s --timeout=3s CMD wget -qO- http://127.0.0.1:3000/livez || exit 1

CMD ["/bin/processor"]
