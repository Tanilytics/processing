FROM golang:1.26-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY main.go .

RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags="-s -w" \
    -o app \
    .

FROM alpine:latest

RUN adduser -D -g '' appuser

WORKDIR /app

COPY --from=builder /app/processing .

RUN chown -R appuser:appuser /app

USER appuser

EXPOSE 3000

CMD ["./app"]
