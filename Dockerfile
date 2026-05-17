# argument for Go version
ARG GO_VERSION=1.26.3

FROM golang:${GO_VERSION}-alpine AS builder

# Install CA certs and create user in builder (these tools don't exist in scratch)
RUN apk add --no-cache ca-certificates=20260413-r0 && \
  adduser -D -g '' -u 1000 appuser

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY cmd/ ./cmd/
COPY internal/ ./internal/

RUN CGO_ENABLED=0 GOOS=linux go build \
  -ldflags="-s -w" \
  -installsuffix 'static' \
  -o bin/processor \
  ./cmd/processor

FROM scratch AS final

WORKDIR /app

# Copy the static binary
COPY --from=builder /app/bin/processor/ /bin/processor

# GeoIP Database
COPY data/GeoLite2-City.mmdb /data/GeoLite2-City.mmdb

# Copy CA certificates so HTTPS/TLS works
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

# Copy user database so we can run as non-root by name
COPY --from=builder /etc/passwd /etc/passwd

USER appuser

EXPOSE 3000

CMD ["/bin/processor"]
