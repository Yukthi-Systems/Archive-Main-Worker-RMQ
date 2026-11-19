# Builder Stage
FROM golang:1.26-alpine AS builder

# Install System Dependencies
RUN apk add --no-cache git ca-certificates

# Set working directory
WORKDIR /app

# Copy dependency manifests and download dependencies
COPY go.mod go.sum ./

# Download Go module dependencies
RUN go mod download

# Copy the rest of the source code
COPY . .

# Build the Go application with optimizations for a small binary size
RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags="-s -w" \
    -o /app/consumer \
    ./cmd/consumer/

# Final Stage
FROM alpine:latest

# Install runtime dependencies
RUN apk add --no-cache ca-certificates tzdata

# set working directory.
WORKDIR /app

# Copy the built binary from the builder stage
COPY --from=builder /app/consumer /app/consumer

# Start the consumer application
ENTRYPOINT ["/app/consumer"]

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD pgrep consumer > /dev/null || exit 1
