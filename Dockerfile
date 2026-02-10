# Build stage
FROM golang:1.24-alpine AS builder

WORKDIR /app

# Install build dependencies
RUN apk add --no-cache git

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Get version from git or use default
ARG VERSION=dev
ARG COMMIT=unknown

# Build binary
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath \
    -ldflags "-X 'github.com/dbsmedya/goarchive/cmd/mysql-archiver/cmd.Version=${VERSION}' \
              -X 'github.com/dbsmedya/goarchive/cmd/mysql-archiver/cmd.Commit=${COMMIT}' \
              -s -w" \
    -o mysql-archiver ./cmd/mysql-archiver

# Final stage
FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /root/

# Copy binary from builder
COPY --from=builder /app/mysql-archiver .

# Make it executable
RUN chmod +x ./mysql-archiver

# Set as entrypoint
ENTRYPOINT ["./mysql-archiver"]

# Default command (show help)
CMD ["--help"]
