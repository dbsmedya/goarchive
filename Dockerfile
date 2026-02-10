# Build stage
FROM golang:1.23-alpine AS builder

WORKDIR /app

# Install build dependencies
RUN apk add --no-cache git ca-certificates

# Set Go environment for reliable module downloads
ENV GOPROXY=https://proxy.golang.org,direct
ENV GOSUMDB=sum.golang.org

# Copy go mod files first for better layer caching
COPY go.mod go.sum ./

# Download dependencies with retry logic
RUN go mod download && go mod verify

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
FROM alpine:3.19

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
