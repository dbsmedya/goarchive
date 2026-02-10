# Build stage
FROM golang:1.24-alpine AS builder

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
    -ldflags "-X 'github.com/dbsmedya/goarchive/cmd/goarchive/cmd.Version=${VERSION}' \
              -X 'github.com/dbsmedya/goarchive/cmd/goarchive/cmd.Commit=${COMMIT}' \
              -s -w" \
    -o goarchive ./cmd/goarchive

# Final stage
FROM alpine:3.19

RUN apk --no-cache add ca-certificates

WORKDIR /root/

# Copy binary from builder
COPY --from=builder /app/goarchive .

# Make it executable
RUN chmod +x ./goarchive

# Set as entrypoint
ENTRYPOINT ["./goarchive"]

# Default command (show help)
CMD ["--help"]
