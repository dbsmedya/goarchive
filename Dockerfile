# Build stage
FROM golang:1.26.8-alpine AS builder

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
    -ldflags "-X 'github.com/dbsmedya/goarchive/v3/cmd/goarchive/cmd.Version=${VERSION}' \
              -X 'github.com/dbsmedya/goarchive/v3/cmd/goarchive/cmd.Commit=${COMMIT}' \
              -s -w" \
    -o goarchive ./cmd/goarchive

# Final stage: a supported Alpine (BAS-1), pinned to its patch release
FROM alpine:3.24.2

# /work is the working directory for any user, sticky like /tmp (IMG-1)
RUN apk --no-cache add ca-certificates \
 && mkdir -p /work && chmod 1777 /work

COPY --from=builder /app/goarchive /usr/local/bin/goarchive

WORKDIR /work

# A numeric user with no passwd entry: GoArchive reads no home directory or user name, and a
# numeric USER is what Kubernetes runAsNonRoot accepts. Override it with docker run --user.
USER 65532:65532

ENTRYPOINT ["/usr/local/bin/goarchive"]

# Default command (show help)
CMD ["--help"]
