FROM golang:1.23-bullseye AS builder

# Build argument for version information
ARG VERSION=dev
ARG BRANCH_NAME=unknown
ARG TARGETPLATFORM

# Install build dependencies including C++ standard library for DuckDB
RUN apt-get update && apt-get install -y git gcc g++ libc6-dev

# Set platform emulation for ARM64 builds to handle ibm-db dependency issues
# This follows the approach from: https://levelup.gitconnected.com/setting-up-ibm-db2-database-in-arm-64-a015105963c7
RUN if [ "$TARGETPLATFORM" = "linux/arm64" ]; then \
        export DOCKER_DEFAULT_PLATFORM=linux/amd64; \
    fi

# Set working directory
WORKDIR /src

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies with cache mount (safe to cache)
RUN --mount=type=cache,target=/root/.cache/go-build go mod download

# Copy source code
COPY . .

# Build the application with version information from build args (with build cache for incremental builds)
RUN --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=1 go build -v -tags="no_duckdb_arrow" -ldflags="-s -w -X main.version=${VERSION} -X main.commit=${BRANCH_NAME}" -o "bin/bruin" .

# Bootstrap ingestr installation
RUN cd /tmp && /src/bin/bruin init bootstrap --in-place && /src/bin/bruin run bootstrap

RUN rm -rf /tmp/bootstrap

# Final stage
FROM debian:12.8-slim

RUN apt-get update && apt-get install -y curl git

RUN adduser --disabled-password --gecos '' bruin

# Copy the built binary from builder stage
COPY --from=builder /src/bin/bruin /usr/local/bin/bruin

# Set working directory and ensure bruin user has write permissions
WORKDIR /workspace
RUN chown -R bruin:bruin /workspace

USER bruin

ENV PATH="/usr/local/bin:${PATH}"

# Add healthcheck
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD bruin version > /dev/null || exit 1

CMD ["bruin"]
