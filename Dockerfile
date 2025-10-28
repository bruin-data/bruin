FROM golang:1.23-bullseye AS builder

# Build argument for version information
ARG VERSION=dev
ARG BRANCH_NAME=unknown

# Install build dependencies including C++ standard library for DuckDB
RUN apt-get update && apt-get install -y git gcc g++ libc6-dev

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

# Final stage
FROM debian:12.8-slim

RUN apt-get update && apt-get install -y \
    curl \
    git \
    build-essential \
    binutils \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

RUN adduser --disabled-password --gecos '' bruin

RUN chown -R bruin:bruin /home/bruin

USER bruin

# Create necessary directories for bruin user
RUN mkdir -p /home/bruin/.local/bin /home/bruin/.local/share

# Copy the built binary from builder stage
COPY --from=builder /src/bin/bruin /home/bruin/.local/bin/bruin

ENV PATH="/home/bruin/.local/bin:${PATH}"

# Bootstrap ingestr installation
RUN cd /tmp && /home/bruin/.local/bin/bruin init bootstrap --in-place && /home/bruin/.local/bin/bruin run bootstrap

RUN rm -rf /tmp/bootstrap

CMD ["bruin"]
