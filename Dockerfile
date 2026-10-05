# syntax=docker/dockerfile:1.6
FROM golang:1.26.8-trixie AS builder

# Build argument for version information
ARG VERSION=dev
ARG BRANCH_NAME=unknown

# Install build dependencies including C++ standard library for DuckDB
RUN apt-get update && apt-get install -y git gcc g++ libc6-dev curl

# Set working directory
WORKDIR /src

# Copy go mod files (including the nested semantic-engine module referenced
# by a local replace directive, so `go mod download` can resolve it)
COPY go.mod go.sum ./
COPY semantic-engine/go.mod semantic-engine/go.sum semantic-engine/

# Download Go dependencies with cache mount
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    go mod download

# Copy source code
COPY . .

# Build the application with version information from build args (with build cache for incremental builds)
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    CGO_ENABLED=1 go build -v -tags="no_duckdb_arrow" -ldflags="-s -w -X main.version=${VERSION} -X main.commit=${BRANCH_NAME}" -o "bin/bruin" .

# Final stage
FROM debian:trixie-slim

RUN apt-get update && apt-get install -y \
    curl \
    git \
    build-essential \
    binutils \
    python3-dev \
    unixodbc \
    libodbc2 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN adduser --disabled-password --gecos '' bruin

RUN chown -R bruin:bruin /home/bruin

USER bruin

# Create necessary directories for bruin user
RUN mkdir -p /home/bruin/.local/bin /home/bruin/.local/share

# Copy the built binary from builder stage
COPY --from=builder /src/bin/bruin /home/bruin/.local/bin/bruin

ENV PATH="/home/bruin/.local/bin:${PATH}"
ENV CC="/usr/bin/gcc"
ENV CFLAGS="-I/usr/include"
ENV LDFLAGS="-L/usr/lib"

# Bootstrap the legacy and current ingestr installations
RUN cd /tmp && /home/bruin/.local/bin/bruin init bootstrap --in-place && /home/bruin/.local/bin/bruin run bootstrap
RUN /home/bruin/.bruin/uv python install 3.9
RUN /home/bruin/.bruin/uv python install 3.10
RUN /home/bruin/.bruin/uv python install 3.11
RUN /home/bruin/.bruin/uv python install 3.12
RUN /home/bruin/.bruin/uv python install 3.13
RUN /home/bruin/.bruin/uv python install 3.14


RUN rm -rf /tmp/bootstrap

CMD ["bruin"]
