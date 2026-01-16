# ============================================
# Stage 1: Go Builder
# ============================================
FROM golang:1.25.4-trixie AS builder

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

# ============================================
# Stage 2: Final image
# Uses pre-built base image with Python & uv already installed
# ============================================
FROM ghcr.io/bruin-data/bruin-base:latest

ENV PATH="/home/bruin/.local/bin:${PATH}"
ENV CC="/usr/bin/gcc"
ENV CFLAGS="-I/usr/include"
ENV LDFLAGS="-L/usr/lib"

# Copy the built binary from builder stage
COPY --from=builder /src/bin/bruin /home/bruin/.local/bin/bruin

# Bootstrap ingestr installation
RUN cd /tmp && /home/bruin/.local/bin/bruin init bootstrap --in-place && /home/bruin/.local/bin/bruin run bootstrap \
    && rm -rf /tmp/bootstrap

CMD ["bruin"]
