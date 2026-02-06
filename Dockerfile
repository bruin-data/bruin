# syntax=docker/dockerfile:1.6
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

# Final stage
FROM debian:trixie-slim

ARG GCS_BUCKET_NAME=gong-release
ARG GCS_PREFIX=releases
ARG RELEASE_TAG=
ARG TARGETOS
ARG TARGETARCH

RUN apt-get update && apt-get install -y \
    curl \
    git \
    build-essential \
    binutils \
    python3-dev \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN adduser --disabled-password --gecos '' bruin

RUN chown -R bruin:bruin /home/bruin

USER bruin

# Create necessary directories for bruin user
RUN mkdir -p /home/bruin/.local/bin /home/bruin/.local/bin/gong /home/bruin/.local/share

# Copy the built binary from builder stage
COPY --from=builder /src/bin/bruin /home/bruin/.local/bin/bruin

# Download gong binaries from GCS (public bucket via HTTPS). Optional: build continues if not found.
USER root
RUN SELECTED_RELEASE="${RELEASE_TAG}" && \
    if [ -z "${SELECTED_RELEASE}" ]; then \
        echo "No release tag provided, downloading latest..." && \
        curl -fsSL "https://storage.googleapis.com/${GCS_BUCKET_NAME}/${GCS_PREFIX}/latest.txt" -o /tmp/latest.txt && \
        SELECTED_RELEASE=$(tr -d '\r\n' < /tmp/latest.txt) && \
        rm -f /tmp/latest.txt && \
        echo "Using latest release: ${SELECTED_RELEASE}"; \
    else \
        echo "Using provided release tag: ${SELECTED_RELEASE}"; \
    fi && \
    GONG_BINARY_NAME="gong_${TARGETARCH}" && \
    GONG_URL="https://storage.googleapis.com/${GCS_BUCKET_NAME}/${GCS_PREFIX}/${SELECTED_RELEASE}/${TARGETOS}/${GONG_BINARY_NAME}" && \
    echo "Downloading gong binary for platform ${TARGETOS}/${TARGETARCH} from ${GONG_URL}..." && \
    (curl -fsSL "${GONG_URL}" -o /home/bruin/.local/bin/gong/gong && \
     chmod +x /home/bruin/.local/bin/gong/gong && \
     chown bruin:bruin /home/bruin/.local/bin/gong/gong && \
     echo "Gong binaries downloaded successfully") || \
    echo "Gong binary not available for ${SELECTED_RELEASE}/${TARGETOS}/${GONG_BINARY_NAME}, skipping"

USER bruin

ENV PATH="/home/bruin/.local/bin:/home/bruin/.local/bin/gong:${PATH}"
ENV CC="/usr/bin/gcc"
ENV CFLAGS="-I/usr/include"
ENV LDFLAGS="-L/usr/lib"

# Bootstrap ingestr installation
RUN cd /tmp && /home/bruin/.local/bin/bruin init bootstrap --in-place && /home/bruin/.local/bin/bruin run bootstrap
RUN /home/bruin/.bruin/uv python install 3.11.9
RUN /home/bruin/.bruin/uv python install 3.10.14
RUN /home/bruin/.bruin/uv python install 3.9.19


RUN rm -rf /tmp/bootstrap

CMD ["bruin"]