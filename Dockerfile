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

# syntax=docker/dockerfile:1.6
# Final stage
FROM debian:trixie-slim

ARG GCS_BUCKET_NAME=gong-release
ARG GCS_PREFIX=releases

RUN apt-get update && apt-get install -y \
    curl \
    git \
    build-essential \
    binutils \
    python3-dev \
    gnupg \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Google Cloud SDK (for gong binary download)
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" \
    | tee /etc/apt/sources.list.d/google-cloud-sdk.list \
    && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg \
    | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg \
    && apt-get update \
    && apt-get install -y google-cloud-cli \
    && rm -rf /var/lib/apt/lists/*

RUN adduser --disabled-password --gecos '' bruin

RUN chown -R bruin:bruin /home/bruin

USER bruin

# Create necessary directories for bruin user
RUN mkdir -p /home/bruin/.local/bin /home/bruin/.local/bin/gong /home/bruin/.local/share

# Copy the built binary from builder stage
COPY --from=builder /src/bin/bruin /home/bruin/.local/bin/bruin

# Download gong binaries from GCS (optional - only if secret is provided)
# This step runs as root to configure gcloud, then switches to bruin user
USER root
RUN --mount=type=secret,id=gcp_key,required=false \
    if [ -f /run/secrets/gcp_key ]; then \
        echo "Downloading gong binaries from GCS..." && \
        gcloud auth activate-service-account --key-file=/run/secrets/gcp_key && \
        gsutil cp "gs://${GCS_BUCKET_NAME}/${GCS_PREFIX}/latest.txt" /tmp/latest.txt && \
        RELEASE_TAG=$(cat /tmp/latest.txt | tr -d '\r\n') && \
        rm -f /tmp/latest.txt && \
        echo "Using release: $RELEASE_TAG" && \
        gsutil -m cp "gs://${GCS_BUCKET_NAME}/${GCS_PREFIX}/${RELEASE_TAG}/*/*" /home/bruin/.local/bin/gong/ && \
        chmod +x /home/bruin/.local/bin/gong/* && \
        chown -R bruin:bruin /home/bruin/.local/bin/gong && \
        echo "Gong binaries downloaded successfully"; \
    else \
        echo "GCP key secret not provided, skipping gong binary download"; \
    fi

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