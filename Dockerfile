# syntax=docker/dockerfile:1.6
FROM golang:1.25.4-trixie AS builder

# Build argument for version information
ARG VERSION=dev
ARG BRANCH_NAME=unknown

# Install build dependencies including C++ standard library for DuckDB
RUN apt-get update && apt-get install -y git gcc g++ libc6-dev curl

# Install Rust toolchain for the SQL parser FFI
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

# Set working directory
WORKDIR /src

# Copy go mod files
COPY go.mod go.sum ./

# Download Go dependencies with cache mount
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    go mod download

# Copy Rust project files first (changes less frequently than Go source)
COPY pkg/sqlparser/rustffi/Cargo.toml pkg/sqlparser/rustffi/Cargo.lock pkg/sqlparser/rustffi/
COPY pkg/sqlparser/rustffi/src pkg/sqlparser/rustffi/src

# Build the Rust SQL parser static library with cache mount
RUN --mount=type=cache,target=/root/.cargo/registry \
    --mount=type=cache,target=/root/.cargo/git \
    --mount=type=cache,target=/src/pkg/sqlparser/rustffi/target \
    cargo build --release --manifest-path pkg/sqlparser/rustffi/Cargo.toml && \
    cp pkg/sqlparser/rustffi/target/release/libbruin_rustsqlparser.a /tmp/libbruin_rustsqlparser.a

# Copy source code
COPY . .

# Restore cached Rust artifact (cache mount is not persisted in the layer)
RUN mkdir -p pkg/sqlparser/rustffi/target/release && \
    cp /tmp/libbruin_rustsqlparser.a pkg/sqlparser/rustffi/target/release/libbruin_rustsqlparser.a

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

# Bootstrap ingestr installation
RUN cd /tmp && /home/bruin/.local/bin/bruin init bootstrap --in-place && /home/bruin/.local/bin/bruin run bootstrap
RUN /home/bruin/.bruin/uv python install 3.9
RUN /home/bruin/.bruin/uv python install 3.10
RUN /home/bruin/.bruin/uv python install 3.11
RUN /home/bruin/.bruin/uv python install 3.12
RUN /home/bruin/.bruin/uv python install 3.13
RUN /home/bruin/.bruin/uv python install 3.14


RUN rm -rf /tmp/bootstrap

CMD ["bruin"]
