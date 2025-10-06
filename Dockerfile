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

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the application with version information from build args
RUN CGO_ENABLED=1 go build -v -tags="no_duckdb_arrow" -ldflags="-s -w -X main.Version=${VERSION} -X main.BranchName=${BRANCH_NAME}" -o "bin/bruin" .

# Final stage
FROM debian:12.8-slim

RUN apt-get update && apt-get install -y curl git

RUN adduser --disabled-password --gecos '' bruin

# Copy the built binary from builder stage
COPY --from=builder /src/bin/bruin /usr/local/bin/bruin

USER bruin

ENV PATH="/usr/local/bin:${PATH}"

CMD ["bruin"]
