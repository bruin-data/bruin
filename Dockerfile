FROM goreleaser/goreleaser-cross:latest as builder

COPY . .

RUN  rm -rf dist && VERSION=0.0.0 goreleaser release --snapshot