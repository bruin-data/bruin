FROM cgr.dev/chainguard/go:latest AS builder

WORKDIR /app 

COPY go.mod ./

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go mod download

COPY . .

RUN make build

FROM cgr.dev/chainguard/go:latest

COPY --from=builder /app/bin/bruin /app/bin/bruin

ENTRYPOINT ["/app/bin/bruin"]
