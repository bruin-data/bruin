FROM cgr.dev/chainguard/go:latest AS builder
WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download && go mod verify

COPY . .

RUN go build -o bruin .

FROM cgr.dev/chainguard/static:latest


COPY --from=builder /app/bruin /usr/bin/


ENTRYPOINT ["/usr/bin/bruin"]
