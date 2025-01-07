FROM debian:bookworm-slim

ARG VERSION=latest

RUN apt-get update && apt-get install -y curl

RUN  adduser --disabled-password --gecos '' bruin

USER bruin

RUN curl -LsSf https://raw.githubusercontent.com/bruin-data/bruin/refs/heads/main/install.sh | sh -s -- -d ${VERSION}

CMD ["home/bruin/.local/bin/bruin"]
