FROM debian:12.8-slim

RUN apt-get update && apt-get install -y curl git

RUN  adduser --disabled-password --gecos '' bruin

USER bruin

ARG VERSION=latest

RUN curl -LsSf https://getbruin.com/install/bruin-cli | sh -s -- -d ${VERSION}

ENV PATH="/home/bruin/.local/bin:${PATH}"

CMD ["bruin"]
