FROM debian:12.8-slim

RUN apt-get update && apt-get install -y curl git

RUN  adduser --disabled-password --gecos '' bruin

USER bruin

ARG VERSION=latest

RUN curl -LsSf https://getbruin.com/install/cli | sh -s -- -d ${VERSION}

ENV PATH="/home/bruin/.local/bin:${PATH}"

RUN cd /tmp && bruin init bootstrap --in-place && bruin run bootstrap

RUN rm -rf bootstrap

CMD ["bruin"]