FROM debian:12.8-slim

RUN apt-get update && apt-get install -y curl git

RUN  adduser --disabled-password --gecos '' bruin

USER bruin

ARG VERSION=latest
ARG TARGETPLATFORM

RUN curl -LsSf https://getbruin.com/install/cli | sh -s -- -d ${VERSION}

ENV PATH="/home/bruin/.local/bin:${PATH}"

# Set platform emulation for ARM64 builds to handle ibm-db dependency issues
# This follows the approach from: https://levelup.gitconnected.com/setting-up-ibm-db2-database-in-arm-64-a015105963c7
RUN if [ "$TARGETPLATFORM" = "linux/arm64" ]; then \
        export DOCKER_DEFAULT_PLATFORM=linux/amd64; \
    fi

RUN cd /tmp && bruin init bootstrap --in-place && bruin run bootstrap

RUN rm -rf bootstrap

CMD ["bruin"]
