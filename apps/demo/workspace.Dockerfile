FROM python:3.15.2-slim-trixie@sha256:63bb298d2b7b88f176f51b5189f54ce6535c997682ea70885d2132f5d473a8a5 AS workspace

ARG TARGETARCH
# The daemon build context is the bin directory from the locked execution package.
RUN --mount=type=bind,from=daemon,target=/daemons \
    case "$TARGETARCH" in \
      amd64) daemon_arch=x64 ;; \
      arm64) daemon_arch=arm64 ;; \
      *) echo "Unsupported workspace architecture: $TARGETARCH" >&2; exit 1 ;; \
    esac \
    && install -m 0755 "/daemons/pi-env-linux-$daemon_arch/pi-env" /usr/local/bin/pi-env \
    && groupadd --gid 10001 workspace \
    && useradd --uid 10001 --gid workspace --create-home workspace \
    && install -d -o workspace -g workspace /workspace /workspace/exports

ENV HOME=/home/workspace
WORKDIR /workspace
USER 10001:10001
CMD ["sleep", "infinity"]

FROM workspace AS sandbox-workspace
USER root
RUN apt-get update && apt-get install -y --no-install-recommends socat
COPY --from=workspace-sdk --chmod=0755 loopback-listener.sh /usr/local/bin/loopback-listener
USER 10001:10001
