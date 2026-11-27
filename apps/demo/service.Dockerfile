ARG NODE_IMAGE=node:26.13.1-bookworm-slim@sha256:0042e17d232a87a2cd485be1da7900fca4d10a4ad11b29e9285da1d2e54798c9

FROM --platform=$BUILDPLATFORM ${NODE_IMAGE} AS dependencies
ARG TARGETARCH
WORKDIR /app
COPY package.json package-lock.json ./
RUN set -eu; \
    case "$TARGETARCH" in \
      amd64) npm_cpu=x64 ;; \
      arm64) npm_cpu=arm64 ;; \
      *) echo "Unsupported service architecture: $TARGETARCH" >&2; exit 1 ;; \
    esac; \
    npm ci --omit=dev --ignore-scripts --no-audit --no-fund --os=linux --cpu="$npm_cpu"

FROM ${NODE_IMAGE}
WORKDIR /app
COPY --from=dependencies /app/node_modules/ node_modules/
COPY *.mjs ./
COPY web/ web/
USER node
CMD ["node", "main.mjs"]
