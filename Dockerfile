# syntax=docker/dockerfile:1
FROM --platform=$BUILDPLATFORM golang:1.27.0-alpine AS build
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src
COPY go.mod ./
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH:-amd64} \
    go build -trimpath -ldflags="-s -w" -o /out/renderd ./cmd/renderd

FROM alpine:3.24 AS typst
ARG TARGETARCH
RUN apk add --no-cache ca-certificates curl xz
RUN set -eu; \
    case "${TARGETARCH:-amd64}" in \
      amd64) arch=x86_64; checksum=a6d077d0a95eed5a2eba715b2dae06be954f624ccbf85758a03f389ded33118c ;; \
      arm64) arch=aarch64; checksum=5aa8d74a3d906e60ea12a66ac2f37f8eef1b14cbad7182a745e393a10c23dcee ;; \
      *) echo "Unsupported architecture"; exit 1 ;; \
    esac; \
    curl --fail --location --retry 3 \
      "https://github.com/typst/typst/releases/download/v0.15.1/typst-${arch}-unknown-linux-musl.tar.xz" \
      --output /tmp/typst.tar.xz; \
    printf '%s  /tmp/typst.tar.xz\n' "$checksum" | sha256sum -c -; \
    mkdir /out; \
    tar -xJf /tmp/typst.tar.xz -C /out --strip-components=1

FROM build AS test
RUN apk add --no-cache git git-daemon gcc musl-dev font-dejavu ca-certificates
COPY --from=typst /out/typst /usr/local/bin/typst
ENV TYPST_BIN=/usr/local/bin/typst CGO_ENABLED=1
RUN go test -race ./... -count=1 && go vet ./...

FROM alpine:3.24 AS runtime
RUN apk add --no-cache git ca-certificates \
    && addgroup -g 10001 app && adduser -D -u 10001 -G app app
COPY --from=build /out/renderd /usr/local/bin/renderd
COPY --from=typst /out/typst /usr/local/bin/typst
USER 10001:10001
WORKDIR /home/app
ENV HTTP_ADDR=:8080 CACHE_DIR=/tmp/typst-render/cache WORK_DIR=/tmp/typst-render/work
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/renderd"]
