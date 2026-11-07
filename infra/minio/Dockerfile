# MinIO server + mc client built from the official open-source (AGPLv3) code.
#
# Why build from source: MinIO stopped publishing free images. `minio/minio`
# and `minio/mc` no longer exist on Docker Hub or quay.io, and the AIStor image
# needs a commercial licence file. The GitHub repos are archived but their
# release tags are still downloadable, so we pin the final community releases.
#
# First build takes a few minutes (Go module download + compile); after that
# Docker caches it.

ARG GO_IMAGE=golang:1.24-alpine
ARG RUNTIME_IMAGE=alpine:3.22

FROM ${GO_IMAGE} AS build
ARG MINIO_RELEASE=RELEASE.2025-10-15T17-29-55Z
ARG MC_RELEASE=RELEASE.2025-08-13T08-35-41Z
ENV CGO_ENABLED=0 GOTOOLCHAIN=auto
RUN apk add --no-cache git
WORKDIR /src
RUN git clone --depth 1 --branch "${MINIO_RELEASE}" https://github.com/minio/minio.git minio \
 && cd minio \
 && go build -trimpath -tags kqueue \
      -ldflags "-s -w -X github.com/minio/minio/cmd.Version=${MINIO_RELEASE} -X github.com/minio/minio/cmd.ReleaseTag=${MINIO_RELEASE}" \
      -o /out/minio .
RUN git clone --depth 1 --branch "${MC_RELEASE}" https://github.com/minio/mc.git mc \
 && cd mc \
 && go build -trimpath -ldflags "-s -w" -o /out/mc .

FROM ${RUNTIME_IMAGE}
RUN apk add --no-cache ca-certificates curl \
 && addgroup -S minio && adduser -S -G minio -h /home/minio minio \
 && mkdir -p /data && chown minio:minio /data
COPY --from=build /out/minio /out/mc /usr/local/bin/
USER minio
EXPOSE 9000 9001
ENTRYPOINT ["minio"]
CMD ["server", "/data", "--console-address", ":9001"]
