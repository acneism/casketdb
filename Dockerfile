# Build the static binary on the host platform and cross-compile for the
# target one, so that a multi-platform build needs no emulation.
FROM --platform=$BUILDPLATFORM golang:1.26 AS build

WORKDIR /src

# Dependencies first, so that changing the source does not download them again.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG TARGETOS
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags="-s -w" -o /out/casketdb ./cmd/casketdb \
    && mkdir -p /out/data

# Distroless: the binary, its certificates and a non-root user, nothing else.
FROM gcr.io/distroless/static-debian13:nonroot

COPY --from=build /out/casketdb /usr/local/bin/casketdb

# The volume inherits the permissions of /data, so it must belong to the
# non-root user (65532) before the volume is mounted over it.
COPY --from=build --chown=65532:65532 /out/data /data
WORKDIR /data
VOLUME /data

# A container is reached from other hosts, so listen on every interface.
# Set CASKETDB_REQUIREPASS, or protected mode refuses those clients.
ENV CASKETDB_ADDR=0.0.0.0:6379
ENV CASKETDB_DIR=/data

# 6379 serves clients. Replication between containers needs
# -raft-listen 0.0.0.0:7000, metrics need -metrics-addr 0.0.0.0:9121.
EXPOSE 6379

# A flag on the command line wins over the environment variable, as usual:
# docker run ... casketdb -appendfsync always
ENTRYPOINT ["/usr/local/bin/casketdb"]
