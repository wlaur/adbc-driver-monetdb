# syntax=docker/dockerfile:1.20@sha256:26147acbda4f14c5add9946e2fd2ed543fc402884fd75146bd342a7f6271dc1d

ARG UBUNTU_VERSION=24.04
ARG UBUNTU_DIGEST=sha256:4fbb8e6a8395de5a7550b33509421a2bafbc0aab6c06ba2cef9ebffbc7092d90

FROM ubuntu:${UBUNTU_VERSION}@${UBUNTU_DIGEST} AS build

ARG BUILD_THREADS=12
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install --yes --no-install-recommends \
        bison \
        build-essential \
        bzip2 \
        ca-certificates \
        cmake \
        curl \
        libbrotli-dev \
        libbz2-dev \
        libgeos-dev \
        liblz4-dev \
        liblzma-dev \
        libpcre2-dev \
        libproj-dev \
        libsnappy-dev \
        libssl-dev \
        libxxhash-dev \
        libzstd-dev \
        pkg-config \
        python3 \
        zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /tmp/monetdb
COPY MonetDB-src.tar.gz .
RUN mkdir source \
    && tar --extract --gzip --file MonetDB-src.tar.gz \
        --directory source --strip-components=1 \
    && rm MonetDB-src.tar.gz

WORKDIR /tmp/monetdb/source
ARG APPLY_PATCH=1
COPY apply-patch.py /tmp/apply-patch.py
RUN if [ "$APPLY_PATCH" = "1" ]; then python3 /tmp/apply-patch.py; else echo "apply-patch.py skipped (APPLY_PATCH=$APPLY_PATCH)"; fi

WORKDIR /tmp/monetdb/source/build
RUN cmake .. \
        -DASSERT=OFF \
        -DCMAKE_BUILD_TYPE=Release \
        -DINT128=ON \
        -DPY3INTEGRATION=OFF \
        -DRELEASE_VERSION=ON \
        -DRINTEGRATION=OFF \
        -DSTRICT=OFF \
        -DWITH_OPENSSL=ON \
        -DWITH_SNAPPY=ON \
        -DWITH_ZSTD=ON \
        -DWITH_BROTLI=ON \
    && cmake --build . --parallel "${BUILD_THREADS}" \
    && cmake --install .

FROM ubuntu:${UBUNTU_VERSION}@${UBUNTU_DIGEST} AS runtime

ENV DEBIAN_FRONTEND=noninteractive
ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8

RUN apt-get update \
    && apt-get install --yes --no-install-recommends \
        bash \
        ca-certificates \
        libbrotli1 \
        libbz2-1.0 \
        libgeos-c1t64 \
        liblz4-1 \
        liblzma5 \
        libpcre2-8-0 \
        libproj25 \
        libsnappy1v5 \
        libssl3t64 \
        libxxhash0 \
        libzstd1 \
        zlib1g \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /usr/local/ /usr/local/
COPY --chmod=0755 entrypoint.sh /usr/local/bin/entrypoint.sh

RUN groupadd --gid 5000 monetdb \
    && useradd --uid 5000 --gid 5000 --create-home monetdb \
    && mkdir --parents /var/monetdb5/dbfarm \
    && chown --recursive monetdb:monetdb /var/monetdb5 \
    && find /usr/bin /usr/sbin -xdev -type f -perm /6000 -exec chmod a-s {} +

LABEL org.opencontainers.image.title="Unofficial MonetDB container (master build)" \
      org.opencontainers.image.description="Native Linux ARM64 build of MonetDB master (pp_hashjoin pipeline merge)" \
      org.opencontainers.image.licenses="MPL-2.0" \
      org.opencontainers.image.vendor="wlaur (unofficial build)"

VOLUME ["/var/monetdb5/dbfarm"]
EXPOSE 50000
USER monetdb
STOPSIGNAL SIGINT
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
