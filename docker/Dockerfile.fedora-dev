# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

FROM fedora:29

MAINTAINER Ahmed Soliman <asoli@fb.com>

ARG PARALLEL=4

# Fedora doesn't have /usr/local/lib in ldconfig so libfolly won't be
# picked up by logdeviced
RUN echo "/usr/local/lib" >> /etc/ld.so.conf && ldconfig

COPY logdevice/build_tools/fedora.deps /tmp/

RUN dnf install -y $(cat /tmp/fedora.deps) && dnf clean all

COPY logdevice logdevice

RUN mkdir /build && cd /build && \
    cmake /logdevice/ && \
    make -j$PARALLEL && \
    make install

WORKDIR /build

ENV LOGDEVICE_TEST_BINARY=/build/bin/logdeviced

CMD /bin/bash
