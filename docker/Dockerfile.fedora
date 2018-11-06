# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

FROM fedora:29

MAINTAINER Ahmed Soliman <asoli@fb.com>

ARG PARALLEL=4

COPY logdevice logdevice

RUN echo "/usr/local/lib" >> /etc/ld.so.conf && ldconfig

RUN dnf install -y $(cat /logdevice/build_tools/fedora.deps) && \
    dnf clean all && \
    mkdir /tmp/build && cd /tmp/build && \
    cmake /logdevice/ && \
    make -j$PARALLEL && \
    make install && \
    cp /tmp/build/bin/ld* /usr/local/bin/ && \
    cp /tmp/build/bin/ld-dev-cluster /usr/local/bin/

RUN rm -rf /tmp/build /logdevice /folly /rocksdb /usr/local/lib/*.a

RUN dnf remove -y cmake autoconf autoconf-archive automake wget file git gcc-c++

ENV LOGDEVICE_TEST_BINARY=/usr/local/bin/logdeviced

EXPOSE 4440 4441 4443 5440

CMD /usr/local/bin/ld-dev-cluster
