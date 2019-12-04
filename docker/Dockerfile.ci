# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# The production image
FROM ubuntu:bionic

# ldshell _requires_ utf-8
ENV LANG C.UTF-8

# Copy LogDevice development tools
COPY _build/bin/ld* \
              _build/bin/logdeviced /usr/local/bin/

# Python tools, ldshell, ldquery and client lib
COPY _build/lib/liblogdevice.so /usr/local/lib/
COPY _build/lib/client.so \
    /usr/local/lib/python3.6/dist-packages/logdevice/client.so
COPY _build/lib/ext.so \
    /usr/local/lib/python3.6/dist-packages/logdevice/ldquery/internal/ext.so
COPY _build/lib/admin_command_client.so \
    /usr/local/lib/python3.6/dist-packages/logdevice/ops/admin_command_client.so
COPY logdevice/ops/ldquery/py/lib.py \
    /usr/local/lib/python3.6/dist-packages/logdevice/ldquery/
COPY logdevice/ops/ldquery/py/__init__.py \
    /usr/local/lib/python3.6/dist-packages/logdevice/ldquery/

# Install runtime dependencies for ld-dev-cluster, ldshell friends.
# To install the ldshell wheel we also need python3 build tools, as
# we depend on python-Levenshtein for which a many-linux binary wheel is not
# available; these are removed following install to keep docker image size low.

COPY logdevice/build_tools/ubuntu_runtime.deps /tmp/logdevice_runtime.deps
COPY logdevice/ops /tmp/logdevice/ops

RUN apt-get update && \
    apt-get install -y --no-install-recommends $(cat /tmp/logdevice_runtime.deps) \
        gcc python3-setuptools python3-dev && \
    python3 -m pip install --user --upgrade setuptools wheel && \
    (cd /tmp/logdevice/ops && python3 setup.py install) && \
    apt-get remove -y --auto-remove gcc python3-setuptools python3-dev && \
    rm -rf /var/lib/apt/lists/*

EXPOSE 4440 4441 4443 5440

CMD /usr/local/bin/ld-dev-cluster
