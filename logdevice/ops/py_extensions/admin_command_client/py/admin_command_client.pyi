#!/usr/bin/env python3

# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

from enum import IntEnum, auto

class AdminCommandClientException(Exception):
    pass

class ConnectionType(IntEnum):
    PLAIN = auto()
    SSL = auto()

class AdminCommandClient:
    def send(
        self, cmd: str, host: str, port: int, timeout: float, conntype: ConnectionType
    ) -> str: ...
