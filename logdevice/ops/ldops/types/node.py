#!/usr/bin/env python3

# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


from dataclasses import dataclass
from typing import Optional

from ldops.types.socket_address import SocketAddress


@dataclass(frozen=True)
class Node:
    node_index: int
    data_addr: Optional[SocketAddress] = None
    name: str = ""  # TODO: Remove default when server_name is supported
