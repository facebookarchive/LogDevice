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
class Cluster:
    name: Optional[str] = None
    admin_server_addr: Optional[SocketAddress] = None

    def __post_init__(self) -> None:
        if not self.name and not self.admin_server_addr:
            raise ValueError("At least one of (name, admin_server_addr) required")
