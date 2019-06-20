#!/usr/bin/env python3

# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


from dataclasses import dataclass
from typing import Optional

from ldops.types.socket_address import SocketAddress
from logdevice.admin.common.types import NodeID


@dataclass(frozen=True)
class Node:
    node_index: Optional[int]
    data_addr: Optional[SocketAddress] = None
    name: Optional[str] = None

    @classmethod
    def from_thrift(cls, node_id: NodeID) -> "Node":
        """
        Parses Thrift-representation of NodeID and returns instance
        """
        return cls(
            node_index=node_id.node_index,
            data_addr=SocketAddress.from_thrift(node_id.address)
            if node_id.address
            else None,
            name=node_id.name,
        )

    def to_thrift(self) -> NodeID:
        """
        Returns Thrift-representation of NodeID to use in communication
        with AdminAPI.
        """
        return NodeID(
            node_index=self.node_index,
            address=self.data_addr.to_thrift() if self.data_addr else None,
            name=self.name if self.name else None,
        )
