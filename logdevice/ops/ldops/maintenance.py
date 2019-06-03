#!/usr/bin/env python3

# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


"""
ldops.cluster
~~~~~~~~~~~

Implements maintenance- and safety-related operations.
"""

from typing import Collection, FrozenSet, Optional

from ldops import admin_api
from ldops.exceptions import LDOpsError
from ldops.types.node import Node
from logdevice.admin.clients import AdminAPI
from logdevice.admin.common.types import NodeID, ShardID
from logdevice.admin.nodes.types import ShardStorageState
from logdevice.admin.safety.types import CheckImpactRequest, CheckImpactResponse


class SafetyError(LDOpsError):
    """
    Exception representing unsafe operation.
    """

    def __init__(
        self,
        message: str = "Operation is unsafe",
        check_impact_response: Optional[CheckImpactResponse] = None,
    ):
        super().__init__(message)
        self.message = message
        self.check_impact_response = check_impact_response

    def __str__(self):
        return f"{self.message}: impact: {self.check_impact_response.impact}"


async def check_impact(
    client: AdminAPI,
    nodes: Optional[Collection[Node]] = None,
    shards: Optional[Collection[ShardID]] = None,
    target_storage_state: ShardStorageState = ShardStorageState.DISABLED,
    disable_sequencers: bool = True,
) -> CheckImpactResponse:
    """
    Performs Safety check and returns CheckImpactResponse. If no nodes and no
    shards passed it still does safety check, but will return current state
    of the cluster.
    """

    def recombine_shards(shards: Collection[ShardID]) -> FrozenSet[ShardID]:
        whole_nodes = set()
        node_ids = set()
        single_shards = set()
        for s in shards:
            if s.shard_index == -1:
                whole_nodes.add(s)
            else:
                single_shards.add(s)
            node_ids.add(s.node.node_index)

        filtered_shards: FrozenSet[ShardID] = frozenset(
            s for s in single_shards if s.node.node_index not in node_ids
        )
        return frozenset(whole_nodes).union(filtered_shards)

    nodes = nodes or []

    shards = shards or []

    req_shards: FrozenSet[ShardID] = recombine_shards(
        list(shards)  # shards is generic Collection, not List
        + [
            ShardID(
                node=NodeID(node_index=n.node_index, address=n.data_addr.to_thrift()),
                shard_index=-1,
            )
            for n in nodes
        ]
    )

    return await admin_api.check_impact(
        client=client,
        req=CheckImpactRequest(
            shards=req_shards,
            target_storage_state=target_storage_state,
            disable_sequencers=disable_sequencers,
        ),
    )


async def ensure_safe(
    client: AdminAPI,
    nodes: Optional[Collection[Node]] = None,
    shards: Optional[Collection[ShardID]] = None,
    target_storage_state: ShardStorageState = ShardStorageState.DISABLED,
    disable_sequencers: bool = True,
) -> None:
    """
    Convenient wrapper around `get_impact` which raises `SafetyError`
    when operation is unsafe.
    """
    resp: CheckImpactResponse = await check_impact(
        client=client,
        nodes=nodes,
        shards=shards,
        target_storage_state=target_storage_state,
        disable_sequencers=disable_sequencers,
    )
    if not resp.impact:
        raise SafetyError(check_impact_response=resp)
