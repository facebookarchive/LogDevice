#!/usr/bin/env python3

# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""
ldops.util.convert
~~~~~~~~~~~

A collection of helpful conversion utility functions
"""

import re
from typing import Dict, List, Mapping, Optional

from logdevice.admin.common.types import (
    LocationScope,
    NodeID,
    ReplicationProperty,
    ShardID,
)
from logdevice.admin.nodes.types import ShardStorageState


__all__: List[str] = ["to_shard_id", "to_storage_state", "to_replication"]

# A string representation of a shard or node. Consists of a node ID and an
# optional shard ID.
# Examples: N0, N1:S5
SHARD_PATTERN = re.compile("^N(?P<node_index>[0-9]+)(:S(?P<shard_index>[0-9]+))?$")


def to_shard_id(scope: str) -> ShardID:
    """
    A conversion utility that takes a Nx:Sy string and convert it into the
    typed ShardID. The 'Sy' part is optional and if unset the generated
    ShardID will have a shard_index set to -1
    """
    scope = scope.upper()
    if not scope:
        raise ValueError(f"Cannot parse empty scope")
    match = SHARD_PATTERN.match(scope)
    if match is None:
        # There were no shards, or invalid.
        raise ValueError(f"Cannot parse '{scope}'. Invalid format!")

    results = match.groupdict()
    shard_index = -1
    if results["shard_index"] is not None:
        shard_index = int(results["shard_index"])
    node_index = int(results["node_index"])
    node = NodeID(node_index=node_index)
    return ShardID(node=node, shard_index=shard_index)


def to_storage_state(raw_state: str) -> ShardStorageState:
    """
    A conversion utility that takes a string and creates a ShardStorageState
    object from. It ignore the case of the input.

    E.g., Valid inputs ["read-only", "read_only", "READ_ONLY", "READ-ONLY", ...]
    """
    normal_state = raw_state.upper().replace("-", "_")
    if not normal_state:
        raise ValueError(f"Cannot parse empty storage-state")
    ret = ShardStorageState.__members__.get(normal_state)
    if ret is None:
        raise ValueError(f"Cannot parse storage-state '{raw_state}'")
    return ret


def to_replication(
    raw_repl: Optional[Mapping[str, int]]
) -> Optional[ReplicationProperty]:
    """
    Converts a dictionary like {'rack': 3, 'node': 2} to the corresponding
    ReplicationProperty object. {LocationScope.RACK: 3, LocationScope.NODE: 2}
    """
    if raw_repl is None:
        return None
    res: Dict[LocationScope, int] = {}
    for scope, value in raw_repl.items():
        normalized_scope = scope.upper()
        replication_scope = LocationScope.__members__.get(normalized_scope)
        if replication_scope is None:
            raise ValueError(f"Unknown location scope '{scope}'")
        res[replication_scope] = value

    return ReplicationProperty(res)
