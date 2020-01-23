#!/usr/bin/env python3

# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import re
from typing import Collection, Set

from ldops.const import ALL_SHARDS
from logdevice.common.types import NodeID, ShardID


def parse_shards(src: Collection[str]) -> Set[ShardID]:
    """
    Parses a list of strings and intrepret as ShardID objects.
    Accepted examples:
        0 => ShardID(0, -1)
        N0 => ShardID(0, -1)
        0:2 => ShardID(0, 2)
        N0:2 => ShardID(0, 2)
        N0:S2 => ShardID(0, 2)
    """
    regex = re.compile(r"^N?(\d+)\:?S?(\d+)?$", flags=re.IGNORECASE)
    res = set()
    for s in src:
        match = regex.search(s)
        if not match:
            raise ValueError(f"Cannot parse shard: {s}")

        node_index = int(match.groups()[0])
        if match.groups()[1] is None:
            shard_index = ALL_SHARDS
        else:
            shard_index = int(match.groups()[1])
        res.add(ShardID(node=NodeID(node_index=node_index), shard_index=shard_index))

    return res
