#!/usr/bin/env python3

# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


from datetime import datetime
from typing import Any, Collection

from logdevice.admin.nodes.types import (
    MaintenanceStatus,
    ShardMaintenanceProgress as ThriftShardMaintenanceProgress,
    ShardOperationalState,
)


class ShardMaintenanceProgress:
    def __init__(
        self,
        status: MaintenanceStatus,
        target_states: Collection[ShardOperationalState],
        created_at: datetime,
        last_updated_at: datetime,
        associated_group_ids: Collection[str],
    ):
        self._underlying = ThriftShardMaintenanceProgress(
            status=status,
            target_states=set(target_states),
            created_at=int(created_at.timestamp() * 1000),
            last_updated_at=int(last_updated_at.timestamp() * 1000),
            associated_group_ids=sorted(list(set(associated_group_ids))),
        )

    def __getattr__(self, name: str) -> Any:
        return getattr(self._underlying, name)

    @classmethod
    def from_thrift(
        cls, src: ThriftShardMaintenanceProgress
    ) -> "ShardMaintenanceProgress":
        return ShardMaintenanceProgress(
            status=src.status,
            target_states=src.target_states,
            created_at=datetime.fromtimestamp(src.created_at // 1000),
            last_updated_at=datetime.fromtimestamp(src.last_updated_at // 1000),
            associated_group_ids=src.associated_group_ids,
        )

    def to_thrift(self) -> ThriftShardMaintenanceProgress:
        return self._underlying

    @property
    def created_at(self) -> datetime:
        return datetime.fromtimestamp(self._underlying.created_at // 1000)

    @property
    def last_updated_at(self) -> datetime:
        return datetime.fromtimestamp(self._underlying.last_updated_at // 1000)
