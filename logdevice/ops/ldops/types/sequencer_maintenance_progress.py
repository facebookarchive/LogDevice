#!/usr/bin/env python3
# pyre-strict

# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


from datetime import datetime
from typing import Any, Collection

from logdevice.admin.nodes.types import (
    MaintenanceStatus,
    SequencerMaintenanceProgress as ThriftSequencerMaintenanceProgress,
    SequencingState,
)


_MILLISECONDS_IN_SECOND = 1000


class SequencerMaintenanceProgress:
    def __init__(
        self,
        status: MaintenanceStatus,
        target_state: SequencingState,
        created_at: datetime,
        last_updated_at: datetime,
        associated_group_ids: Collection[str],
    ) -> None:
        self._underlying = ThriftSequencerMaintenanceProgress(
            status=status,
            target_state=target_state,
            created_at=int(created_at.timestamp() * _MILLISECONDS_IN_SECOND),
            last_updated_at=int(last_updated_at.timestamp() * _MILLISECONDS_IN_SECOND),
            associated_group_ids=sorted(list(set(associated_group_ids))),
        )

    # pyre-ignore
    def __getattr__(self, name: str) -> Any:
        return getattr(self._underlying, name)

    @classmethod
    def from_thrift(
        cls, src: ThriftSequencerMaintenanceProgress
    ) -> "SequencerMaintenanceProgress":
        return SequencerMaintenanceProgress(
            status=src.status,
            target_state=src.target_state,
            created_at=datetime.fromtimestamp(
                src.created_at // _MILLISECONDS_IN_SECOND
            ),
            last_updated_at=datetime.fromtimestamp(
                src.last_updated_at // _MILLISECONDS_IN_SECOND
            ),
            associated_group_ids=src.associated_group_ids,
        )

    def to_thrift(self) -> ThriftSequencerMaintenanceProgress:
        return self._underlying

    @property
    def created_at(self) -> datetime:
        return datetime.fromtimestamp(
            self._underlying.created_at // _MILLISECONDS_IN_SECOND
        )

    @property
    def last_updated_at(self) -> datetime:
        return datetime.fromtimestamp(
            self._underlying.last_updated_at // _MILLISECONDS_IN_SECOND
        )
