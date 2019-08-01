#!/usr/bin/env python3

# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


import operator
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, Tuple

from ldops.exceptions import NodeIsNotASequencerError
from ldops.types.maintenance_overall_status import MaintenanceOverallStatus
from ldops.types.node_view import NodeView
from ldops.types.sequencer_maintenance_progress import SequencerMaintenanceProgress
from ldops.types.shard_maintenance_progress import ShardMaintenanceProgress
from logdevice.admin.common.types import NodeID, ShardID
from logdevice.admin.maintenance.types import MaintenanceDefinition
from logdevice.admin.nodes.types import (
    MaintenanceStatus,
    SequencerState,
    SequencingState,
    ShardOperationalState,
    ShardState,
)


class MaintenanceView:
    def __init__(
        self,
        maintenance: MaintenanceDefinition,
        node_index_to_node_view: Dict[int, NodeView],
    ):
        self._maintenance = maintenance
        self._node_index_to_node_view = node_index_to_node_view

    def __getattr__(self, name: str) -> Any:
        return getattr(self._maintenance, name)

    @property
    def affected_sequencer_node_ids(self) -> Tuple[NodeID, ...]:
        return tuple(
            sorted(set(self.sequencer_nodes), key=operator.attrgetter("node_index"))
        )

    @property
    def affected_sequencer_node_indexes(self) -> Tuple[int, ...]:
        return tuple(
            n.node_index
            for n in self.affected_sequencer_node_ids
            if n.node_index is not None
        )

    @property
    def affected_storage_node_ids(self) -> Tuple[NodeID, ...]:
        return tuple(
            sorted({s.node for s in self.shards}, key=operator.attrgetter("node_index"))
        )

    @property
    def affected_storage_node_indexes(self) -> Tuple[int, ...]:
        return tuple(
            n.node_index
            for n in self.affected_storage_node_ids
            if n.node_index is not None
        )

    @property
    def affected_node_ids(self) -> Tuple[NodeID, ...]:
        return tuple(
            sorted(
                set(self.sequencer_nodes).union({s.node for s in self.shards}),
                key=operator.attrgetter("node_index"),
            )
        )

    @property
    def affected_node_indexes(self) -> Tuple[int, ...]:
        return tuple(
            n.node_index for n in self.affected_node_ids if n.node_index is not None
        )

    @property
    def shard_target_state(self) -> Optional[ShardOperationalState]:
        if self.affects_shards:
            return self._maintenance.shard_target_state
        else:
            return None

    @property
    def sequencer_target_state(self) -> Optional[SequencingState]:
        if self.affects_sequencers:
            return self._maintenance.sequencer_target_state
        else:
            return None

    @property
    def ttl(self) -> Optional[timedelta]:
        if self._maintenance.ttl_seconds == 0:
            return None
        else:
            return timedelta(seconds=self._maintenance.ttl_seconds)

    @property
    def created_on(self) -> Optional[datetime]:
        if self._maintenance.created_on is None:
            return None
        else:
            return datetime.fromtimestamp(self._maintenance.created_on // 1000)

    @property
    def expires_on(self) -> Optional[datetime]:
        if self._maintenance.expires_on is None:
            return None
        else:
            return datetime.fromtimestamp(self._maintenance.expires_on // 1000)

    @property
    def expires_in(self) -> Optional[timedelta]:
        if self.expires_on is None:
            return None
        else:
            return self.expires_on - datetime.now()

    @property
    def affects_shards(self) -> bool:
        return len(self._maintenance.shards) > 0

    @property
    def affects_sequencers(self) -> bool:
        return len(self._maintenance.sequencer_nodes) > 0

    @property
    def num_shards_total(self) -> int:
        return len(self._maintenance.shards)

    @property
    def num_shards_done(self) -> int:
        return sum(
            1
            if self.get_shard_maintenance_status(s) == MaintenanceStatus.COMPLETED
            else 0
            for s in self.shards
        )

    @property
    def are_all_shards_done(self) -> bool:
        return self.num_shards_done == self.num_shards_total

    @property
    def num_sequencers_total(self) -> int:
        return len(self._maintenance.sequencer_nodes)

    @property
    def num_sequencers_done(self) -> int:
        return sum(
            1
            if self.get_sequencer_maintenance_status(n) == MaintenanceStatus.COMPLETED
            else 0
            for n in self.sequencer_nodes
        )

    @property
    def are_all_sequencers_done(self) -> bool:
        return self.num_sequencers_done == self.num_sequencers_total

    @property
    def is_everything_done(self) -> bool:
        return self.are_all_sequencers_done and self.are_all_shards_done

    @property
    def is_blocked(self) -> bool:
        for s in self.shards:
            if self.get_shard_maintenance_status(s) in {
                MaintenanceStatus.BLOCKED_UNTIL_SAFE,
                MaintenanceStatus.REBUILDING_IS_BLOCKED,
            }:
                return True

        for n in self.sequencer_nodes:
            if self.get_sequencer_maintenance_status(n) in {
                MaintenanceStatus.BLOCKED_UNTIL_SAFE,
                MaintenanceStatus.REBUILDING_IS_BLOCKED,
            }:
                return True

        return False

    @property
    def is_completed(self) -> bool:
        for s in self.shards:
            if self.get_shard_maintenance_status(s) != MaintenanceStatus.COMPLETED:
                return False

        for n in self.sequencer_nodes:
            if self.get_sequencer_maintenance_status(n) != MaintenanceStatus.COMPLETED:
                return False

        return True

    @property
    def is_in_progress(self) -> bool:
        return not self.is_blocked and not self.is_completed

    def get_shard_state(self, shard: ShardID) -> ShardState:
        assert shard.node.node_index is not None
        return self._node_index_to_node_view[shard.node.node_index].shard_states[
            shard.shard_index
        ]

    def get_sequencer_state(self, sequencer: NodeID) -> Optional[SequencerState]:
        assert sequencer.node_index is not None
        return self._node_index_to_node_view[sequencer.node_index].sequencer_state

    def get_shards_by_node_index(self, node_index: int) -> Tuple[ShardID, ...]:
        return tuple(s for s in self.shards if s.node.node_index == node_index)

    @property
    def shards(self) -> Tuple[ShardID, ...]:
        return tuple(
            sorted(
                self._maintenance.shards,
                key=lambda x: (x.node.node_index, x.shard_index),
            )
        )

    def get_shard_maintenance_status(self, shard: ShardID) -> MaintenanceStatus:
        shard_state = self.get_shard_state(shard)
        if self.shard_target_state == ShardOperationalState.MAY_DISAPPEAR:
            if shard_state.current_operational_state in {
                ShardOperationalState.DRAINED,
                ShardOperationalState.MAY_DISAPPEAR,
                ShardOperationalState.MIGRATING_DATA,
                ShardOperationalState.PROVISIONING,
            }:
                return MaintenanceStatus.COMPLETED

        if self.shard_target_state == ShardOperationalState.DRAINED:
            if shard_state.current_operational_state == ShardOperationalState.DRAINED:
                return MaintenanceStatus.COMPLETED

        if shard_state.maintenance is not None:
            return shard_state.maintenance.status

        return MaintenanceStatus.NOT_STARTED

    def get_shard_last_updated_at(self, shard: ShardID) -> Optional[datetime]:
        shard_state = self.get_shard_state(shard)
        if shard_state.maintenance is not None:
            return ShardMaintenanceProgress.from_thrift(
                shard_state.maintenance
            ).last_updated_at
        else:
            return None

    def get_sequencer_maintenance_status(self, sequencer: NodeID) -> MaintenanceStatus:
        sequencer_state = self.get_sequencer_state(sequencer)
        if sequencer_state is None:
            raise NodeIsNotASequencerError(f"{sequencer}")
        if self.sequencer_target_state == sequencer_state.state:
            return MaintenanceStatus.COMPLETED
        elif sequencer_state.maintenance is not None:
            return sequencer_state.maintenance.status
        else:
            return MaintenanceStatus.NOT_STARTED

    def get_sequencer_last_updated_at(self, sequencer: NodeID) -> Optional[datetime]:
        sequencer_state = self.get_sequencer_state(sequencer)
        if sequencer_state is None:
            raise NodeIsNotASequencerError(f"{sequencer}")
        elif sequencer_state.maintenance is not None:
            return SequencerMaintenanceProgress.from_thrift(
                sequencer_state.maintenance
            ).last_updated_at
        else:
            return None

    @property
    def overall_status(self) -> MaintenanceOverallStatus:
        if self.is_completed:
            return MaintenanceOverallStatus.COMPLETED
        elif self.is_blocked:
            return MaintenanceOverallStatus.BLOCKED
        else:
            return MaintenanceOverallStatus.IN_PROGRESS
