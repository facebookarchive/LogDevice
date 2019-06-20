#!/usr/bin/env python3

# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import os.path
from collections import Counter
from dataclasses import dataclass
from typing import AbstractSet, List, Mapping, Optional, Sequence, Tuple

from ldops.types.socket_address import SocketAddress
from logdevice.admin.common.types import (
    LocationScope,
    NodeID,
    Role,
    SocketAddressFamily,
)
from logdevice.admin.maintenance.types import MaintenanceDefinition
from logdevice.admin.nodes.types import (
    NodeConfig,
    NodeState,
    SequencerConfig,
    SequencerState,
    SequencingState,
    ServiceState,
    ShardDataHealth,
    ShardOperationalState,
    ShardState,
    ShardStorageState,
    StorageConfig,
)
from logdevice.membership.Membership.types import MetaDataStorageState, StorageState


@dataclass(frozen=True)
class NodeView:
    node_config: NodeConfig
    node_state: NodeState
    maintenances: Tuple[MaintenanceDefinition, ...]

    def __post_init__(self) -> None:
        if self.node_config.node_index != self.node_state.node_index:
            raise ValueError(
                "node_config.node_index does not match node_state.node_index"
            )

    @property
    def node_index(self) -> int:
        return self.node_config.node_index

    @property
    def node_name(self) -> str:
        # If we don't have name in node config, we should use string data_address
        # representation as name
        if self.node_config.name:
            return self.node_config.name
        else:
            return str(self.data_address)

    @property
    def data_address(self) -> SocketAddress:
        return SocketAddress.from_thrift(self.node_config.data_address)

    @property
    def thrift_address(self) -> SocketAddress:
        da: SocketAddress = self.data_address
        if da.address_family == SocketAddressFamily.UNIX:
            assert da.path is not None
            return SocketAddress(
                address_family=da.address_family,
                path=os.path.join(os.path.dirname(da.path), "socket_admin"),
            )
        elif da.address_family == SocketAddressFamily.INET:
            assert da.address is not None
            return SocketAddress(
                address_family=da.address_family, address=da.address, port=6440
            )
        else:
            assert False, "unreachable"  # pragma: nocover

    @property
    def node_id(self) -> NodeID:
        return NodeID(
            node_index=self.node_config.node_index,
            address=self.node_config.data_address,
            # Not self.node_name because AdminAPI expects value from NodeConfig
            name=self.node_config.name,
        )

    @property
    def location(self) -> Optional[str]:
        return self.node_config.location

    @property
    def location_per_scope(self) -> Mapping[LocationScope, str]:
        return self.node_config.location_per_scope

    @property
    def roles(self) -> AbstractSet[Role]:
        return self.node_config.roles

    def has_role(self, role: Role) -> bool:
        return role in self.roles

    @property
    def is_sequencer(self) -> bool:
        return self.has_role(Role.SEQUENCER)

    @property
    def is_storage(self) -> bool:
        return self.has_role(Role.STORAGE)

    @property
    def daemon_state(self) -> ServiceState:
        return self.node_state.daemon_state

    @property
    def sequencer_config(self) -> Optional[SequencerConfig]:
        return self.node_config.sequencer

    @property
    def sequencer_weight(self) -> Optional[float]:
        if self.sequencer_config is not None:
            return self.sequencer_config.weight
        else:
            return None

    @property
    def sequencer_state(self) -> Optional[SequencerState]:
        return self.node_state.sequencer_state

    @property
    def sequencing_state(self) -> Optional[SequencingState]:
        if self.sequencer_state is not None:
            return self.sequencer_state.state
        else:
            return None

    @property
    def storage_config(self) -> Optional[StorageConfig]:
        return self.node_config.storage

    @property
    def storage_weight(self) -> Optional[float]:
        if self.storage_config is not None:
            return self.storage_config.weight
        else:
            return None

    @property
    def num_shards(self) -> Optional[int]:
        if self.storage_config is not None:
            return self.storage_config.num_shards
        else:
            return None

    @property
    def shard_states(self) -> Sequence[ShardState]:
        if self.node_state.shard_states is None:
            return []
        else:
            return self.node_state.shard_states

    @property
    def shards_data_health(self) -> List[ShardDataHealth]:
        return [s.data_health for s in self.shard_states]

    @property
    def shards_data_health_count(self) -> Mapping[ShardDataHealth, int]:
        return Counter(self.shards_data_health)

    @property
    def shards_current_storage_state(self) -> List[ShardStorageState]:
        return [s.current_storage_state for s in self.shard_states]

    @property
    def shards_current_storage_state_count(self) -> Mapping[ShardStorageState, int]:
        return Counter(self.shards_current_storage_state)

    @property
    def shards_current_operational_state(self) -> List[ShardOperationalState]:
        return [s.current_operational_state for s in self.shard_states]

    @property
    def shards_current_operational_state_count(
        self
    ) -> Mapping[ShardOperationalState, int]:
        return Counter(self.shards_current_operational_state)

    @property
    def shards_membership_storage_state(self) -> List[StorageState]:
        return [s.storage_state for s in self.shard_states]

    @property
    def shards_membership_storage_state_count(self) -> Mapping[StorageState, int]:
        return Counter(self.shards_membership_storage_state)

    @property
    def shards_metadata_state(self) -> List[MetaDataStorageState]:
        return [s.metadata_state for s in self.shard_states]

    @property
    def shards_metadata_state_count(self) -> Mapping[MetaDataStorageState, int]:
        return Counter(self.shards_metadata_state)
