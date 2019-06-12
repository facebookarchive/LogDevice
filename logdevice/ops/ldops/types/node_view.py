#!/usr/bin/env python3

# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

from collections import Counter
from dataclasses import dataclass
from typing import Dict, List, Set

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


@dataclass
class NodeView:
    node_config: NodeConfig
    node_state: NodeState
    maintenances: List[MaintenanceDefinition]

    @property
    def nc(self) -> NodeConfig:
        return self.node_config

    @property
    def ns(self) -> NodeState:
        return self.node_state

    @property
    def node_index(self) -> int:
        return self.nc.node_index

    @property
    def ni(self) -> int:
        return self.node_index

    @property
    def name(self) -> str:
        return self.node_name

    @property
    def node_name(self) -> str:
        # If we don't have name in node config, we should use string data_address
        # representation as name
        if self.nc.name:
            return self.nc.name
        else:
            return f"{self.data_address}"

    @property
    def data_address(self) -> SocketAddress:
        return SocketAddress.from_thrift(self.nc.data_address)

    @property
    def thrift_address(self) -> SocketAddress:
        da = self.data_address
        if da.address_family != SocketAddressFamily.INET:
            raise ValueError("Can't calculate Thrift Address from Data Address")
        else:
            return SocketAddress(
                address_family=da.address_family, address=da.address, port=6440
            )

    @property
    def node_id(self) -> NodeID:
        return NodeID(
            node_index=self.nc.node_index,
            address=self.nc.data_address,
            name=self.nc.name,
        )

    @property
    def nn(self) -> str:
        return self.name

    @property
    def location(self) -> str:
        return self.nc.location

    @property
    def location_per_scope(self) -> Dict[LocationScope, str]:
        return self.nc.location_per_scope

    @property
    def roles(self) -> Set[Role]:
        return self.nc.roles

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
        return self.ns.daemon_state

    @property
    def sequencer_config(self) -> SequencerConfig:
        return self.nc.sequencer

    @property
    def sequencer_weight(self) -> float:
        return self.nc.sequencer.weight

    @property
    def sequencer_state(self) -> SequencerState:
        return self.ns.sequencer_state

    @property
    def sequencing_state(self) -> SequencingState:
        return self.ns.sequencer_state.state

    @property
    def storage_config(self) -> StorageConfig:
        return self.nc.storage

    @property
    def storage_weight(self) -> float:
        return self.nc.storage.weight

    @property
    def num_shards(self) -> int:
        return self.nc.storage.num_shards

    @property
    def shard_states(self) -> List[ShardState]:
        return self.ns.shard_states

    @property
    def shards_data_health(self) -> List[ShardDataHealth]:
        return [s.data_health for s in self.shard_states]

    @property
    def shards_data_health_count(self) -> Dict[ShardDataHealth, int]:
        return dict(Counter(self.shards_data_health))

    @property
    def shards_current_storage_state(self) -> List[ShardStorageState]:
        return [s.current_storage_state for s in self.shard_states]

    @property
    def shards_current_storage_state_count(self) -> Dict[ShardStorageState, int]:
        return dict(Counter(self.shards_current_storage_state))

    @property
    def shards_current_operational_state(self) -> List[ShardOperationalState]:
        return [s.current_operational_state for s in self.shard_states]

    @property
    def shards_current_operational_state_count(self) -> Dict[ShardStorageState, int]:
        return dict(Counter(self.shards_current_operational_state))

    @property
    def shards_membership_storage_state(self) -> List[StorageState]:
        return [s.storage_state for s in self.shard_states]

    @property
    def shards_membership_storage_state_count(self) -> Dict[ShardStorageState, int]:
        return dict(Counter(self.shards_membership_storage_state))

    @property
    def shards_metadata_state(self) -> List[MetaDataStorageState]:
        return [s.metadata_state for s in self.shard_states]

    @property
    def shards_metadata_state_count(self) -> Dict[MetaDataStorageState, int]:
        return dict(Counter(self.shards_metadata_state))
