#!/usr/bin/env python3
# pyre-strict

# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import ipaddress
import random
import string
import types
from datetime import datetime
from typing import Dict, List, Optional, Type

from ldops.const import ALL_SHARDS
from ldops.types.cluster import Cluster
from logdevice.admin.common.types import (
    LocationScope,
    NodeID,
    Role,
    ShardID,
    SocketAddress,
    SocketAddressFamily,
)
from logdevice.admin.exceptions.types import MaintenanceMatchError
from logdevice.admin.maintenance.types import (
    MaintenanceDefinition,
    MaintenanceDefinitionResponse,
    MaintenanceProgress,
    MaintenancesFilter,
    RemoveMaintenancesRequest,
    RemoveMaintenancesResponse,
)
from logdevice.admin.nodes.types import (
    NodeConfig,
    NodesConfigResponse,
    NodesFilter,
    NodesStateRequest,
    NodesStateResponse,
    NodeState,
    SequencerConfig,
    SequencerMaintenanceProgress,
    SequencerState,
    SequencingState,
    ServiceState,
    ShardDataHealth,
    ShardMaintenanceProgress,
    ShardOperationalState,
    ShardState,
    ShardStorageState,
    StorageConfig,
)
from logdevice.membership.Membership.types import MetaDataStorageState, StorageState


def gen_SocketAddress() -> SocketAddress:
    return SocketAddress(
        address_family=SocketAddressFamily.INET,
        address=ipaddress.IPv6Address(random.getrandbits(128)).compressed,
        port=4440,
    )


def gen_SequencingState() -> SequencingState:
    return random.choice(
        [
            SequencingState.ENABLED,
            SequencingState.BOYCOTTED,
            SequencingState.DISABLED,
            SequencingState.UNKNOWN,
        ]
    )


def gen_ShardOperationalState() -> ShardOperationalState:
    return random.choice(
        [
            ShardOperationalState.UNKNOWN,
            ShardOperationalState.ENABLED,
            ShardOperationalState.MAY_DISAPPEAR,
            ShardOperationalState.DRAINED,
            # ShardOperationalState.MIGRATING_DATA,
            ShardOperationalState.ENABLING,
            ShardOperationalState.PROVISIONING,
            # ShardOperationalState.PASSIVE_DRAINING,
            ShardOperationalState.INVALID,
        ]
    )


def gen_word(length: Optional[int] = None) -> str:
    if length is None:
        length = random.randint(3, 15)
    VOWELS = "aeiou"
    CONSONANTS = "".join(set(string.ascii_lowercase) - set(VOWELS))
    word = ""
    for i in range(length):
        if i % 2 == 0:
            word += random.choice(CONSONANTS)
        else:
            word += random.choice(VOWELS)
    return word


class MockAdminAPI:
    storage_node_name_tmpl = "logdevices{node_index}.{region}.facebook.com"
    sequencer_node_name_tmpl = "logdeviceq{node_index}.{region}.facebook.com"
    location_tmpl = "{region}.{data_center}.{cluster}.{row}.{rack}"

    def __init__(
        self,
        cluster: Optional[Cluster] = None,
        num_storage_nodes: int = 100,
        shards_per_storage_node: int = 16,
        num_sequencer_nodes: int = 0,
        disaggregated: bool = False,
        distribute_across: LocationScope = LocationScope.ROW,
        num_distribute_across: int = 5,
    ) -> None:
        self.num_storage_nodes = num_storage_nodes
        self.shards_per_storage_node = shards_per_storage_node
        self.num_sequencer_nodes = num_sequencer_nodes
        self.disaggregated = disaggregated
        self.distribute_across = distribute_across
        self.num_distribute_across = num_distribute_across

        self.available_locations: Dict[LocationScope, List[str]] = {}
        self._nc_by_name: Dict[str, NodeConfig] = {}
        self._ns_by_name: Dict[str, NodeState] = {}

        self._nc_by_node_index: Dict[int, NodeConfig] = {}
        self._ns_by_node_index: Dict[int, NodeState] = {}

        self._nc_version = 0
        self._ns_version = 0
        self._maintenances_by_id: Dict[str, MaintenanceDefinition] = {}

        self._gen_done = False

    def _gen_locations(self) -> None:
        available_locations = {}
        for loc_scope in [
            LocationScope.REGION,
            LocationScope.DATA_CENTER,
            LocationScope.CLUSTER,
            LocationScope.ROW,
            LocationScope.RACK,
        ]:
            if loc_scope.value > self.distribute_across.value:
                available_locations[loc_scope] = [gen_word(3)]
            elif loc_scope.value == self.distribute_across.value:
                available_locations[loc_scope] = [
                    gen_word(3) for _ in range(self.num_distribute_across)
                ]
            else:
                available_locations[loc_scope] = [gen_word(3) for _ in range(1000)]

        self.available_locations = available_locations

    def _select_random_location(self) -> Dict[LocationScope, str]:
        return {
            ls: random.choice(self.available_locations[ls])
            for ls in [
                LocationScope.REGION,
                LocationScope.DATA_CENTER,
                LocationScope.CLUSTER,
                LocationScope.ROW,
                LocationScope.RACK,
            ]
        }

    def _loc_to_str(self, loc: Dict[LocationScope, str]) -> str:
        return self.location_tmpl.format(
            region=loc[LocationScope.REGION],
            data_center=loc[LocationScope.DATA_CENTER],
            cluster=loc[LocationScope.CLUSTER],
            row=loc[LocationScope.ROW],
            rack=loc[LocationScope.RACK],
        )

    def _gen(self) -> None:
        self._gen_locations()
        self._gen_nodes()
        self._gen_maintenances()
        self._gen_done = True

    def _gen_nodes(self) -> None:
        ts = int(datetime.now().timestamp())
        self._ns_version = ts
        self._nc_version = ts

        self._nc_by_node_index = {}
        self._nc_by_name = {}
        self._ns_by_node_index = {}
        self._ns_by_name = {}

        # generate storage nodes
        for node_index in range(0, self.num_storage_nodes):
            loc = self._select_random_location()
            name = self.storage_node_name_tmpl.format(
                node_index=node_index, region=loc[LocationScope.REGION]
            )
            nc = NodeConfig(
                node_index=node_index,
                data_address=gen_SocketAddress(),
                roles={Role.SEQUENCER, Role.STORAGE}
                if not self.disaggregated
                else {Role.STORAGE},
                other_addresses=None,
                location=self._loc_to_str(loc),
                sequencer=SequencerConfig(weight=1) if not self.disaggregated else None,
                storage=StorageConfig(
                    weight=1, num_shards=self.shards_per_storage_node
                ),
                location_per_scope=loc,
                name=name,
            )
            ns = NodeState(
                node_index=node_index,
                daemon_state=ServiceState.ALIVE,
                sequencer_state=SequencerState(
                    state=SequencingState.ENABLED, maintenance=None
                )
                if not self.disaggregated
                else None,
                shard_states=[
                    ShardState(
                        data_health=ShardDataHealth.HEALTHY,
                        current_storage_state=ShardStorageState.READ_WRITE,
                        current_operational_state=ShardOperationalState.ENABLED,
                        maintenance=None,
                        storage_state=StorageState.READ_WRITE,
                        metadata_state=MetaDataStorageState.NONE,
                    )
                    for _ in range(self.shards_per_storage_node)
                ],
            )
            self._nc_by_node_index[node_index] = nc
            self._nc_by_name[name] = nc
            self._ns_by_node_index[node_index] = ns
            self._ns_by_name[name] = ns

        # generate sequencer nodes
        if self.disaggregated:
            for node_index in range(
                self.num_storage_nodes,
                self.num_storage_nodes + self.num_sequencer_nodes,
            ):
                loc = self._select_random_location()
                name = self.storage_node_name_tmpl.format(
                    node_index=node_index, region=loc[LocationScope.REGION]
                )
                nc = NodeConfig(
                    node_index=node_index,
                    data_address=gen_SocketAddress(),
                    roles={Role.SEQUENCER},
                    other_addresses=None,
                    location=self._loc_to_str(loc),
                    sequencer=SequencerConfig(weight=1),
                    storage=None,
                    location_per_scope=loc,
                    name=name,
                )
                ns = NodeState(
                    node_index=node_index,
                    daemon_state=ServiceState.ALIVE,
                    sequencer_state=SequencerState(
                        state=SequencingState.ENABLED, maintenance=None
                    ),
                    shard_states=None,
                )
                self._nc_by_node_index[node_index] = nc
                self._nc_by_name[name] = nc
                self._ns_by_node_index[node_index] = ns
                self._ns_by_name[name] = ns

    def _gen_maintenances(self) -> None:
        self._maintenances_by_id = {}

    def _set_shard_current_operational_state(
        self, shard: ShardID, target_state: ShardOperationalState
    ) -> None:
        assert shard.node.node_index is not None
        # pyre-fixme[6]: Expected `int` for 1st param but got `Optional[int]`.
        nc = self._nc_by_node_index[shard.node.node_index]
        # pyre-fixme[6]: Expected `int` for 1st param but got `Optional[int]`.
        ns = self._ns_by_node_index[shard.node.node_index]
        shard_states = []
        for shard_index, shard_state in enumerate(ns.shard_states or []):
            if shard.shard_index == shard_index:
                shard_states.append(shard_state(current_operational_state=target_state))
            else:
                shard_states.append(shard_state)
        new_ns = ns(shard_states=shard_states)
        # pyre-fixme[6]: Expected `int` for 1st param but got `Optional[int]`.
        self._ns_by_node_index[shard.node.node_index] = new_ns
        self._ns_by_name[nc.name] = new_ns

    def _set_shard_maintenance_progress(
        self, shard: ShardID, maintenance_progress: ShardMaintenanceProgress
    ) -> None:
        assert shard.node.node_index is not None
        # pyre-fixme[6]: Expected `int` for 1st param but got `Optional[int]`.
        nc = self._nc_by_node_index[shard.node.node_index]
        # pyre-fixme[6]: Expected `int` for 1st param but got `Optional[int]`.
        ns = self._ns_by_node_index[shard.node.node_index]
        shard_states = []
        for shard_index, shard_state in enumerate(ns.shard_states or []):
            if shard.shard_index == shard_index:
                shard_states.append(shard_state(maintenance=maintenance_progress))
            else:
                shard_states.append(shard_state)
        new_ns = ns(shard_states=shard_states)
        # pyre-fixme[6]: Expected `int` for 1st param but got `Optional[int]`.
        self._ns_by_node_index[shard.node.node_index] = new_ns
        self._ns_by_name[nc.name] = new_ns

    def _set_sequencing_state(
        self, node_id: NodeID, target_state: SequencingState
    ) -> None:
        assert node_id.node_index is not None
        # pyre-fixme[6]: Expected `int` for 1st param but got `Optional[int]`.
        nc = self._nc_by_node_index[node_id.node_index]
        # pyre-fixme[6]: Expected `int` for 1st param but got `Optional[int]`.
        ns = self._ns_by_node_index[node_id.node_index]
        assert ns.sequencer_state is not None
        # pyre-fixme[29]: `Optional[SequencerState]` is not a function.
        new_ns = ns(sequencer_state=ns.sequencer_state(state=target_state))
        # pyre-fixme[6]: Expected `int` for 1st param but got `Optional[int]`.
        self._ns_by_node_index[node_id.node_index] = new_ns
        self._ns_by_name[nc.name] = new_ns

    def _set_sequencer_maintenance_progress(
        self, node_id: NodeID, maintenance_progress: SequencerMaintenanceProgress
    ) -> None:
        assert node_id.node_index is not None
        # pyre-fixme[6]: Expected `int` for 1st param but got `Optional[int]`.
        nc = self._nc_by_node_index[node_id.node_index]
        # pyre-fixme[6]: Expected `int` for 1st param but got `Optional[int]`.
        ns = self._ns_by_node_index[node_id.node_index]
        assert ns.sequencer_state is not None
        new_ns = ns(
            # pyre-fixme[29]: `Optional[SequencerState]` is not a function.
            sequencer_state=ns.sequencer_state(maintenance=maintenance_progress)
        )
        # pyre-fixme[6]: Expected `int` for 1st param but got `Optional[int]`.
        self._ns_by_node_index[node_id.node_index] = new_ns
        self._ns_by_name[nc.name] = new_ns

    async def __aexit__(
        self,
        exc_type: Optional[Type[Exception]],
        exc: Optional[Exception],
        tb: Optional[types.TracebackType],
    ) -> None:
        pass

    async def __aenter__(self) -> "MockAdminAPI":
        if not self._gen_done:
            self._gen()
        return self

    @classmethod
    def _filter_ncs(
        cls, ncs: List[NodeConfig], filter: Optional[NodesFilter]
    ) -> List[NodeConfig]:
        if filter is None:
            return ncs

        if filter.node is not None:
            # pyre-fixme[16]: `Optional` has no attribute `node_index`.
            if filter.node.node_index is not None:
                ncs = [nc for nc in ncs if nc.node_index == filter.node.node_index]
            # pyre-fixme[16]: `Optional` has no attribute `address`.
            if filter.node.address is not None:
                ncs = [nc for nc in ncs if nc.data_address == filter.node.address]
            # pyre-fixme[16]: `Optional` has no attribute `name`.
            if filter.node.name is not None:
                ncs = [nc for nc in ncs if nc.name == filter.node.name]

        if filter.role is not None:
            ncs = [nc for nc in ncs if filter.role in nc.roles]

        if filter.location is not None:
            ncs = [
                nc
                for nc in ncs
                # pyre-fixme[16]: `Optional` has no attribute `startswith`.
                if nc.location is not None and nc.location.startswith(filter.location)
            ]

        return ncs

    async def getNodesConfig(self, filter: NodesFilter) -> NodesConfigResponse:
        ncs = [nc for _, nc in self._nc_by_node_index.items()]
        ncs = self._filter_ncs(ncs, filter)

        return NodesConfigResponse(
            nodes=sorted(ncs, key=lambda x: x.node_index), version=self._nc_version
        )

    async def getNodesState(self, request: NodesStateRequest) -> NodesStateResponse:
        ncs = [nc for _, nc in self._nc_by_node_index.items()]
        ncs = self._filter_ncs(ncs, request.filter)
        filtered_node_indexes = {nc.node_index for nc in ncs}

        nss = [ns for _, ns in self._ns_by_node_index.items()]
        nss = [ns for ns in nss if ns.node_index in filtered_node_indexes]
        return NodesStateResponse(
            states=sorted(nss, key=lambda x: x.node_index), version=self._ns_version
        )

    async def applyMaintenance(
        self, request: MaintenanceDefinition
    ) -> MaintenanceDefinitionResponse:
        # TODO: ungroup if group == False
        shards = []
        for sh in request.shards:
            if sh.shard_index == ALL_SHARDS:
                r = range(self.shards_per_storage_node)
            else:
                r = range(sh.shard_index, sh.shard_index + 1)

            assert sh.node.node_index is not None
            nc = self._nc_by_node_index[sh.node.node_index]  # pyre-ignore
            for shard_index in r:
                shards.append(
                    ShardID(
                        node=NodeID(
                            node_index=nc.node_index,
                            name=nc.name,
                            address=nc.data_address,
                        ),
                        shard_index=shard_index,
                    )
                )

        shards = tuple(sorted(shards, key=lambda s: (s.node.node_index, s.shard_index)))

        seq_nodes = []
        for n in request.sequencer_nodes:
            assert n.node_index is not None
            # pyre-fixme[6]: Expected `int` for 1st param but got `Optional[int]`.
            nc = self._nc_by_node_index[n.node_index]
            seq_nodes.append(
                NodeID(node_index=nc.node_index, name=nc.name, address=nc.data_address)
            )

        mnt = MaintenanceDefinition(
            shards=shards,
            shard_target_state=request.shard_target_state,
            sequencer_nodes=seq_nodes,
            sequencer_target_state=request.sequencer_target_state,
            user=request.user,
            reason=request.reason,
            extras=request.extras,
            skip_safety_checks=request.skip_safety_checks,
            force_restore_rebuilding=request.force_restore_rebuilding,
            group=request.group,
            ttl_seconds=request.ttl_seconds,
            allow_passive_drains=request.allow_passive_drains,
            group_id=gen_word(8),
            last_check_impact_result=None,
            expires_on=1000 * (int(datetime.now().timestamp()) + request.ttl_seconds)
            if request.ttl_seconds
            else None,
            created_on=1000 * int(datetime.now().timestamp()),
            progress=MaintenanceProgress.IN_PROGRESS,
        )
        assert mnt.group_id is not None
        # pyre-fixme[6]: Expected `str` for 1st param but got `Optional[str]`.
        self._maintenances_by_id[mnt.group_id] = mnt
        return MaintenanceDefinitionResponse(maintenances=[mnt])

    @classmethod
    def _filter_mnts(
        cls,
        mnts: List[MaintenanceDefinition],
        filter: Optional[MaintenancesFilter] = None,
    ) -> List[MaintenanceDefinition]:
        if filter is None:
            return mnts

        if filter.group_ids:
            group_ids = set(filter.group_ids)
            mnts = [mnt for mnt in mnts if mnt.group_id in group_ids]

        if filter.user:
            mnts = [mnt for mnt in mnts if mnt.user == filter.user]

        return mnts

    async def getMaintenances(
        self, filter: MaintenancesFilter
    ) -> MaintenanceDefinitionResponse:
        mnts = [mnt for _, mnt in self._maintenances_by_id.items()]
        mnts = self._filter_mnts(mnts, filter)

        return MaintenanceDefinitionResponse(maintenances=mnts)

    async def removeMaintenances(
        self, request: RemoveMaintenancesRequest
    ) -> RemoveMaintenancesResponse:
        mnts = [mnt for _, mnt in self._maintenances_by_id.items()]
        if request.filter:
            mnts = self._filter_mnts(mnts, request.filter)

        if not mnts:
            raise MaintenanceMatchError()

        for mnt in mnts:
            assert mnt.group_id is not None
            # pyre-fixme[6]: Expected `str` for 1st param but got `Optional[str]`.
            del self._maintenances_by_id[mnt.group_id]

        return RemoveMaintenancesResponse(maintenances=mnts)
