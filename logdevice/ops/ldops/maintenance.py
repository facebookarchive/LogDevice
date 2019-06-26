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

from datetime import timedelta
from typing import Collection, FrozenSet, Mapping, Optional, Set

from ldops import admin_api
from ldops.exceptions import LDOpsError
from logdevice.admin.clients import AdminAPI
from logdevice.admin.common.types import NodeID, ShardID
from logdevice.admin.maintenance.types import (
    MaintenanceDefinition,
    MaintenanceDefinitionResponse,
    MaintenancesFilter,
    RemoveMaintenancesRequest,
    RemoveMaintenancesResponse,
)
from logdevice.admin.nodes.types import (
    SequencingState,
    ShardOperationalState,
    ShardStorageState,
)
from logdevice.admin.safety.types import CheckImpactRequest, CheckImpactResponse


class SafetyError(LDOpsError):
    """
    Exception representing unsafe operation.
    """

    def __init__(
        self,
        message: str = "Operation is unsafe",
        check_impact_response: Optional[CheckImpactResponse] = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.check_impact_response = check_impact_response

    def __str__(self) -> str:
        if self.check_impact_response is not None:
            return f"{self.message}: impact: {self.check_impact_response.impact}"
        else:
            return f"{self.message}"


def _recombine_shards(shards: Collection[ShardID]) -> Set[ShardID]:
    whole_nodes = set()
    node_indexes = set()
    single_shards = set()
    for s in shards:
        if s.shard_index == -1:
            whole_nodes.add(s)
            node_indexes.add(s.node.node_index)
        else:
            single_shards.add(s)

    filtered_shards: FrozenSet[ShardID] = frozenset(
        s for s in single_shards if s.node.node_index not in node_indexes
    )
    return whole_nodes.union(filtered_shards)


async def check_impact(
    client: AdminAPI,
    node_ids: Optional[Collection[NodeID]] = None,
    shards: Optional[Collection[ShardID]] = None,
    target_storage_state: ShardStorageState = ShardStorageState.DISABLED,
    disable_sequencers: bool = True,
) -> CheckImpactResponse:
    """
    Performs Safety check and returns CheckImpactResponse. If no node_ids and no
    shards passed it still does safety check, but will return current state
    of the cluster.
    """
    node_ids = set(node_ids or [])
    shards = set(shards or [])

    req_shards: Set[ShardID] = _recombine_shards(
        shards.union(ShardID(node=n, shard_index=-1) for n in node_ids)
    )

    return await admin_api.check_impact(
        client=client,
        req=CheckImpactRequest(
            shards=list(req_shards),
            target_storage_state=target_storage_state,
            disable_sequencers=disable_sequencers,
        ),
    )


async def ensure_safe(
    client: AdminAPI,
    node_ids: Optional[Collection[NodeID]] = None,
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
        node_ids=node_ids,
        shards=shards,
        target_storage_state=target_storage_state,
        disable_sequencers=disable_sequencers,
    )
    if resp.impact:
        raise SafetyError(check_impact_response=resp)


async def get_maintenances(
    client: AdminAPI,
    group_ids: Optional[Collection[str]] = None,
    user: Optional[str] = None,
) -> Collection[MaintenanceDefinition]:
    """
    Queries AdminAPI for maintenances with specified filters.
    Uses AND logic for different arguments.
    """
    group_ids = group_ids or []
    resp: MaintenanceDefinitionResponse = await admin_api.get_maintenances(
        client=client, req=MaintenancesFilter(group_ids=list(group_ids), user=user)
    )
    return resp.maintenances


async def apply_maintenance(
    client: AdminAPI,
    node_ids: Optional[Collection[NodeID]] = None,
    shards: Optional[Collection[ShardID]] = None,
    shard_target_state: Optional[
        ShardOperationalState
    ] = ShardOperationalState.MAY_DISAPPEAR,
    sequencer_nodes: Optional[Collection[NodeID]] = None,
    group: Optional[bool] = True,
    ttl: Optional[timedelta] = None,
    user: Optional[str] = None,
    reason: Optional[str] = None,
    extras: Mapping[str, str] = None,
    skip_safety_checks: Optional[bool] = False,
    allow_passive_drains: Optional[bool] = False,
) -> Collection[MaintenanceDefinition]:
    """
    Applies maintenance to MaintenanceManager.
    If `nodes` argument is specified, they're treated as shards and as
        sequencers simultaneously.

    Can return multiple maintenances if group==False.
    """
    node_ids = set(node_ids or [])
    shards = set(shards or [])
    sequencer_nodes = set(sequencer_nodes or []).union(node_ids)

    if ttl is None:
        ttl = timedelta(seconds=0)

    if user is None:
        user = "__ldops__"

    if reason is None:
        reason = "Not Specified"

    if extras is None:
        extras = {}

    shards = shards.union({ShardID(node=n, shard_index=-1) for n in node_ids})
    shards = _recombine_shards(shards)

    req = MaintenanceDefinition(
        shards=list(shards),
        shard_target_state=shard_target_state,
        sequencer_nodes=[n for n in sequencer_nodes],
        sequencer_target_state=SequencingState.DISABLED,
        user=user,
        reason=reason,
        extras=extras,
        skip_safety_checks=skip_safety_checks,
        group=group,
        ttl_seconds=int(ttl.total_seconds()),
        allow_passive_drains=allow_passive_drains,
    )
    resp: MaintenanceDefinitionResponse = await admin_api.apply_maintenance(
        client=client, req=req
    )
    return resp.maintenances


async def remove_maintenances(
    client: AdminAPI,
    group_ids: Optional[Collection[str]] = None,
    user: Optional[str] = None,
    log_user: Optional[str] = "__ldops__",
    log_reason: Optional[str] = "",
) -> Collection[MaintenanceDefinition]:
    if group_ids is None:
        group_ids = []
    req = RemoveMaintenancesRequest(
        filter=MaintenancesFilter(group_ids=list(set(group_ids)), user=user),
        user=log_user,
        reason=log_reason,
    )
    resp: RemoveMaintenancesResponse = await admin_api.remove_maintenances(
        client=client, req=req
    )
    return resp.maintenances
