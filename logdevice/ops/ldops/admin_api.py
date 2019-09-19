#!/usr/bin/env python3
# pyre-strict

# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""
ldops.admin_api
~~~~~~~~~~~

Low-level wrapper around LogDevice Thrift AdminAPI.
This library generally is not expected to be directly used by external code.
"""

import logging
from typing import Optional

from fb303.types import fb_status
from logdevice.admin.clients import AdminAPI
from logdevice.admin.cluster_membership.types import (
    AddNodesRequest,
    AddNodesResponse,
    MarkShardsAsProvisionedRequest,
    MarkShardsAsProvisionedResponse,
    RemoveNodesRequest,
    RemoveNodesResponse,
    UpdateNodesRequest,
    UpdateNodesResponse,
)
from logdevice.admin.logtree.types import (
    LogGroupCustomCountersRequest,
    LogGroupCustomCountersResponse,
    LogGroupThroughputRequest,
    LogGroupThroughputResponse,
)
from logdevice.admin.maintenance.types import (
    MaintenanceDefinition,
    MaintenanceDefinitionResponse,
    MaintenancesFilter,
    MarkAllShardsUnrecoverableRequest,
    MarkAllShardsUnrecoverableResponse,
    RemoveMaintenancesRequest,
    RemoveMaintenancesResponse,
)
from logdevice.admin.nodes.types import (
    NodesConfigResponse,
    NodesFilter,
    NodesStateRequest,
    NodesStateResponse,
)
from logdevice.admin.safety.types import CheckImpactRequest, CheckImpactResponse
from logdevice.admin.settings.types import SettingsRequest, SettingsResponse


logger: logging.Logger = logging.getLogger(__name__)


async def add_nodes(client: AdminAPI, req: AddNodesRequest) -> AddNodesResponse:
    """
    Wrapper for addNodes() Thrift method
    """
    return await client.addNodes(req)


async def update_nodes(
    client: AdminAPI, req: UpdateNodesRequest
) -> UpdateNodesResponse:
    """
    Wrapper for updateNodes() Thrift method
    """
    return await client.updateNodes(req)


async def remove_nodes(
    client: AdminAPI, req: RemoveNodesRequest
) -> RemoveNodesResponse:
    """
    Wrapper for removeNodes() Thrift method
    """
    return await client.removeNodes(req)


async def get_nodes_config(
    client: AdminAPI, req: Optional[NodesFilter] = None
) -> NodesConfigResponse:
    """
    Wrapper for getNodesConfig() Thrift method
    """
    return await client.getNodesConfig(req or NodesFilter())


async def get_nodes_state(
    client: AdminAPI, req: Optional[NodesStateRequest] = None
) -> NodesStateResponse:
    """
    Wrapper for getNodesState() Thrift method
    """
    return await client.getNodesState(req or NodesStateRequest())


async def add_nodes(client: AdminAPI, req: AddNodesRequest) -> AddNodesResponse:
    """
    Wrapper for addNodes() Thrift method
    """
    return await client.addNodes(req)


async def update_nodes(
    client: AdminAPI, req: UpdateNodesRequest
) -> UpdateNodesResponse:
    """
    Wrapper for updateNodes() Thrift method
    """
    return await client.updateNodes(req)


async def remove_nodes(
    client: AdminAPI, req: RemoveNodesRequest
) -> RemoveNodesResponse:
    """
    Wrapper for removeNodes() Thrift method
    """
    return await client.removeNodes(req)


async def mark_shards_as_provisioned(
    client: AdminAPI, req: MarkShardsAsProvisionedRequest
) -> MarkShardsAsProvisionedResponse:
    """
    Wrapper for markShardsAsProvisionedRequest() Thrift method
    """
    return await client.markShardsAsProvisioned(req)


async def get_maintenances(
    client: AdminAPI, req: Optional[MaintenancesFilter] = None
) -> MaintenanceDefinitionResponse:
    """
    Wrapper for getMaintenances() Thrift method
    """
    return await client.getMaintenances(req or MaintenancesFilter())


async def apply_maintenance(
    client: AdminAPI, req: MaintenanceDefinition
) -> MaintenanceDefinitionResponse:
    """
    Wrapper for applyMaintenance() Thrift method
    """
    return await client.applyMaintenance(req)


async def remove_maintenances(
    client: AdminAPI, req: RemoveMaintenancesRequest
) -> RemoveMaintenancesResponse:
    """
    Wrapper for removeMaintenances() Thrift method
    """
    return await client.removeMaintenances(req)


async def unblock_rebuilding(
    client: AdminAPI, req: MarkAllShardsUnrecoverableRequest
) -> MarkAllShardsUnrecoverableResponse:
    """
    Wrapper for MarkAllShardsUnrecoverable() Thrift method
    """
    return await client.markAllShardsUnrecoverable(req)


async def check_impact(
    client: AdminAPI, req: Optional[CheckImpactRequest] = None
) -> CheckImpactResponse:
    """
    Wrapper for checkImpact() Thrift method
    """
    return await client.checkImpact(req or CheckImpactRequest())


async def get_settings(
    client: AdminAPI, req: Optional[SettingsRequest] = None
) -> SettingsResponse:
    """
    Wrapper for getSettings() Thrift method
    """
    return await client.getSettings(req or SettingsRequest())


async def take_log_tree_snapshot(client: AdminAPI, req: int = 0) -> None:
    """
    Wrapper for takeLogTreeSnapshot() Thrift method
    """
    return await client.takeLogTreeSnapshot(req)


async def get_log_group_throughput(
    client: AdminAPI, req: Optional[LogGroupThroughputRequest] = None
) -> LogGroupThroughputResponse:
    """
    Wrapper for getLogGroupThroughput() Thrift method
    """
    return await client.getLogGroupThroughput(req or LogGroupThroughputRequest())


async def get_log_group_custom_counters(
    client: AdminAPI, req: Optional[LogGroupCustomCountersRequest]
) -> LogGroupCustomCountersResponse:
    """
    Wrapper for getLogGroupCustomCounters() Thrift method
    """
    return await client.getLogGroupCustomCounters(
        req or LogGroupCustomCountersRequest()
    )


async def get_version(client: AdminAPI) -> str:
    """
    Wrapper for getVersion() Thrift method
    """
    return await client.getVersion()


async def get_status(client: AdminAPI) -> fb_status:
    """
    Wrapper for getStatus() Thrift method
    """
    return await client.getStatus()


async def alive_since(client: AdminAPI) -> int:
    """
    Wrapper for aliveSince() Thrift method
    """
    return await client.aliveSince()


async def get_pid(client: AdminAPI) -> int:
    """
    Wrapper for getPid() Thrift method
    """
    return await client.getPid()
