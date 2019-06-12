#!/usr/bin/env python3

# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""
ldops.cluster
~~~~~~~~~~~

Implements cluster-specific operations.
"""

import asyncio
from typing import Dict, FrozenSet, Optional

from ldops import admin_api
from ldops.exceptions import NodeNotFoundError
from ldops.types.cluster import Cluster
from ldops.types.cluster_view import ClusterView
from ldops.types.node import Node
from ldops.types.socket_address import SocketAddress
from logdevice.admin.clients import AdminAPI
from logdevice.admin.common.types import NodeID
from logdevice.admin.exceptions.types import NotSupported
from logdevice.admin.nodes.types import (
    NodeConfig,
    NodesConfigResponse,
    NodesFilter,
    NodesStateResponse,
    NodeState,
)


DEFAULT_THRIFT_PORT = 6440


async def get_cluster_by_hostname(
    hostname: str, port: int = DEFAULT_THRIFT_PORT
) -> Cluster:
    """
    Convenience method which automatically resolves given hostname and returns
    Cluster instance
    """
    return await get_cluster(
        admin_server_addr=SocketAddress.from_host_port(host=hostname, port=port)
    )


async def get_cluster(
    name: Optional[str] = None, admin_server_addr: Optional[SocketAddress] = None
) -> Cluster:
    """
    Factory for Cluster object
    """
    return Cluster(name=name, admin_server_addr=admin_server_addr)


def _get_node_by_node_config(nc: NodeConfig) -> Node:
    return Node(
        node_index=nc.node_index,
        data_addr=SocketAddress.from_thrift(nc.data_address),
        name=nc.name,
    )


async def get_nodes(client: AdminAPI) -> FrozenSet[Node]:
    """
    Returns all nodes available from provided AdminAPI client
    """
    resp: NodesConfigResponse = await admin_api.get_nodes_config(client)
    return frozenset(_get_node_by_node_config(nc) for nc in resp.nodes)


async def get_node_by_node_index(client: AdminAPI, node_index: int) -> Node:
    """
    Returns Node by node index

    Raises:
        logdevice.admin.exceptions.types.NodeNotReady: if node client is
            connected to is not ready yet to process request
        thrift.py3.TransportError: if there's network error while
            communicating with Thrift
        ldops.exceptions.NodeNotFoundError: if there's no such node from
            point of view of AdminAPI provider
    """
    resp: NodesConfigResponse = await admin_api.get_nodes_config(
        client=client, req=NodesFilter(node=NodeID(node_index=node_index))
    )
    if not resp.nodes:
        raise NodeNotFoundError(f"Node not found: node_index=`{node_index}'")

    # There's guarantee from AdminAPI that there CANNOT be more than one
    # node with the same node_index
    return _get_node_by_node_config(resp.nodes[0])


async def get_node_by_name(client: AdminAPI, name: str) -> Node:
    """
    Returns Node by node name

    Raises:
        logdevice.admin.exceptions.types.NodeNotReady: if node client is
            connected to is not ready yet to process request
        thrift.py3.TransportError: if there's network error while
            communicating with Thrift
        ldops.exceptions.NodeNotFoundError: if there's no such node from
            point of view of AdminAPI provider
    """
    resp: NodesConfigResponse = await admin_api.get_nodes_config(
        client=client, req=NodesFilter(node=NodeID(name=name))
    )
    if not resp.nodes:
        raise NodeNotFoundError(f"Node not found: name=`{name}'")

    # There's guarantee from AdminAPI that there CANNOT be more than one
    # node with the same name
    return _get_node_by_node_config(resp.nodes[0])


async def get_nodes_config(client: AdminAPI) -> Dict[Node, NodeConfig]:
    """
    Returns dict from Node to NodeConfig
    """
    resp: NodesConfigResponse = await admin_api.get_nodes_config(client)
    return {_get_node_by_node_config(nc): nc for nc in resp.nodes}


async def get_nodes_state(client: AdminAPI) -> Dict[Node, NodeState]:
    """
    Returns dict from Node to NodeState
    """
    nodes_config_resp: NodesConfigResponse
    nodes_state_resp: NodesStateResponse
    (nodes_config_resp, nodes_state_resp) = await asyncio.gather(
        admin_api.get_nodes_config(client), admin_api.get_nodes_state(client)
    )
    node_index_to_node: Dict[int, Node] = {
        nc.node_index: _get_node_by_node_config(nc) for nc in nodes_config_resp.nodes
    }
    return {node_index_to_node[ns.node_index]: ns for ns in nodes_state_resp.states}


async def get_cluster_view(client: AdminAPI) -> ClusterView:
    """
    Returns ClusterView object
    """
    (nodes_config_resp, nodes_state_resp, maintenances_resp) = await asyncio.gather(
        admin_api.get_nodes_config(client),
        admin_api.get_nodes_state(client),
        admin_api.get_maintenances(client),
        return_exceptions=True,
    )

    if isinstance(maintenances_resp, NotSupported):
        # This exception can be raised from cluster which does not support
        # MaintenanceManager yet
        maintenances = []
    elif isinstance(maintenances_resp, Exception):
        raise maintenances_resp
    else:
        maintenances = maintenances_resp.maintenances

    if isinstance(nodes_config_resp, Exception):
        raise nodes_config_resp

    if isinstance(nodes_state_resp, Exception):
        raise nodes_state_resp

    return ClusterView(
        nodes_config=nodes_config_resp.nodes,
        nodes_state=nodes_state_resp.states,
        maintenances=maintenances,
    )
