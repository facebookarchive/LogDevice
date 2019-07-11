#!/usr/bin/env python3

# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import asyncio
import operator
from collections import Counter
from typing import Tuple
from unittest import TestCase

from ldops.testutil.async_test import async_test
from ldops.testutil.mock_admin_api import MockAdminAPI, gen_SocketAddress
from ldops.types.node_view import NodeView
from ldops.types.socket_address import SocketAddress
from logdevice.admin.common.types import NodeID, Role, SocketAddressFamily
from logdevice.admin.maintenance.types import MaintenanceDefinition, MaintenancesFilter
from logdevice.admin.nodes.types import (
    NodeConfig,
    NodesFilter,
    NodesStateRequest,
    NodeState,
)


class TestNodeView(TestCase):
    @async_test
    async def test_smoke(self):
        ni = 0
        async with MockAdminAPI() as client:
            nodes_config_resp = await client.getNodesConfig(
                NodesFilter(node=NodeID(node_index=ni))
            )
            nodes_state_resp = await client.getNodesState(
                NodesStateRequest(filter=NodesFilter(node=NodeID(node_index=ni)))
            )
            maintenances_resp = await client.getMaintenances(MaintenancesFilter())

        nc = nodes_config_resp.nodes[0]
        ns = nodes_state_resp.states[0]
        mnt_ids = set()
        for mnt in maintenances_resp.maintenances:
            for s in mnt.shards:
                if s.node.node_index == ni:
                    mnt_ids.add(mnt.group_id)
            for n in mnt.sequencer_nodes:
                if n.node_index == ni:
                    mnt_ids.add(mnt.group_id)
        mnts = tuple(
            sorted(
                (
                    mnt
                    for mnt in maintenances_resp.maintenances
                    if mnt.group_id in mnt_ids
                ),
                key=operator.attrgetter("group_id"),
            )
        )

        nv = NodeView(node_config=nc, node_state=ns, maintenances=mnts)

        self._validate(nv, nc, ns, mnts)

    def _validate(
        self,
        nv: NodeView,
        nc: NodeConfig,
        ns: NodeState,
        mnts: Tuple[MaintenanceDefinition, ...],
    ):
        self.assertEqual(nv.node_config, nc)

        self.assertEqual(nv.node_state, ns)

        self.assertEqual(nv.maintenances, mnts)

        self.assertEqual(nv.node_index, nc.node_index)

        if nc.name:
            self.assertEqual(nv.node_name, nc.name)
        else:
            self.assertEqual(
                nv.node_name, str(SocketAddress.from_thrift(nc.data_address))
            )

        self.assertEqual(nv.data_address, SocketAddress.from_thrift(nc.data_address))

        if nv.thrift_address.address_family == SocketAddressFamily.INET:
            assert nv.thrift_address.address is not None

            from_nc = SocketAddress.from_thrift(nc.data_address)
            assert from_nc.address is not None

            self.assertEqual(nv.thrift_address.port, 6440)
            self.assertEqual(
                nv.thrift_address.address.compressed, from_nc.address.compressed
            )

        self.assertEqual(
            nv.node_id,
            NodeID(node_index=nc.node_index, address=nc.data_address, name=nc.name),
        )

        self.assertEqual(nv.location, nc.location)

        self.assertEqual(nv.location_per_scope, nc.location_per_scope)

        self.assertEqual(nv.roles, nc.roles)

        for r in Role:
            self.assertEqual(nv.has_role(r), r in nc.roles)

        self.assertEqual(nv.is_sequencer, Role.SEQUENCER in nc.roles)
        self.assertEqual(nv.is_storage, Role.STORAGE in nc.roles)
        self.assertEqual(nv.daemon_state, ns.daemon_state)

        if Role.SEQUENCER in nc.roles:
            assert nc.sequencer is not None
            self.assertEqual(nv.sequencer_config, nc.sequencer)
            self.assertEqual(nv.sequencer_weight, nc.sequencer.weight)

            assert ns.sequencer_state is not None
            self.assertEqual(nv.sequencer_state, ns.sequencer_state)
            self.assertEqual(nv.sequencing_state, ns.sequencer_state.state)
        else:
            self.assertIsNone(nv.sequencer_config)
            self.assertIsNone(nv.sequencer_state)
            self.assertIsNone(nv.sequencer_weight)
            self.assertIsNone(nv.sequencing_state)

        if Role.STORAGE in nc.roles:
            assert nc.storage is not None
            assert ns.shard_states is not None
            self.assertEqual(nv.storage_config, nc.storage)
            self.assertEqual(nv.storage_weight, nc.storage.weight)
            self.assertEqual(nv.num_shards, nc.storage.num_shards)
            self.assertEqual(nv.shard_states, ns.shard_states)

            self.assertListEqual(
                nv.shards_data_health, [s.data_health for s in ns.shard_states]
            )
            self.assertEqual(
                nv.shards_data_health_count,
                Counter(s.data_health for s in ns.shard_states),
            )

            self.assertListEqual(
                nv.shards_current_storage_state,
                [s.current_storage_state for s in ns.shard_states],
            )

            self.assertEqual(
                nv.shards_current_storage_state_count,
                Counter(s.current_storage_state for s in ns.shard_states),
            )

            self.assertListEqual(
                nv.shards_current_operational_state,
                [s.current_operational_state for s in ns.shard_states],
            )

            self.assertEqual(
                nv.shards_current_operational_state_count,
                Counter(s.current_operational_state for s in ns.shard_states),
            )

            self.assertListEqual(
                nv.shards_membership_storage_state,
                [s.storage_state for s in ns.shard_states],
            )

            self.assertEqual(
                nv.shards_membership_storage_state_count,
                Counter(s.storage_state for s in ns.shard_states),
            )

            self.assertListEqual(
                nv.shards_metadata_state, [s.metadata_state for s in ns.shard_states]
            )

            self.assertEqual(
                nv.shards_metadata_state_count,
                Counter(s.metadata_state for s in ns.shard_states),
            )
        else:
            self.assertIsNone(nv.storage_config)
            self.assertIsNone(nv.storage_weight)
            self.assertIsNone(nv.num_shards)
            self.assertEqual(nv.shard_states, [])

    @async_test
    async def test_mismatch(self):
        async with MockAdminAPI() as client:
            (
                nodes_config_resp,
                nodes_state_resp,
                maintenances_resp,
            ) = await asyncio.gather(
                client.getNodesConfig(NodesFilter(node=NodeID(node_index=0))),
                client.getNodesState(
                    NodesStateRequest(filter=NodesFilter(node=NodeID(node_index=1)))
                ),
                client.getMaintenances(MaintenancesFilter()),
            )
        with self.assertRaises(ValueError):
            NodeView(
                node_config=nodes_config_resp.nodes[0],
                node_state=nodes_state_resp.states[0],
                maintenances=maintenances_resp.maintenances,
            )

    def test_no_name(self):
        addr = gen_SocketAddress()
        self.assertEqual(
            NodeView(
                node_config=NodeConfig(
                    node_index=0,
                    data_address=addr,
                    roles=set(),
                    location_per_scope={},
                    name="",
                ),
                node_state=NodeState(node_index=0),
                maintenances=[],
            ).node_name,
            str(SocketAddress.from_thrift(addr)),
        )

    def test_unix_socket(self):
        nc = NodeConfig(
            node_index=0,
            data_address=SocketAddress(
                address_family=SocketAddressFamily.UNIX, path="/path/to/unix.sock"
            ).to_thrift(),
            roles=set(),
            location_per_scope={},
            name="",
        )
        ns = NodeState(node_index=0)
        mnts = []
        self._validate(NodeView(nc, ns, mnts), nc, ns, mnts)

    def test_empty(self):
        nc = NodeConfig(
            node_index=0,
            data_address=gen_SocketAddress(),
            roles=set(),
            location_per_scope={},
            name="",
        )
        ns = NodeState(node_index=0)
        mnts = []
        self._validate(NodeView(nc, ns, mnts), nc, ns, mnts)
