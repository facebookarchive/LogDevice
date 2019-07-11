#!/usr/bin/env python3

# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import asyncio
import operator
from typing import Dict, List, Tuple
from unittest import TestCase

from ldops.cluster import get_cluster_view
from ldops.maintenance import apply_maintenance
from ldops.testutil.async_test import async_test
from ldops.testutil.mock_admin_api import MockAdminAPI, gen_word
from ldops.types.cluster_view import ClusterView
from ldops.types.node import Node
from ldops.types.node_view import NodeView
from logdevice.admin.common.types import ShardID
from logdevice.admin.maintenance.types import MaintenanceDefinition, MaintenancesFilter
from logdevice.admin.nodes.types import (
    NodeConfig,
    NodesFilter,
    NodesStateRequest,
    NodeState,
)


class TestClusterView(TestCase):
    @async_test
    async def test_smoke(self):
        async with MockAdminAPI() as client:
            cv = await get_cluster_view(client)
            await apply_maintenance(
                client=client,
                shards=[
                    ShardID(
                        node=cv.get_node_view_by_node_index(0).node_id, shard_index=1
                    )
                ],
                sequencer_nodes=[cv.get_node_view_by_node_index(0).node_id],
            )
            (cv, nc_resp, ns_resp, mnts_resp) = await asyncio.gather(
                get_cluster_view(client),
                client.getNodesConfig(NodesFilter()),
                client.getNodesState(NodesStateRequest()),
                client.getMaintenances(MaintenancesFilter()),
            )
        self._validate(cv, nc_resp.nodes, ns_resp.states, mnts_resp.maintenances)

    def _validate(
        self,
        cv: ClusterView,
        ncs: List[NodeConfig],
        nss: List[NodeState],
        mnts: Tuple[MaintenanceDefinition, ...],
    ):
        nis = sorted(nc.node_index for nc in ncs)
        ni_to_nc = {nc.node_index: nc for nc in ncs}
        ni_to_ns = {ns.node_index: ns for ns in nss}
        ni_to_mnts: Dict[int, List[MaintenanceDefinition]] = {ni: [] for ni in nis}
        for mnt in mnts:
            mnt_nis = set()
            for s in mnt.shards:
                assert s.node.node_index is not None
                mnt_nis.add(s.node.node_index)
            for n in mnt.sequencer_nodes:
                assert n.node_index is not None
                mnt_nis.add(n.node_index)
            for ni in mnt_nis:
                ni_to_mnts[ni].append(mnt)

        self.assertEqual(sorted(cv.get_all_node_indexes()), sorted(ni_to_nc.keys()))

        self.assertEqual(
            sorted(cv.get_all_node_views(), key=operator.attrgetter("node_index")),
            sorted(
                (
                    NodeView(
                        node_config=ni_to_nc[ni],
                        node_state=ni_to_ns[ni],
                        maintenances=tuple(ni_to_mnts[ni]),
                    )
                    for ni in ni_to_nc.keys()
                ),
                key=operator.attrgetter("node_index"),
            ),
        )

        self.assertEqual(sorted(cv.get_all_node_names()), sorted(nc.name for nc in ncs))

        self.assertEqual(
            sorted(cv.get_all_maintenance_ids()), sorted(mnt.group_id for mnt in mnts)
        )

        self.assertEqual(
            sorted(cv.get_all_maintenances(), key=operator.attrgetter("group_id")),
            sorted(mnts, key=operator.attrgetter("group_id")),
        )

        for ni in nis:
            nn = ni_to_nc[ni].name
            nc = ni_to_nc[ni]
            ns = ni_to_ns[ni]
            mnts = tuple(ni_to_mnts[ni])
            nv = NodeView(
                node_config=ni_to_nc[ni], node_state=ni_to_ns[ni], maintenances=mnts
            )

            self.assertEqual(cv.get_node_view_by_node_index(ni), nv)
            self.assertEqual(cv.get_node_name_by_node_index(ni), nn)
            self.assertEqual(cv.get_node_config_by_node_index(ni), nc)
            self.assertEqual(cv.get_node_state_by_node_index(ni), ns)
            self.assertEqual(cv.get_maintenances_by_node_index(ni), mnts)

            self.assertEqual(cv.get_node_view_by_node_name(nn), nv)
            self.assertEqual(cv.get_node_index_by_node_name(nn), ni)
            self.assertEqual(cv.get_node_config_by_node_name(nn), nc)
            self.assertEqual(cv.get_node_state_by_node_name(nn), ns)
            self.assertEqual(cv.get_maintenances_by_node_name(nn), mnts)

            self.assertEqual(cv.get_node_view(node_name=nn), nv)
            self.assertEqual(cv.get_node_index(node_name=nn), ni)
            self.assertEqual(cv.get_node_config(node_name=nn), nc)
            self.assertEqual(cv.get_node_state(node_name=nn), ns)
            self.assertEqual(cv.get_maintenances(node_name=nn), mnts)

            self.assertEqual(cv.get_node_view(node_index=ni), nv)
            self.assertEqual(cv.get_node_name(node_index=ni), nn)
            self.assertEqual(cv.get_node_config(node_index=ni), nc)
            self.assertEqual(cv.get_node_state(node_index=ni), ns)
            self.assertEqual(cv.get_maintenances(node_index=ni), mnts)

        with self.assertRaises(ValueError):
            cv.get_node_view(None, None)

        with self.assertRaises(ValueError):
            cv.get_node_config(None, None)

        with self.assertRaises(ValueError):
            cv.get_node_state(None, None)

        with self.assertRaises(ValueError):
            cv.get_maintenances(None, None)

        # mismatch node_index and node_name
        if len(nis) > 1:
            nn = ni_to_nc[nis[0]].name
            ni = nis[1]
            with self.assertRaises(ValueError):
                cv.get_node_view(ni, nn)

            with self.assertRaises(ValueError):
                cv.get_node_config(ni, nn)

            with self.assertRaises(ValueError):
                cv.get_node_state(ni, nn)

            with self.assertRaises(ValueError):
                cv.get_maintenances(ni, nn)

        # non-existent node_index
        with self.assertRaises(KeyError):
            cv.get_node_view(node_index=max(nis) + 1)

        # non-existent node_name
        with self.assertRaises(KeyError):
            nns = {nc.name for nc in ncs}
            while True:
                nn = gen_word()
                if nn not in nns:
                    break
            cv.get_node_view(node_name=nn)

        for mnt in mnts:
            assert mnt.group_id is not None
            self.assertEqual(cv.get_maintenance_by_id(mnt.group_id), mnt)
