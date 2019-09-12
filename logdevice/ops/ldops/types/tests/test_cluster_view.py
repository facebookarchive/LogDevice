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
from ldops.const import ALL_SHARDS
from ldops.exceptions import NodeNotFoundError
from ldops.maintenance import apply_maintenance
from ldops.testutil.async_test import async_test
from ldops.testutil.mock_admin_api import MockAdminAPI, gen_word
from ldops.types.cluster_view import ClusterView
from ldops.types.node_view import NodeView
from logdevice.admin.common.types import NodeID, ShardID
from logdevice.admin.maintenance.types import (
    MaintenanceDefinition,
    MaintenanceProgress,
    MaintenancesFilter,
)
from logdevice.admin.nodes.types import (
    NodeConfig,
    NodesFilter,
    NodesStateRequest,
    NodeState,
    SequencingState,
    ShardOperationalState,
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
            await apply_maintenance(
                client=client,
                node_ids=[cv.get_node_id(node_index=1)],
                user="hello",
                reason="whatever",
            )
            (cv, nc_resp, ns_resp, mnts_resp) = await asyncio.gather(
                get_cluster_view(client),
                client.getNodesConfig(NodesFilter()),
                client.getNodesState(NodesStateRequest()),
                client.getMaintenances(MaintenancesFilter()),
            )
        self._validate(cv, nc_resp.nodes, ns_resp.states, tuple(mnts_resp.maintenances))

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
            node_mnts = tuple(ni_to_mnts[ni])
            nv = NodeView(
                node_config=ni_to_nc[ni],
                node_state=ni_to_ns[ni],
                maintenances=node_mnts,
            )

            self.assertEqual(cv.get_node_view_by_node_index(ni), nv)
            self.assertEqual(cv.get_node_name_by_node_index(ni), nn)
            self.assertEqual(cv.get_node_config_by_node_index(ni), nc)
            self.assertEqual(cv.get_node_state_by_node_index(ni), ns)
            self.assertEqual(cv.get_node_maintenances_by_node_index(ni), node_mnts)

            self.assertEqual(cv.get_node_view_by_node_name(nn), nv)
            self.assertEqual(cv.get_node_index_by_node_name(nn), ni)
            self.assertEqual(cv.get_node_config_by_node_name(nn), nc)
            self.assertEqual(cv.get_node_state_by_node_name(nn), ns)
            self.assertEqual(cv.get_node_maintenances_by_node_name(nn), node_mnts)

            self.assertEqual(cv.get_node_view(node_name=nn), nv)
            self.assertEqual(cv.get_node_index(node_name=nn), ni)
            self.assertEqual(cv.get_node_config(node_name=nn), nc)
            self.assertEqual(cv.get_node_state(node_name=nn), ns)
            self.assertEqual(cv.get_node_maintenances(node_name=nn), node_mnts)

            self.assertEqual(cv.get_node_view(node_index=ni), nv)
            self.assertEqual(cv.get_node_name(node_index=ni), nn)
            self.assertEqual(cv.get_node_config(node_index=ni), nc)
            self.assertEqual(cv.get_node_state(node_index=ni), ns)
            self.assertEqual(cv.get_node_maintenances(node_index=ni), node_mnts)

        with self.assertRaises(ValueError):
            cv.get_node_view(None, None)

        with self.assertRaises(ValueError):
            cv.get_node_config(None, None)

        with self.assertRaises(ValueError):
            cv.get_node_state(None, None)

        with self.assertRaises(ValueError):
            cv.get_node_maintenances(None, None)

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
                cv.get_node_maintenances(ni, nn)

        # non-existent node_index
        with self.assertRaises(NodeNotFoundError):
            cv.get_node_view(node_index=max(nis) + 1)

        # non-existent node_name
        with self.assertRaises(NodeNotFoundError):
            nns = {nc.name for nc in ncs}
            while True:
                nn = gen_word()
                if nn not in nns:
                    break
            cv.get_node_view(node_name=nn)

        for mnt in mnts:
            assert mnt.group_id is not None
            self.assertEqual(cv.get_maintenance_by_id(mnt.group_id), mnt)
            self.assertTupleEqual(
                cv.get_node_indexes_by_maintenance_id(mnt.group_id),
                tuple(
                    sorted(
                        set(
                            {
                                n.node_index
                                for n in mnt.sequencer_nodes
                                if n.node_index is not None
                            }
                        ).union(
                            {
                                s.node.node_index
                                for s in mnt.shards
                                if s.node.node_index is not None
                            }
                        )
                    )
                ),
            )
            self.assertEqual(
                mnt.group_id, cv.get_maintenance_view_by_id(mnt.group_id).group_id
            )

        self.assertListEqual(
            list(sorted(m.group_id for m in mnts)),
            list(sorted(mv.group_id for mv in cv.get_all_maintenance_views())),
        )

        # expand_shards
        self.assertEqual(
            cv.expand_shards(
                shards=[ShardID(node=NodeID(node_index=nis[0]), shard_index=0)]
            ),
            (
                ShardID(
                    node=NodeID(
                        node_index=ni_to_nc[nis[0]].node_index,
                        name=ni_to_nc[nis[0]].name,
                        address=ni_to_nc[nis[0]].data_address,
                    ),
                    shard_index=0,
                ),
            ),
        )
        self.assertEqual(
            len(
                cv.expand_shards(
                    shards=[
                        ShardID(node=NodeID(node_index=nis[0]), shard_index=ALL_SHARDS)
                    ]
                )
            ),
            ni_to_nc[nis[0]].storage.num_shards,
        )
        self.assertEqual(
            len(
                cv.expand_shards(
                    shards=[
                        ShardID(node=NodeID(node_index=nis[0]), shard_index=ALL_SHARDS),
                        ShardID(node=NodeID(node_index=nis[0]), shard_index=ALL_SHARDS),
                        ShardID(node=NodeID(node_index=nis[1]), shard_index=ALL_SHARDS),
                    ]
                )
            ),
            ni_to_nc[nis[0]].storage.num_shards + ni_to_nc[nis[1]].storage.num_shards,
        )
        self.assertEqual(
            len(
                cv.expand_shards(
                    shards=[
                        ShardID(node=NodeID(node_index=nis[0]), shard_index=ALL_SHARDS),
                        ShardID(node=NodeID(node_index=nis[1]), shard_index=0),
                    ],
                    node_ids=[NodeID(node_index=0)],
                )
            ),
            ni_to_nc[nis[0]].storage.num_shards + 1,
        )

        # normalize_node_id
        self.assertEqual(
            cv.normalize_node_id(NodeID(node_index=nis[0])),
            NodeID(
                node_index=nis[0],
                address=ni_to_nc[nis[0]].data_address,
                name=ni_to_nc[nis[0]].name,
            ),
        )
        self.assertEqual(
            cv.normalize_node_id(NodeID(name=ni_to_nc[nis[0]].name)),
            NodeID(
                node_index=nis[0],
                address=ni_to_nc[nis[0]].data_address,
                name=ni_to_nc[nis[0]].name,
            ),
        )

        # search_maintenances
        self.assertEqual(len(cv.search_maintenances()), len(mnts))
        self.assertEqual(
            len(cv.search_maintenances(node_ids=[cv.get_node_id(node_index=3)])), 0
        )
        self.assertEqual(
            len(cv.search_maintenances(node_ids=[cv.get_node_id(node_index=1)])), 1
        )
        self.assertEqual(
            len(
                cv.search_maintenances(
                    shards=[ShardID(node=cv.get_node_id(node_index=0), shard_index=1)]
                )
            ),
            1,
        )

        # shard_target_state
        self.assertEqual(
            len(
                cv.search_maintenances(
                    shard_target_state=ShardOperationalState.MAY_DISAPPEAR
                )
            ),
            2,
        )
        self.assertEqual(
            len(
                cv.search_maintenances(shard_target_state=ShardOperationalState.DRAINED)
            ),
            0,
        )

        # sequencer_target_state
        self.assertEqual(
            len(cv.search_maintenances(sequencer_target_state=SequencingState.ENABLED)),
            0,
        )
        self.assertEqual(
            len(
                cv.search_maintenances(sequencer_target_state=SequencingState.DISABLED)
            ),
            2,
        )

        self.assertEqual(len(cv.search_maintenances(user="hello")), 1)
        self.assertEqual(len(cv.search_maintenances(reason="whatever")), 1)

        self.assertEqual(len(cv.search_maintenances(skip_safety_checks=True)), 0)
        self.assertEqual(len(cv.search_maintenances(skip_safety_checks=False)), 2)

        self.assertEqual(len(cv.search_maintenances(force_restore_rebuilding=True)), 0)
        self.assertEqual(len(cv.search_maintenances(force_restore_rebuilding=False)), 2)

        self.assertEqual(len(cv.search_maintenances(allow_passive_drains=True)), 0)
        self.assertEqual(len(cv.search_maintenances(allow_passive_drains=False)), 2)

        self.assertEqual(len(cv.search_maintenances(group_id=mnts[0].group_id)), 1)

        self.assertEqual(
            len(cv.search_maintenances(progress=MaintenanceProgress.IN_PROGRESS)), 2
        )
