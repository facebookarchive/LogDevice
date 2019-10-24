#!/usr/bin/env python3

# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


from datetime import datetime, timedelta
from typing import Dict
from unittest import TestCase

from ldops.cluster import get_cluster_view
from ldops.exceptions import NodeIsNotASequencerError
from ldops.maintenance import apply_maintenance
from ldops.testutil.async_test import async_test
from ldops.testutil.mock_admin_api import MockAdminAPI
from ldops.types.maintenance_overall_status import MaintenanceOverallStatus
from ldops.types.maintenance_view import MaintenanceView
from ldops.types.node_view import NodeView
from ldops.types.sequencer_maintenance_progress import SequencerMaintenanceProgress
from ldops.types.shard_maintenance_progress import ShardMaintenanceProgress
from logdevice.admin.common.types import ShardID
from logdevice.admin.maintenance.types import MaintenanceDefinition
from logdevice.admin.nodes.types import (
    MaintenanceStatus,
    SequencingState,
    ShardOperationalState,
)


class TestMaintenanceView(TestCase):
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
            cv = await get_cluster_view(client)

        self.validate(
            maintenance_view=list(cv.get_all_maintenance_views())[0],
            maintenance=list(cv.get_all_maintenances())[0],
            node_index_to_node_view={0: cv.get_node_view(node_index=0)},
        )

    @async_test
    async def test_shard_only(self):
        async with MockAdminAPI() as client:
            cv = await get_cluster_view(client)
            await apply_maintenance(
                client=client,
                shards=[
                    ShardID(
                        node=cv.get_node_view_by_node_index(0).node_id, shard_index=1
                    )
                ],
            )
            cv = await get_cluster_view(client)

        self.validate(
            maintenance_view=list(cv.get_all_maintenance_views())[0],
            maintenance=list(cv.get_all_maintenances())[0],
            node_index_to_node_view={0: cv.get_node_view(node_index=0)},
        )

    @async_test
    async def test_sequencer_only(self):
        async with MockAdminAPI() as client:
            cv = await get_cluster_view(client)
            await apply_maintenance(
                client=client,
                sequencer_nodes=[cv.get_node_view_by_node_index(0).node_id],
            )
            cv = await get_cluster_view(client)

        self.validate(
            maintenance_view=list(cv.get_all_maintenance_views())[0],
            maintenance=list(cv.get_all_maintenances())[0],
            node_index_to_node_view={0: cv.get_node_view(node_index=0)},
        )

    @async_test
    async def test_ttl(self):
        async with MockAdminAPI() as client:
            cv = await get_cluster_view(client)
            await apply_maintenance(
                client=client,
                sequencer_nodes=[cv.get_node_view_by_node_index(0).node_id],
                ttl=timedelta(days=2),
            )
            cv = await get_cluster_view(client)

        self.validate(
            maintenance_view=list(cv.get_all_maintenance_views())[0],
            maintenance=list(cv.get_all_maintenances())[0],
            node_index_to_node_view={0: cv.get_node_view(node_index=0)},
        )

    @async_test
    async def test_no_timestamps(self):
        async with MockAdminAPI() as client:
            cv = await get_cluster_view(client)
            await apply_maintenance(
                client=client,
                sequencer_nodes=[cv.get_node_view_by_node_index(0).node_id],
                ttl=timedelta(days=2),
            )
            cv = await get_cluster_view(client)
        mnt = list(cv.get_all_maintenances())[0]
        mnt = mnt(created_on=None, expires_on=None)
        mv = MaintenanceView(
            maintenance=mnt, node_index_to_node_view={0: cv.get_node_view(node_index=0)}
        )
        self.assertIsNone(mv.created_on)
        self.assertIsNone(mv.expires_on)
        self.assertIsNone(mv.expires_in)

    def validate(
        self,
        maintenance_view: MaintenanceView,
        maintenance: MaintenanceDefinition,
        node_index_to_node_view: Dict[int, NodeView],
    ) -> None:
        self.assertEqual(
            maintenance_view.allow_passive_drains, maintenance.allow_passive_drains
        )

        self.assertListEqual(
            sorted(
                ni.node_index for ni in maintenance_view.affected_sequencer_node_ids
            ),
            sorted(sn.node_index for sn in maintenance.sequencer_nodes),
        )

        self.assertTupleEqual(
            maintenance_view.affected_sequencer_node_indexes,
            tuple(sorted(sn.node_index for sn in maintenance.sequencer_nodes)),
        )

        self.assertListEqual(
            sorted(ni.node_index for ni in maintenance_view.affected_storage_node_ids),
            sorted({shard.node.node_index for shard in maintenance.shards}),
        )

        self.assertTupleEqual(
            maintenance_view.affected_storage_node_indexes,
            tuple(sorted({shard.node.node_index for shard in maintenance.shards})),
        )

        self.assertListEqual(
            sorted(ni.node_index for ni in maintenance_view.affected_node_ids),
            sorted(
                {sn.node_index for sn in maintenance.sequencer_nodes}.union(
                    {shard.node.node_index for shard in maintenance.shards}
                )
            ),
        )

        self.assertTupleEqual(
            maintenance_view.affected_node_indexes,
            tuple(
                sorted(
                    {sn.node_index for sn in maintenance.sequencer_nodes}.union(
                        {shard.node.node_index for shard in maintenance.shards}
                    )
                )
            ),
        )

        if len(maintenance.shards) == 0:
            self.assertIsNone(maintenance_view.shard_target_state)
        else:
            self.assertEqual(
                maintenance_view.shard_target_state, maintenance.shard_target_state
            )

        if len(maintenance.sequencer_nodes) == 0:
            self.assertIsNone(maintenance_view.sequencer_target_state)
        else:
            self.assertEqual(
                maintenance_view.sequencer_target_state,
                maintenance.sequencer_target_state,
            )

        if maintenance.ttl_seconds == 0:
            self.assertIsNone(maintenance_view.ttl)
        else:
            assert maintenance_view.ttl is not None
            self.assertEqual(
                maintenance.ttl_seconds,
                # pyre-fixme[16]: Optional type has no attribute `total_seconds`.
                maintenance_view.ttl.total_seconds(),
            )

        if maintenance.created_on is None:
            self.assertIsNone(maintenance_view.created_on)
        else:
            assert maintenance_view.created_on is not None
            self.assertAlmostEqual(
                # pyre-fixme[16]: Optional type has no attribute `timestamp`.
                maintenance_view.created_on.timestamp() * 1000,
                maintenance.created_on,
                1,
            )

        if maintenance.expires_on is None:
            self.assertIsNone(maintenance_view.expires_on)
        else:
            assert maintenance_view.expires_on is not None
            self.assertAlmostEqual(
                maintenance_view.expires_on.timestamp() * 1000,
                maintenance.expires_on,
                1,
            )

            assert maintenance_view.expires_in is not None
            self.assertAlmostEqual(
                maintenance_view.expires_in.total_seconds(),
                # pyre-fixme[16]: Optional type has no attribute `__sub__`.
                (maintenance_view.expires_on - datetime.now()).total_seconds(),
                1,
            )

        self.assertEqual(maintenance_view.affects_shards, len(maintenance.shards) > 0)
        self.assertEqual(
            maintenance_view.affects_sequencers, len(maintenance.sequencer_nodes) > 0
        )

        self.assertEqual(maintenance_view.num_shards_total, len(maintenance.shards))
        self.assertEqual(
            maintenance_view.num_sequencers_total, len(maintenance.sequencer_nodes)
        )

        for shard in maintenance.shards:
            assert shard.node.node_index is not None
            self.assertEqual(
                # pyre-fixme[6]: Expected `int` for 1st param but got `Optional[int]`.
                node_index_to_node_view[shard.node.node_index].shard_states[
                    shard.shard_index
                ],
                maintenance_view.get_shard_state(shard),
            )

        for sn in maintenance.sequencer_nodes:
            assert sn.node_index is not None
            self.assertEqual(
                # pyre-fixme[6]: Expected `int` for 1st param but got `Optional[int]`.
                node_index_to_node_view[sn.node_index].sequencer_state,
                maintenance_view.get_sequencer_state(sn),
            )

        self.assertSetEqual(set(maintenance.shards), set(maintenance_view.shards))

        for node_index in {shard.node.node_index for shard in maintenance.shards}:
            assert node_index is not None
            shards = {
                shard
                for shard in maintenance.shards
                if shard.node.node_index == node_index
            }
            self.assertSetEqual(
                shards, set(maintenance_view.get_shards_by_node_index(node_index))
            )

    @async_test
    async def test_shard_maintenance_status(self):
        ## MAY_DISAPPEAR maintenance
        async with MockAdminAPI() as client:
            cv = await get_cluster_view(client)
            shard = ShardID(
                node=cv.get_node_view_by_node_index(0).node_id, shard_index=1
            )
            await apply_maintenance(
                client=client,
                shards=[shard],
                shard_target_state=ShardOperationalState.MAY_DISAPPEAR,
            )
            cv = await get_cluster_view(client)

            # Just started
            mv = list(cv.get_all_maintenance_views())[0]
            self.assertEqual(
                mv.get_shard_maintenance_status(shard), MaintenanceStatus.NOT_STARTED
            )
            self.assertEqual(mv.num_shards_done, 0)
            self.assertFalse(mv.are_all_shards_done)
            self.assertFalse(mv.is_everything_done)
            self.assertFalse(mv.is_blocked)
            self.assertFalse(mv.is_completed)
            self.assertTrue(mv.is_in_progress)
            self.assertFalse(mv.is_internal)
            self.assertEqual(mv.overall_status, MaintenanceOverallStatus.IN_PROGRESS)

            # In progress
            client._set_shard_maintenance_progress(
                shard,
                ShardMaintenanceProgress(
                    status=MaintenanceStatus.STARTED,
                    target_states=[ShardOperationalState.MAY_DISAPPEAR],
                    created_at=datetime.now(),
                    last_updated_at=datetime.now(),
                    associated_group_ids=["johnsnow"],
                ).to_thrift(),
            )
            cv = await get_cluster_view(client)
            mv = list(cv.get_all_maintenance_views())[0]
            self.assertEqual(
                mv.get_shard_maintenance_status(shard), MaintenanceStatus.STARTED
            )
            self.assertEqual(mv.num_shards_done, 0)
            self.assertFalse(mv.are_all_shards_done)
            self.assertFalse(mv.is_everything_done)
            self.assertFalse(mv.is_blocked)
            self.assertFalse(mv.is_completed)
            self.assertTrue(mv.is_in_progress)
            self.assertFalse(mv.is_internal)
            self.assertEqual(mv.overall_status, MaintenanceOverallStatus.IN_PROGRESS)

            # Blocked
            client._set_shard_maintenance_progress(
                shard,
                ShardMaintenanceProgress(
                    status=MaintenanceStatus.BLOCKED_UNTIL_SAFE,
                    target_states=[ShardOperationalState.MAY_DISAPPEAR],
                    created_at=datetime.now(),
                    last_updated_at=datetime.now(),
                    associated_group_ids=["johnsnow"],
                ).to_thrift(),
            )
            cv = await get_cluster_view(client)
            mv = list(cv.get_all_maintenance_views())[0]
            self.assertTrue(mv.is_blocked)
            self.assertFalse(mv.are_all_shards_done)
            self.assertFalse(mv.is_everything_done)
            self.assertTrue(mv.is_blocked)
            self.assertFalse(mv.is_completed)
            self.assertFalse(mv.is_in_progress)
            self.assertEqual(mv.overall_status, MaintenanceOverallStatus.BLOCKED)

            # Done
            for sos in {
                ShardOperationalState.DRAINED,
                ShardOperationalState.MAY_DISAPPEAR,
                ShardOperationalState.MIGRATING_DATA,
                ShardOperationalState.PROVISIONING,
            }:
                client._set_shard_current_operational_state(shard, sos)
                cv = await get_cluster_view(client)
                mv = list(cv.get_all_maintenance_views())[0]
                self.assertEqual(
                    mv.get_shard_maintenance_status(shard), MaintenanceStatus.COMPLETED
                )
                self.assertEqual(mv.num_shards_done, 1)
                self.assertTrue(mv.are_all_shards_done)
                self.assertTrue(mv.is_everything_done)
                self.assertFalse(mv.is_blocked)
                self.assertTrue(mv.is_completed)
                self.assertFalse(mv.is_in_progress)
                self.assertEqual(mv.overall_status, MaintenanceOverallStatus.COMPLETED)

        ## DRAINED maintenance
        async with MockAdminAPI() as client:
            cv = await get_cluster_view(client)
            shard = ShardID(
                node=cv.get_node_view_by_node_index(0).node_id, shard_index=1
            )
            await apply_maintenance(
                client=client,
                shards=[shard],
                shard_target_state=ShardOperationalState.DRAINED,
            )
            cv = await get_cluster_view(client)

            # Just started
            mv = list(cv.get_all_maintenance_views())[0]
            self.assertEqual(
                mv.get_shard_maintenance_status(shard), MaintenanceStatus.NOT_STARTED
            )
            self.assertEqual(mv.num_shards_done, 0)
            self.assertFalse(mv.are_all_shards_done)

            # May disappear
            client._set_shard_current_operational_state(
                shard, ShardOperationalState.MAY_DISAPPEAR
            )
            cv = await get_cluster_view(client)
            mv = list(cv.get_all_maintenance_views())[0]
            self.assertEqual(
                mv.get_shard_maintenance_status(shard), MaintenanceStatus.NOT_STARTED
            )
            self.assertEqual(mv.num_shards_done, 0)
            self.assertFalse(mv.are_all_shards_done)

            # Done
            client._set_shard_current_operational_state(
                shard, ShardOperationalState.DRAINED
            )
            cv = await get_cluster_view(client)
            mv = list(cv.get_all_maintenance_views())[0]
            self.assertEqual(
                mv.get_shard_maintenance_status(shard), MaintenanceStatus.COMPLETED
            )
            self.assertEqual(mv.num_shards_done, 1)
            self.assertTrue(mv.are_all_shards_done)

    @async_test
    async def test_sequencer_maintenance_status(self):
        async with MockAdminAPI() as client:
            cv = await get_cluster_view(client)
            node_id = cv.get_node_view(node_index=0).node_id
            await apply_maintenance(client=client, sequencer_nodes=[node_id])

            # Just started
            cv = await get_cluster_view(client)
            mv = list(cv.get_all_maintenance_views())[0]
            self.assertEqual(
                mv.get_sequencer_maintenance_status(node_id),
                MaintenanceStatus.NOT_STARTED,
            )
            self.assertEqual(mv.num_sequencers_total, 1)
            self.assertEqual(mv.num_sequencers_done, 0)
            self.assertFalse(mv.are_all_sequencers_done)
            self.assertFalse(mv.is_everything_done)
            self.assertFalse(mv.is_blocked)
            self.assertFalse(mv.is_completed)
            self.assertTrue(mv.is_in_progress)
            self.assertEqual(mv.overall_status, MaintenanceOverallStatus.IN_PROGRESS)

            # In progress
            client._set_sequencer_maintenance_progress(
                node_id=node_id,
                maintenance_progress=SequencerMaintenanceProgress(
                    status=MaintenanceStatus.STARTED,
                    target_state=SequencingState.UNKNOWN,
                    created_at=datetime.now(),
                    last_updated_at=datetime.now(),
                    associated_group_ids=["johnsnow"],
                ).to_thrift(),
            )
            cv = await get_cluster_view(client)
            mv = list(cv.get_all_maintenance_views())[0]
            self.assertEqual(
                mv.get_sequencer_maintenance_status(node_id), MaintenanceStatus.STARTED
            )
            self.assertEqual(mv.num_sequencers_done, 0)
            self.assertFalse(mv.are_all_sequencers_done)
            self.assertFalse(mv.is_everything_done)
            self.assertFalse(mv.is_blocked)
            self.assertFalse(mv.is_completed)
            self.assertTrue(mv.is_in_progress)
            self.assertEqual(mv.overall_status, MaintenanceOverallStatus.IN_PROGRESS)

            # Blocked
            client._set_sequencer_maintenance_progress(
                node_id=node_id,
                maintenance_progress=SequencerMaintenanceProgress(
                    status=MaintenanceStatus.BLOCKED_UNTIL_SAFE,
                    target_state=SequencingState.UNKNOWN,
                    created_at=datetime.now(),
                    last_updated_at=datetime.now(),
                    associated_group_ids=["johnsnow"],
                ).to_thrift(),
            )
            cv = await get_cluster_view(client)
            mv = list(cv.get_all_maintenance_views())[0]
            self.assertEqual(
                mv.get_sequencer_maintenance_status(node_id),
                MaintenanceStatus.BLOCKED_UNTIL_SAFE,
            )
            self.assertEqual(mv.num_sequencers_done, 0)
            self.assertFalse(mv.are_all_sequencers_done)
            self.assertFalse(mv.is_everything_done)
            self.assertTrue(mv.is_blocked)
            self.assertFalse(mv.is_completed)
            self.assertFalse(mv.is_in_progress)
            self.assertEqual(mv.overall_status, MaintenanceOverallStatus.BLOCKED)

            # Done
            client._set_sequencing_state(node_id, SequencingState.DISABLED)
            cv = await get_cluster_view(client)
            mv = list(cv.get_all_maintenance_views())[0]
            self.assertEqual(
                mv.get_sequencer_maintenance_status(node_id),
                MaintenanceStatus.COMPLETED,
            )
            self.assertEqual(mv.overall_status, MaintenanceOverallStatus.COMPLETED)

    @async_test
    async def test_node_is_not_a_sequencer(self):
        async with MockAdminAPI(disaggregated=True) as client:
            cv = await get_cluster_view(client)
            shard = ShardID(
                node=cv.get_node_view_by_node_index(0).node_id, shard_index=1
            )
            await apply_maintenance(
                client=client,
                shards=[shard],
                shard_target_state=ShardOperationalState.DRAINED,
            )
            cv = await get_cluster_view(client)

        mv = list(cv.get_all_maintenance_views())[0]
        node_id = cv.get_node_view_by_node_index(0).node_id
        with self.assertRaises(NodeIsNotASequencerError):
            mv.get_sequencer_maintenance_status(node_id)

        with self.assertRaises(NodeIsNotASequencerError):
            mv.get_sequencer_last_updated_at(node_id)

    @async_test
    async def test_get_shard_last_updated_at(self):
        async with MockAdminAPI() as client:
            cv = await get_cluster_view(client)
            shard = ShardID(
                node=cv.get_node_view_by_node_index(0).node_id, shard_index=1
            )
            await apply_maintenance(
                client=client,
                shards=[shard],
                shard_target_state=ShardOperationalState.DRAINED,
            )
            cv = await get_cluster_view(client)
            mv = list(cv.get_all_maintenance_views())[0]
            self.assertIsNone(mv.get_shard_last_updated_at(shard))

            ts = datetime.now()
            client._set_shard_maintenance_progress(
                shard,
                ShardMaintenanceProgress(
                    status=MaintenanceStatus.STARTED,
                    target_states=[ShardOperationalState.MAY_DISAPPEAR],
                    created_at=ts,
                    last_updated_at=datetime.now(),
                    associated_group_ids=["johnsnow"],
                ).to_thrift(),
            )
            cv = await get_cluster_view(client)
            mv = list(cv.get_all_maintenance_views())[0]
            self.assertTrue(
                mv.get_shard_last_updated_at(shard) - ts < timedelta(seconds=2)
            )

    @async_test
    async def test_get_sequencer_last_updated_at(self):
        async with MockAdminAPI() as client:
            cv = await get_cluster_view(client)
            node_id = cv.get_node_view(node_index=0).node_id
            await apply_maintenance(client=client, sequencer_nodes=[node_id])

            cv = await get_cluster_view(client)
            mv = list(cv.get_all_maintenance_views())[0]
            self.assertIsNone(mv.get_sequencer_last_updated_at(node_id))

            ts = datetime.now()
            client._set_sequencer_maintenance_progress(
                node_id=node_id,
                maintenance_progress=SequencerMaintenanceProgress(
                    status=MaintenanceStatus.STARTED,
                    target_state=SequencingState.UNKNOWN,
                    created_at=datetime.now(),
                    last_updated_at=datetime.now(),
                    associated_group_ids=["johnsnow"],
                ).to_thrift(),
            )
            cv = await get_cluster_view(client)
            mv = list(cv.get_all_maintenance_views())[0]
            self.assertTrue(
                mv.get_sequencer_last_updated_at(node_id) - ts < timedelta(seconds=2)
            )
