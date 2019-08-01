#!/usr/bin/env python3

# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


from datetime import datetime, timedelta
from unittest import TestCase

from ldops.testutil.async_test import async_test
from ldops.types.shard_maintenance_progress import ShardMaintenanceProgress
from logdevice.admin.nodes.types import (
    MaintenanceStatus,
    ShardMaintenanceProgress as ThriftShardMaintenanceProgress,
    ShardOperationalState,
)


class TestShardMaintenanceProgress(TestCase):
    @async_test
    async def test_smoke(self):
        created_at = datetime.now() - timedelta(hours=2)
        last_updated_at = datetime.now() - timedelta(minutes=5)
        maintenance_status = MaintenanceStatus.NOT_STARTED
        target_states = {
            ShardOperationalState.MAY_DISAPPEAR,
            ShardOperationalState.DRAINED,
        }
        associated_group_ids = ["johnsnow"]

        thrift_smp = ThriftShardMaintenanceProgress(
            status=maintenance_status,
            target_states=target_states,
            created_at=int(created_at.timestamp() * 1000),
            last_updated_at=int(last_updated_at.timestamp() * 1000),
            associated_group_ids=associated_group_ids,
        )
        smp = ShardMaintenanceProgress(
            status=maintenance_status,
            target_states=target_states,
            created_at=created_at,
            last_updated_at=last_updated_at,
            associated_group_ids=associated_group_ids,
        )
        self.assertEqual(thrift_smp, smp.to_thrift())
        self.assertEqual(thrift_smp.status, smp.status)
        self.assertEqual(thrift_smp.created_at // 1000, int(smp.created_at.timestamp()))
        self.assertEqual(
            thrift_smp.last_updated_at // 1000, int(smp.last_updated_at.timestamp())
        )

        from_thrift = ShardMaintenanceProgress.from_thrift(thrift_smp)
        self.assertEqual(smp.status, from_thrift.status)
        self.assertSetEqual(set(smp.target_states), set(from_thrift.target_states))
        self.assertEqual(smp.created_at, from_thrift.created_at)
        self.assertEqual(smp.last_updated_at, from_thrift.last_updated_at)
        self.assertSetEqual(
            set(smp.associated_group_ids), set(from_thrift.associated_group_ids)
        )
