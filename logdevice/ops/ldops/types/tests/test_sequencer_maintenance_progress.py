#!/usr/bin/env python3

# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


from datetime import datetime, timedelta
from unittest import TestCase

from ldops.testutil.async_test import async_test
from ldops.types.sequencer_maintenance_progress import (
    _MILLISECONDS_IN_SECOND,
    SequencerMaintenanceProgress,
)
from logdevice.admin.nodes.types import (
    MaintenanceStatus,
    SequencerMaintenanceProgress as ThriftSequencerMaintenanceProgress,
    SequencingState,
)


class TestSequencerMaintenanceProgress(TestCase):
    @async_test
    async def test_smoke(self):
        created_at = datetime.now() - timedelta(hours=2)
        last_updated_at = datetime.now() - timedelta(minutes=5)
        maintenance_status = MaintenanceStatus.NOT_STARTED
        target_state = SequencingState.DISABLED
        associated_group_ids = ["johnsnow"]

        thrift_smp = ThriftSequencerMaintenanceProgress(
            status=maintenance_status,
            target_state=target_state,
            created_at=int(created_at.timestamp() * _MILLISECONDS_IN_SECOND),
            last_updated_at=int(last_updated_at.timestamp() * _MILLISECONDS_IN_SECOND),
            associated_group_ids=associated_group_ids,
        )
        smp = SequencerMaintenanceProgress(
            status=maintenance_status,
            target_state=target_state,
            created_at=created_at,
            last_updated_at=last_updated_at,
            associated_group_ids=associated_group_ids,
        )
        self.assertEqual(thrift_smp, smp.to_thrift())
        self.assertEqual(thrift_smp.status, smp.status)
        self.assertEqual(
            thrift_smp.created_at // _MILLISECONDS_IN_SECOND,
            int(smp.created_at.timestamp()),
        )
        self.assertEqual(
            thrift_smp.last_updated_at // _MILLISECONDS_IN_SECOND,
            int(smp.last_updated_at.timestamp()),
        )

        from_thrift = SequencerMaintenanceProgress.from_thrift(thrift_smp)
        self.assertEqual(smp.status, from_thrift.status)
        self.assertEqual(smp.target_state, from_thrift.target_state)
        self.assertEqual(smp.created_at, from_thrift.created_at)
        self.assertEqual(smp.last_updated_at, from_thrift.last_updated_at)
        self.assertEqual(smp.associated_group_ids, from_thrift.associated_group_ids)
