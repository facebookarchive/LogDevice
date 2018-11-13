# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.
from __future__ import absolute_import, division, print_function, unicode_literals

import random
from datetime import datetime
from time import time as now
from unittest import TestCase

import logdevice.client as ld
import logdevice.integration_test_util
import logdevice.settings as settings


class LogDeviceIntegrationTest(TestCase):
    "Tests that require a local LogDevice cluster"

    cluster = None

    def setUp(self):
        assert not self.cluster
        factory = logdevice.integration_test_util.ClusterFactory()
        factory.set_rocks_db_type(
            logdevice.integration_test_util.RocksDBType.PARTITIONED
        )
        self.cluster = factory.create(3)

    def tearDown(self):
        assert self.cluster
        self.cluster.stop()
        self.cluster = None

    def client(self):
        assert self.cluster
        client = self.cluster.create_client()
        # 10 seconds should be more than enough for a local test
        client.set_timeout(10)
        return client

    def test_basic_read_write(self):
        """ Send strings, bytes, Unicode and expect bytes """
        client = self.client()
        logid = 1

        raw_messages = [b"you say", b"goodbye", b"i say", b"hello"]
        messages = ["you say", "goodbye", b"i say", "hello"]

        for body in messages:
            client.append(logid, body)

        start_lsn = client.find_time(logid, now() - 10)
        print("LSN for ten seconds ago is %d" % start_lsn)

        reader = client.create_reader(1)
        reader.start_reading(logid, start_lsn)

        records = []
        for data, gap in reader:
            if gap is not None:
                print(
                    "gap: log {} type {} lo {} hi {}".format(
                        gap.logid, gap.type, gap.lo, gap.hi
                    )
                )
                self.assertEqual(gap.type, ld.GapType.BRIDGE)
            else:
                print(
                    "data: log %d LSN %ld timestamp %s payload: %r"
                    % (
                        data.logid,
                        data.lsn,
                        datetime.fromtimestamp(data.timestamp),
                        data.payload,
                    )
                )
                records.append(data.payload)
                if len(records) >= 4:
                    reader.stop_iteration()

        reader.stop_reading(logid)
        print("timeout while reading records, exiting")

        self.assertEqual(raw_messages, records)

    def test_client_create(self):
        name = "cluster_name?"
        config = "file:" + self.cluster.config_path

        client = ld.Client(name, config)
        self.assertIsNotNone(client)
        client = ld.Client(name, config, timeout=3)
        self.assertIsNotNone(client)
        settings = {"nagle": False}
        client = ld.Client(name, config, settings=settings)
        self.assertIsNotNone(client)
        client = ld.Client(name, config, 3, settings)
        self.assertIsNotNone(client)

    def test_iterator_stops_at_end(self):
        """The iterator should stop after reaching until-LSN for all logs."""
        NWRITES = 100
        LOGS = [1, 2]
        client = self.client()

        # Keep track of the last written LSN for each log
        last_written_lsn = {logid: logdevice.client.LSN_OLDEST for logid in LOGS}

        rng = random.Random(0xBABABA)
        for _ in range(NWRITES):
            logid = rng.choice(LOGS)
            last_written_lsn[logid] = client.append(logid, "hello")

        reader = client.create_reader(len(LOGS))
        for logid, until_lsn in last_written_lsn.items():
            # Read from LSN_OLDEST to the last written LSN
            reader.start_reading(logid, logdevice.client.LSN_OLDEST, until_lsn)

        nread = 0
        for data, _ in reader:
            if data is not None:
                nread += 1
        self.assertEqual(NWRITES, nread)

    def test_is_log_empty(self):
        client = self.client()
        client.append(1, "test")
        self.assertFalse(client.is_log_empty(1))
        self.assertTrue(client.is_log_empty(2))
        with self.assertRaises(ld.LogDeviceError):
            client.is_log_empty(1000)

    def test_get_max_payload_size(self):
        client = self.client()
        max_size = client.get_max_payload_size()
        self.assertEqual(max_size, 1024 * 1024)

    def test_get_log_range_by_name(self):
        client = self.client()

        range = client.get_log_range_by_name("ns/test_logs")
        self.assertEqual(1, range[0])
        self.assertEqual(2, range[1])

        with self.assertRaises(ld.LogDeviceError):
            client.get_log_range_by_name("this_log_range_does_not_exist")

    def test_get_tail_lsn(self):
        client = self.client()

        lsn1 = client.get_tail_lsn(1)
        self.assertNotEqual(0, lsn1)

        # append a record for log 2, tail lsn should be
        # the lsn of the appended record or higher (if the sequencer has
        # reactivated after the append).
        for i in range(1):
            lsn2 = client.append(2, "blah")
            self.assertNotEqual(0, lsn2)
            lsn3 = client.get_tail_lsn(2)
            if i == 0:
                self.assertTrue(lsn2 <= lsn3 <= lsn2 + 2 ** 32)
                if lsn3 == lsn2:
                    break
            else:
                self.assertEqual(lsn3, lsn2)

        # logid not exist, sequencer cannot be brought up
        with self.assertRaises(ld.LogDeviceError):
            client.get_tail_lsn(9999999)

    def test_validate_settings(self):
        # validation should succeed
        settings.validate_server_settings(
            {"self-initiated-rebuilding-grace-period": "2700s"}
        )
        settings.validate_server_settings(
            {"rocksdb-enable-insert-hint": True, "rocksdb-write-buffer-size": 42}
        )
        # invalid value. validation should fail.
        with self.assertRaises(ValueError):
            settings.validate_server_settings(
                {"self-initiated-rebuilding-grace-period": "2700"}
            )
        # unknown setting. validation should fail
        with self.assertRaises(ValueError):
            settings.validate_server_settings({"unknown": "whatever"})
        # settings is server-only and can't be used in clients. validation
        # should fail
        with self.assertRaises(ValueError):
            settings.validate_client_settings(
                {"self-initiated-rebuilding-grace-period": "2700s"}
            )

    def test_client_set_get(self):
        client = self.client()
        self.assertIsNotNone(client)
        # correct single setting
        ret = client.settings.set(str("node-stats-send-period"), str("66s"))
        out = client.settings.get(str("node-stats-send-period"))
        self.assertEqual(ret, 0)
        self.assertEqual(out, str("66s"))
        # correct multiple settings
        test_settings = {
            str("node-stats-send-period"): str("60s"),
            str("include-cluster-name-on-handshake"): str("false"),
        }
        ret = client.settings.set(test_settings)
        self.assertEqual(ret, 0)
        out = client.settings.get(str("node-stats-send-period"))
        self.assertEqual(out, str("60s"))
        out = client.settings.get(str("include-cluster-name-on-handshake"))
        self.assertEqual(out, str("false"))
        # incorrect set
        with self.assertRaises(ld.LogDeviceError):
            client.settings.set(str("mock-setting"), str("whatever"))
        # incorrect get
        with self.assertRaises(KeyError):
            client.settings.get(str("mock-setting"))
        # another correct single set
        ret = client.settings.set(
            str("include-cluster-name-on-handshake"), str("false")
        )
        out = client.settings.get(str("include-cluster-name-on-handshake"))
        self.assertEqual(ret, 0)
        self.assertEqual(out, str("false"))
