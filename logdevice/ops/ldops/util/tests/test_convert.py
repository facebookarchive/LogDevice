#!/usr/bin/env python3

# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""
Unit tests for `ldops.util.convert` module.
"""

from unittest import TestCase

from ldops.util import convert
from logdevice.admin.common.types import LocationScope, ReplicationProperty
from logdevice.admin.nodes.types import ShardStorageState


class ConvertTestCase(TestCase):
    def test_to_shard_id(self):
        # We cannot accept empty inputs
        input1 = ""
        with self.assertRaises(ValueError):
            convert.to_shard_id(input1)

        # We cannot handle invalid formats
        invalid_input = "Potatoes"
        with self.assertRaises(ValueError):
            convert.to_shard_id(invalid_input)

        # The most common usage of a scope string
        input2 = "N225:S5"
        shard = convert.to_shard_id(input2)
        self.assertEqual(5, shard.shard_index)
        self.assertEqual(225, shard.node.node_index)

        # We should parse this as shard_index=-1
        input3 = "N225"
        shard = convert.to_shard_id(input3)
        self.assertEqual(-1, shard.shard_index)
        self.assertEqual(225, shard.node.node_index)

    def test_to_storage_state(self):
        # We cannot accept empty inputs
        input1 = ""
        with self.assertRaises(ValueError):
            convert.to_storage_state(input1)

        # An invalid storage state
        invalid_input = "Potatoes"
        with self.assertRaises(ValueError):
            convert.to_storage_state(invalid_input)

        # An valid lower/upper storage state
        read_only = "read_only"
        self.assertEqual(
            ShardStorageState.READ_ONLY, convert.to_storage_state(read_only)
        )
        read_only = "read-only"
        self.assertEqual(
            ShardStorageState.READ_ONLY, convert.to_storage_state(read_only)
        )
        read_only = "READ_ONLY"
        self.assertEqual(
            ShardStorageState.READ_ONLY, convert.to_storage_state(read_only)
        )
        read_only = "READ-ONLY"
        self.assertEqual(
            ShardStorageState.READ_ONLY, convert.to_storage_state(read_only)
        )

        read_write = "read-write"
        self.assertEqual(
            ShardStorageState.READ_WRITE, convert.to_storage_state(read_write)
        )

    def test_to_replication(self):
        # Empty dicts return empty replication properties
        self.assertEqual(ReplicationProperty(), convert.to_replication({}))
        # None is returned as none
        self.assertEqual(None, convert.to_replication(None))

        # None is returned as none
        invalid_input1 = {"invalid": 3, "Rack": 4}
        with self.assertRaises(ValueError):
            convert.to_replication(invalid_input1)

        valid_input = {"node": 3, "Rack": 4}
        expected = {}
        expected[LocationScope.RACK] = 4
        expected[LocationScope.NODE] = 3
        expected = ReplicationProperty(expected)
        self.assertEqual(expected, convert.to_replication(valid_input))
