#!/usr/bin/env python3
# pyre-strict

# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""
Unit tests for `ldops.util.helpers` module.
"""

from unittest import TestCase

from ldops.const import ALL_SHARDS
from ldops.util import helpers
from logdevice.common.types import NodeID, ShardID


class HelpersTestCase(TestCase):
    def test_parse_shards_empty(self) -> None:
        """
        Empty input should produce empty output
        """
        self.assertEqual(set(), helpers.parse_shards([]))

    def test_parse_shards_invalid(self) -> None:
        """
        Invalid shards should throw ValueError exceptions.
        """
        # N0:S1, N0, or 0 format should be accepted
        with self.assertRaises(ValueError):
            print(helpers.parse_shards([":S2"]))

        with self.assertRaises(ValueError):
            print(helpers.parse_shards(["N:S2"]))

        with self.assertRaises(ValueError):
            print(helpers.parse_shards(["X0"]))

        with self.assertRaises(ValueError):
            helpers.parse_shards(["N0:B1"])

        with self.assertRaises(ValueError):
            helpers.parse_shards(["N0:S1X"])

    def test_parse_shards_valid1(self) -> None:
        # 5
        self.assertEqual(
            {ShardID(node=NodeID(node_index=5), shard_index=ALL_SHARDS)},
            helpers.parse_shards(["5"]),
        )
        # 5:1
        self.assertEqual(
            {ShardID(node=NodeID(node_index=5), shard_index=1)},
            helpers.parse_shards(["5:1"]),
        )
        # 0:S1
        self.assertEqual(
            {ShardID(node=NodeID(node_index=0), shard_index=1)},
            helpers.parse_shards(["0:S1"]),
        )
        # N0:S1
        self.assertEqual(
            {ShardID(node=NodeID(node_index=0), shard_index=1)},
            helpers.parse_shards(["N0:S1"]),
        )

        # N0 == ShardID(0, ALL_SHARDS)
        self.assertEqual(
            {ShardID(node=NodeID(node_index=0), shard_index=ALL_SHARDS)},
            helpers.parse_shards(["N0"]),
        )

        # N1:S4 == ShardID(1, 4)
        self.assertEqual(
            {ShardID(node=NodeID(node_index=1), shard_index=4)},
            helpers.parse_shards(["N1:S4"]),
        )

        # Allow ignored case
        # n1:S4 == ShardID(1, 4)
        self.assertEqual(
            {ShardID(node=NodeID(node_index=1), shard_index=4)},
            helpers.parse_shards(["n1:S4"]),
        )

    def test_parse_shards_valid2(self) -> None:
        # Parse multiple inputs
        self.assertEqual(
            {
                ShardID(node=NodeID(node_index=0), shard_index=1),
                ShardID(node=NodeID(node_index=1), shard_index=2),
            },
            helpers.parse_shards(["N0:S1", "N1:S2"]),
        )

        # Remove duplicates
        self.assertEqual(
            {
                ShardID(node=NodeID(node_index=0), shard_index=1),
                ShardID(node=NodeID(node_index=1), shard_index=2),
            },
            helpers.parse_shards(["N0:S1", "N1:S2", "N0:s1"]),
        )
