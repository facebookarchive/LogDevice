#!/usr/bin/env python3

# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


from unittest import TestCase

from ldops.types.cluster import Cluster


class TestCluster(TestCase):
    def test_init(self):
        with self.assertRaises(ValueError):
            Cluster()
