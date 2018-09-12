#!/usr/bin/env python3

# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.
from __future__ import absolute_import, division, print_function, unicode_literals

import unittest

import logdevice.client


class LogDeviceUnitTest(unittest.TestCase):
    "Tests that do not need a local LogDevice cluster"

    def setUp(self):
        self._initialLogLevel = logdevice.client.getLoggingLevel()

    def tearDown(self):
        logdevice.client.setLoggingLevel(self._initialLogLevel)

    def test_GapType(self):
        self.assertEqual(str(logdevice.client.GapType.BRIDGE), "BRIDGE")
        self.assertEqual(
            logdevice.client.GapType.BRIDGE, logdevice.client.GapType.BRIDGE
        )
        self.assertNotEqual(
            logdevice.client.GapType.BRIDGE, logdevice.client.GapType.HOLE
        )

    def test_docstring_crash(self):
        # this would previously segfault, due to a recursive casting issue
        # with the boost::python doc generation.  lets verify it stays fixed.
        logdevice.client.Client.append.__doc__

    def test_logdevice_error(self):
        self.assertIsInstance(
            logdevice.client.LogDeviceError("testing"),
            Exception,
            "correct exception ancestry",
        )

    def test_logging_level(self):
        def test(logLevel):
            logdevice.client.setLoggingLevel(logLevel)
            self.assertEquals(logdevice.client.getLoggingLevel(), logLevel)

        loggingLevel = logdevice.client.LoggingLevel
        test(loggingLevel.NONE)
        test(loggingLevel.CRITICAL)
        test(loggingLevel.ERROR)
        test(loggingLevel.WARNING)
        test(loggingLevel.NOTIFY)
        test(loggingLevel.INFO)
        test(loggingLevel.DEBUG)
        test(loggingLevel.SPEW)

        self.assertEqual(
            logdevice.client.parse_log_level(str("error")), loggingLevel.ERROR
        )

    def test_lsn_to_string(self):
        tests = ((283467847824, "e66n6288"), (85899668877, "e20n322957"))

        for test in tests:
            self.assertEqual(logdevice.client.lsn_to_string(test[0]), test[1])
