# Copyright (c) 2018-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# Integration tests disabled as faulty or flakey; removed from test
# list to ensure result of CI run gives a clear signal
set_tests_properties(
  "DataSizeTest.RestartNode"
  "IsLogEmptyTest.LogsTrimmedAway"
  "IsLogEmptyTest.PartialResult"
  "IsLogEmptyTest.RestartNode"
  "NodeSetTest.DeferReleasesUntilMetaDataRead"
  "Parametric/ReadPastGlobalLastReleasedTest.RecoveryStuck/(true,false,1-byteobject<01>)"
  "ReadingIntegrationTest/ReadingIntegrationTest.StatisticsCallback/false"
  "SequencerIntegrationTest.LogRemovalStressTest"
  "SequencerIntegrationTest.MetaDataWritePreempted"
  PROPERTIES DISABLED TRUE)
