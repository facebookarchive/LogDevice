# Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# Integration tests disabled as faulty or flakey; removed from test
# list to ensure result of CI run gives a clear signal
set_tests_properties(
  "ServerBasedNodesConfigurationStoreTest.SuccessScenario"
  "SequencerIntegrationTest.MetaDataWritePreempted"
  PROPERTIES DISABLED TRUE)
