/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/Appender.h"
#include "logdevice/common/EpochSequencer.h"
#include "logdevice/common/ExponentialBackoffTimer.h"
#include "logdevice/common/LibeventTimer.h"
#include "logdevice/common/MetaDataLogWriter.h"
#include "logdevice/common/RecipientSet.h"
#include "logdevice/common/Sequencer.h"
#include "logdevice/common/Timer.h"

using namespace facebook::logdevice;

#define PRINT_SIZE(name) printf("%-30s %lu\n", #name, sizeof(name))

int main(int /*argc*/, char* /*argv*/ []) {
  PRINT_SIZE(Appender);
  PRINT_SIZE(ExponentialBackoffTimer);
  PRINT_SIZE(LibeventTimer);
  PRINT_SIZE(RecipientSet);
  PRINT_SIZE(Sequencer);
  PRINT_SIZE(EpochSequencer);
  return 0;
}
