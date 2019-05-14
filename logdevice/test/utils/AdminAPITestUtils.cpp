/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/test/utils/AdminAPITestUtils.h"

namespace facebook { namespace logdevice {

void retry_until_ready(int32_t attempts,
                       std::chrono::seconds delay,
                       folly::Function<void()> operation) {
  do {
    // let's keep trying until the server hits that version
    try {
      operation();
      // No exception thrown.
      return;
    } catch (thrift::NodeNotReady& e) {
      ld_info("Got NodeNotReady exception, attempts left: %i", attempts);
      // retry
      std::this_thread::sleep_for(delay);
      attempts--;
    }
  } while (attempts > 0);
}
}} // namespace facebook::logdevice
