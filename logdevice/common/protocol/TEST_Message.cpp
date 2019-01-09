/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/protocol/TEST_Message.h"

#include <cstdlib>

namespace facebook { namespace logdevice {

template <>
Message::Disposition TEST_Message::onReceived(const Address&) {
  // does nothing
  return Message::Disposition::NORMAL;
}

}} // namespace facebook::logdevice
