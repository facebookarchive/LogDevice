/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/protocol/DELETE_LOG_METADATA_Message.h"

namespace facebook { namespace logdevice {

template <>
Message::Disposition DELETE_LOG_METADATA_Message::onReceived(const Address&) {
  // Receipt handler lives in server/DELETE_LOG_METADATA_onReceived.cpp; this
  // should never get called.
  std::abort();
}

}} // namespace facebook::logdevice
