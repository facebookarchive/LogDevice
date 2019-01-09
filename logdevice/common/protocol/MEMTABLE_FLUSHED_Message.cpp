/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/protocol/MEMTABLE_FLUSHED_Message.h"

#include <cstdlib>

namespace facebook { namespace logdevice {

template <>
Message::Disposition MEMTABLE_FLUSHED_Message::onReceived(const Address&) {
  // Receipt handler lives in server/MEMTABLE_FLUSHED_onReceived.cpp; this
  // should never get called.
  std::abort();
}

}} // namespace facebook::logdevice
