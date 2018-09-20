/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/include/Err.h"

/**
 * @file NodeStatsMessageCallback is used in the messaging layer to communicate
 * with the NodeStatsHandler
 */

namespace facebook { namespace logdevice {

class NodeStatsMessageCallback {
 public:
  virtual void onMessageSent(Status status) = 0;

  virtual void onReplyFromNode(uint64_t msg_id) = 0;

  virtual ~NodeStatsMessageCallback() = default;
};
}} // namespace facebook::logdevice
