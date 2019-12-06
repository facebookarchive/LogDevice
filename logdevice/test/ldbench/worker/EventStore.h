/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/dynamic.h>

namespace facebook { namespace logdevice { namespace ldbench {
/**
 * Base class for target of storing events
 * Currently, EventStore and StatsStore are providing same interfaces.
 * However, we still separate them out in case of they may need different
 * interfaces in the future.
 */
class EventStore {
 public:
  virtual ~EventStore(){};
  /* *
   * Check if EventStore is ready to receive events
   */
  virtual bool isReady() = 0;
  /* *
   * Log events to persist targets
   * logEvent() is safe to invoke from any thread. The actual logging
   * is or may be asynchronous.
   * @param event_obj
   *   A folly::dynamic object (a container of key-value pairs)
   *   Keys are event attributes, and values are corresponding contents.
   *   E.g.,
   *     folly::dynamic::object("timestamp", 123)("type", "latency")("blob",
   *     200);
   */
  virtual void logEvent(folly::dynamic event_obj) = 0;
};

}}} // namespace facebook::logdevice::ldbench
