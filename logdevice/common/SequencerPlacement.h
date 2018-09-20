/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

namespace facebook { namespace logdevice {

/**
 * @file  Interface used to determine which logs to run Sequencers for on this
 *        server. Implementations usually have access to the Processor and can
 *        activate/deactivate sequencers at any time.
 */

class SequencerPlacement {
 public:
  virtual ~SequencerPlacement() {}

  /**
   * Called during shutdown to give subclasses a chance to move shards to a
   * different server before we stop processing further client requests.
   */
  virtual void requestFailover() {}
};

}} // namespace facebook::logdevice
