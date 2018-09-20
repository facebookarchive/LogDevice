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
 * An interface that will be overridden by plugins to implement an Admin API
 * interface for logdevice.
 */
class AdminServer {
 public:
  /**
   * will be called on server startup, the server startup will fail if this
   * returned false.
   */
  virtual bool start() = 0;
  /**
   * This should stop the admin server and all associated threads. This should
   * be a blocking call that waits until pending work has been processed and
   * all threads have exited.
   */
  virtual void stop() = 0;
  virtual ~AdminServer() {}
};

}} // namespace facebook::logdevice
