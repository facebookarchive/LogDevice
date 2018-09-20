/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>

namespace facebook { namespace logdevice {

class Socket;
struct Settings;
/**
 * @file   a ConnectThrottle object counts successful and failed attempts
 *         to connect to a destination, and calculates a deadline that a Socket
 *         objects then uses to decide whether they even should attempt to
 *         connect(2) to their destination. The objective is to reduce SYN
 *         traffic to down LogDevice nodes.
 */

class ConnectThrottle {
 public:
  explicit ConnectThrottle(Socket* input_socket)
      : current_delay_(std::chrono::milliseconds::zero()),
        down_until_(std::chrono::steady_clock::time_point::min()),
        socket_(input_socket) {}

  /**
   * Advise the object whether a connection attempt succeeded or failed.
   */
  void connectSucceeded();
  void connectFailed();

  /**
   * @return  true if connection is allowed (current time is
   *          past .down_until_), false if not.
   */
  bool mayConnect() const;

  std::chrono::steady_clock::time_point downUntil() const {
    return down_until_;
  }

  virtual ~ConnectThrottle() {}

 protected:
  // tests can override
  virtual std::chrono::steady_clock::time_point now() const {
    return std::chrono::steady_clock::now();
  }

  virtual const Settings& getSettings();

 private:
  std::chrono::milliseconds current_delay_; // last delay between reconnection

  // reject connection attempts until std::chrono::steady_clock is
  // past this deadline. A call to connectSucceeded(true) resets the
  // deadline to zero. A call to connectSucceeded(false) exponentially
  // increases the amount of downtime from the initial value of
  // INITIAL_DOWNTIME up to a cap of MAX_DOWNTIME milliseconds.
  std::chrono::steady_clock::time_point down_until_;

  Socket* socket_;
};

}} // namespace facebook::logdevice
