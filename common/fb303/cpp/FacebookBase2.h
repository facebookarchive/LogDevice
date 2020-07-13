/*
 *  Copyright (c) 2004-present, Facebook, Inc.
 *  All rights reserved.
 *
 *  This source code is licensed under the BSD-style license found in the
 *  LICENSE file in the root directory of this source tree. An additional grant
 *  of patent rights can be found in the PATENTS file in the same directory.
 *
 */
#pragma once

#include <chrono>
#include <unistd.h>

#include <common/fb303/if/gen-cpp2/FacebookService.h>

namespace facebook { namespace fb303 {

class FacebookBase2 : virtual public cpp2::FacebookServiceSvIf {
 public:
  explicit FacebookBase2(std::string /*unused*/) : start_time_(time(nullptr)) {}

  int64_t getPid() override {
    return getpid();
  }

  int64_t aliveSince() override {
    return start_time_.count();
  }

 private:
  std::chrono::seconds start_time_;
};

}} // namespace facebook::fb303
