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

#include <unistd.h>

#include <common/fb303/if/gen-cpp2/FacebookService.h>

namespace facebook { namespace fb303 {

class FacebookBase2 : virtual public cpp2::FacebookServiceSvIf {
 public:
  explicit FacebookBase2(const char*) {}

  int64_t getPid() override {
    return getpid();
  }
};

}} // namespace facebook::fb303
