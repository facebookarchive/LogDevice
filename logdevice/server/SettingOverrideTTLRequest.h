/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <unordered_map>

#include "logdevice/common/debug.h"
#include "logdevice/common/Request.h"
#include "logdevice/common/Worker.h"
#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice {

class Server;
class SettingOverrideTTLRequest;

// Wrapper instead of typedef to allow forward-declaring in ServerWorker.h
struct SettingOverrideTTLRequestMap {
  std::unordered_map<std::string, std::unique_ptr<SettingOverrideTTLRequest>>
      map;
};

/**
 * @file SettingOverrideTTLRequest.h
 *
 * This request will unset the setting when ttl expires. If there is already a
 * timer running, the new timer will override the old one.
 */
class SettingOverrideTTLRequest : public Request {
 public:
  explicit SettingOverrideTTLRequest(std::chrono::microseconds ttl,
                                     std::string name,
                                     Server* server)
      : ttl_(ttl), name_(name), server_(server) {}

  void onTimeout();

  void setupTimer();

  Request::Execution execute() override;

  int getThreadAffinity(int /*nthreads*/) override;

 protected:
  void registerRequest();

  void destroy();

 private:
  LibeventTimer timer_;
  std::chrono::microseconds ttl_;
  std::string name_;
  Server* server_;
};

}} // namespace facebook::logdevice
