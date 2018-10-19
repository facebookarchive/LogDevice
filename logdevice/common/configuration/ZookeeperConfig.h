/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>
#include <cstddef>
#include <vector>

#include <folly/dynamic.h>

#include "logdevice/common/Sockaddr.h"

namespace facebook { namespace logdevice { namespace configuration {

class ZookeeperConfig {
 public:
  // maximum length of cluster name string
  constexpr static const size_t MAX_CLUSTER_NAME = 127;

  explicit ZookeeperConfig() {}

  template <class T>
  ZookeeperConfig(T&& quorum, std::chrono::milliseconds session_timeout)
      : quorum_(std::forward<T>(quorum)), session_timeout_(session_timeout) {}

  /**
   * @return returns a comma-separated list of ip:ports of the ZK servers
   */
  std::string getQuorumString() const;

  /**
   * @return returns the session timeout in milliseconds
   */
  std::chrono::milliseconds getSessionTimeout() const {
    return session_timeout_;
  }

  /**
   * @return returns the quorum as an unsorted vector
   */
  const std::vector<Sockaddr>& getQuorum() const {
    return quorum_;
  }

  static std::unique_ptr<ZookeeperConfig>
  fromJson(const folly::dynamic& parsed);

 private:
  // Addresses of all ZK servers in the quorum we use to store and increment
  // next epoch numbers for logs
  const std::vector<Sockaddr> quorum_;

  // sesstion timeout to pass to zookeeper_init
  std::chrono::milliseconds session_timeout_{0};
};

}}} // namespace facebook::logdevice::configuration
