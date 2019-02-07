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

  // URI scheme for ip-based quroum specifications
  constexpr static const char* URI_SCHEME_IP = "ip";

  explicit ZookeeperConfig() {}

  ZookeeperConfig(std::vector<Sockaddr> quorum,
                  std::chrono::milliseconds session_timeout)
      : quorum_(std::move(quorum)),
        session_timeout_(session_timeout),
        uri_scheme_(URI_SCHEME_IP),
        quorum_string_(makeQuorumString(quorum_)),
        zookeeper_properties_(folly::dynamic::object) {}

  ZookeeperConfig(std::vector<Sockaddr> quorum,
                  std::chrono::milliseconds session_timeout,
                  std::string uri_scheme,
                  std::string quorum_string,
                  folly::dynamic properties)
      : quorum_(std::move(quorum)),
        session_timeout_(session_timeout),
        uri_scheme_(std::move(uri_scheme)),
        quorum_string_(std::move(quorum_string)),
        zookeeper_properties_(std::move(properties)) {}

  ZookeeperConfig(std::string quorum_string,
                  std::string uri_scheme,
                  std::chrono::milliseconds session_timeout,
                  folly::dynamic properties = folly::dynamic::object)
      : session_timeout_(session_timeout),
        uri_scheme_(uri_scheme),
        quorum_string_(quorum_string),
        zookeeper_properties_(std::move(properties)) {}

  /**
   * @return A comma-separated list of ip:ports of the ZK servers
   */
  const std::string& getQuorumString() const {
    return quorum_string_;
  }

  /**
   * @return The session timeout in milliseconds
   */
  std::chrono::milliseconds getSessionTimeout() const {
    return session_timeout_;
  }

  /**
   * @return The quorum as an unsorted vector
   * DEPRECATED
   */
  const std::vector<Sockaddr>& getQuorum() const {
    return quorum_;
  }

  /**
   * @return The scheme of the URI used in the zookeeper_uri property.
   *
   * The scheme indicates the syntax of the address part of the URI. In other
   * words the sheme indicates the format of the quorum string used to
   * initialize Zookeeper clients.
   * Currently, the only supported scheme is "ip". The associated syntax of
   * a quorum is a comma-separated list of IP address and port pairs. For
   * instance, a valid URI may look like the following:
   *    "ip://1.2.3.4:2181,5.6.7.8:2181,9.10.11.12:2181"
   *
   */
  const std::string& getUriScheme() const {
    return uri_scheme_;
  }

  /**
   * @return The URI resolving to the configured zookeeper ensemble.
   */
  std::string getZookeeperUri() const {
    return uri_scheme_ + "://" + quorum_string_;
  }

  /**
   * @return The properties that were left out by the parser
   * as a map of key/value pairs
   */
  const folly::dynamic& getProperties() const {
    return zookeeper_properties_;
  }

  /**
   * @return Zookeeper config as folly dynamic suitable to be serialized as JSON
   * in the main config.
   */
  folly::dynamic toFollyDynamic() const;

  static std::unique_ptr<ZookeeperConfig>
  fromJson(const folly::dynamic& parsed);

  /**
   * Equality operator to ocmpare two Zookeeper config objects
   */
  bool operator==(const ZookeeperConfig& other) const;

 private:
  static std::string makeQuorumString(const std::vector<Sockaddr>& quroum);

  // Addresses of all ZK servers in the quorum we use to store and increment
  // next epoch numbers for logs
  const std::vector<Sockaddr> quorum_;

  // sesstion timeout to pass to zookeeper_init
  std::chrono::milliseconds session_timeout_{0};

  std::string uri_scheme_;
  std::string quorum_string_;
  folly::dynamic zookeeper_properties_;
};

}}} // namespace facebook::logdevice::configuration
