/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>
#include <variant>

#include <boost/noncopyable.hpp>

#include "logdevice/common/NodeID.h"
#include "logdevice/common/configuration/Configuration.h"

namespace facebook { namespace logdevice {

class NodeIDMatcher {
 private:
  enum class Type { NAME, ADDRESS };

 public:
  /**
   * Find the host in the config that has same name as the one that's passed
   * to this function.
   *
   * @return Will always return a non-nullptr matcher.
   */
  static std::unique_ptr<NodeIDMatcher> byName(const std::string& name);

  /**
   * Find the host in the config that has an ip address that matches the ip
   * address of one of the network interfaces of the local system and a port
   * that matches the port this object was constructed with.
   *
   * @return Returns a nullptr if it fails to query the interfaces of this host.
   */
  static std::unique_ptr<NodeIDMatcher> byTCPPort(int port);

  /**
   * Find the host in the config that has a unix domain socket path that matches
   * the unix domain socket path that this object was constructed with.
   *
   * @return A matcher if the socket address is valid, nullptr otherwise.
   */
  static std::unique_ptr<NodeIDMatcher>
  byUnixSocket(const std::string& address);

  /**
   * @return true if the ID represented by this class matches this node's
   * service discovery config.
   */
  bool match(const configuration::nodes::NodeServiceDiscovery& sd) const;

  std::string toString() const;

  explicit NodeIDMatcher(const std::string& name)
      : type_{Type::NAME}, id_{name} {}
  explicit NodeIDMatcher(const std::vector<Sockaddr>& addresses)
      : type_{Type::ADDRESS}, id_{addresses} {}

  ~NodeIDMatcher() {}

 private:
  Type type_;
  std::variant<std::string, std::vector<Sockaddr>> id_;

  bool matchByAddresses(const configuration::nodes::NodeServiceDiscovery& sd,
                        const std::vector<Sockaddr>& addresses) const;

  bool matchByName(const configuration::nodes::NodeServiceDiscovery& sd,
                   const std::string& name) const;
};

/**
 * @file Attempts to figure out this server's NodeID given the configuration
 *       for the cluster, and an ID matcher.
 */
class MyNodeIDFinder : boost::noncopyable {
 public:
  explicit MyNodeIDFinder(std::unique_ptr<NodeIDMatcher> id_matcher)
      : id_matcher_(std::move(id_matcher)) {
    ld_assert(id_matcher_);
  }

  /**
   * Find NodeID of this logdevice instance based on an ID matcher that this
   * object was constructed with. The ID matcher can search the config for
   * certain IP, unix socket path or server's name.
   *
   * @return On success, returns the NodeID, and folly::none on failure.
   */
  folly::Optional<NodeID>
  calculate(const configuration::nodes::NodesConfiguration& config);

 private:
  std::unique_ptr<NodeIDMatcher> id_matcher_;
};

}} // namespace facebook::logdevice
