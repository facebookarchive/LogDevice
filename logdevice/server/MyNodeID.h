/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <boost/noncopyable.hpp>
#include <memory>

#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/NodeID.h"

namespace facebook { namespace logdevice {

/**
 * @file Attempts to figure out this server's NodeID given the configuration
 *       for the cluster, and local network interfaces.
 */

class MyNodeID : boost::noncopyable {
 public:
  /**
   * @param my_port port that the server is listening on.
   */
  explicit MyNodeID(int my_port) : my_port_(my_port) {}

  /**
   * @param unix_socket Path to the unix domain socket the server is listening
   *                    on. Must not be empty.
   */
  explicit MyNodeID(const std::string& unix_socket)
      : my_port_(-1), unix_socket_(unix_socket) {
    ld_check(!unix_socket.empty());
  }

  /**
   * Find the NodeID of this LogDevice instance by looking at the `host` entry
   * in the config that corresponds to the TCP port or unix domain socket path
   * this object was created with.
   *
   * @return On success, returns 0 and populates `out`.  On failure, returns
   *         -1 and sets `out` to an invalid address.
   */
  int calculate(const ServerConfig& config, NodeID& out);

 private:
  /**
   * Find the host in the config that has an ip address that matches the ip
   * address of one of the network interfaces of the local system and a port
   * that matches the port this object was constructed with.
   *
   * @param config ServerConfig of the cluster.
   * @param out The NodeID of this LogDevice instance is written here.
   *
   * @return On success, returns 0 and populates `out`.  On failure, returns
   *         -1 and sets `out` to an invalid address.
   */
  int calculateFromTcpPort(const ServerConfig& config, NodeID& out);

  /**
   * Find the host in the config that has a unix domain socket path that matches
   * the unix domain socket path that this object was constructed with.
   *
   * @param config ServerConfig of the cluster.
   * @param out The NodeID of this LogDevice instance is written here.
   *
   * @return On success, returns 0 and populates `out`.  On failure, returns
   *         -1 and sets `out` to an invalid address.
   */
  int calculateFromUnixSocket(const ServerConfig& config, NodeID& out);

  // If my_port_ != -1, the server will be listening on my_port_. Otherwise, the
  // server will be listening on a unix domain socket whose path is
  // unix_socket_.
  int my_port_;
  std::string unix_socket_;
};

}} // namespace facebook::logdevice
