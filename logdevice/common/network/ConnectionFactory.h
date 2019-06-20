/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>

#include "logdevice/common/ClientID.h"
#include "logdevice/common/Connection.h"
#include "logdevice/common/FlowGroup.h"
#include "logdevice/common/NodeID.h"
#include "logdevice/common/ResourceBudget.h"
#include "logdevice/common/Sockaddr.h"
#include "logdevice/common/Socket.h"
#include "logdevice/common/SocketTypes.h"

namespace facebook { namespace logdevice {

struct Settings;

/**
 * This interface provides a way to create connections for Sender.
 * Implementations are responsible for management of connections.
 */

class IConnectionFactory {
 public:
  virtual std::unique_ptr<Connection>
  createConnection(NodeID node_id,
                   SocketType type,
                   ConnectionType connection_type,
                   FlowGroup& flow_group) const = 0;

  virtual std::unique_ptr<Connection>
  createConnection(int fd,
                   ClientID client_name,
                   const Sockaddr& client_address,
                   ResourceBudget::Token connection_token,
                   SocketType type,
                   ConnectionType conntype,
                   FlowGroup& flow_group) const = 0;

  virtual ~IConnectionFactory() = default;

  virtual void onSettingsUpdated(const Settings& settings) {}
};

class ConnectionFactory : public IConnectionFactory {
 public:
  explicit ConnectionFactory(const Settings& settings) {}

  std::unique_ptr<Connection>
  createConnection(NodeID node_id,
                   SocketType type,
                   ConnectionType connection_type,
                   FlowGroup& flow_group) const override {
    return std::make_unique<Connection>(
        node_id, type, connection_type, flow_group);
  }

  std::unique_ptr<Connection>
  createConnection(int fd,
                   ClientID client_name,
                   const Sockaddr& client_address,
                   ResourceBudget::Token connection_token,
                   SocketType type,
                   ConnectionType connection_type,
                   FlowGroup& flow_group) const override {
    return std::make_unique<Connection>(fd,
                                        client_name,
                                        client_address,
                                        std::move(connection_token),
                                        type,
                                        connection_type,
                                        flow_group);
  }
};

}} // namespace facebook::logdevice
