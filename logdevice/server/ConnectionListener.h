/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <array>
#include <memory>

#include <folly/SocketAddress.h>
#include <folly/container/F14Map.h>
#include <folly/io/async/AsyncTimeout.h>
#include <folly/io/async/EventHandler.h>
#include <folly/net/NetworkSocket.h>

#include "logdevice/common/Processor.h"
#include "logdevice/common/ResourceBudget.h"
#include "logdevice/common/SimpleEnumMap.h"
#include "logdevice/common/SocketTypes.h"
#include "logdevice/server/Listener.h"
#include "logdevice/server/NewConnectionRequest.h"
#include "logdevice/server/ServerProcessor.h"

namespace facebook { namespace logdevice {

/**
 * @file Listens to incoming connections and hands them off to Processor
 *       threads.
 */
class ConnectionListener : public Listener {
 public:
  struct SharedState {
    std::atomic_int last_client_idx_{0};
  };

  /*
   * Represents the listener type.
   */
  enum class ListenerType : uint8_t {
    DATA,
    DATA_SSL,
    GOSSIP,
    SERVER_TO_SERVER,
    MAX,
  };

  static const SimpleEnumMap<ListenerType, std::string>& listenerTypeNames();

  // EventLoop on which ConnectionListener is running
  KeepAlive loop_;

  explicit ConnectionListener(Listener::InterfaceDef iface,
                              KeepAlive loop,
                              std::shared_ptr<SharedState> shared_state,
                              ListenerType listener_type,
                              ResourceBudget& connection_backlog_budget);

  void setProcessor(Processor* processor) {
    processor_ = processor;
  }

  /**
   * Stops listening and frees all events waiting for a read event.
   */
  folly::SemiFuture<folly::Unit> stopAcceptingConnections() override;

  // Callback functions that register connection limits reached
  using ConnectionLimitReachedCallback = std::function<void()>;

  void setConnectionLimitReachedCallback(ConnectionLimitReachedCallback cb);

 protected:
  friend class ConnectionListenerTest;

  void
  connectionAccepted(folly::NetworkSocket fd,
                     const folly::SocketAddress& clientAddr) noexcept override;

  static ConnectionType getConnectionType(folly::NetworkSocket sock);

 private:
  class ReadEventHandler : public folly::EventHandler,
                           public folly::AsyncTimeout {
   public:
    ReadEventHandler(folly::EventBase* eventBase,
                     folly::NetworkSocket sock,
                     ServerProcessor& processor,
                     std::unique_ptr<NewConnectionRequest> connection_request,
                     ConnectionListener* connection_listener)
        : folly::EventHandler(eventBase, folly::NetworkSocket(sock)),
          folly::AsyncTimeout(eventBase),
          sock_(sock),
          processor_(processor),
          connection_request_(std::move(connection_request)),
          connection_listener_(connection_listener) {}

    /**
     * handlerReady() is invoked when the handler is ready.
     * Triggered by libevent on sock/fd when there's some data to be read.
     *
     * @param events  A bitset indicating the events that are ready.
     */
    void handlerReady(uint16_t events) noexcept override;
    /**
     * timeoutExpired() is invoked when the timeout period has expired.
     * Timeout expiring means the timeout has been reached before a read has
     * been registered on selected sock/fd.
     */
    void timeoutExpired() noexcept override;

   protected:
    folly::NetworkSocket sock_;
    ServerProcessor& processor_;
    std::unique_ptr<NewConnectionRequest> connection_request_;
    ConnectionListener* connection_listener_;
  };

  ConnectionLimitReachedCallback conn_limit_reached_cb_{nullptr};

  folly::F14FastMap<folly::NetworkSocket, std::unique_ptr<ReadEventHandler>>
      read_event_handlers_{};

  ResourceBudget& connection_backlog_budget_;
  // Pointer to Processor to hand connections off to. Unowned.
  Processor* processor_ = nullptr;
  std::shared_ptr<SharedState> shared_state_;
  ListenerType listener_type_;
};

}} // namespace facebook::logdevice
