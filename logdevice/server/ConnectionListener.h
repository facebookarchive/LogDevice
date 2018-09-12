/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "event2/event.h"
#include "event2/listener.h"
#include <memory>
#include <vector>

#include "logdevice/common/Processor.h"
#include "logdevice/common/SimpleEnumMap.h"
#include "logdevice/common/WorkerType.h"
#include "logdevice/server/Listener.h"

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
   * Indicates whether this is a gossip, data, or admin api listener
   */
  enum class ListenerType : uint8_t {
    DATA,
    DATA_SSL,
    GOSSIP,
    MAX,
  };

  static const SimpleEnumMap<ListenerType, std::string>& listenerTypeNames();

  explicit ConnectionListener(Listener::InterfaceDef iface,
                              std::shared_ptr<SharedState> shared_state,
                              ListenerType listener_type);

  void setProcessor(Processor* processor) {
    processor_ = processor;
  }

 protected:
  /**
   * Triggered by libevent when there is a new incoming connection.  This
   * assigns a ClientID then hands the socket off to a worker thread.
   */
  void acceptCallback(evutil_socket_t sock,
                      struct sockaddr* addr,
                      int len) override;

 private:
  // Pointer to Processor to hand connections off to. Unowned.
  Processor* processor_ = nullptr;
  std::shared_ptr<SharedState> shared_state_;
  ListenerType listener_type_;
};

}} // namespace facebook::logdevice
