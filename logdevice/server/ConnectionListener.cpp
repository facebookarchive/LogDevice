/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/ConnectionListener.h"

#include <memory>
#include <netdb.h>
#include <pthread.h>
#include <string>

#include <sys/socket.h>

#include "event2/listener.h"
#include "event2/util.h"
#include "logdevice/common/ClientID.h"
#include "logdevice/common/Request.h"
#include "logdevice/common/Sockaddr.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/libevent/compat.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/include/Err.h"
#include "logdevice/server/FailureDetector.h"
#include "logdevice/server/NewConnectionRequest.h"
#include "logdevice/server/ServerProcessor.h"

namespace facebook { namespace logdevice {

ConnectionListener::ConnectionListener(
    Listener::InterfaceDef iface,
    KeepAlive loop,
    std::shared_ptr<SharedState> shared_state,
    ListenerType listener_type,
    ResourceBudget& connection_backlog_budget)
    : Listener(std::move(iface), loop),
      connection_backlog_budget_(connection_backlog_budget),
      shared_state_(shared_state),
      listener_type_(listener_type) {
  ld_check(shared_state);
}

const SimpleEnumMap<ConnectionListener::ListenerType, std::string>&
ConnectionListener::listenerTypeNames() {
  // Note that thread names are limited to 16 characters. Use them wisely.
  static SimpleEnumMap<ConnectionListener::ListenerType, std::string>
      listener_names({
          {ConnectionListener::ListenerType::DATA, "ld:conn-listen"},
          {ConnectionListener::ListenerType::DATA_SSL, "ld:sconn-listen"},
          {ConnectionListener::ListenerType::GOSSIP, "ld:gossip"},
      });
  return listener_names;
}

void ConnectionListener::acceptCallback(evutil_socket_t sock,
                                        const folly::SocketAddress& addr) {
  ld_check(processor_ != nullptr);
  ServerProcessor* processor = checked_downcast<ServerProcessor*>(processor_);
  Sockaddr sockaddr(addr);

  // Check if accepting this connection pushed us over the limit.  Since there's
  // only one ConnectionListener thread, and this is called soon after accept(),
  // we're able to react promptly in case there's a burst of new connections.
  auto token = processor->conn_budget_incoming_.acquireToken();
  if (!token) {
    STAT_INCR(processor->stats_, dropped_connection_limit);
    RATELIMIT_WARNING(std::chrono::seconds(10),
                      1,
                      "Rejecting a connection from %s because the limit "
                      "has been reached.",
                      sockaddr.toString().c_str());
    LD_EV(evutil_closesocket)(sock);
    return;
  }
  auto conn_backlog_token = connection_backlog_budget_.acquireToken();

  if (!conn_backlog_token) {
    STAT_INCR(processor->stats_, dropped_connection_burst);
    RATELIMIT_WARNING(std::chrono::seconds(10),
                      1,
                      "Rejecting a connection from %s because the burst limit "
                      "has been reached.",
                      sockaddr.toString().c_str());
    LD_EV(evutil_closesocket)(sock);
    return;
  }
  // By default we want the processor to select a worker thread for us.
  worker_id_t wid{-1};
  SocketType sock_type;
  WorkerType target_worker_type = WorkerType::GENERAL;

  if (listener_type_ == ListenerType::GOSSIP) {
    ld_check(processor->failure_detector_);
    sock_type = SocketType::GOSSIP;
    target_worker_type = WorkerType::FAILURE_DETECTOR;
  } else {
    sock_type = SocketType::DATA;
  }

  std::unique_ptr<Request> request = std::make_unique<NewConnectionRequest>(
      sock,
      wid,
      sockaddr,
      std::move(token),
      std::move(conn_backlog_token),
      sock_type,
      isSSL() ? ConnectionType::SSL : ConnectionType::PLAIN,
      target_worker_type);

  int rv;
  STAT_INCR(processor->stats_, num_backlog_connections);
  // The processor will route gossip request because WorkerType is
  // WorkerType::FAILURE_DETECTOR in that case
  rv = processor->postRequest(request);

  if (rv != 0) {
    STAT_DECR(processor->stats_, num_backlog_connections);
    ld_error("Error passing accepted connection to %s thread. "
             "postRequest() reported %s.",
             listenerTypeNames()[listener_type_].c_str(),
             error_description(err));
    LD_EV(evutil_closesocket)(sock);
    // ~NewConnectionRequest() will also destroy the token, thus releasing the
    // fd from connection_backlog_budget_.
  }
}

}} // namespace facebook::logdevice
