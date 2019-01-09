/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/NewConnectionRequest.h"

#include "logdevice/common/Sender.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/debug.h"

namespace facebook { namespace logdevice {

Request::Execution NewConnectionRequest::execute() {
  Worker* w = Worker::onThisThread();
  ld_check(w != nullptr);
  std::shared_ptr<const Settings> settings =
      std::make_shared<const Settings>(w->settings());

  if (sock_type_ == SocketType::GOSSIP && settings->ssl_on_gossip_port) {
    conntype_ = ConnectionType::SSL;
  }

  int rv = w->sender().addClient(
      fd_, client_addr_, std::move(conn_token_), sock_type_, conntype_);

  if (rv == 0) {
    ld_debug("A new connection from %s is running on "
             "worker #%d",
             client_addr_.toString().c_str(),
             int(w->idx_));
  } else {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    10,
                    "Failed to create a logdevice::Socket for a new client "
                    "connection from %s on worker #%d: %s",
                    client_addr_.toString().c_str(),
                    int(w->idx_),
                    error_description(err));
  }

  return Execution::COMPLETE;
}

int NewConnectionRequest::getThreadAffinity(int /*nthreads*/) {
  return worker_id_.val_;
}

}} // namespace facebook::logdevice
