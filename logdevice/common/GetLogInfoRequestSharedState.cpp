/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/GetLogInfoRequestSharedState.h"

#include "logdevice/common/GetLogInfoRequest.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/UpdateableConfig.h"

namespace facebook { namespace logdevice {

namespace {
// This is the callback to be called whenever the socket that we use for
// config change notifications is closed.
class GLISocketClosedCallback : public SocketCallback {
 public:
  explicit GLISocketClosedCallback() {}

  void operator()(Status st, const Address& name) override {
    RATELIMIT_WARNING(std::chrono::seconds(10),
                      1,
                      "Closed socket to GetLogInfoRequest target node %s with "
                      "reason %s",
                      name.toString().c_str(),
                      error_description(st));
    if (Worker::onThisThread()->shuttingDown()) {
      // if we're shutting down, no point in reloading the config
      return;
    }
    Worker* worker = Worker::onThisThread();
    auto& map = worker->runningGetLogInfo().per_node_map;
    if (map.size() == 0) {
      // There are no GetLogInfoFromNodeRequests running, just reload the
      // config
      Worker::onThisThread()
          ->getUpdateableConfig()
          ->updateableLogsConfig()
          ->invalidate();
      return;
    }
    auto it = map.begin();
    while (it != map.end()) {
      // calling finalize() will erase the request from the map, thus
      // invalidating the current iterator, so we have to fetch the next one
      // beforehand.
      auto next_it = std::next(it);
      // defer the config reload to individual GetLogInfoRequest retries
      it->second->finalize(E::FAILED);
      it = next_it;
    }
  }
};
} // namespace

GetLogInfoRequestSharedState::GetLogInfoRequestSharedState()
    : socket_callback_(std::make_unique<GLISocketClosedCallback>()) {}

}} // namespace facebook::logdevice
