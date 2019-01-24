/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>

#include "logdevice/common/NodeID.h"
#include "logdevice/common/RateLimiter.h"

namespace facebook { namespace logdevice {
class SocketCallback;

// The following struct holds the state shared between all GetLogInfoRequests,
// GetLogInfoFromNodeRequests and RemoteLogsConfig
struct GetLogInfoRequestSharedState {
  std::mutex mutex_;

  // Node ID that LOGS_CONFIG_API messages will be sent to
  NodeID node_id_;

  // callback that will be called when the socket to the target node closes
  std::unique_ptr<SocketCallback> socket_callback_;

  // Worker ID that will process GetLogInfoRequests
  worker_id_t worker_id_{-1};

  // A flag that enables/disables sending LOGS_CONFIG_API messages. This is used
  // to delay message sending while the config is being reloaded (since we
  // drop all the caches on config reload).
  bool message_sending_enabled_{false};

  // When set to true, this means we have to change the node we are sending
  // messages to
  bool change_node_id_{true};

  // Version of the structure. Bumped whenever a change of the taret node is
  // requested. Used to avoid duplicate requests to change a node when
  // multiple GetLogInfoRequests run in parallel
  uint64_t current_version_{1};

  // True if the current version got any LOGS_CONFIG_API_REPLY messages
  bool current_version_got_replies_ = false;

  // Limits the number of config reloads in a period of time. This prevents
  // saturating the client workers with target node changes in a rare case
  // when all nodes are unavailable to the client.
  RateLimiter rate_limiter_{rate_limit_t(3, std::chrono::seconds(3))};

  GetLogInfoRequestSharedState();

  // This is called before reloading the config due to connection failures.
  // If this method returns false, the config reload has already been
  // requested by another GetLogInfoRequest and no action is necessary,
  // or the rate limit for target changes has been hit, or no logs
  // configuration information has been received since the last config update,
  // so no config invalidation is necessary
  bool considerReloadingConfig(uint64_t sent_to_version) {
    std::unique_lock<std::mutex> lock(mutex_);
    if (sent_to_version != current_version_) {
      // A config reloading request has already been submitted since the
      // client sent its request. No need to duplicate it again, just return
      // from here so the client can retry with the new setting.
      return false;
    }

    // Too many target changes done already
    if (!rate_limiter_.isAllowed()) {
      return false;
    }

    // We have to choose a new node
    change_node_id_ = true;

    // We should only actually invalidate the config if there was any logs
    // configuration information received since the last notification. If none
    // was received, there's no point in the invalidation.
    bool should_invalidate_config = current_version_got_replies_;

    // Bump version
    ++current_version_;
    current_version_got_replies_ = false;

    if (should_invalidate_config) {
      // Disabling further sends - all the caches will be dropped iwhen the
      // config is reloaded, and we'll likely have to send another request,
      // so it's better to delay the request until the reload finishes.
      message_sending_enabled_ = false;
    }

    return should_invalidate_config;
  }

  bool useReplyFromVersion(uint64_t reply_version) {
    std::unique_lock<std::mutex> lock(mutex_);
    if (reply_version != current_version_) {
      // Do not use stale replies
      return false;
    }
    current_version_got_replies_ = true;
    return true;
  }

  bool useConfigChangedMessageFrom(NodeID node) {
    ld_check(node.isNodeID());
    std::unique_lock<std::mutex> lock(mutex_);
    return node_id_.isNodeID() && node.index() == node_id_.index();
  }
};

}} // namespace facebook::logdevice
