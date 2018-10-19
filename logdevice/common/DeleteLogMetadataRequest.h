/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <set>

#include <folly/Conv.h>
#include <folly/SharedMutex.h>

#include "logdevice/common/FireAndForgetRequest.h"
#include "logdevice/common/Metadata.h"
#include "logdevice/common/Request.h"
#include "logdevice/common/RequestType.h"
#include "logdevice/common/ShardID.h"
#include "logdevice/common/Timer.h"
#include "logdevice/common/protocol/DELETE_LOG_METADATA_Message.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

class ClientImpl;

/**
 * @file DeleteLogMetadataRequest sends `DELETE_LOG_METADATA_Message`s to a
 * subset of shards in the cluster (typically all of them), deleting a certain
 * type of metadata in a range of log ids. Then, it waits for the
 * corresponding `DELETE_LOG_METADATA_REPLY_Message`s from the shards or a
 * timeout event (whichever comes first), and it calls the `callback_` to
 * inform its user of the obtained results.
 *
 * This request was initially intended to be triggered by the
 * `delete-log-metadata` command in ldshell.
 */

class DeleteLogMetadataRequest : public FireAndForgetRequest {
 public:
  using Callback = std::function<void(
      const std::set<ShardID>& /* shards that didn't answer */,
      const std::set<ShardID>& /* shards that failed to delete */
      )>;

  /**
   *  create a DeleteLogMetadataRequest
   *
   *  @param first_log_id       first log id to consider the range
   *  @param last_log_id        last log id to consider the range
   *  @param log_metadata_type  type of log metadata to delete
   *  @param shards             shards to send deletion request to
   *  @param callback           callback to report nodes that didn't respond
   */
  DeleteLogMetadataRequest(logid_t first_log_id,
                           logid_t last_log_id,
                           LogMetadataType log_metadata_type,
                           const std::set<ShardID>& shards,
                           Callback* callback);

  std::string describe() const override {
    return folly::to<std::string>("DeleteLogMetadataRequest(",
                                  first_log_id_.val_,
                                  ", ",
                                  last_log_id_.val_,
                                  ", ",
                                  static_cast<int32_t>(log_metadata_type_),
                                  ")");
  }

  void executionBody() override;
  void onReply(ShardID sid, Status status);

 private:
  void initTimer();
  void sendMessages();
  void onTimeout();
  void finalize();

  const logid_t first_log_id_;
  const logid_t last_log_id_;
  const LogMetadataType log_metadata_type_;
  const Callback* callback_;

  std::set<ShardID> destShards_;
  std::set<ShardID> recvFrom_;
  std::set<ShardID> successfulDels_;

  std::atomic<int> numPendingReplies_;

  std::unique_ptr<Timer> request_timer_;
};

}} // namespace facebook::logdevice
