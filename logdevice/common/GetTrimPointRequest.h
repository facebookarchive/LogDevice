/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "logdevice/common/Request.h"
#include "logdevice/common/RequestType.h"
#include "logdevice/common/Timer.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/protocol/GET_TRIM_POINT_Message.h"
#include "logdevice/common/protocol/GET_TRIM_POINT_REPLY_Message.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

class GetTrimPointRequest;
class Sequencer;

// Wrapper instead of typedef to allow forward-declaring in Worker.h
struct GetTrimPointRequestMap {
  std::unordered_map<logid_t,
                     std::unique_ptr<GetTrimPointRequest>,
                     logid_t::Hash>
      map;
};

class GetTrimPointRequest : public Request {
 public:
  explicit GetTrimPointRequest(logid_t log_id,
                               std::chrono::seconds request_interval)
      : Request(RequestType::GET_TRIM_POINT),
        log_id_(log_id),
        request_interval_(request_interval) {
    ld_check(log_id_ != LOGID_INVALID);
  }

  Execution execute() override;

  /**
   * Called by the messaging layer after it successfully sends out our
   * GET_TRIM_POINT message, or fails to do so.
   */
  void onMessageSent(ShardID to, Status status);

  /**
   * Called when we receive a GET_TRIM_POINT_REPLY message from a storage node.
   */
  void onReply(ShardID from, Status status, lsn_t trim_point);

  /**
   * Called when request timer fires
   */
  void onRequestTimeout();

  /**
   * decide which thread to run this request on, derived from Request
   */
  int getThreadAffinity(int nthreads) override;

  logid_t getLogID() const {
    return log_id_;
  }

  ~GetTrimPointRequest() override {}

 private:
  const logid_t log_id_;
  const std::chrono::milliseconds request_interval_;
  std::unique_ptr<Timer> request_timer_;

 protected:
  /**
   * Construct a GET_TRIM_POINT_Message and send it to given shard.
   */
  virtual void sendTo(ShardID shard);

  /**
   * start request timer
   */
  virtual void start();

  virtual std::shared_ptr<const configuration::nodes::NodesConfiguration>
  getNodesConfiguration() const;

  virtual void updateTrimPoint(Status status, lsn_t trim_point) const;

  /**
   * get current sequencer
   */
  std::shared_ptr<Sequencer> getSequencer() const;
};

}} // namespace facebook::logdevice
