/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/AppendRequestBase.h"
#include "logdevice/common/Timestamp.h"
#include "logdevice/common/protocol/APPENDED_Message.h"
#include "logdevice/common/protocol/APPEND_Message.h"

namespace facebook { namespace logdevice {

/**
 *  @file InternalAppendRequest is type of AppendRequest that was generated
 *  locally by logdevice code running on the sequencer node, not on the client.
 *  It doesn't deal with APPEND/APPENDED messages, sequencer selection, probing,
 *  and all the other stuff that AppendRequest has to deal with since it is not
 *  colocated with the sequencer.
 */

class InternalAppendRequest : public AppendRequestBase {
 public:
  using Callback = std::function<void(Status, lsn_t, NodeID, RecordTimestamp)>;
  explicit InternalAppendRequest(Callback cb)
      : AppendRequestBase(RequestType::INTERNAL_APPEND), cb_(std::move(cb)) {}

  ~InternalAppendRequest() override;

  Execution execute() override {
    registerRequest();
    return Execution::CONTINUE;
  }

  void setAppenderRunning() {
    appender_running_ = true;
  }

  void onReplyReceived(const APPENDED_Header& reply,
                       const Address& from,
                       ReplySource source_type) override;

  // This class doesn't send probes
  void onProbeSendError(Status, NodeID) override {
    ld_check(false);
  }
  void onProbeReply(const APPEND_PROBE_REPLY_Header& /*reply*/,
                    const Address& /*from*/) override {
    ld_check(false);
  }

  folly::Optional<APPENDED_Header> getReply() const {
    return reply_;
  }

 private:
  Callback cb_;
  bool appender_running_ = false;
  folly::Optional<APPENDED_Header> reply_;
};

// Creates and starts an InternalAppendRequest. Returns a reply if one is
// received synchronously from AppenderPrep (this is typically due to an error)
// or folly::none if the request was scheduled asynchronously (typical
// behaviour)
folly::Optional<APPENDED_Header>
runInternalAppend(logid_t logid,
                  AppendAttributes attrs,
                  PayloadHolder payload,
                  InternalAppendRequest::Callback callback,
                  APPEND_flags_t flags,
                  int checksum_bits,
                  uint32_t timeout_ms,
                  // Passed through to AppenderPrep::setMessageCount():
                  uint32_t append_message_count = 1,
                  folly::Optional<epoch_t> acceptable_epoch = folly::none);

// overload of the above function but the caller owns the payload and needs to
// ensure that the payload remain available until the append completes
folly::Optional<APPENDED_Header>
runInternalAppend(logid_t logid,
                  AppendAttributes attrs,
                  const Payload& payload,
                  InternalAppendRequest::Callback callback,
                  APPEND_flags_t flags,
                  int checksum_bits,
                  uint32_t timeout_ms,
                  // Passed through to AppenderPrep::setMessageCount():
                  uint32_t append_message_count = 1,
                  folly::Optional<epoch_t> acceptable_epoch = folly::none);

}} // namespace facebook::logdevice
