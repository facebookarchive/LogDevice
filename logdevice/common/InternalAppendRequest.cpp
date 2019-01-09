/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/InternalAppendRequest.h"

#include "logdevice/common/AppenderPrep.h"
#include "logdevice/common/MetaDataLogWriter.h"
#include "logdevice/common/Worker.h"

namespace facebook { namespace logdevice {

InternalAppendRequest::~InternalAppendRequest() {
  if (!appender_running_) {
    // Appender was never started but we may have registered the Request
    // with the Worker.
    //
    // TODO maybe make AppendRequestBase auto-unlink?
    auto& runningAppends = Worker::onThisThread()->runningAppends().map;
    auto it = runningAppends.find(id_);
    ld_check(it != runningAppends.end());
    it->second.release(); // avoid double-free
    runningAppends.erase(it);
  }
}

void InternalAppendRequest::onReplyReceived(
    const APPENDED_Header& reply,
    const Address& /*from*/,
    ReplySource /* not used here, used only in e2e tracing */) {
  reply_ = reply;
  if (appender_running_) {
    cb_(reply.status, reply.lsn, reply.redirect, reply.timestamp);
    destroy();
  }
}

folly::Optional<APPENDED_Header>
runInternalAppend(logid_t logid,
                  AppendAttributes attrs,
                  PayloadHolder payload,
                  InternalAppendRequest::Callback callback,
                  APPEND_flags_t flags,
                  int checksum_bits,
                  uint32_t timeout_ms,
                  uint32_t append_message_count,
                  folly::Optional<epoch_t> acceptable_epoch) {
  // Create an InternalAppendRequest, through which Appender will inform us of
  // the outcome of the write; execute() it inline (this registers the Request
  // with the Worker).
  std::unique_ptr<InternalAppendRequest> req(
      new InternalAppendRequest(std::move(callback)));
  request_id_t rqid = req->id_;
  // Need to execute the Request right away so that AppenderPrep::execute()
  // can find it if it needs to deliver a reply
  req->execute();

  APPEND_Header hdr;
  hdr.rqid = rqid;
  hdr.logid = logid;
  hdr.seen = EPOCH_INVALID;
  hdr.timeout_ms = timeout_ms;
  hdr.flags = flags;
  hdr.flags |= appendFlagsForChecksum(checksum_bits);

  std::shared_ptr<AppenderPrep> append_prep =
      std::make_shared<AppenderPrep>(std::move(payload));

  // TODO this is a lie, there is no APPEND message; refactor to be cleaner
  append_prep
      ->setAppendMessage(hdr, LSN_INVALID, ClientID::INVALID, std::move(attrs))
      .disallowBatching()
      .setAppendMessageCount(append_message_count)
      .setAcceptableEpoch(acceptable_epoch)
      .execute();

  if (req->getReply().hasValue()) {
    // Uh oh, AppenderPrep::execute() failed synchronously and invoked the
    // callback with an error code. Return it to the caller.
    return req->getReply();
  }

  // The Appender now lives in the Sequencer and will invoke the callback at a
  // later time.  The BatchedAppendRequest will get deleted then in the
  // callback.
  req->setAppenderRunning();
  req.release();
  return folly::none;
}

folly::Optional<APPENDED_Header>
runInternalAppend(logid_t logid,
                  AppendAttributes attrs,
                  const Payload& payload,
                  InternalAppendRequest::Callback callback,
                  APPEND_flags_t flags,
                  int checksum_bits,
                  uint32_t timeout_ms,
                  uint32_t append_message_count,
                  folly::Optional<epoch_t> acceptable_epoch) {
  return runInternalAppend(logid,
                           std::move(attrs),
                           PayloadHolder(payload, PayloadHolder::UNOWNED),
                           std::move(callback),
                           flags,
                           checksum_bits,
                           timeout_ms,
                           append_message_count,
                           acceptable_epoch);
}

}} // namespace facebook::logdevice
