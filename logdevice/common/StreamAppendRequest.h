/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/AppendRequest.h"

namespace facebook { namespace logdevice {

/**
 *  @file StreamAppendRequest is a Request that attempts to append a record
 * to a log as part of a stream, sending an APPEND message over the network
 * to the sequencer additionally providing the ordering semantics.
 */
class StreamAppendRequest : public AppendRequest {
 public:
  /**
   * Constructor used by clients to submit an original StreamAppendRequest to a
   * Processor. Mostly a wrapper around AppendRequest.
   *
   * @param client  ClientBridge to make write token checks through, may be
   *                nullptr if bypassWriteTokenCheck() is called
   * @param logid   log to append the record to
   * @param payload record payload
   * @param timeout cancel the request and report E::TIMEDOUT to client
   *                if a reply is not received for this many milliseconds
   * @param callback functor to call when a reply is received or on timeout
   * @param stream_rqid Request id that contains stream id and sequence number
   *                    of the request within the stream.
   * @param stream_resume Denotes if this is the first message in the stream for
   *                      the current epoch.
   */
  StreamAppendRequest(ClientBridge* client,
                      logid_t logid,
                      AppendAttributes attrs,
                      const Payload& payload,
                      std::chrono::milliseconds timeout,
                      append_callback_t callback,
                      write_stream_request_id_t stream_rqid,
                      bool stream_resume = false)
      : AppendRequest(client,
                      logid,
                      std::move(attrs),
                      payload,
                      timeout,
                      std::move(callback)),
        stream_rqid_(stream_rqid),
        stream_resume_(stream_resume) {}

  /**
   * Constructor used by clients to submit an original StreamAppendRequest to a
   * Processor. Mostly a wrapper around AppendRequest.
   *
   * @param client  ClientBridge to make write token checks through, may be
   *                nullptr if bypassWriteTokenCheck() is called
   * @param logid   log to append the record to
   * @param payload record payload which becomes owned by append request and
   *                will automatically be released after calling callback.
   * @param timeout cancel the request and report E::TIMEDOUT to client
   *                if a reply is not received for this many milliseconds
   * @param callback functor to call when a reply is received or on timeout
   * @param stream_rqid Request id that contains stream id and sequence number
   *                    of the request within the stream.
   * @param stream_resume Denotes if this is the first message in the stream for
   *                      the current epoch.
   */
  StreamAppendRequest(ClientBridge* client,
                      logid_t logid,
                      AppendAttributes attrs,
                      std::string payload,
                      std::chrono::milliseconds timeout,
                      append_callback_t callback,
                      write_stream_request_id_t stream_rqid,
                      bool stream_resume = false)
      : AppendRequest(client,
                      logid,
                      std::move(attrs),
                      std::move(payload),
                      timeout,
                      std::move(callback)),
        stream_rqid_(stream_rqid),
        stream_resume_(stream_resume) {}

 protected:
  // tests can override
  StreamAppendRequest(ClientBridge* client,
                      logid_t logid,
                      AppendAttributes attrs,
                      const Payload& payload,
                      std::chrono::milliseconds timeout,
                      append_callback_t callback,
                      std::unique_ptr<SequencerRouter> router,
                      write_stream_request_id_t stream_rqid,
                      bool stream_resume)
      : AppendRequest(client,
                      logid,
                      std::move(attrs),
                      payload,
                      timeout,
                      std::move(callback),
                      std::move(router)),
        stream_rqid_(stream_rqid),
        stream_resume_(stream_resume) {}

  // overriding to include additional flags for stream info
  APPEND_flags_t getAppendFlags() override;

  // overriding to include stream_request_id in APPEND_Message
  std::unique_ptr<APPEND_Message> createAppendMessage() override;

 private:
  // contains stream id and the sequence number of request in the stream.
  write_stream_request_id_t stream_rqid_;

  // denotes if the request is the first in the stream for the epoch. This value
  // is used to set APPEND_Header::STREAM_RESUME flag on the append message.
  bool stream_resume_;
};
}} // namespace facebook::logdevice
