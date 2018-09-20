/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <unordered_map>

#include "logdevice/common/Request.h"
#include "logdevice/include/Err.h"

/**
 * @file Base class for different type of append state machines:
 *
 * - Append running in a client context, sending an APPEND message over the
 *   network to the sequencer
 * - Append running on a sequencer after batching together original appends
 *
 * The common code registers the append in `Worker::runningAppends()', which
 * allows the request to be looked up after asynchronous operations.
 */

namespace facebook { namespace logdevice {

struct Address;
struct APPENDED_Header;
struct APPEND_PROBE_REPLY_Header;
class AppendRequestBase;

// Wrapper instead of typedef to allow forward-declaring in Worker.h
struct AppendRequestMap {
  std::unordered_map<request_id_t,
                     std::unique_ptr<AppendRequestBase>,
                     request_id_t::Hash>
      map;
};

// Used in e2e tracing when we need to know the source of the reply message
enum class ReplySource : uint16_t {
  APPEND = 0,
  PROBE = 1,
};

class AppendRequestBase : public Request {
 public:
  explicit AppendRequestBase(RequestType type) : Request(type) {}

  ~AppendRequestBase() override {}

  /**
   * Called when an APPENDED message is received.
   *
   * @param reply  header of the APPENDED message received
   * @param from   address of node that sent the reply
   * @param source_type the source of the reply which can be either APPEND or
   * PROBE - used only in e2e tracing
   */
  virtual void
  onReplyReceived(const APPENDED_Header& reply,
                  const Address& from,
                  ReplySource source_type = ReplySource::APPEND) = 0;
  /**
   * Called when an APPEND_PROBE_REPLY message is received.
   */
  virtual void onProbeReply(const APPEND_PROBE_REPLY_Header& reply,
                            const Address& from) = 0;
  /**
   * APPEND_PROBE::onSent() calls this in case of an error.
   */
  virtual void onProbeSendError(Status st, NodeID to) = 0;

 protected:
  // Inserts this AppendRequest to Worker's runningAppends map, transferring
  // ownership.
  //
  // Virtual to allow overriding in tests; production subclasses expected to
  // call, not override.
  virtual void registerRequest();

  // Destroys this AppendRequest object by removing it from Worker's map of
  // running appends.
  //
  // Virtual to allow overriding in tests; production subclasses expected to
  // call, not override.
  virtual void destroy();
};

}} // namespace facebook::logdevice
