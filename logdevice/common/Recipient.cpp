/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/Recipient.h"

#include "logdevice/common/Appender.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/stats/ServerHistograms.h"
#include "logdevice/common/stats/Stats.h"

namespace facebook { namespace logdevice {

void Recipient::SocketClosedCallback::operator()(Status st,
                                                 const Address& name) {
  ld_check(!name.isClientAddress());

  appender_->onSocketClosed(st, shard_, this);
}

void Recipient::SocketClosedCallback::activate() {
  int rv = appender_->registerOnSocketClosed(shard_.asNodeID(), *this);
  ld_check(rv == 0);
}

void Recipient::ResendStoreCallback::operator()(FlowGroup&, std::mutex&) {
  ld_check(msg_);
  ld_check(set_message_ts_.time_since_epoch().count());
  HISTOGRAM_ADD(
      Worker::stats(), store_bw_wait_latency, usec_since(set_message_ts_));
  appender_->sendDeferredSTORE(std::move(msg_), shard_);
}

void Recipient::ResendStoreCallback::cancelled(Status st) {
  ld_check(msg_);
  appender_->onDeferredSTORECancelled(std::move(msg_), shard_, st);
}

void Recipient::setState(State state) {
  ld_check(state != State::REQUEST_PENDING);
  state_ = state;

  // The following logic ensures that a SocketClosedCallback is activated while
  // state_ == State::OUTSTANDING.
  if (state_ == State::OUTSTANDING) {
    ld_check(!on_socket_closed_.active());
    on_socket_closed_.activate();
  } else if (outcomeKnown()) {
    on_socket_closed_.deactivate();
  }
}

bool Recipient::requestPending() const {
  return state_ == State::REQUEST_PENDING;
}

bool Recipient::failed() const {
  return state_ != State::REQUEST_PENDING && state_ != State::STORED &&
      state_ != State::OUTSTANDING;
}

bool Recipient::stored() const {
  return state_ == State::STORED;
}

const char* Recipient::reasonString(State reason) {
  static_assert((int)State::Count == 14,
                "please update Recipient::reasonString() after updating "
                "Recipient::State");
  switch (reason) {
    case State::REQUEST_PENDING:
      return "REQUEST_PENDING";
    case State::OUTSTANDING:
      return "OUTSTANDING";
    case State::STORED:
      return "STORED";
    case State::NO_SPC:
      return "NO SPACE";
    case State::SEND_FAILED:
      return "SEND FAILED";
    case State::SOCK_CLOSE:
      return "PEER CLOSED";
    case State::SHUTDOWN:
      return "SHUTDOWN";
    case State::STORE_FAILED:
      return "STORE FAILED";
    case State::STORE_DISABLED:
      return "DISABLED";
    case State::SOMEONE_IS_REBUILDING:
      return "SOMEONE IS REBUILDING";
    case State::DROPPED:
      return "DROPPED";
    case State::SOFT_PREEMPTED:
      return "SOFT PREEMPTED";
    case State::PREEMPTED:
      return "PREEMPTED";
    case State::CHECKSUM_MISMATCH:
      return "CHECKSUM_MISMATCH";
    default:
      return "UNKNOWN";
  }
}

}} // namespace facebook::logdevice
