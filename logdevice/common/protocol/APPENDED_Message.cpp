/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#define __STDC_FORMAT_MACROS
#include "APPENDED_Message.h"

#include <cstdint>

#include "event2/buffer.h"
#include "logdevice/common/AppendRequestBase.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/protocol/ProtocolReader.h"
#include "logdevice/common/protocol/ProtocolWriter.h"
#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice {

__thread uint32_t APPENDED_Message::last_seq_batching_offset;

const APPENDED_Header& APPENDED_Header::
operator=(const Legacy_APPENDED_Header& legacy) {
  rqid = legacy.rqid;
  lsn = legacy.lsn;
  timestamp = RecordTimestamp::zero();
  redirect = legacy.redirect;
  status = legacy.status;
  flags = legacy.flags;

  return *this;
}

const Legacy_APPENDED_Header& Legacy_APPENDED_Header::
operator=(const APPENDED_Header& hdr) {
  rqid = hdr.rqid;
  lsn = hdr.lsn;
  redirect = hdr.redirect;
  status = hdr.status;
  flags = hdr.flags;

  if (hdr.flags != flags) {
    RATELIMIT_CRITICAL(
        std::chrono::seconds(1),
        10,
        "PROTOCOL ERROR: conversion to legacy header loses data. "
        "flags:0x%x != legacy_flags:0x%x",
        hdr.flags,
        flags);
  }

  return *this;
}

void APPENDED_Message::serialize(ProtocolWriter& writer) const {
  if (writer.proto() >=
      Compatibility::ProtocolVersion::RECORD_TIMESTAMP_IN_APPENDED_MSG) {
    writer.write(header_);
  } else {
    Legacy_APPENDED_Header legacy;
    legacy = header_;
    writer.write(legacy);
  }

  ld_check((header_.flags & APPENDED_Header::INCLUDES_SEQ_BATCHING_OFFSET) ==
           seq_batching_offset.hasValue());
  if (seq_batching_offset.hasValue()) {
    uint32_t offset = seq_batching_offset.value();
    writer.write(offset);
  }
}

MessageReadResult APPENDED_Message::deserialize(ProtocolReader& reader) {
  APPENDED_Header hdr;
  hdr.flags = 0;
  if (reader.proto() >=
      Compatibility::ProtocolVersion::RECORD_TIMESTAMP_IN_APPENDED_MSG) {
    reader.read(&hdr);
  } else {
    Legacy_APPENDED_Header legacy;
    reader.read(&legacy);
    hdr = legacy;
  }

  std::unique_ptr<APPENDED_Message> m(new APPENDED_Message(hdr));

  if (hdr.flags & APPENDED_Header::INCLUDES_SEQ_BATCHING_OFFSET) {
    uint32_t offset;
    reader.read(&offset);
    m->seq_batching_offset = offset;
  }
  return reader.resultMsg(std::move(m));
}

Message::Disposition APPENDED_Message::onReceived(const Address& from) {
  Worker* w = Worker::onThisThread();
  ld_check(w);

  ld_debug("Got an APPENDED message for %s from %s. rqid = %" PRIu64
           ", flags=%u, status = %s",
           lsn_to_string(header_.lsn).c_str(),
           Sender::describeConnection(from).c_str(),
           uint64_t(header_.rqid),
           (uint32_t)header_.flags,
           error_name(header_.status));

  if (from.isClientAddress()) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    10,
                    "PROTOCOL ERROR: got APPENDED message from client %s",
                    Sender::describeConnection(from).c_str());
    err = E::PROTO;
    return Disposition::ERROR;
  }

  if (header_.status == E::PREEMPTED) {
    if (!header_.redirect.isNodeID()) {
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      10,
                      "Received an APPENDED message from %s with PREEMPTED and "
                      "an invalid NodeID",
                      Sender::describeConnection(from).c_str());
      err = E::PROTO;
      return Disposition::ERROR;
    }

    if (header_.redirect == from.id_.node_) {
      // Self-redirects are possible (although very unlikely) if a sequencer
      // successfully sealed the log while it was still running Appenders for a
      // previous epoch.
      RATELIMIT_WARNING(std::chrono::seconds(1),
                        10,
                        "Received an APPENDED message from %s with PREEMPTED "
                        "and a redirect to itself",
                        Sender::describeConnection(from).c_str());
    }
  }

  // Update the thread-local copy of `seq_batching_offset'.  See the header
  // file for an explanation.
  last_seq_batching_offset = seq_batching_offset.value_or(0);

  auto pos = w->runningAppends().map.find(header_.rqid);
  if (pos != w->runningAppends().map.end()) {
    ld_check(pos->second);
    pos->second->onReplyReceived(header_, from, ReplySource::APPEND);
  } else {
    ld_debug("Request id %" PRIu64 " not found in the map of running Append "
             "requests",
             uint64_t(header_.rqid));
  }

  return Disposition::NORMAL;
}

std::vector<std::pair<std::string, folly::dynamic>>
APPENDED_Message::getDebugInfo() const {
  std::vector<std::pair<std::string, folly::dynamic>> res;

  auto flagsToString = [](APPENDED_flags_t flags) {
    folly::small_vector<std::string, 4> strings;
#define FLAG(x)                     \
  if (flags & APPENDED_Header::x) { \
    strings.emplace_back(#x);       \
  }
    FLAG(INCLUDES_SEQ_BATCHING_OFFSET)
    FLAG(NOT_REPLICATED)
    FLAG(REDIRECT_NOT_ALIVE)
#undef FLAG
    return folly::join('|', strings);
  };

  auto add = [&](const char* key, folly::dynamic val) {
    res.emplace_back(key, std::move(val));
  };

  add("rqid", header_.rqid.val());
  add("lsn", lsn_to_string(header_.lsn));
  add("timestamp", header_.timestamp.time_since_epoch().count());
  add("redirect", toString(header_.redirect));
  add("status", error_name(header_.status));
  add("flags", flagsToString(header_.flags));

  if (seq_batching_offset.hasValue()) {
    add("seq_batching_offset", seq_batching_offset.value());
  }

  return res;
}

}} // namespace facebook::logdevice
