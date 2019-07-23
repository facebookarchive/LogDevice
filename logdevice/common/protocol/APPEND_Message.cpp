/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#define __STDC_FORMAT_MACROS
#include "logdevice/common/protocol/APPEND_Message.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <cstdint>
#include <limits>
#include <memory>
#include <stdlib.h>

#include <folly/stats/MultiLevelTimeSeries.h>

#include "logdevice/common/AppendRequest.h"
#include "logdevice/common/AppenderPrep.h"
#include "logdevice/common/Checksum.h"
#include "logdevice/common/MetaDataLogWriter.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/plugin/OpenTracerFactory.h"
#include "logdevice/common/plugin/PluginRegistry.h"
#include "logdevice/common/protocol/ProtocolReader.h"
#include "logdevice/common/protocol/ProtocolWriter.h"
#include "logdevice/common/stats/Stats.h"

namespace facebook { namespace logdevice {

void APPEND_Message::serialize(ProtocolWriter& writer) const {
  ld_check(payload_.valid());

  // Only write the header without any flags corresponding to stream writer if
  // protocol does not support it.

  // Checking some conditions on stream requests.
  // WRITE_STREAM_RESUME -> WRITE_STREAM_REQUEST.
  // WRITE_STREAM_REQUEST <-> write_stream_request_id is valid.
  if (header_.flags & APPEND_Header::WRITE_STREAM_RESUME) {
    ld_check((header_.flags & APPEND_Header::WRITE_STREAM_REQUEST) > 0);
  }
  ld_check_eq((header_.flags & APPEND_Header::WRITE_STREAM_REQUEST) > 0,
              write_stream_request_id_valid(write_stream_request_id_));
  APPEND_Header proto_supported_header(header_);
  if (writer.proto() < Compatibility::ProtocolVersion::STREAM_WRITER_SUPPORT) {
    proto_supported_header.flags &= ~(APPEND_Header::WRITE_STREAM_REQUEST |
                                      APPEND_Header::WRITE_STREAM_RESUME);
  }
  writer.write(proto_supported_header);
  if (header_.flags & APPEND_Header::LSN_BEFORE_REDIRECT) {
    writer.write(lsn_before_redirect_);
  }

  if (header_.flags & APPEND_Header::CUSTOM_KEY) {
    uint8_t optional_keys_length = attrs_.optional_keys.size();
    writer.write(optional_keys_length);
    for (const auto& key_pair : attrs_.optional_keys) {
      uint8_t type = static_cast<uint8_t>(key_pair.first);
      ld_check(type <= std::numeric_limits<uint8_t>::max());
      writer.write(type);
      ld_check(key_pair.second.length() <=
               std::numeric_limits<uint16_t>::max());
      writer.writeLengthPrefixedVector(key_pair.second);
    }
  }

  ld_check(bool(header_.flags & APPEND_Header::CUSTOM_COUNTERS) ==
           attrs_.counters.hasValue());
  if (header_.flags & APPEND_Header::CUSTOM_COUNTERS) {
    ld_check(attrs_.counters->size() <= std::numeric_limits<uint8_t>::max());
    uint8_t counters_length = attrs_.counters->size();
    writer.write(counters_length);

    for (auto counter : attrs_.counters.value()) {
      writer.write(counter.first);
      writer.write(counter.second);
    }
  }

  if (header_.flags & APPEND_Header::E2E_TRACING_ON) {
    ld_check(e2e_tracing_context_.size() < MAX_E2E_TRACING_CONTEXT_SIZE);
    if (e2e_tracing_context_.size() < MAX_E2E_TRACING_CONTEXT_SIZE) {
      // write tracing information
      writer.writeLengthPrefixedVector(e2e_tracing_context_);
    } else {
      writer.writeLengthPrefixedVector(std::string(""));
    }
  }

  // Inserting stream request information just before checksum and payload.
  if (writer.proto() >= Compatibility::ProtocolVersion::STREAM_WRITER_SUPPORT) {
    if (header_.flags & APPEND_Header::WRITE_STREAM_REQUEST) {
      writer.write(
          &write_stream_request_id_, sizeof(write_stream_request_id_t));
    }
  }

  // If we are supposed to checksum the payload, calculate it now and inject the
  // checksum at the front of the payload.
  if (header_.flags & APPEND_Header::CHECKSUM) {
    int checksum_bits =
        (header_.flags & APPEND_Header::CHECKSUM_64BIT) ? 64 : 32;
    if (writer.isBlackHole()) {
      // no need to checksum anything, just add the appropriate number of bytes
      writer.write(nullptr, checksum_bits / 8);
    } else {
      // The const_cast here is needed because getPayload() is not necessarily
      // physically constant (it may linearize the evbuffer) but is logically
      // constant.  APPEND message sending is not a tricky multithreaded
      // context (this worker created the PayloadHolder) so this is fine.
      Payload payload = const_cast<PayloadHolder&>(payload_).getPayload();
      char buf[8];
      Slice chkblob = checksum_bytes(Slice(payload), checksum_bits, buf);
      writer.write(chkblob.data, chkblob.size);
    }
  }

  payload_.serialize(writer);
}

void APPEND_Message::onSent(Status st, const Address& to) const {
  Message::onSent(st, to);
  if (st == E::OK) {
    return;
  }

  // notify AppendRequest of failure so that it can quickly unblock the client

  Worker* w = Worker::onThisThread();
  ld_check(w);

  auto pos = w->runningAppends().map.find(header_.rqid);
  if (pos != w->runningAppends().map.end()) {
    ld_check(pos->second);
    checked_downcast<AppendRequest*>(pos->second.get())
        ->noReply(st, to, /*request_sent=*/false);
    // the above lines will remove AppendRequest at pos->second from
    // Worker::runningAppends() map and call its destructor, which will call
    // that request SocketClosedCallback's destructor, which will unregister
    // the callback so that it's never called. Simple, right?
  } else {
    ld_debug("Request id %" PRIu64 " not found in the map of running Append "
             "requests. Send status was %s.",
             uint64_t(header_.rqid),
             error_description(st));
  }
}

MessageReadResult APPEND_Message::deserialize(ProtocolReader& reader) {
  return deserialize(reader, Worker::settings().max_payload_inline);
}

MessageReadResult APPEND_Message::deserialize(ProtocolReader& reader,
                                              size_t max_payload_inline) {
  APPEND_Header header;
  header.flags = 0;
  reader.read(&header);

  lsn_t lsn_before_redirect{LSN_INVALID};
  if (header.flags & APPEND_Header::LSN_BEFORE_REDIRECT) {
    reader.read(&lsn_before_redirect);
  }

  AppendAttributes attrs;

  if (header.flags & APPEND_Header::CUSTOM_KEY) {
    uint8_t optional_keys_length;
    reader.read(&optional_keys_length);
    for (uint8_t i = 0; i < optional_keys_length; ++i) {
      if (!reader.ok()) {
        break;
      }
      uint8_t type;
      std::string str;
      reader.read(&type);
      reader.readLengthPrefixedVector(&str);
      attrs.optional_keys.insert(
          std::make_pair(static_cast<KeyType>(type), str));
    }
  }

  if (header.flags & APPEND_Header::CUSTOM_COUNTERS) {
    uint8_t counters_length;
    reader.read(&counters_length);
    attrs.counters.emplace();
    for (uint8_t i = 0; i < counters_length; ++i) {
      if (!reader.ok()) {
        break;
      }
      uint8_t key;
      int64_t counter;
      reader.read(&key);
      reader.read(&counter);
      attrs.counters->emplace(key, counter);
    }
  }

  std::string tracing_info;

  if (header.flags & APPEND_Header::E2E_TRACING_ON) {
    reader.readLengthPrefixedVector(&tracing_info);
  }

  write_stream_request_id_t req_id = WRITE_STREAM_REQUEST_ID_INVALID;
  if (reader.proto() >= Compatibility::ProtocolVersion::STREAM_WRITER_SUPPORT) {
    if (header.flags & APPEND_Header::WRITE_STREAM_REQUEST) {
      reader.read(&req_id);
    }
  }

  size_t payload_size = reader.bytesRemaining();
  ld_check(payload_size < Message::MAX_LEN);
  PayloadHolder ph = PayloadHolder::deserialize(
      reader,
      payload_size,
      /*zero_copy*/ payload_size > max_payload_inline);

  return reader.result([&] {
    return new APPEND_Message(header,
                              lsn_before_redirect,
                              std::move(attrs),
                              std::move(ph),
                              std::move(tracing_info),
                              req_id);
  });
}

Message::Disposition APPEND_Message::onReceived(const Address& from) {
  // Track the client-supplied payload size in a stat
  ssize_t payload_size = payload_.size();
  if (header_.flags & APPEND_Header::CHECKSUM) {
    payload_size -= (header_.flags & APPEND_Header::CHECKSUM_64BIT) ? 8 : 4;
  }
  StatsHolder* stats = Worker::stats();
  STAT_INCR(stats, append_received);
  STAT_ADD(stats, append_payload_bytes, payload_size);
  // Bump the per-log-group stats
  if (auto log_path = Worker::getConfig()->getLogGroupPath(header_.logid)) {
    LOG_GROUP_TIME_SERIES_ADD(
        stats, append_in_bytes, log_path.value(), payload_size);
    if (attrs_.counters.hasValue()) {
      LOG_GROUP_CUSTOM_COUNTERS_ADD(
          stats, log_path.value(), attrs_.counters.value());
    }
  }
  WORKER_LOG_STAT_ADD(header_.logid, append_payload_bytes, payload_size);

  std::shared_ptr<opentracing::Tracer> e2e_tracer;
  std::unique_ptr<opentracing::Span> append_message_receive_span;
  if (!e2e_tracing_context_.empty()) {
    auto ot_plugin = Worker::onThisThread()
                         ->processor_->getPluginRegistry()
                         ->getSinglePlugin<OpenTracerFactory>(
                             PluginType::OPEN_TRACER_FACTORY);
    if (ot_plugin) {
      e2e_tracer = ot_plugin->createOTTracer();

      ld_check(e2e_tracer);
      // receiving a non empty tracing context means that we should create
      // tracing spans
      std::stringstream sstream(e2e_tracing_context_);
      auto extracted_context_ = e2e_tracer->Extract(sstream);

      append_message_receive_span = e2e_tracer->StartSpan(
          "APPEND_Message_receive", {ChildOf(extracted_context_->get())});
      append_message_receive_span->SetTag("source_client_id", from.toString());
      append_message_receive_span->SetTag(
          "lsn_before_redirect", lsn_before_redirect_);
    }
  }

  std::shared_ptr<AppenderPrep> append_prep =
      std::make_shared<AppenderPrep>(std::move(payload_));
  append_prep->setAppendMessage(header_,
                                lsn_before_redirect_,
                                from.asClientID(),
                                std::move(attrs_),
                                std::move(e2e_tracer),
                                std::move(append_message_receive_span));
  append_prep->execute();

  return Disposition::NORMAL;
}

APPEND_flags_t appendFlagsForChecksum(int checksum_bits) {
  APPEND_flags_t flags = 0;
  if (checksum_bits > 0) {
    flags |= APPEND_Header::CHECKSUM;
    if (checksum_bits == 64) {
      flags |= APPEND_Header::CHECKSUM_64BIT;
    } else {
      ld_check(checksum_bits == 32);
    }
  }

  // CHECKSUM_PARITY should be the XNOR of the other two bits.  XNOR ensures
  // that at least one bit is always set so that we can detect the case where
  // the entire flags byte gets zeroed out due to a code issue.
  if (bool(flags & APPEND_Header::CHECKSUM) ==
      bool(flags & APPEND_Header::CHECKSUM_64BIT)) {
    flags |= APPEND_Header::CHECKSUM_PARITY;
  }

  return flags;
}

bool APPEND_Message::cancelled() const {
  auto& runningAppends = Worker::onThisThread()->runningAppends().map;
  auto it = runningAppends.find(header_.rqid);

  // the message is cancelled if the append request is destroyed
  return (it == runningAppends.end());
}

std::vector<std::pair<std::string, folly::dynamic>>
APPEND_Message::getDebugInfo() const {
  std::vector<std::pair<std::string, folly::dynamic>> res;

  auto flagsToString = [](APPEND_flags_t flags) {
    folly::small_vector<std::string, 4> strings;
#define FLAG(x)                   \
  if (flags & APPEND_Header::x) { \
    strings.emplace_back(#x);     \
  }
    FLAG(LSN_BEFORE_REDIRECT)
    FLAG(CHECKSUM)
    FLAG(CHECKSUM_64BIT)
    FLAG(CHECKSUM_PARITY)
    FLAG(NO_REDIRECT)
    FLAG(REACTIVATE_IF_PREEMPTED)
    FLAG(BUFFERED_WRITER_BLOB)
    FLAG(CUSTOM_KEY)
    FLAG(NO_ACTIVATION)
    FLAG(CUSTOM_COUNTERS)
#undef FLAG
    return folly::join('|', strings);
  };

  auto add = [&](const char* key, folly::dynamic val) {
    res.emplace_back(key, std::move(val));
  };
  add("rqid", header_.rqid.val());
  add("log_id", toString(header_.logid));
  add("epoch_seen", header_.seen.val());
  add("client_timeout_ms", header_.timeout_ms);
  add("flags", flagsToString(header_.flags));
  add("lsn_before_redirect", lsn_to_string(lsn_before_redirect_));
  add("payload_size", payload_.size());
  if (!attrs_.optional_keys.empty()) {
    folly::dynamic map{folly::dynamic::object()};
    for (auto& kv : attrs_.optional_keys) {
      map[toString(kv.first)] = kv.second;
    }
    add("optional_keys", std::move(map));
  }
  if (attrs_.counters.hasValue() && !attrs_.counters->empty()) {
    folly::dynamic map{folly::dynamic::object()};
    for (auto& kv : attrs_.counters.value()) {
      map[toString(kv.first)] = kv.second;
    }
    add("counters", std::move(map));
  }
  return res;
}

}} // namespace facebook::logdevice
