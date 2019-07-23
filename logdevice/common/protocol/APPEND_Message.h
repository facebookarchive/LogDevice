/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <cstdint>
#include <string>

#include <folly/Optional.h>

#include "logdevice/common/PayloadHolder.h"
#include "logdevice/common/Request.h"
#include "logdevice/common/protocol/Message.h"
#include "logdevice/include/Record.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

// type of APPEND_Header::flags field
typedef uint32_t APPEND_flags_t;

struct APPEND_Header {
  request_id_t rqid; // id of the AppendRequest to deliver reply to
  logid_t logid;     // id of the log to append this record to

  // The highest epoch number seen by this client for this log so far.
  // If this APPEND somehow gets routed to a Sequencer whose epoch number
  // is smaller than .seen, the Sequencer must check EpochStore for the
  // current epoch number and fail the request with Status::PREEMPTED.
  // This field is currently unused (always set to EPOCH_INVALID). When
  // we migrate to a gossip-based failure detector we will use this field
  // to guarantee that LSNs assigned to consecutive records appended to
  // the same log by the same client thread always increase monotonically.
  epoch_t seen;

  uint32_t timeout_ms;  // client timeout in milliseconds
  APPEND_flags_t flags; // bitset of flag constants defined below

  // Value 1 is available for use

  // If this is a redirection of an append previously sent to another sequencer
  // but preempted there, send that LSN to help with avoiding silent duplicates.
  // The LSN follows the header on the wire.
  static constexpr APPEND_flags_t LSN_BEFORE_REDIRECT = 1u << 1; //   2
  // If set in .flags, the payload is prefixed with a checksum.  The length of
  // the checksum depends on the CHECKSUM_64BIT flag; if it is set, the
  // checksum is 64 bits, otherwise 32 bits.
  static constexpr APPEND_flags_t CHECKSUM = 1u << 2;       //    4
  static constexpr APPEND_flags_t CHECKSUM_64BIT = 1u << 3; //    8
  // XNOR of the other two checksum bits.  Ensures that single-bit corruptions
  // in the checksum bits (or flags getting zeroed out) get detected.
  static constexpr APPEND_flags_t CHECKSUM_PARITY = 1u << 4; //   16
  // If set in .flags, a sequencer node that receives the message will make an
  // attempt to process it, possibly (re)activating a sequencer, even if it
  // would otherwise send a redirect.
  static constexpr APPEND_flags_t NO_REDIRECT = 1u << 5; //   32
  // Indicates that a sequencer node that receives the message should reactivate
  // a sequencer object if the existing one was preempted.
  static constexpr APPEND_flags_t REACTIVATE_IF_PREEMPTED = 1u << 6; //   64
  // Append contains a blob composed by BufferedWriter
  static constexpr APPEND_flags_t BUFFERED_WRITER_BLOB = 1u << 7; //  128
  // Append contains a custom key provided by the client.
  static constexpr APPEND_flags_t CUSTOM_KEY = 1u << 8; //  256
  // Don't activate a sequencer if it's unavailable/preempted. Currently only
  // used by internal appends.
  static constexpr APPEND_flags_t NO_ACTIVATION = 1u << 9; //  512

  static constexpr APPEND_flags_t CUSTOM_COUNTERS = 1u << 10; // 1024

  static constexpr APPEND_flags_t E2E_TRACING_ON = 1u << 11; // 2048
  // Append request belongs to a stream.
  static constexpr APPEND_flags_t WRITE_STREAM_REQUEST = 1u << 12; // 4096
  // Used by stream writer to denote that the append message is next in
  // stream and can be accepted unconditionally by a sequencer.
  static constexpr APPEND_flags_t WRITE_STREAM_RESUME = 1u << 13; // 8192

  static constexpr APPEND_flags_t FORCE = NO_REDIRECT | REACTIVATE_IF_PREEMPTED;
} __attribute__((__packed__));

class APPEND_Message : public Message {
 public:
  APPEND_Message(const APPEND_Header& header,
                 lsn_t lsn_before_redirect,
                 AppendAttributes attrs,
                 PayloadHolder payload,
                 std::string e2e_tracing_context = "")
      : Message(MessageType::APPEND, TrafficClass::APPEND),
        header_(header),
        lsn_before_redirect_(lsn_before_redirect),
        attrs_(std::move(attrs)),
        payload_(std::move(payload)),
        e2e_tracing_context_(std::move(e2e_tracing_context)) {}

  APPEND_Message(const APPEND_Header& header,
                 lsn_t lsn_before_redirect,
                 AppendAttributes attrs,
                 PayloadHolder payload,
                 std::string e2e_tracing_context,
                 write_stream_request_id_t req_id)
      : Message(MessageType::APPEND, TrafficClass::APPEND),
        header_(header),
        lsn_before_redirect_(lsn_before_redirect),
        attrs_(std::move(attrs)),
        payload_(std::move(payload)),
        e2e_tracing_context_(std::move(e2e_tracing_context)),
        write_stream_request_id_(req_id) {}

  APPEND_Message(APPEND_Message&&) = delete;
  APPEND_Message(const APPEND_Message&) = delete;
  APPEND_Message& operator=(const APPEND_Message&) = delete;
  APPEND_Message& operator=(APPEND_Message&&) = delete;

  ~APPEND_Message() override {}

  // see Message.h
  void serialize(ProtocolWriter& writer) const override;
  void onSent(Status st, const Address& to) const override;
  Disposition onReceived(const Address& from) override;
  bool cancelled() const override;

  static Message::deserializer_t deserialize;

  int8_t getExecutorPriority() const override {
    return folly::Executor::HI_PRI;
  }

  // Overload of deserialize that does not need to run in an EventLoop
  // context
  static MessageReadResult deserialize(ProtocolReader&,
                                       size_t max_payload_inline);

  const APPEND_Header header_;

  virtual std::vector<std::pair<std::string, folly::dynamic>>
  getDebugInfo() const override;

 private:
  // If this is a redirection of an append previously sent to another sequencer,
  // this is the LSN generated there.  Used for removing silent duplicates.
  lsn_t lsn_before_redirect_;

  // Additional append attributes
  AppendAttributes attrs_;

  // Message payload stored either in a flat buffer or in an evbuffer,
  // depending on how the message object was constructed and the
  // payload size.
  // Owned either by us or by AppendRequest.
  PayloadHolder payload_;

  // Need a serialization of the tracing information gathered so far
  std::string e2e_tracing_context_;

  // Write stream request id
  write_stream_request_id_t write_stream_request_id_ =
      WRITE_STREAM_REQUEST_ID_INVALID;

  friend class ChecksumTest;
  friend class MessageSerializationTest;
  friend class E2ETracingSerializationTest;
  friend class MockStreamAppendRequest;
};

/**
 * Calculates the bitmask of flags that should be set in APPEND_Header::flags
 * for the given number of checksum bits (which typically comes from
 * `Settings::checksum_bits').
 */
APPEND_flags_t appendFlagsForChecksum(int checksum_bits);

}} // namespace facebook::logdevice
