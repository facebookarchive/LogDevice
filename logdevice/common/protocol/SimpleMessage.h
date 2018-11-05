/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <string>
#include <type_traits>

#include "logdevice/common/configuration/TrafficClass.h"
#include "logdevice/common/protocol/Message.h"
#include "logdevice/common/protocol/MessageType.h"
#include "logdevice/common/protocol/ProtocolReader.h"
#include "logdevice/common/protocol/ProtocolWriter.h"
#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice {

/**
 * @file  a template class for all messages that consist of a fixed-size header
 *        and an optional variable size blob.
 *
 * @param Header        header class. Must be a trivially copyable type.
 * @param MESSAGE_TYPE  a constant of type logdevice::MessageType for
 *                      the type of message represented by this class
 * @param include_blob  set to true if the message will have a variable-size
 *                      blob.
 */

template <class Header,
          MessageType MESSAGE_TYPE,
          TrafficClass TRAFFIC_CLASS,
          bool include_blob = true>
class SimpleMessage : public Message {
  typedef uint32_t blob_size_t;
  // TODO: uncomment this whenever gcc ships it.
  // static_assert(std::is_trivially_copyable<Header>::value,
  //               "Header must be a trivially copyable type");
 public:
  explicit SimpleMessage(const Header& header, TrafficClass tc = TRAFFIC_CLASS)
      : Message(MESSAGE_TYPE, tc), header_(header) {}

  // This constructor is only available if include_blob = true
  template <class String,
            bool B = include_blob,
            typename std::enable_if<B, int>::type = 0>
  explicit SimpleMessage(const Header& header,
                         String&& blob,
                         TrafficClass tc = TRAFFIC_CLASS)
      : Message(MESSAGE_TYPE, tc),
        header_(header),
        blob_(std::forward<String>(blob)) {}

  void serialize(ProtocolWriter& writer) const override;

  static MessageReadResult deserialize(ProtocolReader& reader);

  // See Message.h. MUST be specialized in every template instantiation.
  Disposition onReceived(const Address& from) override;

  // See Message.h. MAY be specialized if the instantiation needs to
  // process onSent() events.
  void onSent(Status st, const Address& to) const override {
    Message::onSent(st, to);
  }
  // See Message.h. MAY be specialized if the instantiation needs a different
  // implementation.
  bool allowUnencrypted() const override {
    return Message::allowUnencrypted();
  }
  // See Message.h. MAY be specialized. It's recommended to specialize it for
  // all messages to make message tracing more useful.
  std::vector<std::pair<std::string, folly::dynamic>>
  getDebugInfo() const override {
    return Message::getDebugInfo();
  }

  uint16_t getMinProtocolVersion() const override {
    return Message::getMinProtocolVersion();
  }

  bool warnAboutOldProtocol() const override {
    return Message::warnAboutOldProtocol();
  }

  const Header& getHeader() const {
    return header_;
  }
  const std::string& getBlob() const {
    return blob_;
  }

 protected:
  Header header_;
  std::string blob_;

  // used by deserialize() factory
  SimpleMessage() : Message(MESSAGE_TYPE, TRAFFIC_CLASS), header_() {}
};

template <class Header,
          MessageType MESSAGE_TYPE,
          TrafficClass TRAFFIC_CLASS,
          bool include_blob>
void SimpleMessage<Header, MESSAGE_TYPE, TRAFFIC_CLASS, include_blob>::
    serialize(ProtocolWriter& writer) const {
  writer.write(header_);
  if (include_blob) {
    const blob_size_t size = blob_.size();
    ld_check(blob_.size() <= Message::MAX_LEN - sizeof(header_) - sizeof(size));
    writer.write(size);
    if (size > 0) {
      writer.write(blob_.data(), size);
    }
  }
}

template <class Header,
          MessageType MESSAGE_TYPE,
          TrafficClass TRAFFIC_CLASS,
          bool include_blob>
MessageReadResult
SimpleMessage<Header, MESSAGE_TYPE, TRAFFIC_CLASS, include_blob>::deserialize(
    ProtocolReader& reader) {
  std::unique_ptr<SimpleMessage> m(
      new SimpleMessage<Header, MESSAGE_TYPE, TRAFFIC_CLASS, include_blob>());
  reader.read(&m->header_);
  if (include_blob) {
    blob_size_t blob_length = 0;
    reader.read(&blob_length);
    m->blob_.resize(blob_length);
    reader.read(const_cast<char*>(m->blob_.data()), m->blob_.size());
  }
  reader.allowTrailingBytes();
  return reader.resultMsg(std::move(m));
}

}} // namespace facebook::logdevice
