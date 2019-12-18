/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/network/MessageReader.h"

#include <folly/io/async/AsyncSocketException.h>

#include "logdevice/common/IProtocolHandler.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/settings/Settings.h"

namespace facebook { namespace logdevice {
MessageReader::MessageReader(IProtocolHandler& proto_handler, uint16_t proto)
    : next_buffer_allocation_size_(sizeof(ProtocolHeader)),
      read_buf_(folly::IOBuf::create(next_buffer_allocation_size_)),
      proto_handler_(proto_handler),
      proto_(proto) {}

void MessageReader::getReadBuffer(void** bufReturn, size_t* lenReturn) {
  if (!proto_handler_.good()) {
    *bufReturn = nullptr;
    *lenReturn = 0;
    return;
  }
  if (expecting_protocol_header_) {
    ld_check_le(next_buffer_allocation_size_, sizeof(recv_message_ph_));
    auto offset = sizeof(recv_message_ph_) - next_buffer_allocation_size_;
    void* ptr =
        static_cast<uint8_t*>(static_cast<void*>(&recv_message_ph_)) + offset;
    *bufReturn = ptr;
    *lenReturn = next_buffer_allocation_size_;
  } else {
    *bufReturn = read_buf_->writableTail();
    *lenReturn = std::min(read_buf_->tailroom(), next_buffer_allocation_size_);
  }
}

void MessageReader::prepareMessageBody() {
  // Allocate the next read buffer to include current
  // message's body. If checksum is not present in header,those 8 bytes in
  // header belong to the message body. Copy the 8 bytes read earlier as part
  // of ProtocolHeader if necessary, into this newly allocated buffer.
  expecting_protocol_header_ = false;
  auto payload_size = recv_message_ph_.len -
      ProtocolHeader::bytesNeeded(recv_message_ph_.type, proto_);
  next_buffer_allocation_size_ = payload_size;
  read_buf_ = folly::IOBuf::create(next_buffer_allocation_size_);
  // Add the 8 byte of message read if cksum is absent into the read_buf and
  // move the writableTail().
  if (!ProtocolHeader::needChecksumInHeader(recv_message_ph_.type, proto_)) {
    memcpy(read_buf_->writableTail(),
           &recv_message_ph_.cksum,
           sizeof(recv_message_ph_.cksum));
    read_buf_->append(sizeof(recv_message_ph_.cksum));
    next_buffer_allocation_size_ -= read_buf_->length();
    recv_message_ph_.cksum = 0;
  }
  ld_check_le(next_buffer_allocation_size_, read_buf_->capacity());
}

bool MessageReader::validateHeader() {
  // Validate the read header.
  auto retval = proto_handler_.validateProtocolHeader(recv_message_ph_);
  if (!retval) {
    next_buffer_allocation_size_ = 0;
    folly::AsyncSocketException ex(folly::AsyncSocketException::CORRUPTED_DATA,
                                   "Invalid Protocol Header received.",
                                   -1);
    proto_handler_.notifyErrorOnSocket(ex);
    return false;
  }

  return true;
}

void MessageReader::readDataAvailable(size_t len) noexcept {
  ld_check(proto_handler_.good());
  ld_check_ge(next_buffer_allocation_size_, len);
  next_buffer_allocation_size_ -= len;
  if (!expecting_protocol_header_) {
    read_buf_->append(len);
    if (next_buffer_allocation_size_ > 0) {
      return;
    }
    auto message_len = recv_message_ph_.len -
        ProtocolHeader::bytesNeeded(recv_message_ph_.type, proto_);
    // read_buf_ capacity can be greater than message len but that capacity
    // should not be used.
    ld_check_le(message_len, read_buf_->capacity());
    ld_check(read_buf_->length() == message_len);
    proto_handler_.dispatchMessageBody(recv_message_ph_, std::move(read_buf_));
    expecting_protocol_header_ = true;
    next_buffer_allocation_size_ = sizeof(ProtocolHeader);
    recv_message_ph_ = ProtocolHeader();
  } else {
    if (next_buffer_allocation_size_ == 0 && validateHeader()) {
      prepareMessageBody();
    }
  }
}
}} // namespace facebook::logdevice
