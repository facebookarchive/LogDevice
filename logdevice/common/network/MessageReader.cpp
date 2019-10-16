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
  if (proto_handler_.good()) {
    *bufReturn = read_buf_->writableTail();
    *lenReturn = std::min(read_buf_->tailroom(), next_buffer_allocation_size_);
  } else {
    *bufReturn = nullptr;
    *lenReturn = 0;
  }
}
size_t MessageReader::bytesExpected() {
  size_t protohdr_bytes =
      ProtocolHeader::bytesNeeded(recv_message_ph_.type, proto_);

  if (expecting_protocol_header_) {
    return protohdr_bytes;
  } else {
    return recv_message_ph_.len - protohdr_bytes;
  }
}

void MessageReader::readMessageHeader() {
  // Read the minimum fields of the header. If checksum is present read it out,
  // otherwise those 8 bytes belong to the message body. Validate the header. If
  // no errors, allocate the next read buffer to include current message's  body
  // and next message's header. Copy in the 8 bytes read earlier as part of
  // ProtocolHeader if necessary, into this newly allocated buffer. Move this
  // buffer into read_buf_.
  ProtocolHeader* hdr = (ProtocolHeader*)read_buf_->data();
  recv_message_ph_.len = hdr->len;
  recv_message_ph_.type = hdr->type;
  size_t trim_header =
      ProtocolHeader::bytesNeeded(recv_message_ph_.type, proto_);
  bool checksum_in_header =
      ProtocolHeader::needChecksumInHeader(recv_message_ph_.type, proto_);
  if (checksum_in_header) {
    recv_message_ph_.cksum = hdr->cksum;
  }
  read_buf_->trimStart(trim_header);
  // Do basic checks on protocol header before proceeding.
  auto retval = proto_handler_.validateProtocolHeader(recv_message_ph_);
  if (!retval) {
    next_buffer_allocation_size_ = 0;
    folly::AsyncSocketException ex(folly::AsyncSocketException::CORRUPTED_DATA,
                                   "Invalid Protocol Header received.",
                                   -1);
    proto_handler_.notifyErrorOnSocket(ex);
    return;
  }
  expecting_protocol_header_ = false;
  next_buffer_allocation_size_ = bytesExpected() + sizeof(ProtocolHeader);
  auto new_read_buf = folly::IOBuf::create(next_buffer_allocation_size_);
  // Add the 8 byte of message read if cksum is absent into the read_buf and
  // move the writableTail().
  ld_check_ge(new_read_buf->capacity(), read_buf_->length());
  memcpy(new_read_buf->writableTail(), read_buf_->data(), read_buf_->length());
  new_read_buf->append(read_buf_->length());
  read_buf_->trimStart(new_read_buf->length());
  // There should be nothing left in the buffer once the header is read.
  ld_check_eq(read_buf_->length(), 0);
  next_buffer_allocation_size_ -= new_read_buf->length();
  read_buf_ = std::move(new_read_buf);
  ld_check_le(next_buffer_allocation_size_, read_buf_->capacity());
}

void MessageReader::readDataAvailable(size_t len) noexcept {
  ld_check(proto_handler_.good());
  read_buf_->append(len);
  next_buffer_allocation_size_ -= len;
  if (!expecting_protocol_header_) {
    auto message_len = bytesExpected();
    ld_check_le(message_len, read_buf_->capacity());
    if (read_buf_->length() < message_len) {
      return;
    }
    // Clone the read buffer, trim end of the buffer to get just message body.
    // Forward the message body to connection for further processing. Check if
    // next message header is already available in the buffer.
    auto msg_body = read_buf_->clone();
    if (msg_body->length() > message_len) {
      msg_body->trimEnd(read_buf_->length() - message_len);
    }
    int rv = proto_handler_.dispatchMessageBody(
        recv_message_ph_, std::move(msg_body));
    read_buf_->trimStart(message_len);
    expecting_protocol_header_ = true;
    // dispatchMessageBody can close the socket if it hits a error return
    // rightaway. Continue to see if we already have header in the readbuffer.
    // If that is the case read the header.
    if (rv != 0 && err != E::NOBUFS) {
      return;
    }
    if (read_buf_->length() == 0) {
      return;
    }
    // fallthrough to check if next header is already read in buffer if it is
    // the case then continue to process that.
  }

  if (read_buf_->length() < sizeof(ProtocolHeader)) {
    return;
  }
  readMessageHeader();
}
}} // namespace facebook::logdevice
