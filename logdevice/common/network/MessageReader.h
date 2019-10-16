/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once
#include <folly/io/IOBuf.h>
#include <folly/io/async/AsyncSocket.h>
#include <folly/io/async/AsyncSocketException.h>

#include "logdevice/common/IProtocolHandler.h"
#include "logdevice/common/protocol/ProtocolHeader.h"

namespace facebook { namespace logdevice {
/**
 * MessageReader is installed in AsyncSocket to receive data read from
 * socket and forward it to Connection for further processing.
 *
 * Reader is in two states it's receiving protocol header or receiving a
 * message body. First thing received on the socket is the protocol header,
 * followed by message body and so on and so forth.
 *
 * Reader maintains a buffer which it shares with AsyncSocket and all the
 * data is read into this buffer. The size of this buffer allocation depends on
 * the state we are in currently. The first buffer allocated has size equal to
 * the protocol header size. Once a protocol header is received, we allocate
 * another buffer which equals the size of message plus the next protocol
 * header. Once the message is completely read we create a clone of this buffer
 * and forward just the message body to connection for further processing. Once
 * the next message's header is received completely we create another buffer of
 * size next message's body + another protocol header and drop reference to this
 * existing buffer.
 *
 * On invoking getReadBuffer, get the read_buf_ writableTail and remaining
 * message length or protocol header length to be read into the buffer.
 *
 * On invoking readDataAvailable, check if the header or message body was read
 * completely. If we were expecting header and it was read completely, save the
 * header, drop existing read_buf_ and allocate a new buffer using the message
 * len in header. If the message was read completely, clone the buffer adjust
 * the pointers to align with message body length and forward the protocol
 * header and message body to connection for further processing.
 */
class MessageReader : public folly::AsyncSocket::ReadCallback {
 public:
  MessageReader(IProtocolHandler& conn, uint16_t proto);

  ~MessageReader() override {}
  /*
   * @param bufReturn getReadBuffer() should update *bufReturn to contain the
   *                  address of the read buffer.  This parameter will never
   *                  be nullptr.
   * @param lenReturn getReadBuffer() should update *lenReturn to contain the
   *                  maximum number of bytes that may be written to the read
   *                  buffer.  This parameter will never be nullptr.
   */
  void getReadBuffer(void** bufReturn, size_t* lenReturn) override;

  void readDataAvailable(size_t len) noexcept override;

  void readEOF() noexcept override {
    folly::AsyncSocketException ex(
        folly::AsyncSocketException::END_OF_FILE, "Socket read end of file.");
    readErr(ex);
  }

  void readErr(const folly::AsyncSocketException& ex) noexcept override {
    proto_handler_.notifyErrorOnSocket(ex);
  }

 private:
  void expectProtocolHeader();
  void expectMessageBody();
  size_t bytesExpected();
  void readMessageHeader();

  bool expecting_protocol_header_{true};
  size_t next_buffer_allocation_size_;
  std::unique_ptr<folly::IOBuf> read_buf_;
  ProtocolHeader recv_message_ph_;
  IProtocolHandler& proto_handler_;
  uint16_t proto_;
};

}} // namespace facebook::logdevice
