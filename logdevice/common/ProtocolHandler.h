/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once
#include <string>

#include "logdevice/common/Address.h"
#include "logdevice/common/IProtocolHandler.h"
#include "logdevice/common/libevent/LibEventCompatibility.h"
#include "logdevice/include/Err.h"

namespace folly {
class AsyncSocketException;
}

namespace facebook { namespace logdevice {
class Connection;
/**
 * ProtocolHandler implements the functionality listed for IProtocolHandler.
 * ProtocolHandler provides thread safe method to send messages over to the
 * remote endpoint. It receives messages and passes them to the specific
 * handler, so that they can be processed by the right WorkContext.
 *
 * In future, ProtocolHandler owns the AsyncSocket to the remote endpoint and
 * has reference to the EvBase attached to the AsyncSocket. On invoking the send
 * API, ProtocolHandler serializes the message on the caller thread and
 * schedules it to be sent from the EvBase thread. Once sent, it notifies the
 * higher layers about the message sent.
 *
 * All of the AsyncSocket callbacks get an instance of ProtocolHandler to notify
 * errors on the socket to higher layers.
 *
 * AsyncSocket write callback uses ProtocolHandler to indicate successfully
 * write of a message into the socket. This notification is forwarded to the
 * higher layer by ProtocolHandler.
 *
 * AsyncSocket read callback uses ProtocolHandler validate Protocolheader. Read
 * callback passes the read message body to ProtocolHandler instance once
 * the message is read completely. ProtocolHandler uses the header and
 * fetches the registered handler to process the message body.
 *
 */
class ProtocolHandler : public IProtocolHandler {
 public:
  ProtocolHandler(Connection* conn, EvBase* evBase);

  ~ProtocolHandler() override {}

  /**
   * Validation of protocol header on network thread on reading a message
   * header from the socket.
   */
  bool validateProtocolHeader(const ProtocolHeader& hdr) const override;

  /**
   * Read message body and pass it to the handler to complete message
   * processing.
   */
  int dispatchMessageBody(const ProtocolHeader& hdr,
                          std::unique_ptr<folly::IOBuf> body) override;

  /**
   * Notify about the error hit on socket.
   */
  void notifyErrorOnSocket(const folly::AsyncSocketException& err) override;

  /**
   * Notify bytes written successfully into the socket.
   */
  void notifyBytesWritten(size_t nbytes) override;

  /**
   * Get event used notify connection about the sent message.
   */
  EvTimer* getSentEvent() {
    return &buffer_passed_to_tcp_;
  }

  /**
   * Method used by various AsyncSocket callback to translate exception to
   * logdevice::Status.
   */
  static Status translateToLogDeviceStatus(folly::AsyncSocketException ex);

 private:
  Connection* const conn_;
  EvTimer buffer_passed_to_tcp_;
  EvTimer set_error_on_socket_;
  const Address peer_name_;
};

}} // namespace facebook::logdevice
