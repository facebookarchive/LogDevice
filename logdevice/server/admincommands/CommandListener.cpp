/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <array>

#include <folly/Memory.h>
#include <folly/io/async/AsyncSSLSocket.h>
#include <folly/io/async/AsyncSocket.h>
#include <folly/io/async/AsyncTimeout.h>
#include <folly/io/async/AsyncTransport.h>
#include <folly/io/async/EventBase.h>

#include "logdevice/common/Processor.h"
#include "logdevice/common/RequestType.h"
#include "logdevice/common/Semaphore.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/libevent/compat.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/include/Err.h"
#include "logdevice/server/Server.h"
#include "logdevice/server/admincommands/AdminCommand.h"
#include "logdevice/server/read_path/LogStorageState.h"
#include "logdevice/server/storage_tasks/ShardedStorageThreadPool.h"

namespace facebook { namespace logdevice {

constexpr static size_t kBufferAllocationSize = 1024;
constexpr static size_t kTotalReceiveBuffer = 1024 * 1024;

namespace {
const uint8_t kSSLHandshakeRecordTag = 0x16;
} // namespace

AdminCommandConnection::TLSSensingCallback::TLSSensingCallback(
    AdminCommandConnection& connection,
    const folly::NetworkSocket& fd)
    : fd_(fd), connection_(connection) {}

void AdminCommandConnection::TLSSensingCallback::getReadBuffer(
    void** bufReturn,
    size_t* lenReturn) {
  *lenReturn = 0;
  *bufReturn = nullptr;

  uint8_t byte = 0;
  auto rv = folly::netops::recv(fd_, &byte, 1, MSG_PEEK);
  if (rv != 1) {
    ld_critical("There is no 1 byte in the socket for TLS detection.");
    connection_.closeConnectionAndDestroyObject();
  }

  ld_debug("read data is available for peeking");
  if (byte == kSSLHandshakeRecordTag) {
    ld_debug("TLS detected");
    auto ctx = connection_.listener_.ssl_fetcher_.getSSLContext(true, true);
    if (!ctx) {
      ld_error("no SSL context, dropping connection");
      connection_.closeConnectionAndDestroyObject();
      return;
    }

    auto ssl_socket = folly::AsyncSSLSocket::UniquePtr(
        new folly::AsyncSSLSocket(ctx, std::move(connection_.socket_)));
    ssl_socket->setReadCB(&connection_);
    ssl_socket->sslAccept(nullptr);
    connection_.socket_ = std::move(ssl_socket);
    connection_.tls_sensing_.reset();
    return;
  }

  ld_debug("TLS is not detected");
  connection_.socket_ = folly::AsyncSocket::UniquePtr(
      new folly::AsyncSocket(std::move(connection_.socket_)));
  connection_.socket_->setReadCB(&connection_);
  return;
}

void AdminCommandConnection::TLSSensingCallback::readDataAvailable(
    size_t) noexcept {
  ld_check(false);
}

void AdminCommandConnection::TLSSensingCallback::readEOF() noexcept {}

void AdminCommandConnection::TLSSensingCallback::readErr(
    const folly::AsyncSocketException&) noexcept {
  ld_error("tls sensing read error");
}

CommandListener::CommandListener(Listener::InterfaceDef iface,
                                 KeepAlive loop,
                                 Server* server)
    : Listener(std::move(iface), loop),
      server_(server),
      server_settings_(server_->getServerSettings()),
      ssl_fetcher_(
          server_->getParameters()->getProcessorSettings()->ssl_cert_path,
          server_->getParameters()->getProcessorSettings()->ssl_key_path,
          server_->getParameters()->getProcessorSettings()->ssl_ca_path,
          server_->getParameters()
              ->getProcessorSettings()
              ->ssl_cert_refresh_interval),
      command_processor_(server),
      loop_(loop) {
  ld_check(server_);
}

AdminCommandConnection::AdminCommandConnection(size_t id,
                                               folly::NetworkSocket fd,
                                               CommandListener& listener,
                                               const folly::SocketAddress& addr,
                                               folly::EventBase* evb)
    : id_(id),
      addr_(addr),
      cursor_(&read_buffer_, kBufferAllocationSize),
      listener_(listener),
      tls_sensing_(std::make_unique<TLSSensingCallback>(*this, fd)) {
  socket_ = folly::AsyncSocket::UniquePtr(new folly::AsyncSocket(evb, fd));
  socket_->setReadCB(tls_sensing_.get());
}

void AdminCommandConnection::getReadBuffer(void** bufReturn,
                                           size_t* lenReturn) {
  cursor_.ensure(kBufferAllocationSize);
  *bufReturn = cursor_.writableData();
  *lenReturn = cursor_.length();
  ld_debug("read data is available, %p %lu",
           cursor_.writableData(),
           cursor_.length());
}

void AdminCommandConnection::readDataAvailable(size_t length) noexcept {
  ld_debug("reading data, %lu", length);
  if (read_buffer_.chainLength() > kTotalReceiveBuffer) {
    closeConnectionAndDestroyObject();
    return;
  }

  cursor_.append(length);

  folly::io::Cursor last_block(read_buffer_.front());
  last_block.advanceToEnd();
  last_block -= length;
  size_t full_commands = 0;
  // Checks if we got new endline. (And counts how many new lines we got)
  // Invariant that there was no endlines before.
  while (!last_block.isAtEnd()) {
    full_commands += (last_block.read<char>() == '\n');
  }

  for (int i = 0; i < full_commands; ++i) {
    folly::io::Cursor read_cursor(read_buffer_.front());
    std::string command = read_cursor.readTerminatedString('\n');
    // Trims commands to maintain invariant. (no endlines in the buf).
    read_buffer_.trimStart(command.size() + 1);
    if (command.size() > 0 && command.back() == '\r') {
      command.pop_back();
    }

    auto result =
        listener_.command_processor_.processCommand(command.c_str(), addr_);
    socket_->writeChain(this, std::move(result));
  }
}

void AdminCommandConnection::closeConnectionAndDestroyObject() {
  if (destruction_scheduled_) {
    return;
  }
  destruction_scheduled_ = true;
  listener_.loop_->add([this] {
    ld_debug("Closed connection from %s (id %zu, fd %d)",
             addr_.describe().c_str(),
             id_,
             socket_->getNetworkSocket().toFd());
    listener_.conns_.erase(id_);
  });
}

void AdminCommandConnection::writeErr(
    size_t /* bytesWritten */,
    const folly::AsyncSocketException&) noexcept {
  ld_error("write error");
  closeConnectionAndDestroyObject();
}

void AdminCommandConnection::readErr(
    const folly::AsyncSocketException&) noexcept {
  ld_error("read error");
  closeConnectionAndDestroyObject();
}

void AdminCommandConnection::readEOF() noexcept {
  closeConnectionAndDestroyObject();
}

void CommandListener::connectionAccepted(
    folly::NetworkSocket sock,
    const folly::SocketAddress& addr) noexcept {
  const size_t id = next_conn_id_++;
  auto res =
      conns_.emplace(std::piecewise_construct,
                     std::forward_as_tuple(id),
                     std::forward_as_tuple(id, sock, *this, addr, loop_.get()));
  ld_debug("Accepted connection from %s (id %zu, fd %d)",
           addr.describe().c_str(),
           id,
           sock.toFd());

  const size_t conn_limit = server_settings_->command_conn_limit;
  while (conns_.size() >= conn_limit) {
    // no more connections available, free the oldest one (with the smallest id)
    ld_check(conns_.begin() != conns_.end());
    conns_.erase(conns_.begin());
  }
  ld_check(res.second);
}

}} // namespace facebook::logdevice
