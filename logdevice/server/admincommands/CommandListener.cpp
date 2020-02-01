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
#include <folly/io/async/AsyncTransport.h>
#include <folly/io/async/EventBase.h>
#include <folly/synchronization/Baton.h>

#include "logdevice/common/Processor.h"
#include "logdevice/common/RequestType.h"
#include "logdevice/common/Semaphore.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/include/Err.h"
#include "logdevice/server/Server.h"
#include "logdevice/server/admincommands/AdminCommand.h"
#include "logdevice/server/read_path/LogStorageState.h"
#include "logdevice/server/storage_tasks/ShardedStorageThreadPool.h"

namespace facebook { namespace logdevice {

constexpr static size_t kBufferAllocationSize = 1024;
constexpr static size_t kTotalReceiveBuffer = 1024 * 1024;

using fizz::server::AsyncFizzServer;

namespace {
const uint8_t kSSLHandshakeRecordTag = 0x16;

class WriteCallback : public folly::AsyncWriter::WriteCallback {
 public:
  static WriteCallback*
  createWriteCallback(folly::DelayedDestruction::DestructorGuard guard) {
    return new WriteCallback(std::move(guard));
  }

  void writeSuccess() noexcept override {
    delete this;
  }

  void writeErr(size_t bytesWritten,
                const folly::AsyncSocketException& ex) noexcept override {
    ld_error("Write to admin command client after %lu bytes failed with "
             "exception: %s",
             bytesWritten,
             ex.what());
    delete this;
  }

 private:
  explicit WriteCallback(folly::DelayedDestruction::DestructorGuard guard)
      : guard_(std::move(guard)) {}
  ~WriteCallback() override {
    ld_debug("WriteCallback destroyed and destruction guard is freed");
  }

  const folly::DelayedDestruction::DestructorGuard guard_;
};

} // namespace

class AdminCommandConnection::ReadEventHandler : public folly::EventHandler {
 public:
  ReadEventHandler(folly::EventBase* eventBase,
                   folly::NetworkSocket sock,
                   AdminCommandConnection& connection)
      : folly::EventHandler(eventBase, folly::NetworkSocket(sock)),
        evb_(eventBase),
        sock_(sock),
        connection_(connection) {}

  void handlerReady(uint16_t) noexcept override {
    uint8_t byte = 0;
    folly::netops::recv(sock_, &byte, 1, MSG_PEEK);
    if (byte == kSSLHandshakeRecordTag) {
      ld_debug("TLS detected, trying TLS 1.3 for %s",
               connection_.addr_.describe().c_str());
      auto ctx = connection_.listener_.ssl_fetcher_.getFizzServerContext();
      if (!ctx) {
        ld_error("no SSL context, dropping connection with %s",
                 connection_.addr_.describe().c_str());
        folly::netops::close(sock_);
        connection_.closeConnectionAndDestroyObject();
        return;
      }

      auto socket = folly::AsyncSocket::UniquePtr(
          new folly::AsyncSocket(evb_, std::move(sock_)));
      connection_.fizz_server_ = AsyncFizzServer::UniquePtr(
          new AsyncFizzServer(std::move(socket), ctx));
      connection_.fizz_server_->accept(&connection_);
      return;
    }

    ld_debug(
        "TLS is not detected for %s", connection_.addr_.describe().c_str());
    connection_.socket_ =
        folly::AsyncSocket::UniquePtr(new folly::AsyncSocket(evb_, sock_));
    connection_.socket_->setReadCB(&connection_);
  }

 protected:
  folly::EventBase* evb_;
  folly::NetworkSocket sock_;
  AdminCommandConnection& connection_;
};

CommandListener::CommandListener(
    Listener::InterfaceDef iface,
    KeepAlive loop,
    CommandProcessor* command_processor,
    UpdateableSettings<ServerSettings> server_settings,
    SSLFetcher ssl_fetcher)
    : Listener(std::move(iface), loop),
      command_processor_(command_processor),
      server_settings_(std::move(server_settings)),
      ssl_fetcher_(std::move(ssl_fetcher)),
      loop_(loop) {}

AdminCommandConnection::AdminCommandConnection(size_t id,
                                               folly::NetworkSocket fd,
                                               CommandListener& listener,
                                               const folly::SocketAddress& addr,
                                               folly::EventBase* evb)
    : id_(id),
      addr_(addr),
      cursor_(&read_buffer_, kBufferAllocationSize),
      listener_(listener),
      evb_(evb),
      fd_(fd) {}

bool AdminCommandConnection::detectTLS() {
  read_event_handler_ = std::make_unique<ReadEventHandler>(evb_, fd_, *this);
  return read_event_handler_->registerHandler(
      ReadEventHandler::EventFlags::READ);
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
  ld_check(shutdown_ == false);
  DestructorGuard guard(this);
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
        listener_.command_processor_->processCommand(command.c_str(), addr_);
    ld_assert(fizz_server_ || socket_);
    if (fizz_server_) {
      fizz_server_->writeChain(
          WriteCallback::createWriteCallback(
              folly::DelayedDestruction::DestructorGuard(this)),
          std::move(result));
    } else if (socket_) {
      socket_->writeChain(WriteCallback::createWriteCallback(
                              folly::DelayedDestruction::DestructorGuard(this)),
                          std::move(result));
    }
  }
}

void AdminCommandConnection::closeConnectionAndDestroyObject() {
  if (shutdown_) {
    return;
  }
  listener_.conns_.erase(id_);
}

void AdminCommandConnection::readErr(
    const folly::AsyncSocketException&) noexcept {
  ld_error("read error");
  closeConnectionAndDestroyObject();
}

void AdminCommandConnection::readEOF() noexcept {
  ld_debug("read EOF");
  closeConnectionAndDestroyObject();
}

void CommandListener::connectionAccepted(
    folly::NetworkSocket sock,
    const folly::SocketAddress& addr) noexcept {
  const size_t id = next_conn_id_++;
  AdminCommandConnection::UniquePtr connection(
      new AdminCommandConnection(id, sock, *this, addr, loop_.get()));
  auto res = conns_.emplace(id, std::move(connection));
  ld_debug("Accepted connection from %s (id %zu, fd %d)",
           addr.describe().c_str(),
           id,
           sock.toFd());
  ld_check(res.second);

  if (!res.first->second->detectTLS()) {
    folly::netops::close(sock);
    conns_.erase(res.first);
    ld_error("Cannot register TLS detection event.");
    return;
  }

  const size_t conn_limit = server_settings_->command_conn_limit;
  while (conns_.size() >= conn_limit) {
    // no more connections available, free the oldest one (with the smallest
    // id)
    ld_check(conns_.begin() != conns_.end());
    conns_.erase(conns_.begin());
  }
  ld_check(res.second);
}

CommandListener::~CommandListener() {
  folly::Baton baton;
  loop_->add([&baton, this]() mutable {
    conns_.clear();
    baton.post();
  });
  baton.wait();
}

AdminCommandConnection::~AdminCommandConnection() {
  shutdown_ = true;
  if (fizz_server_) {
    fizz_server_->closeNow();
  } else if (socket_) {
    socket_->closeNow();
  }
}

void AdminCommandConnection::fizzHandshakeSuccess(
    AsyncFizzServer* /* fizz_server_ */) noexcept {
  ld_debug("fizzHandshakeSuccess for %s", addr_.describe().c_str());
  fizz_server_->setReadCB(this);
}

void AdminCommandConnection::fizzHandshakeError(
    AsyncFizzServer* /* fizz_server_ */,
    folly::exception_wrapper ex) noexcept {
  ld_warning("fizzHandshakeError for %s: %s",
             addr_.describe().c_str(),
             ex.get_exception()->what());
  closeConnectionAndDestroyObject();
}

void AdminCommandConnection::fizzHandshakeAttemptFallback(
    std::unique_ptr<folly::IOBuf> clientHello) {
  ld_debug("fizzHandshakeAttemptFallback for %s, trying AsyncSSLSocket",
           addr_.describe().c_str());

  auto socket = fizz_server_->getUnderlyingTransport<folly::AsyncSocket>();
  auto fd = socket->detachNetworkSocket().toFd();
  fizz_server_.reset();

  auto ctx = listener_.ssl_fetcher_.getSSLContext(true);
  if (!ctx) {
    ld_error("no SSL context, dropping connection");
    closeConnectionAndDestroyObject();
    return;
  }

  socket_ = folly::AsyncSSLSocket::UniquePtr(
      new folly::AsyncSSLSocket(ctx, evb_, folly::NetworkSocket::fromFd(fd)));
  socket_->setReadCB(this);
  socket_->setPreReceivedData(std::move(clientHello));

  auto* ssl_socket = dynamic_cast<folly::AsyncSSLSocket*>(socket_.get());
  ssl_socket->sslAccept(nullptr);
}
}} // namespace facebook::logdevice
