/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/ops/admin_command_client/AdminCommandClient.h"

#include <deque>

#include <fizz/client/AsyncFizzClient.h>
#include <fizz/protocol/DefaultCertificateVerifier.h>
#include <folly/Memory.h>
#include <folly/io/async/AsyncSSLSocket.h>
#include <folly/io/async/AsyncSocket.h>
#include <folly/io/async/AsyncTimeout.h>
#include <folly/io/async/AsyncTransport.h>
#include <folly/io/async/EventBase.h>

#include "logdevice/common/debug.h"

using folly::AsyncSocketException;
using folly::AsyncSSLSocket;
using folly::EventBase;
using folly::SSLContext;

namespace facebook { namespace logdevice {

class AdminClientConnection
    : public folly::AsyncSocket::ConnectCallback,
      public folly::AsyncTransportWrapper::ReadCallback,
      public folly::AsyncTransportWrapper::WriteCallback,
      public fizz::client::AsyncFizzClient::HandshakeCallback {
 public:
  AdminClientConnection(EventBase* evb,
                        const AdminCommandClient::Request& rr,
                        std::chrono::milliseconds timeout)
      : evb_(evb),
        // both fizz and unencrypted flow use plain AsyncSocket
        socket_(new folly::AsyncSocket(evb)),
        request_(rr),
        // We always try fizz (i.e. TLS 1.3) for encrypted connection first,
        // we fallback to AsyncSSLSocket on failure
        tryFizz_(rr.conntype_ == AdminCommandClient::ConnectionType::ENCRYPTED),
        timeout_(timeout) {}

  ~AdminClientConnection() override {
    if (fizz_client_) {
      fizz_client_->setReadCB(nullptr);
      fizz_client_->closeNow();
    } else {
      socket_->setReadCB(nullptr);
      socket_->closeNow();
    }
  }

  virtual folly::SemiFuture<AdminCommandClient::Response> connect() {
    connectImpl();
    return promise_.getSemiFuture();
  }

 private:
  void connectSuccess() noexcept override {
    socket_->setRecvBufSize(1 * 1024 * 1024);
    if (tryFizz_) {
      fizzHandshake();
    } else {
      socketWrite();
    }
  }

  void socketWrite() {
    ld_check(!fizz_client_);
    auto& request = request_.request;
    socket_->write(this, request.data(), request.size());
    if (request.back() != '\n') {
      socket_->write(this, "\n", 1);
    }
    socket_->setReadCB(this);
  }

  void connectErr(const AsyncSocketException& ex) noexcept override {
    response_.success = false;
    response_.failure_reason = "CONNECTION_ERROR";
    ld_debug("Could not connect to %s: %s",
             request_.sockaddr.describe().c_str(),
             ex.what());
    done();
  }

  void getReadBuffer(void** bufReturn, size_t* lenReturn) override {
    // If the last buffer's first character is a nul byte, we never wrote
    // anything to it, so return it.
    if (!result_.empty() && !result_.back().empty() && result_.back()[0] == 0) {
      *bufReturn = (void*)(result_.back().data());
      *lenReturn = result_.back().size();
    } else {
      // Otherwise allocate a new buffer.
      std::string buffer(8192, '\0');
      *bufReturn = (void*)buffer.data();
      *lenReturn = buffer.size();
      result_.push_back(std::move(buffer));
    }
  }

  void readDataAvailable(size_t length) noexcept override {
    ld_check(length > 0);
    result_.back().resize(length);

    if (result_.size() >= 8) {
      // fold all buffers together to reclaim some memory
      for (int i = 1; i < result_.size(); i++) {
        result_[0] += result_[i];
      }
      result_.resize(1);
    }

    // Concatenating the end of the buffer if the last chunk is less than
    // the end marker
    std::string eof("END\r\n");
    while (result_.size() > 1 && result_.back().size() < eof.size()) {
      result_[result_.size() - 2] += result_.back();
      result_.resize(result_.size() - 1);
    }

    // Checking if we're done here
    if (result_.back().size() >= eof.size()) {
      if (result_.back().compare(
              result_.back().size() - eof.size(), eof.size(), eof) == 0) {
        result_.back().resize(result_.back().size() - eof.size());
        success_ = true;
        done();
      }
    }
  }

  void readEOF() noexcept override {
    if (!done_) {
      done();
    }
  }

  void readErr(const AsyncSocketException& ex) noexcept override {
    ld_debug("Error reading from %s: %s",
             request_.sockaddr.describe().c_str(),
             ex.what());
    success_ = false;
    response_.failure_reason = "READ_ERROR";
    done();
  }

  void writeSuccess() noexcept override {
    // Don't care
  }

  void writeErr(size_t /*bytesWritten*/,
                const AsyncSocketException& ex) noexcept override {
    ld_debug("Error writing to %s: %s",
             request_.sockaddr.describe().c_str(),
             ex.what());
    response_.failure_reason = "WRITE_ERROR";
    success_ = false;
    done();
  }

  void done() {
    using std::chrono::steady_clock;
    steady_clock::time_point tdone = steady_clock::now();
    response_.success = success_;

    auto& response = response_.response;
    response.clear();

    // If the last buffer's first character is a nul byte, discard it.
    if (!result_.empty() && !result_.back().empty() && result_.back()[0] == 0) {
      result_.resize(result_.size() - 1);
    }

    size_t size = 0;
    for (auto const& s : result_) {
      size += s.size();
    }
    response.reserve(size);
    for (auto const& s : result_) {
      response += s;
    }

    if (fizz_client_) {
      fizz_client_->setReadCB(nullptr);
    } else {
      socket_->setReadCB(nullptr);
    }
    promise_.setValue(response_);
    steady_clock::time_point tend = steady_clock::now();
    double d1 = std::chrono::duration_cast<std::chrono::duration<double>>(
                    tdone - tstart_)
                    .count();
    double d2 =
        std::chrono::duration_cast<std::chrono::duration<double>>(tend - tdone)
            .count();
    ld_log(
        d1 + d2 > 0.5 ? dbg::Level::INFO : dbg::Level::DEBUG,
        "Response from %s has %lu chunks, response size is %lu, "
        "fetching data took %.3fs, preparing response took %.3fs. Command: %s",
        request_.sockaddr.describe().c_str(),
        result_.size(),
        size,
        d1,
        d2,
        request_.request.c_str());
    result_.clear();
  }

  void connectImpl() {
    auto* type = tryFizz_
        ? "TLS_1.3"
        : (request_.conntype_ == AdminCommandClient::ConnectionType::ENCRYPTED
               ? "TLS"
               : "PLAIN");
    ld_debug(
        "Connecting to %s with %s", request_.sockaddr.describe().c_str(), type);
    tstart_ = std::chrono::steady_clock::now();
    socket_->connect(this, request_.sockaddr, timeout_.count());
  }

  void fizzHandshake() {
    ld_check(!fizz_client_);
    fizz_client_ = fizz::client::AsyncFizzClient::UniquePtr(
        new fizz::client::AsyncFizzClient(
            std::move(socket_),
            std::make_shared<fizz::client::FizzClientContext>()));
    fizz_client_->connect(this,
                          std::shared_ptr<const fizz::CertificateVerifier>(),
                          folly::none,
                          folly::none);
  }

  void fizzHandshakeSuccess(fizz::client::AsyncFizzClient*) noexcept override {
    ld_debug("fizzHandshakeSuccess connecting to %s",
             request_.sockaddr.describe().c_str());

    auto& request = request_.request;
    auto cmd = std::make_unique<folly::IOBuf>();
    folly::io::Appender output(cmd.get(), request.size() + 1);

    output(request);
    if (request.back() != '\n') {
      output("\n");
    }
    fizz_client_->writeChain(nullptr, std::move(cmd));
    fizz_client_->setReadCB(this);
  }

  /**
   * There's no transparent fallback mechanism in fizz client,
   * so we cannot distinguish between protocol version negotiation failure
   * and any other failure, thus we're falling back to AsyncSSLSocket here.
   */
  void fizzHandshakeError(fizz::client::AsyncFizzClient*,
                          folly::exception_wrapper ex) noexcept override {
    ld_debug("fizzHandshakeError connecting to %s: %s",
             request_.sockaddr.describe().c_str(),
             ex ? ex.get_exception()->what() : "Unknown");
    fizz_client_.reset();
    socket_.reset(new AsyncSSLSocket(std::make_shared<SSLContext>(), evb_));
    // make sure connectSuccess() will not go into an infinite loop
    tryFizz_ = false;
    connectImpl();
  }

  folly::EventBase* evb_;
  folly::AsyncSocket::UniquePtr socket_;
  const AdminCommandClient::Request request_;
  AdminCommandClient::Response response_;
  std::vector<std::string> result_;
  bool tryFizz_{false};
  bool success_{false};
  bool done_{false};
  folly::Promise<AdminCommandClient::Response> promise_;
  std::chrono::milliseconds timeout_;
  std::chrono::steady_clock::time_point tstart_;
  fizz::client::AsyncFizzClient::UniquePtr fizz_client_;
};

std::vector<folly::SemiFuture<AdminCommandClient::Response>>
AdminCommandClient::asyncSend(
    const std::vector<AdminCommandClient::Request>& rr,
    std::chrono::milliseconds command_timeout,
    std::chrono::milliseconds connect_timeout) const {
  std::vector<folly::SemiFuture<AdminCommandClient::Response>> futures;
  futures.reserve(rr.size());

  for (auto& r : rr) {
    futures.push_back(
        folly::via(executor_.get())
            .then([executor = executor_.get(),
                   r,
                   connect_timeout,
                   command_timeout](auto&&) mutable {
              auto evb = executor->getEventBase();
              auto connection = std::make_unique<AdminClientConnection>(
                  evb, r, connect_timeout);
              auto fut = connection->connect();
              return std::move(fut)
                  .via(evb)
                  .onTimeout(command_timeout,
                             [] {
                               return AdminCommandClient::Response{
                                   "", false, "TIMEOUT"};
                             })
                  // After either timing-out or getting a response, let's
                  // destroy the connection.
                  .thenValue([c = std::move(connection)](auto response) {
                    return response;
                  });
            }));
  }

  return futures;
}

std::vector<AdminCommandClient::Response>
AdminCommandClient::send(const std::vector<AdminCommandClient::Request>& rr,
                         std::chrono::milliseconds command_timeout,
                         std::chrono::milliseconds connect_timeout) const {
  return collectAllSemiFuture(asyncSend(rr, command_timeout, connect_timeout))
      .via(executor_.get())
      .thenValue(
          [](std::vector<folly::Try<AdminCommandClient::Response>> results) {
            std::vector<AdminCommandClient::Response> ret;
            ret.reserve(results.size());
            for (const auto& result : results) {
              if (result.hasValue()) {
                ret.emplace_back(result.value());
              } else {
                ret.emplace_back(std::string(),
                                 false,
                                 result.exception().what().toStdString());
              }
            }
            return ret;
          })
      .get();
}

}} // namespace facebook::logdevice
