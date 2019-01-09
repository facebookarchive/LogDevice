/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/ops/admin_command_client/AdminCommandClient.h"

#include <deque>

#include <folly/Memory.h>
#include <folly/io/async/AsyncSSLSocket.h>
#include <folly/io/async/AsyncSocket.h>
#include <folly/io/async/AsyncTimeout.h>
#include <folly/io/async/AsyncTransport.h>
#include <folly/io/async/EventBase.h>

#include "logdevice/common/debug.h"

using folly::AsyncSocket;
using folly::AsyncSocketException;
using folly::AsyncSSLSocket;
using folly::AsyncTimeout;
using folly::EventBase;
using folly::SSLContext;

namespace facebook { namespace logdevice {

class AdminClientConnection
    : public folly::AsyncSocket::ConnectCallback,
      public folly::AsyncTransportWrapper::ReadCallback,
      public folly::AsyncTransportWrapper::WriteCallback {
 public:
  AdminClientConnection(EventBase* evb,
                        AdminCommandClient::RequestResponse& rr,
                        std::function<void()> done_callback,
                        std::chrono::milliseconds timeout,
                        std::shared_ptr<folly::SSLContext> context)
      : socket_(),
        request_response_(rr),
        done_callback_(done_callback),
        timeout_(timeout) {
    if (request_response_.conntype_ ==
        AdminCommandClient::ConnectionType::ENCRYPTED) {
      ld_check(context);
      socket_ = AsyncSSLSocket::newSocket(context, evb);
    } else {
      socket_ = AsyncSocket::newSocket(evb);
    }
  }

  virtual void connect() {
    bool ssl{request_response_.conntype_ ==
             AdminCommandClient::ConnectionType::ENCRYPTED};
    ld_debug("Connecting to %s with %s",
             request_response_.sockaddr.describe().c_str(),
             (ssl) ? "SSL" : "PLAIN");
    tstart_ = std::chrono::steady_clock::now();
    socket_->connect(this, request_response_.sockaddr, timeout_.count());
  }

  void connectSuccess() noexcept override {
    auto& request = request_response_.request;
    socket_->setRecvBufSize(1 * 1024 * 1024);
    socket_->write(this, request.data(), request.size());
    if (request.back() != '\n') {
      socket_->write(this, "\n", 1);
    }
    socket_->setReadCB(this);
  }

  void connectErr(const AsyncSocketException& ex) noexcept override {
    request_response_.success = false;
    request_response_.failure_reason = "CONNECTION_ERROR";
    ld_error("Could not connect to %s: %s",
             request_response_.sockaddr.describe().c_str(),
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
    ld_error("Error reading from %s: %s",
             request_response_.sockaddr.describe().c_str(),
             ex.what());
    success_ = false;
    request_response_.failure_reason = "READ_ERROR";
    done();
  }

  void writeSuccess() noexcept override {
    // Don't care
  }

  void writeErr(size_t /*bytesWritten*/,
                const AsyncSocketException& ex) noexcept override {
    ld_error("Error writting to %s: %s",
             request_response_.sockaddr.describe().c_str(),
             ex.what());
    request_response_.failure_reason = "WRITE_ERROR";
    success_ = false;
    done();
  }

  void done() {
    using std::chrono::steady_clock;
    steady_clock::time_point tdone = steady_clock::now();
    request_response_.success = success_;

    auto& response = request_response_.response;
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

    socket_->setReadCB(nullptr);
    done_callback_();
    steady_clock::time_point tend = steady_clock::now();
    double d1 = std::chrono::duration_cast<std::chrono::duration<double>>(
                    tdone - tstart_)
                    .count();
    double d2 =
        std::chrono::duration_cast<std::chrono::duration<double>>(tend - tdone)
            .count();
    ld_info("Response from %s has %lu chunks, response size is %lu, "
            "fetching data took %.1fs, preparing response took %.1fs",
            request_response_.sockaddr.describe().c_str(),
            result_.size(),
            size,
            d1,
            d2);
    result_.clear();
  }

  ~AdminClientConnection() override {
    // Deregister socket callback to avoid readEOF to be invoked
    // when we close the socket.
    socket_->setReadCB(nullptr);
    socket_->close();
  }

 private:
  std::shared_ptr<AsyncSocket> socket_;
  AdminCommandClient::RequestResponse& request_response_;
  std::vector<std::string> result_;
  bool success_{false};
  bool done_{false};
  std::function<void()> done_callback_;
  std::chrono::milliseconds timeout_;
  std::chrono::steady_clock::time_point tstart_;
};

std::vector<folly::SemiFuture<AdminCommandClient::RequestResponse*>>
AdminCommandClient::semifuture_send(
    std::vector<AdminCommandClient::RequestResponse>& rr,
    std::chrono::milliseconds command_timeout,
    std::chrono::milliseconds connect_timeout) {
  std::shared_ptr<
      std::vector<folly::Promise<AdminCommandClient::RequestResponse*>>>
      proms = std::make_shared<
          std::vector<folly::Promise<AdminCommandClient::RequestResponse*>>>(
          rr.size());
  std::vector<folly::SemiFuture<AdminCommandClient::RequestResponse*>> futures;
  futures.reserve(rr.size());
  for (auto& p : *proms) {
    futures.emplace_back(p.getSemiFuture());
  }

  ld_check(num_threads_ > 0);
  size_t actual_num_threads = num_threads_;
  int num_requests_per_thread = rr.size() / actual_num_threads + 1;
  if (num_threads_ > rr.size()) {
    actual_num_threads = rr.size();
    num_requests_per_thread = 1;
  }
  executor_ =
      std::make_unique<folly::CPUThreadPoolExecutor>(actual_num_threads);
  for (int k = 0; k < actual_num_threads; k++) {
    int start = k * num_requests_per_thread;
    if (start >= rr.size()) {
      break;
    }
    executor_->add([proms,
                    command_timeout,
                    connect_timeout,
                    start,
                    num_requests_per_thread,
                    &rr]() mutable {
      bool timed_out = false;
      size_t connections_done = 0;
      size_t connections_size;
      auto& promises = *proms;
      {
        // Scope event_base and connections objects
        EventBase event_base;
        std::vector<std::unique_ptr<AdminClientConnection>> connections;

        auto timeout = AsyncTimeout::make(
            event_base, [&]() noexcept { timed_out = true; });
        timeout->scheduleTimeout(command_timeout);
        auto context = std::make_shared<folly::SSLContext>();

        for (size_t i = start;
             i < rr.size() && i - start < num_requests_per_thread;
             ++i) {
          auto connection = std::make_unique<AdminClientConnection>(
              &event_base,
              rr[i],
              [i, &connections_done, &promises, &rr]() {
                ++connections_done;
                promises[i].setValue(&rr[i]);
              },
              connect_timeout,
              context);
          connection->connect();
          connections.push_back(std::move(connection));
        }
        connections_size = connections.size();
        do {
          event_base.loopOnce();
        } while (connections_done < connections_size && !timed_out);
      }

      if (timed_out) {
        ld_error("Timed out after %lums reading from %lu nodes",
                 command_timeout.count(),
                 connections_size - connections_done);
        for (size_t i = start;
             i < rr.size() && i - start < num_requests_per_thread;
             ++i) {
          if (!rr[i].success && rr[i].failure_reason.empty()) {
            rr[i].failure_reason = "TIMEOUT";
            if (!promises[i].isFulfilled()) {
              ++connections_done;
              promises[i].setValue(&rr[i]);
              if (connections_done == connections_size) {
                break;
              }
            }
          }
        }
      }
    });
  }

  return futures;
}

void AdminCommandClient::send(
    std::vector<AdminCommandClient::RequestResponse>& rr,
    std::chrono::milliseconds command_timeout,
    std::chrono::milliseconds connect_timeout) {
  collectAllSemiFuture(semifuture_send(rr, command_timeout, connect_timeout))
      .wait();
  terminate();
}

}} // namespace facebook::logdevice
