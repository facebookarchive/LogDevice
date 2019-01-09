/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/test/utils/nc.h"

#include <folly/io/async/AsyncSSLSocket.h>
#include <folly/io/async/AsyncSocket.h>
#include <folly/io/async/EventBase.h>

namespace facebook { namespace logdevice { namespace test {

std::string nc(const folly::SocketAddress& addr,
               const std::string& input,
               std::string* out_error,
               bool ssl) {
  struct Callback : folly::AsyncSocket::ConnectCallback,
                    folly::AsyncSocket::WriteCallback,
                    folly::AsyncSocket::ReadCallback {
    folly::EventBase* base_;
    const std::string& input_;
    folly::SocketAddress addr_;
    std::shared_ptr<folly::AsyncSocket> socket_;
    std::string error;
    char buf_[8192];
    std::string out;
    bool done{false};

    explicit Callback(folly::EventBase* base,
                      const folly::SocketAddress& addr,
                      const std::string& input,
                      bool ssl)
        : base_(base), input_(input), addr_(addr), socket_() {
      if (ssl) {
        auto sslContext = std::make_shared<folly::SSLContext>();
        socket_ = folly::AsyncSSLSocket::newSocket(sslContext, base);
      } else {
        socket_ = folly::AsyncSocket::newSocket(base);
      }
    }

    void start() {
      socket_->connect(this, addr_);
    }

    void complete() {
      socket_->setReadCB(nullptr);
      socket_->close();
      done = true;
    }
    void handleError(const folly::AsyncSocketException& ex) {
      if (error.empty()) {
        error = ex.what();
      }
      complete();
    }

    // ConnectCallback.
    void connectSuccess() noexcept override {
      socket_->write(this, input_.data(), input_.size());
      socket_->setReadCB(this);
    }

    void connectErr(const folly::AsyncSocketException& ex) noexcept override {
      handleError(ex);
    }

    // WriteCallback.
    void writeSuccess() noexcept override {}
    void writeErr(size_t /* bytesWritten */,
                  const folly::AsyncSocketException& ex) noexcept override {
      handleError(ex);
    }

    // ReadCallback.
    void getReadBuffer(void** bufReturn, size_t* lenReturn) override {
      *bufReturn = buf_;
      *lenReturn = sizeof(buf_);
    }
    void readDataAvailable(size_t len) noexcept override {
      out.append(buf_, len);
      std::string end_marker("END\r\n");
      if (out.size() >= end_marker.size() &&
          out.compare(out.size() - end_marker.size(),
                      end_marker.size(),
                      end_marker) == 0) {
        complete();
      }
    }
    void readEOF() noexcept override {
      complete();
    }
    void readErr(const folly::AsyncSocketException& ex) noexcept override {
      handleError(ex);
    }
  };

  folly::EventBase base;

  Callback cb(&base, addr, input, ssl);
  cb.start();

  do {
    base.loopOnce();
  } while (!cb.done);

  if (out_error) {
    *out_error = cb.error;
  }

  return cb.error.empty() ? cb.out : "";
}

}}} // namespace facebook::logdevice::test
