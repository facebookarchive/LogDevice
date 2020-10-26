/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/thrift/compat/ThriftSession.h"

#include <memory>
#include <string>

#include <sys/stat.h>

#include "logdevice/common/NetworkDependencies.h"
#include "logdevice/common/checks.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/if/gen-cpp2/ApiModel_types.h"
#include "logdevice/common/if/gen-cpp2/LogDeviceAPIAsyncClient.h"
#include "logdevice/common/protocol/HELLO_Message.h"
#include "logdevice/common/protocol/MessageType.h"
#include "logdevice/common/protocol/MessageTypeNames.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/stats/Stats.h"

using apache::thrift::ClientBufferedStream;
using apache::thrift::ClientReceiveState;
using apache::thrift::RequestCallback;
using apache::thrift::ResponseAndClientBufferedStream;

using facebook::logdevice::thrift::LogDeviceAPIAsyncClient;
using facebook::logdevice::thrift::SessionResponse;

namespace facebook { namespace logdevice {

ThriftSession::ThriftSession(ConnectionInfo&& info, NetworkDependencies& deps)
    : info_(std::move(info)), deps_(deps), description_(info_.describe()) {
  serializer_ = std::make_unique<ThriftMessageSerializer>(description_);
}

ThriftSession::~ThriftSession() {
  if (isHandshakeAttempted() && !isClosed()) {
    ld_error("Session %s has not been closed before destruction",
             description_.c_str());
    ld_check(false);
  }
};

void ThriftSession::setInfo(ConnectionInfo&& new_info) {
  // Peer name is not allowed to change
  ld_check(info_.peer_name == new_info.peer_name);

  if (*new_info.principal != *info_.principal) {
    // Only incoming connections are authenticated
    ld_check(new_info.peer_name.isClientAddress());
    // No changes to principal apart from one-time upgrade from empty
    ld_check(info_.principal->isEmpty());
  }
  info_ = std::move(new_info);
}

const Settings& ThriftSession::settings() const {
  return deps_.getSettings();
}

uint16_t ThriftSession::getProtocolVersion() const {
  return info_.protocol.value_or(settings().max_protocol);
}

bool ThriftSession::checkProtocol() const {
  ld_check(info_.protocol.hasValue());
  uint16_t protocol = info_.protocol.value();
  bool valid = protocol >= Compatibility::MIN_PROTOCOL_SUPPORTED &&
      protocol <= Compatibility::MAX_PROTOCOL_SUPPORTED &&
      protocol <= settings().max_protocol;
  if (!valid) {
    RATELIMIT_ERROR(std::chrono::seconds(5),
                    1,
                    "Session %s received invalid protocol version during "
                    "handshake, protocol: %u, max protocol %u",
                    description_.c_str(),
                    protocol,
                    settings().max_protocol);
  }
  return valid;
}

std::unique_ptr<thrift::Message> ThriftSession::serialize(const Message& msg) {
  return serializer_->toThrift(msg, getProtocolVersion());
}

std::unique_ptr<Message> ThriftSession::deserialize(thrift::Message&& msg) {
  return serializer_->fromThrift(std::move(msg), getProtocolVersion());
}

void ThriftSession::onMessage(thrift::Message&& serialized_msg) {
  ld_check(isHandshakeAttempted());
  // TODO(mmhg): Aquire resource token to avoid overload
  auto msg = deserialize(std::move(serialized_msg));
  if (msg == nullptr) {
    close(err);
    return;
  }
  // Tell the Worker that we're processing a message, so it can time it.
  // The time will include message's deserialization, checksumming,
  // onReceived, destructor and Socket's processing overhead.
  RunContext run_context(msg->type_);
  deps_.onStartedRunning(run_context);
  SCOPE_EXIT {
    deps_.onStoppedRunning(run_context);
  };

  // TODO(mmhg): Dispatch message
  if (getState() == State::HANDSHAKING) {
    if (!handleHandshakeMessage(std::move(*msg))) {
      return;
    }
    if (!checkProtocol()) {
      close(E::PROTO);
      return;
    }
    ld_spew("%s negotiated protocol %d", description_.c_str(), *info_.protocol);
    advance(State::ESTABLISHED);
  }
}

void ThriftSession::fillDebugInfo(InfoSocketsTable&) const {
  // TODO(mmhg): Implement this
  ld_check(false);
}

namespace {

/**
 * Adapts Thift async API for sendMessage and calls provided functions
 * - on_sent_cb_ when message acknowledged by Thrift layer
 * - on_error_cb if error happens before message sent by Thrift
 * We intentionally ignore result (it is empty) and any errors happening on the
 * peer during processing: these errors are not supported by our current network
 * stack anyway.
 */
class SendMessageCb : public RequestCallback {
 public:
  using OnSentCb = std::function<void()>;
  using OnErrorCb = std::function<void(folly::exception_wrapper&&)>;
  using OnReplyCb = std::function<void(ClientReceiveState&&)>;

  SendMessageCb(const std::string& session_description,
                std::shared_ptr<ThriftSession::State> session_state,
                OnErrorCb&& on_error_cb,
                OnSentCb&& on_sent_cb = []() {},
                OnReplyCb&& on_reply_cb = [](auto&&) {})
      : session_description_(session_description),
        session_state_(std::move(session_state)),
        on_sent_cb_(std::move(on_sent_cb)),
        on_error_cb_(std::move(on_error_cb)) {}

  void requestSent() override {
    if (isSessionClosed()) {
      return;
    }
    on_sent_cb_();
    sent_ = true;
  }

  void replyReceived(ClientReceiveState&& state) override {
    // noop, the result is empty now anyway
  }

  void requestError(ClientReceiveState&& state) override {
    if (isSessionClosed()) {
      return;
    }
    ld_check(state.isException());
    if (!sent_) {
      on_error_cb_(std::move(state.exception()));
    } else {
      RATELIMIT_WARNING(std::chrono::seconds(5),
                        1,
                        "Session %s received remote Thrift error %s",
                        session_description_.c_str(),
                        state.exception().what().c_str());
    }
  }

 private:
  const std::string& session_description_;
  const std::shared_ptr<ThriftSession::State> session_state_;
  OnSentCb on_sent_cb_;
  OnErrorCb on_error_cb_;
  bool sent_ = false;

  bool isSessionClosed() const {
    return *session_state_ == ThriftSession::State::CLOSED;
  }
};

class ServerMessageCb {
 public:
  explicit ServerMessageCb(std::shared_ptr<ThriftSession::State> session_state,
                           ServerSession& session)
      : session_state_(std::move(session_state)), session_(session) {}

  void operator()(folly::Try<thrift::Message>&& t) {
    if (*session_state_ == ThriftSession::State::CLOSED) {
      return;
    }
    if (t.hasValue()) {
      session_.onMessage(std::move(t.value()));
    } else if (t.hasException()) {
      session_.onSessionError(std::move(t.exception()));
    } else {
      session_.onSessionComplete();
    }
  }

 private:
  const std::shared_ptr<ThriftSession::State> session_state_;
  ServerSession& session_;
};
} // namespace

ServerSession::ServerSession(ConnectionInfo&& info,
                             NetworkDependencies& deps,
                             std::unique_ptr<LogDeviceAPIAsyncClient> client)
    : ThriftSession(std::move(info), deps), client_(std::move(client)) {
  ld_check(client_);
  ld_check(info_.peer_name.isNodeAddress());
}

int ServerSession::connect() {
  if (isHandshakeAttempted()) {
    err = getState() == State::ESTABLISHED ? E::ISCONN : E::ALREADY;
    return -1;
  }
  ld_check(getState() == State::NEW);
  // Setup callbacks
  SendMessageCb::OnErrorCb on_error = [this](folly::exception_wrapper&& ew) {
    onHandshakeError(std::move(ew));
  };
  SendMessageCb::OnReplyCb on_reply = [this](ClientReceiveState&& state) {
    onHandshakeReply(std::move(state));
  };
  auto cb = std::make_unique<SendMessageCb>(
      description_, state_, std::move(on_error), []() {}, std::move(on_reply));
  // Create and serialize HELLO_Message
  auto hello = deps_.createHelloMessage(info_.peer_name.asNodeID());
  thrift::SessionRequest request;
  auto serialized = serialize(*hello);
  if (!serialized) {
    // err set by serialization
    return -1;
  }
  request.set_helloMessage(serialized->move_legacyMessage());
  // Establish Thrift session
  client_->createSession(std::move(cb), request);
  // Setup handshake timeout
  std::chrono::milliseconds timeout = settings().handshake_timeout;
  if (timeout.count() > 0) {
    handshake_timer_ =
        deps_.createTimer([this]() { onHandshakeTimeout(); }, timeout);
  }
  // Update session's state
  info_.is_active->store(true);
  advance(State::HANDSHAKING);
  return 0;
}

void ServerSession::onHandshakeTimeout() {
  RATELIMIT_WARNING(std::chrono::seconds(10),
                    10,
                    "Handshake timeout occurred (session: %s).",
                    description_.c_str());
  close(E::TIMEDOUT);
  STAT_INCR(deps_.getStats(), handshake_timeouts);
}

void ServerSession::onHandshakeError(folly::exception_wrapper&& ew) {
  RATELIMIT_WARNING(
      std::chrono::seconds(1),
      1,
      "Handshake attempt failed on connection %s, reason: %s",
      description_.c_str(),
      ew.has_exception_ptr() ? std::move(ew).what().c_str() : "UNKNOWN");
  close(E::CONNFAILED);
}

void ServerSession::onHandshakeReply(ClientReceiveState&& state) {
  ResponseAndClientBufferedStream<SessionResponse, thrift::Message> result;
  auto ex = LogDeviceAPIAsyncClient::recv_wrapped_createSession(result, state);
  if (ex) {
    onHandshakeError(std::move(ex));
  } else {
    registerStream(std::move(result.stream));
  }
}

void ServerSession::registerStream(
    ClientBufferedStream<thrift::Message>&& stream) {
  ld_check(getState() == State::HANDSHAKING);
  ServerMessageCb cb(state_, *this);
  auto sub = std::move(stream).subscribeExTry(deps_.getExecutor(), cb);
  subscription_ = std::make_unique<Subscription>(std::move(sub));
}

bool ServerSession::handleHandshakeMessage(Message&& msg) {
  ld_check(getState() == State::HANDSHAKING);
  if (!isACKMessage(msg.type_)) {
    RATELIMIT_ERROR(
        std::chrono::seconds(10),
        1,
        "Session %s received unexpected message during handshake %s",
        description_.c_str(),
        messageTypeNames()[msg.type_].c_str());
    close(E::PROTO);
    return false;
  }
  deps_.processACKMessage(msg, info_);
  handshake_timer_.reset();
  return true;
}

void ServerSession::onSessionError(folly::exception_wrapper&& ew) {
  RATELIMIT_INFO(std::chrono::seconds(10),
                 1,
                 "Session %s hit error %s",
                 description_.c_str(),
                 std::move(ew).what().c_str());
  close(E::CONNFAILED);
}

void ServerSession::onSessionComplete() {
  RATELIMIT_INFO(std::chrono::seconds(10),
                 1,
                 "Session %s was closed by peer",
                 description_.c_str());
  close(E::PEER_CLOSED);
}

void ServerSession::close(Status reason) {
  if (getState() == State::CLOSED) {
    return;
  }

  advance(State::CLOSED);
  info_.is_active->store(false);
  if (subscription_) {
    subscription_->cancel();
    std::move(*subscription_).detach();
  }

  auto debug_level =
      (reason == E::CONNFAILED || reason == E::TIMEDOUT || reason == E::IDLE)
      ? dbg::Level::DEBUG
      : dbg::Level::INFO;
  RATELIMIT_LEVEL(debug_level,
                  std::chrono::seconds(10),
                  10,
                  "Closing session %s. Reason: %s",
                  description_.c_str(),
                  error_description(reason));

  handshake_timer_.reset();
  // TODO(mmhg): Decrement active session counters
}

ClientSession::ClientSession(ConnectionInfo&& info, NetworkDependencies& deps)
    : ThriftSession(std::move(info), deps) {
  ld_check(info_.peer_name.isClientAddress());
}

void ClientSession::close(Status) {
  // TODO(mmhg): Implement this
  ld_check(false);
}

}} // namespace facebook::logdevice
