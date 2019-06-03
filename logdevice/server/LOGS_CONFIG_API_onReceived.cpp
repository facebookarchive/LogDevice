/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/LOGS_CONFIG_API_onReceived.h"

#include "logdevice/common/LogsConfigApiRequest.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/logs/LogsConfigManager.h"
#include "logdevice/common/protocol/LOGS_CONFIG_API_Message.h"
#include "logdevice/common/protocol/LOGS_CONFIG_API_REPLY_Message.h"

namespace facebook { namespace logdevice {

static Message::Disposition send_error_reply(const LOGS_CONFIG_API_Message* msg,
                                             const Address& to,
                                             Status status) {
  LOGS_CONFIG_API_REPLY_Header reply_hdr{
      .client_rqid = msg->header_.client_rqid,
      .config_version = 0,
      .status = status,
      .total_payload_size = 0,
      .origin = msg->header_.origin};
  auto reply = std::make_unique<LOGS_CONFIG_API_REPLY_Message>(reply_hdr, "");
  if (Worker::onThisThread()->sender().sendMessage(std::move(reply), to) != 0) {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    10,
                    "error sending LOGS_CONFIG_API_REPLY message to client %s: "
                    "%s. Closing socket",
                    Sender::describeConnection(to).c_str(),
                    error_name(err));
    err = E::INTERNAL;
    return Message::Disposition::ERROR;
  }
  return Message::Disposition::NORMAL;
}

// This is a 1-1 copy of the old logic in GET_LOG_INFO_Message.cpp
//
// This is the callback to be called whenever the socket that we use for config
// change notifications is closed on the node. It erases the client from the
// list of clients that are subscribed to config changes.
namespace {
class LogsConfigApiMessageServerSocketClosedCallback : public SocketCallback {
 public:
  explicit LogsConfigApiMessageServerSocketClosedCallback(ClientID cid)
      : cid_(cid) {}

  void operator()(Status st, const Address& name) override {
    Worker::onThisThread()->configChangeSubscribers_.erase(cid_);
    ld_info("LogsConfig subscription destroyed for RemoteLogsConfig client %s "
            "due to peer socket closing with: %s",
            name.toString().c_str(),
            error_name(st));
    delete this;
  }

 private:
  ClientID cid_;
};
} // namespace

Message::Disposition
LOGS_CONFIG_API_onReceived(LOGS_CONFIG_API_Message* msg,
                           const Address& from,
                           PermissionCheckStatus permission_status) {
  Worker* w = Worker::onThisThread();
  // Remember which worker this was posted on so we can post the response
  // // on the same worker.
  auto respond_to_worker = w->idx_.val();
  auto respond_to_worker_type = w->worker_type_;
  auto tracer = std::make_unique<LogsConfigApiTracer>(w->getTraceLogger());
  auto lcm_worker_type = LogsConfigManager::workerType(w->processor_);

  Status st = PermissionChecker::toStatus(permission_status);
  if (st != E::OK) {
    RATELIMIT_LEVEL(st == E::ACCESS ? dbg::Level::WARNING : dbg::Level::INFO,
                    std::chrono::seconds(2),
                    1,
                    "LOGS_CONFIG_API_Message from %s failed with %s",
                    Sender::describeConnection(from).c_str(),
                    error_description(st));
    return send_error_reply(msg, from, st);
  }

  if (msg->header_.subscribe_to_config_) {
    // Subscribing the client to notifications of config changes
    ClientID cid = from.id_.client_;
    auto inserted = w->configChangeSubscribers_.insert(cid);

    // Registering on socket close callback if we haven't done this already
    if (inserted.second) {
      // This callback commits suicide with 'delete this' after being called.
      LogsConfigApiMessageServerSocketClosedCallback* onclose =
          new LogsConfigApiMessageServerSocketClosedCallback(cid);
      int rv = w->sender().registerOnSocketClosed(from, *onclose);
      if (rv != 0) {
        (*onclose)(E::FAILED, from);
      }
    }
  }

  std::unique_ptr<Request> req =
      std::make_unique<LogsConfigManagerRequest>(msg->header_,
                                                 from,
                                                 std::move(msg->blob_),
                                                 respond_to_worker,
                                                 respond_to_worker_type,
                                                 lcm_worker_type,
                                                 std::move(tracer));

  int rv = w->processor_->postRequest(req);

  if (rv != 0) {
    if (err != E::NOBUFS) {
      ld_check_eq(err, E::SHUTDOWN);
      err = E::INTERNAL;
      return Message::Disposition::ERROR;
    }

    // Try to send a reply with NOBUFS status to make client retry without
    // waiting for timeout.
    return send_error_reply(msg, from, E::NOBUFS);
  }

  return Message::Disposition::NORMAL;
}

}} // namespace facebook::logdevice
