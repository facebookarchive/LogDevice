/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/protocol/LOGS_CONFIG_API_Message.h"
#include "logdevice/common/GetLogInfoRequest.h"
#include <folly/Memory.h>
#include "logdevice/common/LogsConfigApiRequest.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/logs/LogsConfigManager.h"
#include "logdevice/common/protocol/LOGS_CONFIG_API_REPLY_Message.h"

namespace facebook { namespace logdevice {

void LOGS_CONFIG_API_Message::serialize(ProtocolWriter& writer) const {
  writer.write(&header_, LOGS_CONFIG_API_Header::headerSize(writer.proto()));
  const blob_size_t size = blob_.size();
  ld_check(blob_.size() <= Message::MAX_LEN -
               LOGS_CONFIG_API_Header::headerSize(writer.proto()) -
               sizeof(size));
  writer.write(size);
  if (size > 0) {
    writer.write(blob_.data(), size);
  }
}

MessageReadResult LOGS_CONFIG_API_Message::deserialize(ProtocolReader& reader) {
  std::unique_ptr<LOGS_CONFIG_API_Message> m(new LOGS_CONFIG_API_Message());

  // Set properties for old protocol (LOGS_CONFIG_API_SUBSCRIPTIONS)
  m->header_.origin = LogsConfigRequestOrigin::LOGS_CONFIG_API_REQUEST;
  m->header_.subscribe_to_config_ = false;

  reader.read(&m->header_, LOGS_CONFIG_API_Header::headerSize(reader.proto()));
  blob_size_t blob_length = 0;
  reader.read(&blob_length);
  if (blob_length) {
    m->blob_.resize(blob_length);
    reader.read(&m->blob_.front(), blob_length);
  }

  reader.allowTrailingBytes();
  return reader.resultMsg(std::move(m));
}

void LOGS_CONFIG_API_Message::onSent(Status status, const Address& to) const {
  switch (header_.origin) {
    case LogsConfigRequestOrigin::LOGS_CONFIG_API_REQUEST: {
      if (status != E::OK) {
        auto& rqmap =
            Worker::onThisThread()->runningLogsConfigApiRequests().map;
        auto it = rqmap.find(header_.client_rqid);
        if (it != rqmap.end()) {
          it->second->onError(status);
        }
      }
      break;
    }
    case LogsConfigRequestOrigin::REMOTE_LOGS_CONFIG_REQUEST: {
      // Report to GetLogInfoFromNodeRequest of the send result
      auto& rqmap = Worker::onThisThread()->runningGetLogInfo().per_node_map;
      auto it = rqmap.find(header_.client_rqid);
      if (it != rqmap.end()) {
        it->second->onMessageSent(to.id_.node_, status);
      }
      break;
    }
  }
}

// This is a 1-1 copy of the logic in GET_LOG_INFO_Message.cpp
// LOGS_CONFIG_API_Message will soon replace these and then all of the code
// there can be removed
//
// This is the callback to be called whenever the socket that we use for config
// change notifications is closed on the node. It erases the client from the
// list of clients that are subscribed to config changes.
namespace {
class LogsConfigApiMessageServerSocketClosedCallback : public SocketCallback {
 public:
  explicit LogsConfigApiMessageServerSocketClosedCallback(ClientID cid)
      : cid_(cid) {}

  void operator()(Status /*st*/, const Address& /*name*/) override {
    Worker::onThisThread()->configChangeSubscribers_.erase(cid_);
    delete this;
  }

 private:
  ClientID cid_;
};
} // namespace

Message::Disposition LOGS_CONFIG_API_Message::onReceived(const Address& from) {
  Worker* w = Worker::onThisThread();
  // Remember which worker this was posted on so we can post the response
  // // on the same worker.
  auto respond_to_worker = w->idx_.val();
  auto respond_to_worker_type = w->worker_type_;
  auto tracer = std::make_unique<LogsConfigApiTracer>(w->getTraceLogger());
  auto lcm_worker_type = LogsConfigManager::workerType(w->processor_);

  if (header_.subscribe_to_config_) {
    // Subscribing the client to notifications of config changes
    ClientID cid = from.id_.client_;
    auto inserted =
        Worker::onThisThread()->configChangeSubscribers_.insert(cid);

    // Registering on socket close callback if we haven't done this already
    if (inserted.second) {
      // This callback commits suicide with 'delete this' after being called.
      LogsConfigApiMessageServerSocketClosedCallback* onclose =
          new LogsConfigApiMessageServerSocketClosedCallback(cid);
      int rv = Worker::onThisThread()->sender().registerOnSocketClosed(
          from, *onclose);
      if (rv != 0) {
        (*onclose)(E::FAILED, from);
      }
    }
  }

  std::unique_ptr<Request> req =
      std::make_unique<LogsConfigManagerRequest>(header_,
                                                 from,
                                                 std::move(blob_),
                                                 respond_to_worker,
                                                 respond_to_worker_type,
                                                 lcm_worker_type,
                                                 std::move(tracer));

  int rv = w->processor_->postRequest(req);

  if (rv != 0) {
    if (err != E::NOBUFS) {
      ld_check_eq(err, E::SHUTDOWN);
      err = E::INTERNAL;
      return Disposition::ERROR;
    }

    // Try to send a reply with NOBUFS status to make client retry without
    // waiting for timeout.

    LOGS_CONFIG_API_REPLY_Header hdr{.client_rqid = header_.client_rqid,
                                     .config_version = 0,
                                     .status = E::NOBUFS,
                                     .total_payload_size = 0,
                                     .origin = header_.origin};

    auto msg = std::make_unique<LOGS_CONFIG_API_REPLY_Message>(hdr, "");
    w->sender().sendMessage(std::move(msg), from); // Ignore errors.
  }

  return Disposition::NORMAL;
}

}} // namespace facebook::logdevice
