/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/protocol/GET_LOG_INFO_Message.h"

#include <folly/Memory.h>
#include <folly/json.h>

#include "logdevice/common/GetLogInfoRequest.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/LocalLogsConfig.h"
#include "logdevice/common/protocol/GET_LOG_INFO_REPLY_Message.h"
#include "logdevice/common/stats/Stats.h"

namespace facebook { namespace logdevice {

static void send_reply(const Address& to,
                       request_id_t client_rqid,
                       Status status,
                       std::string result_json) {
  GET_LOG_INFO_REPLY_Header header = {client_rqid, status};
  auto message = std::make_unique<GET_LOG_INFO_REPLY_Message>(
      header, std::move(result_json));
  Worker::onThisThread()->sender().sendMessage(std::move(message), to);
}

static void send_error(const Address& to,
                       request_id_t client_rqid,
                       Status status) {
  send_reply(to, client_rqid, status, std::string());
}

// This is the callback to be called whenever the socket that we use for config
// change notifications is closed on the node. It erases the client from the
// list of clients that are subscribed to config changes.
class GetLogInfoServerSocketClosedCallback : public SocketCallback {
 public:
  explicit GetLogInfoServerSocketClosedCallback(ClientID cid) : cid_(cid) {}

  void operator()(Status /*st*/, const Address& /*name*/) override {
    Worker::onThisThread()->configChangeSubscribers_.erase(cid_);
    delete this;
  }

 private:
  ClientID cid_;
};

template <>
Message::Disposition GET_LOG_INFO_Message::onReceived(const Address& from) {
  if (!from.isClientAddress()) {
    ld_error("got GET_LOG_INFO message from non-client %s",
             Sender::describeConnection(from).c_str());
    err = E::PROTO;
    return Disposition::ERROR;
  }

  Worker* worker = Worker::onThisThread();
  if (!worker->isAcceptingWork()) {
    ld_debug("Ignoring GET_LOG_INFO message: not accepting more work");
    send_error(from, header_.client_rqid, E::SHUTDOWN);
    return Disposition::NORMAL;
  }

  // Subscribing the client to notifications of config changes
  ClientID cid = from.id_.client_;
  auto inserted = Worker::onThisThread()->configChangeSubscribers_.insert(cid);

  // Registering on socket close callback if we haven't done this already
  if (inserted.second) {
    // This callback commits suicide with 'delete this' after being called.
    GetLogInfoServerSocketClosedCallback* onclose =
        new GetLogInfoServerSocketClosedCallback(cid);
    int rv =
        Worker::onThisThread()->sender().registerOnSocketClosed(from, *onclose);
    if (rv != 0) {
      // Apparently, we don't have a socket to the client anymore? Dropping
      // the client from the subscriber list and deleting the callback
      (*onclose)(E::FAILED, from);
    }
  }

  auto config = Worker::getConfig();
  ld_check(config->logsConfig()->isLocal());

  if (!config->logsConfig()->isFullyLoaded()) {
    send_error(from, header_.client_rqid, E::NOTREADY);
    return Disposition::NORMAL;
  }

  const LogsConfig::LogGroupInDirectory* log = nullptr;

  logid_t log_id = header_.log_id; // for request_type == BY_ID

  switch (header_.request_type) {
    case GET_LOG_INFO_Header::Type::BY_NAME: {
      std::pair<logid_t, logid_t> range =
          config->logsConfig()->getLogRangeByName(blob_);
      // log_id could be LOGID_INVALID if range doesn't exist
      log_id = range.first;
      ld_debug(": requested BY_NAME name=%s res=%lu - %lu from=%s",
               blob_.c_str(),
               range.first.val(),
               range.second.val(),
               Sender::describeConnection(from).c_str());
    }
      // no break - continuing to get the actual info below
    case GET_LOG_INFO_Header::Type::BY_ID: {
      WORKER_LOG_STAT_INCR(log_id, get_log_info_received);
      log = config->getLogGroupInDirectoryByIDRaw(log_id);
      ld_debug(": requested BY_ID id=%lu found=%s from=%s",
               log_id.val(),
               log ? "yes" : "no",
               Sender::describeConnection(from).c_str());
      if (!log) {
        send_error(from, header_.client_rqid, E::NOTFOUND);
        return Disposition::NORMAL;
      }

      // for legacy reasons, this expects a different id string
      // when reading a metadata log. We manually patch this here
      // since metadata logs do not have an interval string and
      // for -some- reason, all attributes were also included
      auto log_dynamic = log->toFollyDynamic(false);
      if (MetaDataLog::isMetaDataLog(log_id)) {
        log_dynamic["id"] = std::to_string(log_id.val());
        log_dynamic["name"] = "metadata logs";
      }
      send_reply(from, header_.client_rqid, E::OK, folly::toJson(log_dynamic));
      return Disposition::NORMAL;
    } break;
    case GET_LOG_INFO_Header::Type::BY_NAMESPACE: {
      ld_debug(": requested BY_NAMESPACE namespace=%s from=%s",
               blob_.c_str(),
               Sender::describeConnection(from).c_str());

      auto logs = config->logsConfig()->getLogRangesByNamespace(blob_);
      folly::dynamic result = folly::dynamic::array;
      ld_check(result.isArray());

      for (auto& iter : logs) {
        auto& range = iter.second;
        ld_check(range.first != LOGID_INVALID);
        ld_check(range.second != LOGID_INVALID);
        ld_check(range.second >= range.first);
        auto id_log = config->getLogGroupInDirectoryByIDRaw(range.first);
        ld_check(id_log != nullptr);
        result.push_back(id_log->toFollyDynamic());
      }

      // serializing JSON
      std::string json = folly::toJson(result);
      send_reply(from, header_.client_rqid, E::OK, std::move(json));
      return Disposition::NORMAL;
    } break;
    case GET_LOG_INFO_Header::Type::ALL:
      // TODO: t7003894
      ld_check(!"not implemented");
      break;
  }
  err = E::PROTO;
  return Disposition::ERROR;
}

template <>
void GET_LOG_INFO_Message::onSent(Status status, const Address& to) const {
  ld_debug(": message=GET_LOG_INFO st=%s to=%s",
           error_name(status),
           Sender::describeConnection(to).c_str());

  // Inform the GetLogInfoFromNodeRequest of the outcome of sending the message
  auto& rqmap = Worker::onThisThread()->runningGetLogInfo().per_node_map;
  auto it = rqmap.find(header_.client_rqid);
  if (it != rqmap.end()) {
    it->second->onMessageSent(to.id_.node_, status);
  }
}

template<>
std::vector<std::pair<std::string, folly::dynamic>>
GET_LOG_INFO_Message::getDebugInfo() const {
  std::vector<std::pair<std::string, folly::dynamic>> res;

  res.emplace_back("client_rqid", header_.client_rqid.val());

  using Type = GET_LOG_INFO_Header::Type;
  switch (header_.request_type) {
    case Type::BY_ID:
      res.emplace_back("type", "by_id");
      res.emplace_back("log_id", header_.log_id.val());
      break;
    case Type::BY_NAME:
      res.emplace_back("type", "by_name");
      res.emplace_back("name", blob_);
      break;
    case Type::BY_NAMESPACE:
      res.emplace_back("type", "by_namespace");
      res.emplace_back("namespace", blob_);
      break;
    case Type::ALL:
      res.emplace_back("type", "all");
      break;
  }

  return res;
}

}} // namespace facebook::logdevice
