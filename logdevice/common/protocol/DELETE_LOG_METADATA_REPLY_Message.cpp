/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/protocol/DELETE_LOG_METADATA_REPLY_Message.h"

#include "logdevice/common/Address.h"
#include "logdevice/common/DeleteLogMetadataRequest.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/ShardID.h"
#include "logdevice/common/Worker.h"

namespace facebook { namespace logdevice {

template <>
Message::Disposition
DELETE_LOG_METADATA_REPLY_Message::onReceived(const Address& from) {
  if (from.isClientAddress()) {
    ld_error("Received DELETE_LOG_METADATA_REPLY(%s) from client %s",
             header_.toString().c_str(),
             Sender::describeConnection(from).c_str());
    err = E::PROTO;
    return Disposition::ERROR;
  }

  RATELIMIT_INFO(
      std::chrono::seconds(1),
      10,
      "Received DELETE_LOG_METADATA_REPLY(%s) for "
      "DeleteLogMetadataRequest rqid:%lu from %s(%s), with status=%s",
      header_.toString().c_str(),
      header_.client_rqid.val(),
      from.id_.node_.toString().c_str(),
      Sender::describeConnection(from).c_str(),
      error_name(header_.status));
  Worker* w = Worker::onThisThread();

  auto& map = w->runningFireAndForgets().map;

  auto it = map.find(header_.client_rqid);

  if (it != map.end()) {
    auto* req = dynamic_cast<DeleteLogMetadataRequest*>(&*it->second);
    ld_check(req);
    req->onReply(
        ShardID(from.asNodeID().index(), header_.shard_idx), header_.status);
  } else {
    ld_warning(
        "Received a DELETE_LOG_METADATA_REPLY(%s), rqid:%lu, from %s(%s),"
        " for which the corresponding request is not present.",
        header_.toString().c_str(),
        header_.client_rqid.val(),
        from.id_.node_.toString().c_str(),
        Sender::describeConnection(from).c_str());
  }
  return Message::Disposition::NORMAL;
}

}} // namespace facebook::logdevice
