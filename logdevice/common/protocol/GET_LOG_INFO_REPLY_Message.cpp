/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "GET_LOG_INFO_REPLY_Message.h"

#include "logdevice/common/GetLogInfoRequest.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Worker.h"

namespace facebook { namespace logdevice {

template <>
Message::Disposition
GET_LOG_INFO_REPLY_Message::onReceived(const Address& from) {
  if (from.isClientAddress()) {
    ld_error("got GET_LOG_INFO_REPLY message from client %s",
             Sender::describeConnection(from).c_str());
    err = E::PROTO;
    return Disposition::ERROR;
  }

  // Status gets validated in GetLogInfoFromNodeRequest::onReply() where
  // we switch on it

  auto& rqmap = Worker::onThisThread()->runningGetLogInfo().per_node_map;
  auto it = rqmap.find(header_.client_rqid);
  if (it != rqmap.end()) {
    it->second->onReply(from.id_.node_, header_.status, std::move(blob_));
  }
  return Disposition::NORMAL;
}

}} // namespace facebook::logdevice
