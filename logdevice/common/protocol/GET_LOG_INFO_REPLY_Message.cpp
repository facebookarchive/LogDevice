/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/protocol/GET_LOG_INFO_REPLY_Message.h"

#include "logdevice/common/GetLogInfoRequest.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Worker.h"

namespace facebook { namespace logdevice {

template <>
Message::Disposition
GET_LOG_INFO_REPLY_Message::onReceived(const Address& from) {
  // T25310022 - New clients do not use this message anymore
  // To be removed once all clients use LOGS_CONFIG_API_REPLY
  return Disposition::NORMAL;
}

template <>
std::vector<std::pair<std::string, folly::dynamic>>
GET_LOG_INFO_REPLY_Message::getDebugInfo() const {
  std::vector<std::pair<std::string, folly::dynamic>> res;

  res.emplace_back("client_rqid", header_.client_rqid.val());
  res.emplace_back("status", error_name(header_.status));
  res.emplace_back("blob_size", blob_.size());

  return res;
}

}} // namespace facebook::logdevice
