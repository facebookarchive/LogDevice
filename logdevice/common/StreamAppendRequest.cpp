/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/StreamAppendRequest.h"

namespace facebook { namespace logdevice {

APPEND_flags_t StreamAppendRequest::getAppendFlags() {
  APPEND_flags_t append_flags = AppendRequest::getAppendFlags();
  append_flags |= APPEND_Header::WRITE_STREAM_REQUEST;
  if (stream_resume_) {
    append_flags |= APPEND_Header::WRITE_STREAM_RESUME;
  }
  return append_flags;
}

std::unique_ptr<APPEND_Message> StreamAppendRequest::createAppendMessage() {
  auto append_flags = getAppendFlags();
  APPEND_Header header = {id_,
                          record_.logid,
                          getSeenEpoch(record_.logid),
                          uint32_t(std::min<decltype(getTimeout())::rep>(
                              getTimeout().count(), UINT_MAX)),
                          append_flags};

  return std::make_unique<APPEND_Message>(
      header,
      getPreviousLsn(),
      // The key and payload may point into user-owned memory or be backed by a
      // std::string contained in this class.  Do not transfer ownership to
      // APPEND_Message.
      getAppendAttributes(),
      PayloadHolder(record_.payload, PayloadHolder::UNOWNED),
      getTracingContext(),
      stream_rqid_);
}

}} // namespace facebook::logdevice
