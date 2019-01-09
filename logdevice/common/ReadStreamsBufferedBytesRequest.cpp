/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/ReadStreamsBufferedBytesRequest.h"

#include "logdevice/common/Worker.h"
#include "logdevice/common/client_read_stream/AllClientReadStreams.h"

namespace facebook { namespace logdevice {

Request::Execution ReadStreamsBufferedBytesRequest::execute() {
  Worker* w = Worker::onThisThread();
  size_t streamBuffersSize = 0;
  for (auto& stats_handle_ : read_handles_) {
    auto currStream =
        w->clientReadStreams().getStream(stats_handle_.read_stream_id);
    if (currStream) {
      streamBuffersSize += currStream->getBytesBuffered();
    }
  }
  if (callback_) {
    callback_(streamBuffersSize);
  }
  return Execution::COMPLETE;
}

}} // namespace facebook::logdevice
