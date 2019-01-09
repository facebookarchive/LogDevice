/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/ResumeReadingRequest.h"

#include "logdevice/common/DataRecordOwnsPayload.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/client_read_stream/AllClientReadStreams.h"

namespace facebook { namespace logdevice {

Request::Execution ResumeReadingRequest::execute() {
  Worker* w = Worker::onThisThread();
  ClientReadStream* stream =
      w->clientReadStreams().getStream(reading_handle_.read_stream_id);
  if (stream != nullptr) {
    stream->resumeReading();
    // Stream may be deleted here.
  }
  return Execution::COMPLETE;
}

}} // namespace facebook::logdevice
