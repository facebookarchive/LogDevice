/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/Request.h"
#include "logdevice/common/RequestType.h"
#include "logdevice/common/types_internal.h"

namespace facebook { namespace logdevice {

/**
 * @file Request sent by ReaderImpl when it makes enough progress through the
 *       queue for ClientReadStream to request more records from storage nodes.
 */

class ReaderProgressRequest : public Request {
 public:
  explicit ReaderProgressRequest(ReadingHandle handle)
      : Request(RequestType::READER_PROGRESS), handle_(handle) {}

  Execution execute() override;

  int getThreadAffinity(int /*nthreads*/) override {
    return handle_.worker_id.val_;
  }

 private:
  ReadingHandle handle_;
};

}} // namespace facebook::logdevice
