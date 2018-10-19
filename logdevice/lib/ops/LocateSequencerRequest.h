/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/Processor.h"
#include "logdevice/common/SequencerLocator.h"
#include "logdevice/common/Worker.h"
#include "logdevice/lib/ClientImpl.h"

namespace facebook { namespace logdevice {

class LocateSequencerRequest : public Request {
 public:
  using Callback =
      std::function<void(Status status, logid_t logid, NodeID node_id)>;

  explicit LocateSequencerRequest(logid_t logid, Callback* cb)
      : Request(RequestType::LOCATE_SEQUENCER), log_(logid), cb_(cb) {
    ld_check(logid != LOGID_INVALID);
    ld_check(cb_ != nullptr);
  }

  Request::Execution execute() override;
  WorkerType getWorkerTypeAffinity() override;

 private:
  logid_t log_;
  const Callback* cb_;
};

}} // namespace facebook::logdevice
