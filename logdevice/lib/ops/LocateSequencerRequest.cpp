/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/lib/ops/LocateSequencerRequest.h"

namespace facebook { namespace logdevice {

Request::Execution LocateSequencerRequest::execute() {
  Worker* w = Worker::onThisThread();
  SequencerLocator& seqlocator = *(w->processor_->sequencer_locator_);

  seqlocator.locateSequencer(log_, *cb_);
  return Execution::COMPLETE;
}

WorkerType LocateSequencerRequest::getWorkerTypeAffinity() {
  return WorkerType::GENERAL;
}

}} // namespace facebook::logdevice
