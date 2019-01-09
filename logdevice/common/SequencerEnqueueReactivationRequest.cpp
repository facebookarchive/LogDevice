/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/SequencerEnqueueReactivationRequest.h"

#include "logdevice/common/SequencerBackgroundActivator.h"
#include "logdevice/common/Worker.h"

namespace facebook { namespace logdevice {

int SequencerEnqueueReactivationRequest::getThreadAffinity(int nthreads) {
  return SequencerBackgroundActivator::getThreadAffinity(nthreads);
}

Request::Execution SequencerEnqueueReactivationRequest::execute() {
  auto& seq_activator = Worker::onThisThread()->sequencerBackgroundActivator();
  if (!seq_activator) {
    seq_activator = std::make_unique<SequencerBackgroundActivator>();
  }
  seq_activator->schedule(std::move(reactivation_list_));
  return Execution::COMPLETE;
}

}} // namespace facebook::logdevice
