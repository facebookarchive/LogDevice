/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/MetaDataTracer.h"

#include "logdevice/common/util.h"

namespace facebook { namespace logdevice {

void MetaDataTracer::trace(Status st, lsn_t appended_lsn) {
  if (action_ == Action::NONE) {
    // no action specified - no trace needed
    return;
  }
  if (!logger_) {
    // no logger - no trace
    return;
  }
  auto duration_us = usec_since(start_time_);

  auto sample = std::make_unique<TraceSample>();
  sample->addIntValue("log_id", log_id_.val());
  sample->addNormalValue("action", actionToStr(action_));
  if (old_metadata_.hasValue()) {
    sample->addNormalValue("metadata_old", old_metadata_.value().toString());
  }
  if (new_metadata_.isValid()) {
    sample->addNormalValue("metadata_new", new_metadata_.toString());
    sample->addIntValue(
        "effective_since_epoch", new_metadata_.h.effective_since.val());
  }
  sample->addNormalValue("status", error_name(st));
  sample->addIntValue("duration_us", duration_us);
  if (appended_lsn != LSN_INVALID) {
    sample->addNormalValue(
        "appended_lsn", folly::to<std::string>(appended_lsn));
  }

  logger_->pushSample(METADATA_TRACER, 1, std::move(sample));
}

}} // namespace facebook::logdevice
