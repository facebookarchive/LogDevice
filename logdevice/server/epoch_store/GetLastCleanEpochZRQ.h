/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <cstdio>

#include "logdevice/common/debug.h"
#include "logdevice/server/epoch_store/LastCleanEpochZRQ.h"
#include "logdevice/server/epoch_store/LogMetaData.h"

namespace facebook { namespace logdevice {

/**
 * @file  this ZookeeperEpochStoreRequest subclass drives ZookeeperEpochStore to
 *        execute a plain read of the epoch number from the <logid>/lce
 *        znode.
 */

class GetLastCleanEpochZRQ : public LastCleanEpochZRQ {
 public:
  using LastCleanEpochZRQ::LastCleanEpochZRQ;

  // see ZookeeperEpochStoreRequest.h
  NextStep applyChanges(LogMetaData& log_metadata,
                        bool value_existed) override {
    if (!value_existed) {
      err = E::NOTFOUND;
      return NextStep::FAILED;
    }
    auto [_, tail_record_ref] = referenceFromLogMetaData(log_metadata);

    ld_check(tail_record_ref.isValid());
    ld_check(!tail_record_ref.containOffsetWithinEpoch());

    err = E::OK; // on success signal to caller that processing must be
    return NextStep::STOP; // terminated and a completion request posted
  }

  // see ZookeeperEpochStoreRequest.h
  int composeZnodeValue(LogMetaData& /*log_metadata*/,
                        char* /*buf*/,
                        size_t /*size*/) const override {
    ld_check(false);
    RATELIMIT_CRITICAL(std::chrono::seconds(1),
                       10,
                       "INTERNAL ERROR. composeZnodeValue() called for a "
                       "read-only ZookeeperEpochStoreRequest");
    return -1;
  }
};

}} // namespace facebook::logdevice
