/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <cstdio>

#include "logdevice/common/EpochStoreLastCleanEpochFormat.h"
#include "logdevice/common/debug.h"
#include "logdevice/server/LastCleanEpochZRQ.h"

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
  NextStep onGotZnodeValue(const char* znode_value,
                           int znode_value_len) override {
    ld_check(epoch_ == EPOCH_INVALID); // must not yet have been initialized

    if (!znode_value) {
      err = E::NOTFOUND;
      return NextStep::FAILED;
    }

    epoch_t parsed_epoch;
    TailRecord parsed_tail;

    int rv = EpochStoreLastCleanEpochFormat::fromLinearBuffer(
        znode_value, znode_value_len, logid_, &parsed_epoch, &parsed_tail);

    if (rv != 0) {
      err = E::BADMSG;
      return NextStep::FAILED;
    }

    ld_check(parsed_tail.isValid());
    ld_check(!parsed_tail.containOffsetWithinEpoch());
    epoch_ = parsed_epoch;
    tail_record_ = parsed_tail;

    err = E::OK; // on success signal to caller that processing must be
    return NextStep::STOP; // terminated and a completion request posted
  }

  // see ZookeeperEpochStoreRequest.h
  int composeZnodeValue(char* /*buf*/, size_t /*size*/) override {
    ld_check(false);
    RATELIMIT_CRITICAL(std::chrono::seconds(1),
                       10,
                       "INTERNAL ERROR. composeZnodeValue() called for a "
                       "read-only ZookeeperEpochStoreRequest");
    return -1;
  }
};

}} // namespace facebook::logdevice
