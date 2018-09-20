/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/include/Reader.h"

#include <algorithm>

#include "logdevice/common/types_internal.h"

namespace facebook { namespace logdevice {

lsn_t Reader::nextFromLsnWhenStuck(lsn_t stuck_lsn, lsn_t tail_lsn) {
  if (tail_lsn != LSN_INVALID || stuck_lsn == LSN_INVALID) {
    // tail_lsn is known or stuck_lsn is not known. Recommend reading from
    // beginning of tail epoch. Will return LSN_OLDEST if both tail_lsn and
    // stuck_lsn are not known (LSN_INVALID).
    return std::max(stuck_lsn, compose_lsn(lsn_to_epoch(tail_lsn), ESN_MIN));
  } else {
    // stuck_lsn is known but tail_lsn is not known. Readers shall skip to the
    // beginning of the next epoch (relative to stuck_lsn). This is the
    // minimum LSN where readers might make progress from.
    auto epoch = lsn_to_epoch(stuck_lsn).val();
    if (epoch_t{epoch} < EPOCH_MAX) {
      return compose_lsn(epoch_t{epoch + 1}, ESN_MIN);
    } else {
      // ESN overflow. Cannot skip further.
      return stuck_lsn;
    }
  }
}

}} // namespace facebook::logdevice
