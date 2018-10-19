/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/include/AsyncReader.h"

#include "logdevice/common/types_internal.h"
#include "logdevice/include/Reader.h"

namespace facebook { namespace logdevice {

lsn_t AsyncReader::nextFromLsnWhenStuck(lsn_t stuck_lsn, lsn_t tail_lsn) {
  return Reader::nextFromLsnWhenStuck(stuck_lsn, tail_lsn);
}

}} // namespace facebook::logdevice
