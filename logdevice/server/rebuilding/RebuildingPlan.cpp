/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/server/rebuilding/RebuildingPlan.h"

namespace facebook { namespace logdevice {

void RebuildingPlan::addEpochRange(epoch_t since,
                                   epoch_t until,
                                   std::shared_ptr<EpochMetaData> metadata) {
  const auto epoch_interval =
      epoch_ranges_t::interval_type(since.val_, until.val_ + 1);
  addEpochRange(epoch_interval, std::move(metadata));
}
void RebuildingPlan::addEpochRange(epoch_ranges_t::interval_type epoch_range,
                                   std::shared_ptr<EpochMetaData> metadata) {
  epochsToRead.insert(std::make_pair(epoch_range, std::move(metadata)));
}

std::string RebuildingPlan::toString() const {
  std::string res = "until:" + logdevice::toString(untilLSN) + "; ";

  for (auto it = epochsToRead.begin(); it != epochsToRead.end(); ++it) {
    if (!res.empty()) {
      res += ", ";
    }
    res += "e" + logdevice::toString(it->first.lower()) + "-e" +
        logdevice::toString(it->first.upper() - 1) + ":" +
        it->second->toString();
  }

  return res;
}

}} // namespace facebook::logdevice
