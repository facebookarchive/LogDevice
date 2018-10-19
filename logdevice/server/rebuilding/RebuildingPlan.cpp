/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
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

std::shared_ptr<EpochMetaData>
RebuildingPlan::lookUpEpoch(epoch_t epoch,
                            std::pair<epoch_t, epoch_t>* out_range) {
  // For this we'll be treating the boost::interval_map as if it were an
  // std::map<right_open_interval, T>, which it essentially is.

  // Find the first epoch range in epochsToRead whose lower end is strictly
  // above `epoch`.
  auto it = epochsToRead.upper_bound(
      boost::icl::right_open_interval<epoch_t::raw_type>(
          epoch.val(), epoch.val() + 1));
  epoch_t next_epoch =
      (it == epochsToRead.end()) ? EPOCH_MAX : epoch_t(it->first.lower());

  if (it == epochsToRead.begin()) {
    // We're below first epoch to read.
    *out_range = std::make_pair(EPOCH_INVALID, next_epoch);
    return nullptr;
  }

  // Find the last epoch range whose lower end is <= `epoch`.
  --it;

  if (it->first.upper() > epoch.val()) {
    // Found our epoch metadata.
    *out_range =
        std::make_pair(epoch_t(it->first.lower()), epoch_t(it->first.upper()));
    return it->second;
  }

  // `epoch` is in the empty space between `it` and `next_epoch`.
  *out_range = std::make_pair(epoch_t(it->first.upper()), next_epoch);
  return nullptr;
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
