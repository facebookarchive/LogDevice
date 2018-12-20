/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/Sampling.h"

#include <sstream>

#include <folly/Format.h>

#include "logdevice/common/toString.h"

namespace facebook { namespace logdevice {

void ProbabilityDistribution::assign(std::initializer_list<double> il) {
  assign(il.begin(), il.end());
}

void ProbabilityDistribution::push_back(double w) {
  weight_cumsum_.push_back(totalWeight() + w);
}

double ProbabilityDistribution::prefixSum(size_t count) const {
  ld_check(count <= weight_cumsum_.size());
  return count ? weight_cumsum_[count - 1] : 0;
}
double ProbabilityDistribution::weight(size_t idx) const {
  double w = prefixSum(idx + 1) - prefixSum(idx);
  ld_check(w >= 0);
  return w;
}
size_t ProbabilityDistribution::findPrefixBySum(double s) const {
  return std::upper_bound(weight_cumsum_.begin(), weight_cumsum_.end(), s) -
      weight_cumsum_.begin();
}

double ProbabilityDistribution::totalWeight() const {
  return size() ? weight_cumsum_.back() : 0.;
}

std::string ProbabilityDistribution::toString() const {
  std::vector<std::string> weightsAsStrings;
  for (size_t i = 0; i < size(); ++i) {
    weightsAsStrings.push_back(folly::sformat("{:0.3f}", weight(i)));
  }
  return logdevice::toString(weightsAsStrings);
}

double ProbabilityDistributionAdjustment::prefixSum(size_t count) const {
  auto it = added_cumsum_.lower_bound(count);
  if (it == added_cumsum_.begin()) {
    return 0;
  }
  --it;
  return it->second;
}

double ProbabilityDistributionAdjustment::weight(size_t idx) const {
  auto it = added_cumsum_.find(idx);
  if (it == added_cumsum_.end()) {
    return 0;
  }
  double s = it->second;
  if (it == added_cumsum_.begin()) {
    return s;
  }
  --it;
  return s - it->second;
}

void ProbabilityDistributionAdjustment::addWeight(size_t idx, double delta) {
  auto rv = added_cumsum_.emplace(idx, 0);
  if (rv.second && rv.first != added_cumsum_.begin()) {
    auto it = rv.first;
    --it;
    rv.first->second = it->second;
  }
  for (; rv.first != added_cumsum_.end(); ++rv.first) {
    rv.first->second += delta;
  }
}

double ProbabilityDistributionAdjustment::revert(size_t idx) {
  auto it = added_cumsum_.find(idx);
  if (it == added_cumsum_.end()) {
    return 0;
  }
  double w = it->second;
  if (it != added_cumsum_.begin()) {
    auto it2 = it;
    --it2;
    w -= it2->second;
  }
  it = added_cumsum_.erase(it);
  for (; it != added_cumsum_.end(); ++it) {
    it->second -= w;
  }
  return w;
}

void ProbabilityDistributionAdjustment::clear() {
  added_cumsum_.clear();
}

double ProbabilityDistributionAdjustment::totalAddedWeight() const {
  if (added_cumsum_.empty()) {
    return 0;
  }
  auto it = added_cumsum_.end();
  --it;
  return it->second;
}

double AdjustedProbabilityDistribution::prefixSum(size_t count) const {
  double add = diff_ ? diff_->prefixSum(count) : 0.;
  double res = base_->prefixSum(count) + add;
  ld_check(res >= -Sampling::EPSILON * base_->totalWeight());
  return res;
}

double AdjustedProbabilityDistribution::weight(size_t idx) const {
  return base_->weight(idx) + (diff_ ? diff_->weight(idx) : 0.);
}

size_t AdjustedProbabilityDistribution::findPrefixBySum(double s) const {
  if (!diff_) {
    return base_->findPrefixBySum(s);
  }

  // Here we use the fact that SmallMap iterator happens to provide random
  // access. If you're changing implementation of SmallMap to not provide that,
  // this binary search would need to be changed a bit.
  auto add = diff_->added_cumsum_.begin();
  auto add_size = diff_->added_cumsum_.size();

  // First do a binary search in `add`, then binary search in the corresponding
  // range of base_->weight_cumsum_. These two searches could be nested instead,
  // but that would be O((log n) * (log k)), while this is O((log n) + (log k)).
  // Invariant: prefixSum(add[le].idx) <= s, prefixSum(add[gt].idx) > s
  // (where add[-1].idx = -1, add[add_size].idx = base_->size()).
  int le = -1, gt = (int)add_size;
  while (gt - le > 1) {
    size_t k = (size_t)((le + gt) / 2);
    size_t next_idx = add[k].first;
    double delta = k ? add[k - 1].second : 0.;
    double sum = base_->prefixSum(next_idx) + delta;
    if (sum > s) {
      gt = k;
    } else {
      le = k;
    }
  }
  if (le == -1 && base_->prefixSum(0) > s) {
    return 0;
  }
  size_t first_idx = le >= 0 ? add[le].first : 0ul;
  size_t next_idx = gt < (int)add_size ? add[gt].first : base_->size();
  double delta = le >= 0 ? add[le].second : 0.;

  return std::upper_bound(base_->weight_cumsum_.begin() + first_idx,
                          base_->weight_cumsum_.begin() + next_idx,
                          s - delta) -
      base_->weight_cumsum_.begin();
}

double AdjustedProbabilityDistribution::totalWeight() const {
  return base_->totalWeight() + (diff_ ? diff_->totalAddedWeight() : 0.);
}

std::string AdjustedProbabilityDistribution::toString() const {
  std::stringstream ss;
  ss << "[";
  ss.precision(3);
  ss.setf(std::ios::fixed, std::ios::floatfield);
  for (size_t i = 0; i < size(); ++i) {
    if (i) {
      ss << ", ";
    }
    double w = base_->weight(i);
    double d = diff_ ? diff_->weight(i) : 0.;
    ss << w + d;
    if (d != 0.) {
      ss << "(" << w;
      ss.setf(std::ios::showpos); // print sign even for positive numbers
      ss << d << ")";
      ss.unsetf(std::ios::showpos);
    }
  }
  ss << "]";
  return ss.str();
}

}} // namespace facebook::logdevice
