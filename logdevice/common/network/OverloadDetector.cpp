/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/network/OverloadDetector.h"

#include <algorithm>

#include "folly/Random.h"
#include "logdevice/common/Connection.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/stats/Stats.h"

namespace facebook::logdevice {

std::chrono::milliseconds OverloadDetectorDependencies::getLoopPeriod() const {
  return settings().overload_detector_period;
}

uint32_t OverloadDetectorDependencies::getOverloadThreshold() const {
  return settings().overload_detector_threshold;
}

uint32_t OverloadDetectorDependencies::getPercentile() const {
  return settings().overload_detector_percentile;
}

double OverloadDetectorDependencies::getMinBufLengthsRead() const {
  return settings().overload_detector_freshness_factor;
}

const Settings& OverloadDetectorDependencies::settings() const {
  return Worker::settings();
}

std::shared_ptr<const configuration::nodes::NodesConfiguration>
OverloadDetectorDependencies::getNodesConfiguration() {
  return Worker::onThisThread()->getNodesConfiguration();
}

Socket* OverloadDetectorDependencies::getSocketFor(node_index_t nid) {
  return Worker::onThisThread()->sender().findServerSocket(nid);
}

ssize_t OverloadDetectorDependencies::getTcpRecvBufOccupancy(node_index_t nid) {
  auto s = getSocketFor(nid);
  return s ? s->getTcpRecvBufOccupancy() : -1;
}

size_t OverloadDetectorDependencies::getTcpRecvBufSize(node_index_t nid) {
  auto s = getSocketFor(nid);
  return s ? s->getTcpRecvBufSize() : 0;
}

uint64_t OverloadDetectorDependencies::getNumBytesReceived(node_index_t nid) {
  auto s = getSocketFor(nid);
  return s ? s->getNumBytesReceived() : 0;
}

OverloadDetector::OverloadDetector(
    std::unique_ptr<OverloadDetectorDependencies> deps)
    : deps_(std::move(deps)) {
  WORKER_STAT_INCR(num_workers_tracked_by_overload_detector);
}

OverloadDetector::~OverloadDetector() {
  overloaded_ = false;
  maybeBumpStats();
  WORKER_STAT_DECR(num_workers_tracked_by_overload_detector);
}

void OverloadDetector::start() {
  if (!deps_->settings().server) {
    // this is only used by clients
    loop_timer_ = std::make_unique<Timer>([this]() { runOnce(); });
    ld_debug("Starting overload detector");
    loop_timer_->activate(std::chrono::microseconds::zero());
  }
}

void OverloadDetector::runOnce() {
  updateOverloaded();
  maybeBumpStats();
  issueTimers();
  cleanupHashMaps();

  if (loop_timer_) {
    loop_timer_->activate(deps_->getLoopPeriod());
  }
}

void OverloadDetector::updateOverloaded() {
  auto threshold = deps_->getOverloadThreshold();
  std::vector<uint8_t> samples;
  samples.reserve(recv_q_data_.size());
  for (const auto& [_, data] : recv_q_data_) {
    uint64_t freshness_threshold =
        static_cast<uint64_t>(data.buf_size * deps_->getMinBufLengthsRead());
    if (data.bytes_rcvd_delta >= freshness_threshold ||
        data.util_pct >= threshold) {
      samples.push_back(data.util_pct);
    }
  }
  if (samples.size() == 0) {
    ld_debug("No samples found. We are not overloaded.");
    overloaded_ = false;
    return;
  }
  auto pct = deps_->getPercentile();

  size_t percentile_position =
      std::min(samples.size() * pct / 100, samples.size() - 1);
  std::nth_element(
      samples.begin(), samples.begin() + percentile_position, samples.end());

  overloaded_ = (samples[percentile_position] >= threshold);
  ld_debug("Got %zu samples. The %uth percentile has %hhu%% occupation of "
           "recv-q. We are %soverloaded.",
           samples.size(),
           pct,
           samples[percentile_position],
           overloaded_ ? "" : "not ");
}

void OverloadDetector::issueTimers() {
  auto period = deps_->getLoopPeriod();
  auto& service_discovery =
      deps_->getNodesConfiguration()->getServiceDiscovery();
  ld_check(service_discovery);
  for (const auto& [nid, _] : *service_discovery) {
    decltype(period) delay{folly::Random::rand64(period.count())};
    auto [it, __] = recv_q_timer_.try_emplace(
        nid, [this, nid = nid]() { updateSampleFor(nid); });
    it->second.activate(delay);
  }
}

void OverloadDetector::cleanupHashMaps() {
  auto& service_discovery =
      deps_->getNodesConfiguration()->getServiceDiscovery();
  std::vector<node_index_t> to_delete;
  for (const auto& [nid, _] : recv_q_timer_) {
    if (!service_discovery->hasNode(nid)) {
      to_delete.push_back(nid);
    }
  }
  for (auto nid : to_delete) {
    recv_q_timer_.erase(nid);
  }

  to_delete.clear();
  for (const auto& [nid, _] : recv_q_data_) {
    if (!service_discovery->hasNode(nid)) {
      to_delete.push_back(nid);
    }
  }
  for (auto nid : to_delete) {
    recv_q_data_.erase(nid);
  }
}

void OverloadDetector::updateSampleFor(node_index_t nid) {
  auto occupancy = deps_->getTcpRecvBufOccupancy(nid);
  auto capacity = deps_->getTcpRecvBufSize(nid);
  auto rcvd_bytes = deps_->getNumBytesReceived(nid);
  if (occupancy >= 0 && capacity > 0) {
    if (100 * occupancy >= capacity * deps_->getOverloadThreshold()) {
      ld_spew("Socket for nid %u has recv-q with %ld bytes unread and capacity "
              "of %lu bytes.",
              nid,
              occupancy,
              capacity);
    }
    if (capacity < occupancy) {
      occupancy = capacity;
    }

    auto rcvd_bytes_delta = rcvd_bytes;
    if (recv_q_data_.contains(nid)) {
      rcvd_bytes_delta -= recv_q_data_.at(nid).lst_bytes_rcvd;
    }

    recv_q_data_.insert_or_assign(
        nid,
        Data{.util_pct = static_cast<uint8_t>(100 * occupancy / capacity),
             .lst_bytes_rcvd = rcvd_bytes,
             .bytes_rcvd_delta = rcvd_bytes_delta,
             .buf_size = capacity});
  } else {
    // Failed to obtain data. Ignore.
    recv_q_data_.erase(nid);
  }
}

void OverloadDetector::maybeBumpStats() {
  if (overloaded_ != last_reported_state_) {
    WORKER_STAT_ADD(num_overloaded_workers, overloaded_ ? +1 : -1);
    last_reported_state_ = overloaded_;
  }
}

} // namespace facebook::logdevice
