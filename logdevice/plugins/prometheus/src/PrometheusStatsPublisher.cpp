/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

/**
 * @author Mohamed Bassem 
 */

#include "PrometheusStatsPublisher.h"

#include <folly/Format.h>
#include <folly/Random.h>
#include <logdevice/common/PriorityMap.h>
#include <logdevice/common/configuration/NodeLocation.h>
#include <logdevice/common/configuration/TrafficClass.h>
#include <logdevice/common/debug.h>
#include <logdevice/common/protocol/MessageTypeNames.h>
#include <logdevice/common/stats/Histogram.h>
#include <logdevice/common/stats/Stats.h>
#include <prometheus/check_names.h>

using prometheus::Family;
using prometheus::Gauge;

namespace facebook { namespace logdevice {
namespace {

using namespace facebook::logdevice;

class PrometheusEnumerationCallback : public Stats::EnumerationCallbacks {
 public:
  PrometheusEnumerationCallback(PrometheusStatsPublisher* publisher,
                                const std::string& stats_name)
      : publisher_(publisher), stats_name_(stats_name) {}

  virtual ~PrometheusEnumerationCallback() {}

  void updateCounter(const std::string& name,
                     std::map<std::string, std::string> labels,
                     double val) {
    if (!prometheus::CheckMetricName(name)) {
      // TODO replace the dots with other character.
      // TODO figure out what's wrong with the failing stats.
      return;
    }
    auto& family = publisher_->getFamily(name, stats_name_);
    auto& counter = family.Add(std::move(labels));
    counter.Set(val);
  }

  // Simple stats.
  void stat(const std::string& name, int64_t val) override {
    updateCounter(name, {}, val);
  }
  // Per-message-type stats.
  void stat(const std::string& name,
            MessageType message_type,
            int64_t val) override {
    updateCounter(
        name, {{"message_type", messageTypeNames()[message_type]}}, val);
  }
  // Per-shard stats.
  void stat(const std::string& name,
            shard_index_t shard,
            int64_t val) override {
    updateCounter(name, {{"shard_index", std::to_string(shard)}}, val);
  }
  // Per-traffic-class stats.
  void stat(const std::string& name, TrafficClass tc, int64_t val) override {
    updateCounter(name, {{"traffic_class", trafficClasses()[tc]}}, val);
  }
  // Per-flow-group stats.
  void stat(const std::string& name,
            NodeLocationScope flow_group,
            int64_t val) override {
    // Not needed as it's included in the per-flow-group per message one.
  }
  // Per-flow-group-and-msg-priority stats.
  void stat(const std::string& name,
            NodeLocationScope flow_group,
            Priority priority,
            int64_t val) override {
    updateCounter(name,
                  {{"flow_group", NodeLocation::scopeNames()[flow_group]},
                   {"priority", PriorityMap::toName()[priority]}},
                  val);
  }
  // Per-msg-priority stats (totals of the previous one).
  void stat(const std::string& name, Priority, int64_t val) override {
    // Not needed as it's included in the per-flow-group per message one.
  }
  // Per-request-type stats.
  void stat(const std::string& name, RequestType type, int64_t val) override {
    updateCounter(name, {{"request_type", requestTypeNames[type]}}, val);
  }
  // Per-storage-task-type stats.
  void stat(const std::string& name,
            StorageTaskType type,
            int64_t val) override {
    updateCounter(name, {{"storage_task_type", toString(type)}}, val);
  }
  // Per-worker stats (only for workers of type GENERAL).
  void stat(const std::string& name,
            worker_id_t worker_id,
            uint64_t val) override {
    updateCounter(name, {{"worker_id", std::to_string(worker_id.val())}}, val);
  }
  // Per-log stats.
  void stat(const char* name,
            const std::string& log_group,
            int64_t val) override {
    updateCounter(name, {{"log_group", log_group}}, val);
  }
  // Simple histograms.
  void histogram(const std::string& name,
                 const HistogramInterface& hist) override {
    updateCounter(name, {{"percentile", "50"}}, hist.estimatePercentile(0.5));
    updateCounter(name, {{"percentile", "90"}}, hist.estimatePercentile(0.9));
    updateCounter(name, {{"percentile", "99"}}, hist.estimatePercentile(0.99));
    updateCounter(name, {{"percentile", "max"}}, hist.estimatePercentile(1));
  }
  // Per-shard histograms.
  void histogram(const std::string& name,
                 shard_index_t shard,
                 const HistogramInterface& hist) override {
    updateCounter(
        name,
        {{"shard_index", std::to_string(shard)}, {"percentile", "50"}},
        hist.estimatePercentile(0.5));
    updateCounter(
        name,
        {{"shard_index", std::to_string(shard)}, {"percentile", "90"}},
        hist.estimatePercentile(0.9));
    updateCounter(
        name,
        {{"shard_index", std::to_string(shard)}, {"percentile", "99"}},
        hist.estimatePercentile(0.99));
    updateCounter(
        name,
        {{"shard_index", std::to_string(shard)}, {"percentile", "max"}},
        hist.estimatePercentile(1));
  }

 private:
  PrometheusStatsPublisher* publisher_;
  std::string stats_name_;
};
} // namespace

PrometheusStatsPublisher::PrometheusStatsPublisher(const std::string& listen_addr)
    : registry_(std::make_shared<prometheus::Registry>()) {
  // TODO figure out a way to make the port work in dev clusters
  exposer_ = std::make_unique<prometheus::Exposer>(listen_addr);
  exposer_->RegisterCollectable(registry_);
  ld_info("Listening on addr %s", listen_addr.c_str());
}


PrometheusStatsPublisher::PrometheusStatsPublisher(std::shared_ptr<prometheus::Registry> registry)
    : registry_(std::move(registry)) {}

void PrometheusStatsPublisher::publish(
    const std::vector<const Stats*>& current,
    const std::vector<const Stats*>& previous,
    std::chrono::milliseconds elapsed) {
  for (const auto& c : current) {
    auto cb = PrometheusEnumerationCallback(this, c->params->get()->getStatsSetName());
    c->enumerate(&cb);
  }
}

void PrometheusStatsPublisher::addRollupEntity(std::string entity) {}

Family<Gauge>& PrometheusStatsPublisher::getFamily(const std::string& name,
                                                   const std::string& stats_name) {
  auto it = famililes_.find(name);
  if (it == famililes_.end()) {
    auto& fam = prometheus::BuildGauge()
                    .Name(name)
                    .Help("")
                    .Labels({{"source", stats_name}})
                    .Register(*registry_);
    auto new_f = famililes_.emplace(name, fam);
    it = new_f.first;
  }
  return it->second;
}

}} // namespace facebook::logdevice
