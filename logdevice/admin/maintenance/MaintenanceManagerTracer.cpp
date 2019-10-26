/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/admin/maintenance/MaintenanceManagerTracer.h"

namespace facebook { namespace logdevice { namespace maintenance {

namespace {
using namespace logdevice::configuration::nodes;

constexpr auto kMaintenanceManagerSampleSource = "maintenance_manager";

std::string getNodeNameByIdx(const ServiceDiscoveryConfig& svd,
                             node_index_t idx) {
  auto node_svd = svd.getNodeAttributesPtr(idx);
  return node_svd != nullptr ? node_svd->name
                             : folly::sformat("UNKNOWN({})", idx);
}

// A helper function to convert container of items that have a toString
// implementation to a set of strings.
template <typename T,
          typename std::enable_if<to_string_detail::is_container<T>::value &&
                                  !to_string_detail::has_to_string_method<
                                      T>::value>::type* = nullptr>
std::set<std::string> toStringSet(T container) {
  std::set<std::string> ret;
  for (const auto& itm : container) {
    ret.insert(toString(itm));
  }
  return ret;
}

void populateSampleFromMaintenances(
    TraceSample& sample,
    const std::vector<thrift::MaintenanceDefinition>& maintenances,
    const ServiceDiscoveryConfig& svd) {
  std::set<std::string> maintenance_ids, users,
      maintenances_skipping_safety_check;
  std::set<ShardID> shards_affected;
  std::set<node_index_t> node_ids_affected;
  std::set<std::string> node_name_affected;

  for (const auto& maintenance : maintenances) {
    maintenance_ids.insert(maintenance.group_id_ref().value());
    users.insert(maintenance.get_user());

    if (maintenance.get_skip_safety_checks()) {
      maintenances_skipping_safety_check.insert(
          maintenance.group_id_ref().value());
    }

    for (const auto& shard : maintenance.get_shards()) {
      auto nid = shard.get_node().node_index_ref().value();
      shards_affected.emplace(nid, shard.get_shard_index());
      node_ids_affected.emplace(nid);
      node_name_affected.emplace(getNodeNameByIdx(svd, nid));
    }

    for (const auto& node : maintenance.get_sequencer_nodes()) {
      node_ids_affected.emplace(node.node_index_ref().value());
      node_name_affected.emplace(
          getNodeNameByIdx(svd, node.node_index_ref().value()));
    }
  }

  // Maintenances info
  sample.addSetValue("maintenance_ids", std::move(maintenance_ids));
  sample.addSetValue("users", std::move(users));
  sample.addSetValue(
      "shards_affected", toStringSet(std::move(shards_affected)));
  sample.addSetValue(
      "node_ids_affected", toStringSet(std::move(node_ids_affected)));
  sample.addSetValue("node_names_affected", std::move(node_name_affected));
  sample.addSetValue("maintenances_skipping_safety_checker",
                     std::move(maintenances_skipping_safety_check));
}

} // namespace

MaintenanceManagerTracer::MaintenanceManagerTracer(
    std::shared_ptr<TraceLogger> logger)
    : SampledTracer(std::move(logger)) {}

void MaintenanceManagerTracer::trace(PeriodicStateSample sample) {
  auto sample_builder =
      [sample = std::move(sample)]() mutable -> std::unique_ptr<TraceSample> {
    auto trace_sample = std::make_unique<TraceSample>();

    // Metadata
    trace_sample->addNormalValue("event", "PERIODIC_EVALUATION_SAMPLE");
    trace_sample->addIntValue(
        "verbosity", static_cast<int>(Verbosity::VERBOSE));
    trace_sample->addNormalValue(
        "sample_source", kMaintenanceManagerSampleSource);
    trace_sample->addIntValue(
        "maintenance_state_version", sample.maintenance_state_version);
    trace_sample->addIntValue("ncm_nc_version", sample.ncm_version.val());
    trace_sample->addIntValue(
        "published_nc_ctime_ms",
        sample.nc_published_time.toMilliseconds().count());

    // Maintenances
    populateSampleFromMaintenances(
        *trace_sample, sample.current_maintenances, *sample.service_discovery);

    // Progress
    trace_sample->addSetValue(
        "maintenances_unknown",
        toStringSet(
            sample
                .maintenances_progress[thrift::MaintenanceProgress::UNKNOWN]));
    trace_sample->addSetValue(
        "maintenances_blocked_until_safe",
        toStringSet(sample.maintenances_progress
                        [thrift::MaintenanceProgress::BLOCKED_UNTIL_SAFE]));
    trace_sample->addSetValue(
        "maintenances_in_progress",
        toStringSet(sample.maintenances_progress
                        [thrift::MaintenanceProgress::IN_PROGRESS]));
    trace_sample->addSetValue(
        "maintenances_in_completed",
        toStringSet(sample.maintenances_progress
                        [thrift::MaintenanceProgress::COMPLETED]));

    // Latencies
    trace_sample->addIntValue(
        "evaluation_latency_ms", sample.evaluation_watch.duration.count());
    trace_sample->addIntValue("safety_checker_latency_ms",
                              sample.safety_check_watch.duration.count());

    auto avg_ncm_latency =
        (sample.pre_safety_checker_ncm_watch.duration.count() +
         sample.post_safety_checker_ncm_watch.duration.count()) /
        2;
    trace_sample->addIntValue("ncm_update_latency_ms", avg_ncm_latency);

    return trace_sample;
  };

  publish(kMaintenanceManagerTracer, std::move(sample_builder));
}

}}} // namespace facebook::logdevice::maintenance
