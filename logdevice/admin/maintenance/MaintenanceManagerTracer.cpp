/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/admin/maintenance/MaintenanceManagerTracer.h"

#include "logdevice/common/MetaDataTracer.h"

using namespace facebook::logdevice::configuration::nodes;

namespace facebook { namespace logdevice { namespace maintenance {

namespace {

constexpr auto kMaintenanceManagerSampleSource = "maintenance_manager";
constexpr auto kMaintenanceAPISampleSource = "maintenance_api";
constexpr auto kRebuildingCoordinatorSampleSource = "rebuilding_coordinator";

std::string getNodeNameByIdx(const ServiceDiscoveryConfig& svd,
                             node_index_t idx) {
  auto node_svd = svd.getNodeAttributesPtr(idx);
  return node_svd != nullptr ? node_svd->name
                             : folly::sformat("UNKNOWN({})", idx);
}

// Helper function to determine the log string for a maintenance operation
std::string getPriorityStr(const MaintenanceDefinition& maintenance) {
  if (maintenance.priority_ref().has_value()) {
    auto priority = maintenance.priority_ref().value();
    switch (priority) {
      case MaintenancePriority::IMMINENT:
        return "imminent";
      case MaintenancePriority::HIGH:
        return "high";
      case MaintenancePriority::MEDIUM:
        return "medium";
      case MaintenancePriority::LOW:
        return "low";
    }
  } else {
    return {};
  }
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
  std::set<std::string> maintenance_ids;
  std::set<std::string> users;
  std::set<std::string> maintenances_skipping_safety_check;
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

void MaintenanceManagerTracer::trace(MetadataNodesetUpdateSample sample) {
  auto sample_builder =
      [sample = std::move(sample)]() mutable -> std::unique_ptr<TraceSample> {
    auto trace_sample = std::make_unique<TraceSample>();

    // Metadata
    trace_sample->addNormalValue("event", "METADATA_NODESET_UPDATE");
    trace_sample->addIntValue("verbosity", static_cast<int>(Verbosity::EVENTS));
    trace_sample->addNormalValue(
        "sample_source", kMaintenanceManagerSampleSource);
    trace_sample->addIntValue(
        "maintenance_state_version", sample.maintenance_state_version);
    trace_sample->addIntValue("ncm_nc_version", sample.ncm_version.val());
    trace_sample->addIntValue(
        "published_nc_ctime_ms",
        sample.nc_published_time.toMilliseconds().count());

    // Metadata nodeset
    folly::F14FastSet<node_index_t> added_nodes;
    std::set_difference(sample.new_metadata_node_ids.begin(),
                        sample.new_metadata_node_ids.end(),
                        sample.old_metadata_node_ids.begin(),
                        sample.old_metadata_node_ids.end(),
                        std::inserter(added_nodes, added_nodes.begin()));

    folly::F14FastSet<node_index_t> removed_nodes;
    std::set_difference(sample.old_metadata_node_ids.begin(),
                        sample.old_metadata_node_ids.end(),
                        sample.new_metadata_node_ids.begin(),
                        sample.new_metadata_node_ids.end(),
                        std::inserter(removed_nodes, removed_nodes.begin()));

    trace_sample->addSetValue(
        "old_metadata_node_idx", toStringSet(sample.old_metadata_node_ids));
    trace_sample->addSetValue(
        "new_metadata_node_idx", toStringSet(sample.new_metadata_node_ids));
    trace_sample->addSetValue("metadata_nodes_added", toStringSet(added_nodes));
    trace_sample->addSetValue(
        "metadata_nodes_removed", toStringSet(removed_nodes));

    // The NC update
    if (sample.ncm_update_status != Status::OK) {
      trace_sample->addIntValue("ncm_update_error", 1);
      trace_sample->addIntValue("error", 1);
      trace_sample->addNormalValue(
          "ncm_update_error_reason", error_name(sample.ncm_update_status));
    }
    trace_sample->addIntValue(
        "ncm_update_latency_ms", sample.ncm_update_watch.duration.count());
    return trace_sample;
  };

  publish(kMaintenanceManagerTracer, std::move(sample_builder));
}

void MaintenanceManagerTracer::trace(PurgedMaintenanceSample sample) {
  auto sample_builder =
      [sample = std::move(sample)]() mutable -> std::unique_ptr<TraceSample> {
    auto trace_sample = std::make_unique<TraceSample>();

    // Metadata
    trace_sample->addNormalValue("event", "MAINTENANCES_PURGED");
    trace_sample->addIntValue("verbosity", static_cast<int>(Verbosity::EVENTS));
    trace_sample->addNormalValue(
        "sample_source", kMaintenanceManagerSampleSource);
    trace_sample->addIntValue(
        "maintenance_state_version", sample.maintenance_state_version);
    trace_sample->addIntValue("ncm_nc_version", sample.ncm_version.val());
    trace_sample->addIntValue(
        "published_nc_ctime_ms",
        sample.nc_published_time.toMilliseconds().count());

    // Removed maintenances
    trace_sample->addSetValue(
        "maintenance_ids", toStringSet(sample.removed_maintenances));
    trace_sample->addNormalValue("reason", sample.reason);
    trace_sample->addIntValue("error", sample.error ? 1 : 0);
    if (sample.error) {
      trace_sample->addNormalValue("error_reason", sample.error_reason);
    }
    return trace_sample;
  };

  publish(kMaintenanceManagerTracer, std::move(sample_builder));
}

void MaintenanceManagerTracer::trace(ApplyMaintenanceAPISample sample) {
  auto sample_builder =
      [sample = std::move(sample)]() mutable -> std::unique_ptr<TraceSample> {
    auto trace_sample = std::make_unique<TraceSample>();

    // Metadata
    trace_sample->addNormalValue("event", "APPLY_MAINTENANCE_REQUEST");
    trace_sample->addIntValue("verbosity", static_cast<int>(Verbosity::EVENTS));
    trace_sample->addNormalValue("sample_source", kMaintenanceAPISampleSource);
    trace_sample->addIntValue(
        "maintenance_state_version", sample.maintenance_state_version);
    trace_sample->addIntValue("ncm_nc_version", sample.ncm_version.val());
    trace_sample->addIntValue(
        "published_nc_ctime_ms",
        sample.nc_published_time.toMilliseconds().count());

    // Added maintenances
    populateSampleFromMaintenances(
        *trace_sample, sample.added_maintenances, *sample.service_discovery);

    // Extra fields for this request
    if (!sample.added_maintenances.empty()) {
      // Pick the TTL/priority of any of them, they all originated from the same
      // maintenance.
      trace_sample->addNormalValue(
          "priority", getPriorityStr(sample.added_maintenances.at(0)));
      trace_sample->addIntValue(
          "ttl_seconds", sample.added_maintenances.at(0).ttl_seconds);
    }

    trace_sample->addIntValue("error", sample.error ? 1 : 0);
    if (sample.error) {
      trace_sample->addNormalValue("error_reason", sample.error_reason);
    }
    return trace_sample;
  };

  publish(kMaintenanceManagerTracer, std::move(sample_builder));
}

void MaintenanceManagerTracer::trace(RemoveMaintenanceAPISample sample) {
  auto sample_builder =
      [sample = std::move(sample)]() mutable -> std::unique_ptr<TraceSample> {
    auto trace_sample = std::make_unique<TraceSample>();

    // Metadata
    trace_sample->addNormalValue("event", "REMOVE_MAINTENANCE_REQUEST");
    trace_sample->addIntValue("verbosity", static_cast<int>(Verbosity::EVENTS));
    trace_sample->addNormalValue("sample_source", kMaintenanceAPISampleSource);
    trace_sample->addIntValue(
        "maintenance_state_version", sample.maintenance_state_version);
    trace_sample->addIntValue("ncm_nc_version", sample.ncm_version.val());
    trace_sample->addIntValue(
        "published_nc_ctime_ms",
        sample.nc_published_time.toMilliseconds().count());

    // Removed maintenances
    populateSampleFromMaintenances(
        *trace_sample, sample.removed_maintenances, *sample.service_discovery);

    // Remove reason
    // TODO: Add a separate field in the tracer for the actor of the remove.
    trace_sample->addNormalValue(
        "reason", folly::sformat("{}: {}", sample.user, sample.reason));

    trace_sample->addIntValue("error", sample.error ? 1 : 0);
    if (sample.error) {
      trace_sample->addNormalValue("error_reason", sample.error_reason);
    }
    return trace_sample;
  };

  publish(kMaintenanceManagerTracer, std::move(sample_builder));
}

void MaintenanceManagerTracer::trace(ApplyMaintenanceInternalSample sample) {
  auto sample_builder =
      [sample = std::move(sample)]() mutable -> std::unique_ptr<TraceSample> {
    auto trace_sample = std::make_unique<TraceSample>();

    // Metadata
    trace_sample->addNormalValue("event", "APPLY_MAINTENANCE_INTERNAL");
    trace_sample->addIntValue("verbosity", static_cast<int>(Verbosity::EVENTS));
    trace_sample->addNormalValue(
        "sample_source", kRebuildingCoordinatorSampleSource);
    trace_sample->addIntValue(
        "maintenance_state_version", sample.maintenance_state_version);
    trace_sample->addIntValue("ncm_nc_version", sample.ncm_version.val());
    trace_sample->addIntValue(
        "published_nc_ctime_ms",
        sample.nc_published_time.toMilliseconds().count());

    // Added maintenances
    populateSampleFromMaintenances(
        *trace_sample, {sample.added_maintenance}, *sample.service_discovery);

    // Add priority
    trace_sample->addNormalValue(
        "priority", getPriorityStr(sample.added_maintenance));

    // Add reason
    trace_sample->addNormalValue(
        "reason", folly::sformat("internal: {}", sample.reason));

    trace_sample->addIntValue(
        "ttl_seconds", *sample.added_maintenance.ttl_seconds_ref());

    trace_sample->addIntValue("error", sample.error ? 1 : 0);
    if (sample.error) {
      trace_sample->addNormalValue("error_reason", sample.error_reason);
    }
    return trace_sample;
  };

  publish(kMaintenanceManagerTracer, std::move(sample_builder));
}

void MaintenanceManagerTracer::trace(RemoveMaintenanceInternalSample sample) {
  auto sample_builder =
      [sample = std::move(sample)]() mutable -> std::unique_ptr<TraceSample> {
    auto trace_sample = std::make_unique<TraceSample>();

    // Metadata
    trace_sample->addNormalValue("event", "REMOVE_MAINTENANCE_INTERNAL");
    trace_sample->addIntValue("verbosity", static_cast<int>(Verbosity::EVENTS));
    trace_sample->addNormalValue(
        "sample_source", kRebuildingCoordinatorSampleSource);
    trace_sample->addIntValue(
        "maintenance_state_version", sample.maintenance_state_version);
    trace_sample->addIntValue("ncm_nc_version", sample.ncm_version.val());
    trace_sample->addIntValue(
        "published_nc_ctime_ms",
        sample.nc_published_time.toMilliseconds().count());

    // Removed maintenance
    trace_sample->addSetValue("maintenance_ids", {sample.removed_maintenance});
    trace_sample->addSetValue("shards_affected", {toString(sample.shard)});
    const node_index_t& node = sample.shard.node();
    trace_sample->addSetValue("node_ids_affected", {toString(node)});
    trace_sample->addSetValue(
        "node_names_affected",
        {getNodeNameByIdx(*sample.service_discovery, node)});

    // Remove reason
    trace_sample->addNormalValue(
        "reason", folly::sformat("internal: {}", sample.reason));

    trace_sample->addIntValue("error", sample.error ? 1 : 0);
    if (sample.error) {
      trace_sample->addNormalValue("error_reason", sample.error_reason);
    }
    return trace_sample;
  };

  publish(kMaintenanceManagerTracer, std::move(sample_builder));
}

}}} // namespace facebook::logdevice::maintenance
