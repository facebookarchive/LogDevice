/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <array>

#include "logdevice/common/RequestType.h"
#include "logdevice/common/StorageTask-enums.h"
#include "logdevice/common/protocol/MessageType.h"
#include "logdevice/common/stats/Histogram.h"
#include "logdevice/common/stats/HistogramBundle.h"

namespace facebook { namespace logdevice {

/**
 * Non sharded server histograms
 */
struct ServerHistograms : public HistogramBundle {
  HistogramBundle::MapType getMap() override {
    return {
        {"append_latency", &append_latency},
        {"store_bw_wait_latency", &store_bw_wait_latency},
        {"write_to_read_latency", &write_to_read_latency},
        {"store_timeouts", &store_timeouts},
        {"requests_queue_latency", &requests_queue_latency},
        {"hi_pri_requests_latency", &hi_pri_requests_latency},
        {"lo_pri_requests_latency", &lo_pri_requests_latency},
        {"mid_pri_requests_latency", &mid_pri_requests_latency},
        {"gossip_queue_latency", &gossip_queue_latency},
        {"gossip_recv_latency", &gossip_recv_latency},
        {"traffic_shaper_bw_dispatch_latency", &traffic_shaper_bw_dispatch},
        {"log_recovery_seal_latency", &log_recovery_seal_node},
        {"log_recovery_digesting_latency", &log_recovery_digesting},
        {"log_recovery_mutation_latency", &log_recovery_mutation},
        {"log_recovery_cleaning_latency", &log_recovery_cleaning},
        {"log_recovery_epoch_recovery_latency", &log_recovery_epoch},
        {"log_recovery_epoch_recovery_restarts", &log_recovery_epoch_restarts},
        {"flow_groups_run_event_loop_delay", &flow_groups_run_event_loop_delay},
        {"flow_groups_run_event_loop_delay_rt",
         &flow_groups_run_event_loop_delay_rt},
        {"flow_groups_run_time", &flow_groups_run_time},
        {"flow_groups_run_time_rt", &flow_groups_run_time_rt},
        {"logsconfig_manager_tree_clone_latency",
         &logsconfig_manager_tree_clone_latency},
        {"logsconfig_manager_delta_apply_latency",
         &logsconfig_manager_delta_apply_latency},
        {"background_thread_duration", &background_thread_duration},
        {"nodes_configuration_manager_propagation_latency",
         &nodes_configuration_manager_propagation_latency},
#define REQUEST_TYPE(name)              \
  {"request_execution_duration." #name, \
   &request_execution_duration[int(RequestType::name)]},
#include "logdevice/common/request_types.inc" // nolint
        {"request_execution_duration.INVALID",
         &request_execution_duration[int(RequestType::INVALID)]},
#define MESSAGE_TYPE(name, _)          \
  {"message_callback_duration." #name, \
   &message_callback_duration[int(MessageType::name)]},
#include "logdevice/common/message_types.inc" // nolint
#define STORAGE_TASK_TYPE(type, name, unused2) \
  {"storage_task_response_duration." name,     \
   &storage_task_response_duration[int(StorageTaskType::type)]},
#include "logdevice/common/storage_task_types.inc" // nolint
    };
  }
  // Latency of appends as seen by the sequencer
  LatencyHistogram append_latency;

  LatencyHistogram write_to_read_latency;

  LatencyHistogram store_bw_wait_latency;

  LatencyHistogram store_timeouts;

  // Latency of posting a request
  LatencyHistogram requests_queue_latency;

  LatencyHistogram hi_pri_requests_latency;

  LatencyHistogram mid_pri_requests_latency;

  LatencyHistogram lo_pri_requests_latency;

  // How long the gossip requests stay in the pipe
  LatencyHistogram gossip_queue_latency;

  // Time elapsed since a gossip message was sent by a node
  // to the time it was actually taken out from recepient pipe
  LatencyHistogram gossip_recv_latency;

  // Time delay between the deadline for releasing the next
  // quantum of bandwidth and the TrafficShaper actually
  // releasing the quantum.
  LatencyHistogram traffic_shaper_bw_dispatch;

  // Time taken to seal a node participating in recovery.
  CompactLatencyHistogram log_recovery_seal_node;

  // Time taken to complete the digest phase in recovering an epoch.
  CompactLatencyHistogram log_recovery_digesting;

  // Time taken to complete the mutation phase in recovering an epoch.
  CompactLatencyHistogram log_recovery_mutation;

  // Time taken to complete the cleaning phase in recovering an epoch.
  CompactLatencyHistogram log_recovery_cleaning;

  // Time taken to complete the recovery for an epoch.
  CompactLatencyHistogram log_recovery_epoch;

  // number of restarts in epoch recovery
  NoUnitHistogram log_recovery_epoch_restarts;

  // Time between when we trigger the flow_groups_run_requested libevent event,
  // and when it actually runs.
  LatencyHistogram flow_groups_run_event_loop_delay;
  LatencyHistogram flow_groups_run_event_loop_delay_rt;

  // Duration of Sender::runFlowGroups.
  LatencyHistogram flow_groups_run_time;
  LatencyHistogram flow_groups_run_time_rt;

  // How long does it take the LogsConfigManager to clone a LogsConfigTree and
  // make it available to the rest of the system
  LatencyHistogram logsconfig_manager_tree_clone_latency;

  // How long does it take to apply deltas to LogsConfig Tree
  LatencyHistogram logsconfig_manager_delta_apply_latency;

  CompactLatencyHistogram background_thread_duration;

  // How long did it take between when the config is published and when it
  // was received on the server in msec.
  CompactLatencyHistogram nodes_configuration_manager_propagation_latency;

  std::array<CompactLatencyHistogram, static_cast<int>(RequestType::MAX)>
      request_execution_duration;
  std::array<CompactLatencyHistogram, static_cast<int>(MessageType::MAX)>
      message_callback_duration;
  std::array<CompactLatencyHistogram, static_cast<int>(StorageTaskType::MAX)>
      storage_task_response_duration;
};

}} // namespace facebook::logdevice
