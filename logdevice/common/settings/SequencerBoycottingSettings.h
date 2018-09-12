/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>

#include "logdevice/common/settings/UpdateableSettings.h"
#include "logdevice/common/settings/Validators.h"
#include "logdevice/common/types_internal.h"

/**
 * Settings for the sequencer boycotting feature.
 */

namespace facebook { namespace logdevice {

struct SequencerBoycottingSettings {
  void defineSettings(SettingEasyInit& init);

  // Client only setting:
  // The period at which node stats are sent into the cluster
  // Currently only 30s is tracked, if going above that has to updated. Can be
  // found in PerNodeTimeSeriesStats in Stats.h
  std::chrono::milliseconds node_stats_send_period;

  // Client only setting:
  // Defines the delay before trying to resend a NODE_STATS_Message
  chrono_expbackoff_t<std::chrono::milliseconds> node_stats_send_retry_delay;

  // Client only setting:
  // Defines the accepted delay between successfully sending a
  // NODE_STATS_Message and then receiving a NODE_STATS_REPLY_Message
  // If it times out, a new node will be chosen and the stats will be sent there
  std::chrono::milliseconds node_stats_timeout_delay;

  // How long the nodes save the stats sent from the clients
  std::chrono::milliseconds node_stats_retention_on_nodes;

  // The period at which NodeStatsController requests stats from all other nodes
  // Should be smaller than node_stats_retention_on_nodes
  std::chrono::milliseconds node_stats_controller_aggregation_period;

  // A NodeStatsController waits this long for the other nodes to respond to
  // before aggregating the responses
  std::chrono::milliseconds node_stats_controller_response_timeout;

  // the amount of nodes that may be boycotted
  // 0 will ensure that no nodes are boycotted
  unsigned int node_stats_max_boycott_count;
  std::chrono::milliseconds node_stats_boycott_duration;

  // For how long should a node be an outlier before it gets boycotted
  std::chrono::milliseconds node_stats_boycott_grace_period;

  // see its entry in Settings.cpp
  double node_stats_boycott_sensitivity;

  // how many STDs from mean are required for a node to be an outlier
  double node_stats_boycott_required_std_from_mean;

  // A node's success ratio has to be smaller than the average success ratio by
  // the amount defined by M * 100%. Only used if
  // node-stats-boycott-use-rmsd is true
  double node_stats_boycott_relative_margin;

  // how often should a node check if it's a controller or not
  std::chrono::milliseconds node_stats_controller_check_period;

  // see Settings.cpp
  unsigned int node_stats_send_worst_client_count;
  // require this many clients reporting stats for a boycott to be valid.
  unsigned int node_stats_boycott_required_client_count;
  // throw away this many of the worst values reported by clients Will throw
  // away at most node_count * node_stats_send_worst_client_count
  double node_stats_remove_worst_percentage;

  // (experimental) Use RMSD for outlier detection in sequencer boycotting.
  bool node_stats_boycott_use_rmsd;

  // if this value is true, the entire system for boycotting will continue to
  // work as expected, but it will no longer affect sequencer placement
  bool boycotts_observe_only;
};

}} // namespace facebook::logdevice
