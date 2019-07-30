/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/settings/SequencerBoycottingSettings.h"

#include "logdevice/common/commandline_util_chrono.h"

using namespace facebook::logdevice::setting_validators;

namespace facebook { namespace logdevice {

void SequencerBoycottingSettings::defineSettings(SettingEasyInit& init) {
  using namespace SettingFlag;
  init("node-stats-send-period",
       &node_stats_send_period,
       "15s",
       validate_positive<ssize_t>(),
       "Send per-node stats into the cluster with this period. Currently only "
       "30s of stats is tracked on the clients, so a value above 30s will "
       "not have any effect.",
       CLIENT,
       SettingsCategory::SequencerBoycotting);
  init("node-stats-send-retry-delay",
       &node_stats_send_retry_delay,
       "5ms..1s",
       validate_nonnegative<ssize_t>(),
       "When sending per-node stats into the cluster, and the message failed, "
       "wait this much before retrying.",
       CLIENT | REQUIRES_RESTART, /* Used when initializing NodeStatsHandler */
                                  /* (during worker thread start) */
       SettingsCategory::SequencerBoycotting);
  init("node-stats-timeout-delay",
       &node_stats_timeout_delay,
       "2s",
       validate_positive<ssize_t>(),
       "Wait this long for an acknowledgement that the sent node stats "
       "message was received before sending the stats to another node",
       CLIENT,
       SettingsCategory::SequencerBoycotting);
  init("node-stats-retention-on-nodes",
       &node_stats_retention_on_nodes,
       "300s", // 5m
       validate_positive<ssize_t>(),
       "Save node stats sent from the clients on the nodes for this duration",
       SERVER,
       SettingsCategory::SequencerBoycotting);
  init("node-stats-controller-aggregation-period",
       &node_stats_controller_aggregation_period,
       "30s",
       validate_positive<ssize_t>(),
       "The period at which the controller nodes requests stats from all nodes "
       "in the cluster. Should be smaller than node-stats-retention-on-nodes",
       SERVER,
       SettingsCategory::SequencerBoycotting);
  init("node-stats-controller-response-timeout",
       &node_stats_controller_response_timeout,
       "2s",
       validate_positive<ssize_t>(),
       "A controller node waits this long between requesting stats from the "
       "other nodes, and aggregating the received stats",
       SERVER,
       SettingsCategory::SequencerBoycotting);
  init("node-stats-max-boycott-count",
       &node_stats_max_boycott_count,
       "0", // use 0 as default until more tests have been performed
       validate_nonnegative<ssize_t>(),
       "How many nodes may be boycotted. 0 will in addition to not allowing "
       "any nodes to be boycotted, it also ensures no nodes become controller "
       "nodes",
       SERVER,
       SettingsCategory::SequencerBoycotting);
  init("node-stats-boycott-duration",
       &node_stats_boycott_duration,
       "30min",
       validate_nonnegative<ssize_t>(),
       "How long a boycott should be active for. 0 will ensure that boycotts "
       "has no effect, but controller nodes will still report outliers",
       SERVER,
       SettingsCategory::SequencerBoycotting);
  init("node-stats-boycott-grace-period",
       &node_stats_boycott_grace_period,
       "300s",
       validate_nonnegative<ssize_t>(),
       "If a node is an consecutively deemed an outlier for this amount of "
       "time, allow it to be boycotted",
       SERVER,
       SettingsCategory::SequencerBoycotting);
  init("node-stats-boycott-sensitivity",
       &node_stats_boycott_sensitivity,
       "0",
       validate_nonnegative<double>(),
       "If node-stats-boycott-sensitivity is set to e.g. 0.05, then nodes with "
       "a success ratio at or above 95% will not be boycotted",
       SERVER,
       SettingsCategory::SequencerBoycotting);
  init("node-stats-boycott-required-std-from-mean",
       &node_stats_boycott_required_std_from_mean,
       "3",
       validate_nonnegative<double>(),
       "A node has to have a success ratio lower than (mean - X * STD) to be "
       "considered an outlier. X being the value of "
       "node-stats-boycott-required-std-from-mean",
       SERVER,
       SettingsCategory::SequencerBoycotting);
  init("node-stats-boycott-relative-margin",
       &node_stats_boycott_relative_margin,
       "0.15",
       validate_nonnegative<double>(),
       "If this is set to 0.05, a node's append success ratio has to be 5% "
       "smaller than the average success ratio of all nodes in the cluster. "
       "While node-stats-boycott-sensitivity is an absolute threshold, this "
       "setting defines a sensitivity threshold relative to the average of all "
       "success ratios. Only used if node-stats-boycott-use-rmsd is true",
       SERVER,
       SettingsCategory::SequencerBoycotting);
  init("node-stats-controller-check-period",
       &node_stats_controller_check_period,
       "60s",
       validate_positive<ssize_t>(),
       "A node will check if it's a controller or not with the given period",
       SERVER,
       SettingsCategory::SequencerBoycotting);
  init("node-stats-send-worst-client-count",
       &node_stats_send_worst_client_count,
       "20",
       validate_nonnegative<ssize_t>(),
       "Once a node has aggregated the values sent from writers, there may be "
       "some amount of writers that are in a bad state and report 'false' "
       "values. By setting this value, the "
       "`node-stats-send-worst-client-count` worst values reported by clients "
       "per node will be sent separately to the controller, which can then "
       "take a decision if the writer is functioning correctly or not.",
       SERVER,
       SettingsCategory::SequencerBoycotting);
  init("node-stats-boycott-required-client-count",
       &node_stats_boycott_required_client_count,
       "1",
       validate_positive<ssize_t>(),
       "Require at least values from this many clients before a boycott may "
       "occur",
       SERVER,
       SettingsCategory::SequencerBoycotting);
  init("node-stats-remove-worst-percentage",
       &node_stats_remove_worst_percentage,
       "0.2",
       validate_range<double>(0.0, 1.0),
       "Will throw away the worst X\% of values reported by clients, to a "
       "maximum of node-count * node-stats-send-worst-client-count",
       SERVER,
       SettingsCategory::SequencerBoycotting);
  init("node-stats-boycott-use-rmsd",
       &node_stats_boycott_use_rmsd,
       "true",
       nullptr, // no validation
       "Use a new outlier detection algorithm",
       SERVER,
       SettingsCategory::SequencerBoycotting);

  init(
      "node-stats-boycott-use-adaptive-duration",
      &node_stats_boycott_use_adaptive_duration,
      "false",
      nullptr,
      "(experimental) Use the new adaptive boycotting durations instead of the "
      "fixed one",
      SERVER | EXPERIMENTAL,
      SettingsCategory::SequencerBoycotting);

  init("node-stats-boycott-min-adaptive-duration",
       &node_stats_boycott_min_adaptive_duration,
       "30min",
       validate_positive<ssize_t>(),
       "(experimental) The minmum (and default) adaptive boycotting duration",
       SERVER | EXPERIMENTAL,
       SettingsCategory::SequencerBoycotting);

  init("node-stats-boycott-max-adaptive-duration",
       &node_stats_boycott_max_adaptive_duration,
       "24h",
       validate_positive<ssize_t>(),
       "(experimental) The maximum adaptive boycotting duration",
       SERVER | EXPERIMENTAL,
       SettingsCategory::SequencerBoycotting);

  init("node-stats-boycott-adaptive-duration-increase-factor",
       &node_stats_boycott_adaptive_duration_increase_factor,
       "2",
       validate_positive<ssize_t>(),
       "(experimental) the multiplicative increase factor of the adaptive"
       "boycotting duration",
       SERVER | EXPERIMENTAL,
       SettingsCategory::SequencerBoycotting);

  init("node-stats-boycott-adaptive-duration-decrease-rate",
       &node_stats_boycott_adaptive_duration_decrease_rate,
       "1min",
       validate_positive<ssize_t>(),
       "(experimental) the additive decrease rate of the adaptive boycotting"
       "duration",
       SERVER | EXPERIMENTAL,
       SettingsCategory::SequencerBoycotting);

  init("node-stats-boycott-adaptive-duration-decrease-time-step",
       &node_stats_boycott_adaptive_duration_decrease_time_step,
       "30s",
       validate_positive<ssize_t>(),
       "(experimental) the time step of the decrease of the adaptive boycotting"
       "duration",
       SERVER | EXPERIMENTAL,
       SettingsCategory::SequencerBoycotting);
}
}} // namespace facebook::logdevice
