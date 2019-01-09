/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/settings/GossipSettings.h"

#include <iostream>

#include "logdevice/common/commandline_util_chrono.h"

namespace facebook { namespace logdevice {

std::istream& operator>>(std::istream& in, GossipSettings::SelectionMode& val) {
  std::string token;
  in >> token;
  if (token == "random") {
    val = GossipSettings::SelectionMode::RANDOM;
  } else if (token == "round-robin") {
    val = GossipSettings::SelectionMode::ROUND_ROBIN;
  } else {
    in.setstate(std::ios::failbit);
  }
  return in;
}

void GossipSettings::defineSettings(SettingEasyInit& init) {
  using namespace SettingFlag;

  init("gossip-enabled",
       &enabled,
       "true",
       nullptr, // no validation
       "Use a gossip-based failure detector. Deprecated, must always be "
       "enabled.",
       SERVER | REQUIRES_RESTART | DEPRECATED,
       SettingsCategory::FailureDetector);
  init("gossip-interval",
       &gossip_interval,
       "100ms",
       nullptr, // no validation
       "How often to send a gossip message. Lower values improve detection "
       "time, but make nodes more chatty.",
       SERVER | REQUIRES_RESTART,
       SettingsCategory::FailureDetector);
  init("gossip-threshold",
       &gossip_failure_threshold,
       "30",
       nullptr, // no validation
       "Specifies after how many gossip intervals of inactivity a node "
       "is "
       "marked as dead. Lower values reduce detection time, but make "
       "false "
       "positives more likely.",
       SERVER,
       SettingsCategory::FailureDetector);
  init("failover-blacklist-threshold",
       &failover_blacklist_threshold,
       "100",
       nullptr, // no validation
       "How many gossip intervals to ignore a node for after it "
       "performed "
       "a graceful failover",
       SERVER,
       SettingsCategory::FailureDetector);
  init("min-gossips-for-stable-state",
       &min_gossips_for_stable_state,
       "3",
       nullptr, // no validation
       "After receiving how many gossips, should the "
       "FailureDetector consider "
       "itself stable and start doing state transitions of "
       "cluster nodes based "
       "on incoming gossips.",
       SERVER,
       SettingsCategory::FailureDetector);
  init("gossip-time-skew",
       &gossip_time_skew_threshold,
       "10s",
       nullptr, // no validation
       "How much delay is acceptable in receiving a gossip "
       "message.",
       SERVER,
       SettingsCategory::FailureDetector);
  init("failover-wait-time",
       &failover_wait_time,
       "3s",
       nullptr, // no validation
       "How long to wait for the failover request to "
       "be propagated to other "
       "nodes",
       SERVER,
       SettingsCategory::FailureDetector);
  init("suspect-duration",
       &suspect_duration,
       "10s",
       nullptr, // no validation
       "How long to keep a node in an intermediate "
       "state before marking it as "
       "available. Larger values make the cluster "
       "less prone to node flakiness, "
       "but extend the time needed for sequencer "
       "nodes to start participating.",
       SERVER,
       SettingsCategory::FailureDetector);
  init("gossip-logging-duration",
       &gossip_msg_dump_duration,
       "0s",
       nullptr, // no validation
       "How long to keep logging "
       "FailureDetector/Gossip related "
       "activity "
       "after server comes up.",
       SERVER,
       SettingsCategory::FailureDetector);
  init("gossip-mode",
       &mode,
       "random",
       nullptr, // no validation
       "How to select a node to send a "
       "gossip message to. One of: "
       "'round-robin', "
       "'random' (default)",
       SERVER | REQUIRES_RESTART,
       SettingsCategory::FailureDetector);
  init("ignore-isolation",
       &ignore_isolation,
       "false",
       nullptr, // no validation
       "Ignore isolation detection. If "
       "set, a sequencer will accept "
       "append "
       "operations even if a majority "
       "of nodes appear to be dead.",
       SERVER,
       SettingsCategory::FailureDetector);
  init("gcs-wait-duration",
       &gcs_wait_duration,
       "1s",
       nullptr, // no validation
       "How long to wait for "
       "get-cluster-state reply to "
       "come, to "
       "initialize state of "
       "cluster nodes. Bringup is "
       "sent after this"
       " reply comes or after "
       "timeout",
       SERVER,
       SettingsCategory::FailureDetector);
};

}} // namespace facebook::logdevice
