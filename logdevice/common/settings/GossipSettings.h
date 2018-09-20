/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>

#include "logdevice/common/settings/UpdateableSettings.h"

/**
 * @file  Settings specific to the gossip-based failure detector.
 */

namespace facebook { namespace logdevice {

struct GossipSettings : public SettingsBundle {
  enum class SelectionMode {
    RANDOM,
    ROUND_ROBIN,
  };

  // See .cpp for docs and defaults
  bool enabled;
  std::chrono::milliseconds gossip_interval;
  int gossip_failure_threshold;
  int failover_blacklist_threshold;
  int min_gossips_for_stable_state;
  std::chrono::milliseconds gossip_time_skew_threshold;
  std::chrono::milliseconds failover_wait_time;
  std::chrono::milliseconds suspect_duration;
  std::chrono::seconds gossip_msg_dump_duration;
  SelectionMode mode;
  // Determines whether to ignore isolation detection
  // This settings is used to bypass the minority partition check in the
  // failure detector, which is by default enabled. The failure detector
  // normally reports that fact and appends fail with E::ISOLATED. This is
  // meant to prevent two sequencers to append records to the same log if they
  // are isolated from each other. Although they would not run in the same
  // epoch, the records' timestamp would interleave and that could introduce
  // inconsistencies. If the application does not care about interleaved
  // timestamps, it can disable this check with the below settings.
  bool ignore_isolation;
  std::chrono::milliseconds gcs_wait_duration;

  const char* getName() const override {
    return "GossipSettings";
  }
  void defineSettings(SettingEasyInit& init) override;

 private:
  // Only UpdateableSettings can create this bundle to ensure defaults are
  // populated.
  GossipSettings() {}
  friend class UpdateableSettingsRaw<GossipSettings>;
};

}} // namespace facebook::logdevice
