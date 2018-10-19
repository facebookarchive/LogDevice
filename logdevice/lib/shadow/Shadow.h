/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include <folly/Optional.h>

#include "logdevice/common/configuration/LogsConfig.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/settings/UpdateableSettings.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/include/ConfigSubscriptionHandle.h"
#include "logdevice/include/LogAttributes.h"
#include "logdevice/include/LogsConfigTypes.h"
#include "logdevice/lib/ClientProcessor.h"

/**
 * @file Implementation of transparent traffic shadowing
 *
 *       Traffic shadowing is enabled for a log range by using the "shadow"
 *       LogsConfig parameter, which defines a destination cluster, shadow
 *       ratio, and other parameters. Shadowing is an opt-in feature, so must
 *       be explicitly enabled for a client in client settings. Shadowing will
 *       be transparently enabled or disabled based on the shadow LogsConfig
 *       parameter in combination with the client setting. Additionally, each
 *       client will only maintain shadowing info about ranges that they are
 *       interested in, i.e. ranges that they have recently appended to.
 *
 *       TODO statistics (t20081407), refactor logging (t20117512)
 */

#define LD_SHADOW_PREFIX "[shadow] "

namespace facebook { namespace logdevice {

class AppendRequest;
class MockShadow;
class ShadowClientFactory;

class Shadow {
 private:
  using Mutex = std::recursive_mutex;

 public:
  using Attrs = folly::Optional<logsconfig::LogAttributes::Shadow>;

  /**
   * Constructs a Shadow object that enables/disables shadowing based
   * on parameters in the provided LogsConfig and ClientSettings.
   *
   * @param origin_name     Name of origin cluster
   * @param origin_config   Config of origin cluster, used to update shadowing
   *                        based on changes to server or LogsConfig
   * @param client_settings Client settings used to determine if shadowing is
   *                        globally disabled for this client
   */
  Shadow(std::string origin_name,
         std::shared_ptr<UpdateableConfig> origin_config,
         UpdateableSettings<Settings> client_settings,
         StatsHolder* stats);

  virtual ~Shadow();

  /**
   * Takes an append request and shadows it to the configured shadow cluster,
   * if applicable for the given log id. The provided AppendRequest is not
   * used for processing, just for convenience.
   *
   * @return 0 if shadowing was performed, -1 otherwise with err set to one of
   *         E::SHADOW_* (not necessarily a failure)
   */
  int appendShadow(const AppendRequest& req);

  /**
   * Determines whether shadowing is enabled for the given log ID, and if so,
   * checks if this append should be shadowed, based on the configured ratio.
   * If shadowing should be performed, returns the respective shadow
   * configuration; otherwise return folly::none. If the client has not opted-in
   * to shadowing, then it will be disabled for all logs.
   *
   * Calling this method has the side effect of updating the internal cache
   * of which log ranges were most recently appended to (only if shadowing is
   * enabled client-wide).
   *
   * @return The corresponding shadow config, or folly::none with err set to one
   *         of E::SHADOW_*
   */
  Attrs checkShadowConfig(logid_t logid);

 protected:
  Shadow(std::string origin_name,
         std::shared_ptr<UpdateableConfig> origin_config,
         UpdateableSettings<Settings> client_settings,
         StatsHolder* stats,
         std::unique_ptr<ShadowClientFactory> shadow_factory);

 private:
  // Path should be used for more efficient lookups on update, but currently
  // doesn't seem possible to get path from logid. So for, it just stores name
  // and isn't needed for anything useful
  struct ShadowInfo {
    std::string path;
    Attrs attrs;
    double counter;
  };

  Shadow(const Shadow&) = delete;            // non-copyable
  Shadow& operator=(const Shadow&) = delete; // non-assignable

  void reset(); // Called when shadowing disabled, to free resources
  void loadLogRangeForID(logid_t logid);
  void
  updateLogGroup(const std::shared_ptr<const LogsConfig::LogGroupNode>& group);
  bool checkAndUpdateRatio(ShadowInfo& shadow_info);

  void onSettingsUpdate();
  void onLogsConfigUpdate();
  void onLogGroupLoaded(std::shared_ptr<const LogsConfig::LogGroupNode> group,
                        logid_t original);

  std::string origin_name_;
  std::shared_ptr<UpdateableConfig> origin_config_;
  UpdateableSettings<Settings> client_settings_;
  StatsHolder* stats_;
  std::unique_ptr<ShadowClientFactory> shadow_factory_;

  UpdateableSettings<Settings>::SubscriptionHandle settings_sub_handle_;
  ConfigSubscriptionHandle logsconfig_sub_handle_;
  bool initial_initialization_done_{false};
  std::atomic_bool client_shadow_enabled_{false};
  std::chrono::milliseconds client_timeout_{std::chrono::milliseconds(0)};

  /**
   * Members used to cache log ranges for relatively quick logid -> range lookup
   * TODO Look into using folly::EvictingCacheMap or similar (t19726582)
   */
  // Caches the last range that was appended to
  folly::Optional<logid_range_t> last_used_range_;
  // List of all ranges this client appended to since shadowing was enabled
  // TODO refactor this to use non-overlapping interval tree (t19726582)
  std::vector<logid_range_t> range_cache_;
  // Log ids that have been used before but their ranges are still being loaded
  std::unordered_set<logid_t> pending_logs_;
  // Local cache of shadow information for log ranges we are interested in
  std::unordered_map<logid_range_t, ShadowInfo> shadow_map_;
  // Mutex used to protect access to log range cache and shadow information
  // Recursive in case callbacks for async calls are run on calling thread
  // TODO look into folly::SharedMutex (t20016799)
  mutable Mutex shadow_mutex_;

  friend class MockShadow;
};

}} // namespace facebook::logdevice
