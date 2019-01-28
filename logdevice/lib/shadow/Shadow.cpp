/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/lib/shadow/Shadow.h"

#include <algorithm>

#include "logdevice/common/AppendRequest.h"
#include "logdevice/lib/shadow/ShadowClient.h"

namespace facebook { namespace logdevice {

bool logIDInRange(logid_t logid, logid_range_t range) {
  return logid >= range.first && logid <= range.second;
}

Shadow::Shadow(std::string origin_name,
               std::shared_ptr<UpdateableConfig> origin_config,
               UpdateableSettings<Settings> client_settings,
               StatsHolder* stats)
    : Shadow(origin_name,
             origin_config,
             client_settings,
             stats,
             std::make_unique<ShadowClientFactory>(origin_name,
                                                   stats,
                                                   client_settings)) {}

Shadow::Shadow(std::string origin_name,
               std::shared_ptr<UpdateableConfig> origin_config,
               UpdateableSettings<Settings> client_settings,
               StatsHolder* stats,
               std::unique_ptr<ShadowClientFactory> shadow_factory)
    : origin_name_(std::move(origin_name)),
      origin_config_(std::move(origin_config)),
      client_settings_(std::move(client_settings)),
      stats_(stats),
      shadow_factory_(std::move(shadow_factory)) {
  // Subscribe to updates
  settings_sub_handle_ =
      client_settings_.subscribeToUpdates([this] { onSettingsUpdate(); });
  logsconfig_sub_handle_ =
      origin_config_->updateableLogsConfig()->subscribeToUpdates(
          [this] { onLogsConfigUpdate(); });

  // Initial settings update
  onSettingsUpdate();
}

Shadow::~Shadow() = default;

int Shadow::appendShadow(const AppendRequest& req) {
  logid_t logid = req.getRecordLogID();
  Attrs shadow_attrs = checkShadowConfig(logid); // sets err
  if (!shadow_attrs.hasValue()) {
    return -1;
  }

  // If we get this far, make sure to record this shadow attempt
  // Any errors after this count as general traffic shadowing failures, and
  // be compared against total shadow attempts to get an accurate shadowing
  // failure rate.
  STAT_INCR(stats_, client.shadow_append_attempt);

  std::shared_ptr<ShadowClient> shadow_client =
      shadow_factory_->get(shadow_attrs->destination());
  if (shadow_client == nullptr) {
    STAT_INCR(stats_, client.shadow_client_not_loaded);
    if (client_settings_->shadow_client_creation_retry_interval.count()) {
      shadow_factory_->createAsync(shadow_attrs, true /*is_a_retry*/);
    }
    err = E::SHADOW_LOADING;
    return -1;
  }

  Payload payload;
  AppendAttributes req_attrs;
  std::tie(payload, req_attrs) = req.getShadowData();

  int rv = shadow_client->append(
      logid, payload, req_attrs, req.getBufferedWriterBlobFlag());
  if (rv == -1) {
    // TODO detailed scuba stats T20416930 including error code
    STAT_INCR(stats_, client.shadow_append_failed);
  }
  return rv;
}

Shadow::Attrs Shadow::checkShadowConfig(logid_t logid) {
  if (!client_shadow_enabled_) {
    err = E::SHADOW_DISABLED;
    return folly::none;
  }

  // Determine which log range this logid belongs to, requires locking
  // This function will just return false immediately if it can't acquire the
  // lock because not worth blocking
  std::unique_lock<Mutex> shadow_lock(shadow_mutex_, std::try_to_lock);
  if (!shadow_lock.owns_lock()) {
    err = E::SHADOW_BUSY;
    return folly::none;
  }

  folly::Optional<logid_range_t> range;
  // First check the last used range, then check the range cache
  if (last_used_range_.hasValue() &&
      logIDInRange(logid, last_used_range_.value())) {
    range = last_used_range_.value();
  } else {
    auto found = std::find_if(range_cache_.begin(),
                              range_cache_.end(),
                              [&](auto r) { return logIDInRange(logid, r); });
    if (found != range_cache_.end()) {
      range = *found;
    }
  }

  // If all else failed, then we need to look up the log range that this logid
  // belongs to. This operation might take time, so it will be done async. The
  // current payload will be dropped from shadowing until the range is resolved.
  if (!range.hasValue()) {
    loadLogRangeForID(logid); // sets err
    return folly::none;
  } else {
    last_used_range_ = range;
  }

  // Lookup shadow parameters based on determined log range
  // If we have range information, shadow information should have already been
  // loaded as well
  ShadowInfo& shadow_info = shadow_map_[range.value()];
  if (shadow_info.attrs.hasValue()) {
    if (checkAndUpdateRatio(shadow_info)) {
      return shadow_info.attrs;
    } else {
      err = E::SHADOW_SKIP;
      return folly::none;
    }
  } else {
    err = E::SHADOW_UNCONFIGURED;
    return folly::none;
  }
}

// ShadowClientFactory::reset will also shut down helper thread
void Shadow::reset() {
  std::lock_guard<Mutex> range_cache_lock(shadow_mutex_);
  last_used_range_.clear();
  range_cache_.clear();
  pending_logs_.clear();
  shadow_map_.clear();
  shadow_factory_->reset();
}

// **NOTE** Assumes shadow lock has already been acquired
void Shadow::loadLogRangeForID(logid_t logid) {
  if (pending_logs_.find(logid) != pending_logs_.end()) {
    // Request has already been sent for this logid
    err = E::SHADOW_LOADING;
    return;
  } else {
    pending_logs_.insert(logid);
  }
  auto lc = origin_config_->getLogsConfig();
  if (lc == nullptr) {
    ld_warning(LD_SHADOW_PREFIX "LogsConfig is not present");
    err = E::SHADOW_UNCONFIGURED;
    return;
  }
  ld_debug(LD_SHADOW_PREFIX "Loading log group for logid %ld", logid.val());
  lc->getLogGroupByIDAsync(logid, [logid, this](auto group) {
    this->onLogGroupLoaded(group, logid);
  });
  // This needs to be here, since the above might have already called the
  // callback in sync due to LocalLogsConfig, so err might have another value
  err = E::SHADOW_LOADING;
}

// **NOTE** Assumes shadow lock has already been acquired
void Shadow::updateLogGroup(
    const std::shared_ptr<const LogsConfig::LogGroupNode>& group) {
  logid_range_t range = group->range();
  auto& shadow_attrs = group->attrs().shadow();
  if (shadow_attrs.hasValue()) {
    const auto& shadow_info =
        shadow_map_[range] = {group->name(), shadow_attrs.value(), 0.0};
    shadow_factory_->createAsync(shadow_info.attrs, false /* is_a_retry */);
    ld_info(LD_SHADOW_PREFIX
            "Updated log group '%s' with shadow dest='%s' and ratio=%f",
            shadow_info.path.c_str(),
            shadow_info.attrs->destination().c_str(),
            shadow_info.attrs->ratio());
  } else {
    shadow_map_[range] = {group->name(), folly::none, 0.0};
    ld_info(LD_SHADOW_PREFIX "Updated log group '%s' with shadow unconfigured",
            group->name().c_str());
  }
}

// **NOTE** Assumes shadow lock has already been acquired
//          Assumes shadow attrs has a value
bool Shadow::checkAndUpdateRatio(ShadowInfo& shadow_info) {
  double ratio = shadow_info.attrs->ratio();
  double update = shadow_info.counter + ratio;
  if (update >= 0.99) {
    shadow_info.counter = update - 1.0;
    return true;
  } else {
    shadow_info.counter = update;
    return false;
  }
}

void Shadow::onSettingsUpdate() {
  // It's expected that settings update callbacks are called from a single
  // thread, so we access stuff without mutex here.
  bool enabled_in_settings = client_settings_->traffic_shadow_enabled;
  auto timeout_in_settings = client_settings_->shadow_client_timeout;
  if (enabled_in_settings != client_shadow_enabled_ ||
      timeout_in_settings != client_timeout_ || !initial_initialization_done_) {
    initial_initialization_done_ = true;
    client_shadow_enabled_ = enabled_in_settings;
    client_timeout_ = timeout_in_settings;
    reset();
    if (client_shadow_enabled_) {
      shadow_factory_->start(client_timeout_);
    }
    ld_info(LD_SHADOW_PREFIX
            "Traffic shadowing %s, shadow client timeout: %ldms",
            (client_shadow_enabled_ ? "ENABLED" : "DISABLED"),
            client_timeout_.count());
  }
}

void Shadow::onLogsConfigUpdate() {
  if (!client_shadow_enabled_) {
    return;
  }

  // Update shadow parameters to all ranges this client is interested in
  // This is done by ID, not path, since currently it isn't possible to get the
  // full path of a log range just from its ID (TODO t20016989)
  auto lc = origin_config_->getLogsConfig();
  std::unique_lock<Mutex> shadow_lock(shadow_mutex_);
  for (const auto& range : range_cache_) {
    // This should be cached locally... if not then might be a problem?
    const auto group = lc->getLogGroupByIDShared(range.first);
    updateLogGroup(group);
  }
}

void Shadow::onLogGroupLoaded(
    std::shared_ptr<const LogsConfig::LogGroupNode> group,
    logid_t original) {
  if (group == nullptr) {
    // metadata log fallback - ignore for now?
    ld_debug(LD_SHADOW_PREFIX "Failed to load log group");
    return;
  }

  std::unique_lock<Mutex> shadow_lock(shadow_mutex_);
  logid_range_t range = group->range();
  range_cache_.push_back(range);
  pending_logs_.erase(original);
  updateLogGroup(group);
}

}} // namespace facebook::logdevice
