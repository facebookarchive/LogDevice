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

#include <logdevice/common/plugin/StatsPublisherFactory.h>
#include <logdevice/common/settings/Settings.h>
#include <logdevice/common/settings/SettingsUpdater.h>
#include <logdevice/common/settings/UpdateableSettings.h>

#include "PrometheusSettings.h"
#include "PrometheusStatsPublisher.h"

namespace facebook { namespace logdevice {

class PrometheusStatsPublisherFactory : public StatsPublisherFactory {
 public:
  virtual ~PrometheusStatsPublisherFactory() = default;

  PluginType type() const override {
    return PluginType::STATS_PUBLISHER_FACTORY;
  }

  std::unique_ptr<StatsPublisher> operator()(UpdateableSettings<Settings>,
                                             int num_db_shards) override {
    if (prometheus_settings_->prometheus_listen_addr.empty()) {
      ld_error("Prometheus was used as the stats publisher, but the listen "
               "address is not set. Will not load the plugin.");
      return nullptr;
    }
    return std::make_unique<PrometheusStatsPublisher>(
        prometheus_settings_->prometheus_listen_addr);
  }

  std::string identifier() const override {
    return "logdevice-prometheus";
  }

  std::string displayName() const override {
    return "Logdevice Prometheus";
  }

  virtual void addOptions(SettingsUpdater* updater) override {
    updater->registerSettings(prometheus_settings_);
  }

 private:
  UpdateableSettings<PrometheusSettings> prometheus_settings_;
};

}} // namespace facebook::logdevice
