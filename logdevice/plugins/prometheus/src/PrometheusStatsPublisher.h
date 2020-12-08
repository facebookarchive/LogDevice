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

#include <unordered_map>

#include <logdevice/common/StatsPublisher.h>
#include <prometheus/exposer.h>
#include <prometheus/family.h>
#include <prometheus/gauge.h>
#include <prometheus/registry.h>

namespace facebook { namespace logdevice {

class PrometheusStatsPublisher : public StatsPublisher {
 public:
  PrometheusStatsPublisher(const std::string& listen_addr);

  // Used for tests
  PrometheusStatsPublisher(std::shared_ptr<prometheus::Registry> registry);

  virtual ~PrometheusStatsPublisher() = default;

  void publish(const std::vector<const Stats*>& current,
               const std::vector<const Stats*>& previous,
               std::chrono::milliseconds elapsed) override;

  void addRollupEntity(std::string entity) override;

  prometheus::Family<prometheus::Gauge>& getFamily(const std::string& name, const std::string& stats_name);

 private:
  std::unique_ptr<prometheus::Exposer> exposer_;
  std::shared_ptr<prometheus::Registry> registry_;
  std::unordered_map<std::string, prometheus::Family<prometheus::Gauge>&>
      famililes_;
};

}} // namespace facebook::logdevice
