/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/admin/if/gen-cpp2/maintenance_types.h"
#include "logdevice/admin/maintenance/ClusterMaintenanceWrapper.h"
#include "logdevice/common/NodeID.h"
#include "logdevice/common/SampledTracer.h"
#include "logdevice/common/membership/types.h"

namespace facebook { namespace logdevice { namespace maintenance {

constexpr auto kMaintenanceManagerTracer = "maintenance_manager";

class MaintenanceManagerTracer : public SampledTracer {
  using MaintenanceIDs = folly::F14FastSet<std::string>;

 public:
  /**
   * A struct to track the duration of a certain event.
   */
  struct StopWatch {
    std::chrono::milliseconds duration;

    void begin() {
      start_ = std::chrono::steady_clock::now();
      started_ = true;
    }

    void end() {
      if (!started_) {
        return;
      }
      duration = std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now() - start_);
    }

    bool isStarted() {
      return started_;
    }

   private:
    std::chrono::steady_clock::time_point start_;
    bool started_{false};
  };

  /**
   * A struct that hold some of the common fields between all samples.
   */
  struct SampleBase {
    bool error{false};
    std::string error_reason{""};
    std::string reason{""};
    Status ncm_update_status{Status::OK};

    lsn_t maintenance_state_version;
    SystemTimestamp nc_published_time;
    membership::MembershipVersion::Type ncm_version;
  };

  /**
   * This sample represents the state of the world at the "end" of the
   * maintenance manager evaluation loop.
   */
  struct PeriodicStateSample : public SampleBase {
    std::vector<thrift::MaintenanceDefinition> current_maintenances;
    folly::F14FastMap<thrift::MaintenanceProgress,
                      folly::F14FastSet<std::string>>
        maintenances_progress;
    // Used for node_idx -> node_name traslation
    std::shared_ptr<const configuration::nodes::ServiceDiscoveryConfig>
        service_discovery;

    StopWatch pre_safety_checker_ncm_watch;
    StopWatch post_safety_checker_ncm_watch;
    StopWatch safety_check_watch;
    StopWatch evaluation_watch;
  };

  explicit MaintenanceManagerTracer(std::shared_ptr<TraceLogger> logger);

  void trace(PeriodicStateSample);

  /**
   * The local default for this is 100% of samples unless explicity overridden
   * by the config file.
   */
  folly::Optional<double> getDefaultSamplePercentage() const override {
    return 100;
  }

 private:
  /**
   * Allows users to filter by the level of verbosity they are interested in.
   */
  enum class Verbosity {
    NONE = 0 /* Just for reference, shouldn't be used. */,
    EVENTS = 5 /* Interesting events: new maintenance, metadata change, etc. */,
    VERBOSE = 10 /* For noisy samples like periodic evaluation */,
  };
};

}}} // namespace facebook::logdevice::maintenance
