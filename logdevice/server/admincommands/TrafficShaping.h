/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <algorithm>

#include <folly/ScopeGuard.h>

#include "logdevice/common/Priority.h"
#include "logdevice/common/PriorityMap.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/NodeLocation.h"
#include "logdevice/server/admincommands/AdminCommand.h"

namespace facebook { namespace logdevice { namespace commands {

class TrafficShaping : public AdminCommand {
  using AdminCommand::AdminCommand;

 private:
  std::string scope_name_;
  std::string priority_name_;
  std::string read_traffic_class_;
  bool clear_{false};
  bool enable_{false};
  bool disable_{false};
  int64_t guaranteed_bytes_per_second_{-1};
  int64_t max_bytes_per_second_{-1};
  int64_t max_burst_bytes_{-1};

 public:
  void getOptions(
      boost::program_options::options_description& out_options) override {
    out_options.add_options()(
        "clear-local-overrides", boost::program_options::bool_switch(&clear_))(
        "scope", boost::program_options::value<std::string>(&scope_name_))(
        "default-read-traffic-class",
        boost::program_options::value<std::string>(&read_traffic_class_))(
        "disable", boost::program_options::bool_switch(&disable_))(
        "enable", boost::program_options::bool_switch(&enable_))(
        "priority",
        boost::program_options::value<std::string>(&priority_name_))(
        "guaranteed-bytes-per-second",
        boost::program_options::value<int64_t>(&guaranteed_bytes_per_second_))(
        "max-bytes-per-second",
        boost::program_options::value<int64_t>(&max_bytes_per_second_))(
        "max-burst-bytes",
        boost::program_options::value<int64_t>(&max_burst_bytes_));
  }
  std::string getUsage() override {
    return "traffic_shaping "
           "[--clear-local-overrides]"
           "[--default-read-traffic-class=NAME]"
           "[--scope=NAME|ALL"
           " [--enable|--disable]"
           " [--priority=NAME|ALL"
           " [--guaranteed-bytes-per-second=<int64_t>]"
           " [--max-bytes-per-second=<int64_t>]"
           " [--max-burst-bytes=<int64_t>]]]";
  }

  void run() override {
    if (clear_) {
      auto& config = server_->getProcessor()->config_;
      auto result = config->updateableServerConfig()->updateOverrides(
          [](const std::shared_ptr<ServerConfig>&,
             ServerConfig::Overrides& overrides) {
            overrides.trafficShapingConfig.reset();
          });
      if (result != 0) {
        out_.printf("Config verification failed after clearing "
                    "local traffic shaping overrides. "
                    "Changes not applied.\r\n");
      }
      return;
    }

    // Validate
    TrafficClass tc = TrafficClass::INVALID;
    if (!read_traffic_class_.empty()) {
      std::transform(read_traffic_class_.begin(),
                     read_traffic_class_.end(),
                     read_traffic_class_.begin(),
                     ::toupper);
      tc = trafficClasses().reverseLookup(read_traffic_class_);
      if (tc == TrafficClass::INVALID) {
        out_.printf("traffic_shaping: --traffic-class must be a valid traffic "
                    "class.\r\n");
      }
    }

    const std::string pqueue_pname = "PRIORITY_QUEUE";
    const int pqueue_pidx = asInt(Priority::NUM_PRIORITIES);
    NodeLocationScope scope = NodeLocationScope::INVALID;
    NodeLocationScope last_scope = NodeLocationScope::ROOT;
    if (!scope_name_.empty()) {
      std::transform(scope_name_.begin(),
                     scope_name_.end(),
                     scope_name_.begin(),
                     ::toupper);
      if (scope_name_ == "ALL") {
        scope = NodeLocationScope::NODE;
      } else {
        scope = NodeLocation::scopeNames().reverseLookup(scope_name_);
        if (scope == NodeLocationScope::INVALID) {
          out_.printf("traffic_shaping: --scope must be \"ALL\" "
                      "or name a valid scope.\r\n");
          return;
        }
        last_scope = scope;
      }
    }

    if (enable_ || disable_) {
      if (scope == NodeLocationScope::INVALID) {
        out_.printf("traffic_shaping: --scope is requied for --enable or "
                    "--disable.\r\n");
        return;
      }
      if (enable_ && disable_) {
        out_.printf("traffic_shaping: --scope cannot be both enabled and "
                    "disabled.\r\n");
        return;
      }
    }

    int start_pidx = asInt(Priority::INVALID);
    int last_pidx = asInt(Priority::NUM_PRIORITIES);
    if (!priority_name_.empty()) {
      std::transform(priority_name_.begin(),
                     priority_name_.end(),
                     priority_name_.begin(),
                     ::toupper);
      if (scope == NodeLocationScope::INVALID) {
        out_.printf("traffic_shaping: --scope is required "
                    "when specifying --priority.\r\n");
        return;
      }

      if (priority_name_ == "ALL") {
        start_pidx = asInt(Priority::MAX);
      } else if (priority_name_ == pqueue_pname) {
        start_pidx = asInt(Priority::NUM_PRIORITIES);
      } else {
        auto priority = PriorityMap::toName().reverseLookup(priority_name_);
        if (priority == Priority::INVALID) {
          out_.printf("traffic_shaping: --priority must be \"ALL\" "
                      "or name a valid priority level.\r\n");
          return;
        }
        start_pidx = last_pidx = asInt(priority);
      }
      if (guaranteed_bytes_per_second_ == -1 && max_bytes_per_second_ == -1 &&
          max_burst_bytes_ == -1) {
        out_.printf(
            "traffic_shaping: one or more of --guaranteed-bytes-per-second, "
            "--max-bytes-per-second, and --max-burst-bytes must be specified "
            "when using --priority.\r\n");
        return;
      }
      if (guaranteed_bytes_per_second_ < -1 || max_bytes_per_second_ < -1 ||
          max_burst_bytes_ < -1) {
        out_.printf("traffic_shaping: --guaranteed-bytes-per-second, "
                    "--max-bytes-per-second, and --max-burst-bytes must be "
                    "greater than or equal to 0.\r\n");
      }
    } else if (guaranteed_bytes_per_second_ != -1 ||
               max_bytes_per_second_ != -1 || max_burst_bytes_ != -1) {
      out_.printf(
          "traffic_shaping: --priority must be provided when specifying "
          "any of --guaranteed-bytes-per-second, --max-bytes-per-second, "
          "or --max-burst-bytes.\r\n");
      return;
    }

    // Execute
    auto orig_cfg = server_->getProcessor()->config_->get();
    auto tsc = std::make_unique<configuration::TrafficShapingConfig>(
        orig_cfg->serverConfig()->getTrafficShapingConfig());

    if (tc != TrafficClass::INVALID) {
      out_.printf("traffic_shaping::default-read-traffic-class %s => %s\r\n",
                  trafficClasses()[tsc->default_read_traffic_class].c_str(),
                  trafficClasses()[tc].c_str());
      tsc->default_read_traffic_class = tc;
    }

    for (auto& policy_it : tsc->flowGroupPolicies) {
      auto& scope = policy_it.first;
      auto& fgp = policy_it.second;
      const auto sname = NodeLocation::scopeNames()[scope].c_str();

      if (!fgp.configured()) {
        if (scope_name_ == "ALL") {
          // Allow "ALL" iterations to ignore unconfigured scopes.
          continue;
        }
        out_.printf(
            "traffic_shaping: scope %s was not configured at startup and thus "
            " cannot have its settings changed.\r\n",
            sname);
        return;
      }

      if (enable_ || disable_) {
        out_.printf("traffic_shaping::%s %s => %s\r\n",
                    sname,
                    fgp.enabled() ? "enabled" : "disabled",
                    enable_ ? "enabled" : "disabled");
        fgp.setEnabled(enable_);
      }

      for (auto pidx = start_pidx; pidx <= last_pidx; ++pidx) {
        auto& bucket_policy = fgp.entries[pidx];
        auto p = static_cast<Priority>(pidx);
        const auto pname = pidx == pqueue_pidx
            ? pqueue_pname.c_str()
            : PriorityMap::toName()[p].c_str();

        if (max_bytes_per_second_ != -1) {
          out_.printf(
              "traffic_shaping::%s::%s::max-bytes-per-second %jd => %jd\r\n",
              sname,
              pname,
              (intmax_t)bucket_policy.max_bw,
              (intmax_t)max_bytes_per_second_);
          bucket_policy.max_bw = max_bytes_per_second_;
        }
        if (guaranteed_bytes_per_second_ != -1) {
          if (guaranteed_bytes_per_second_ > bucket_policy.max_bw) {
            out_.printf(
                "traffic_shaping::%s::%s::guaranteed-bytes-per-second limited "
                "by max-bytes-per-second.\r\n",
                sname,
                pname);
            guaranteed_bytes_per_second_ = bucket_policy.max_bw;
          }
          out_.printf("traffic_shaping::%s::%s::guaranteed-bytes-per-second "
                      "%jd => %jd\r\n",
                      sname,
                      pname,
                      (intmax_t)bucket_policy.guaranteed_bw,
                      (intmax_t)guaranteed_bytes_per_second_);
          bucket_policy.guaranteed_bw = guaranteed_bytes_per_second_;
        }
        if (max_burst_bytes_ != -1) {
          out_.printf("traffic_shaping::%s::%s::max-burst-bytes %jd => %jd\r\n",
                      sname,
                      pname,
                      (intmax_t)bucket_policy.capacity,
                      (intmax_t)max_burst_bytes_);
          bucket_policy.capacity = max_burst_bytes_;
        }
      }
    }

    auto& config = server_->getProcessor()->config_;
    auto result = config->updateableServerConfig()->updateOverrides(
        [&](const std::shared_ptr<ServerConfig>&,
            ServerConfig::Overrides& overrides) {
          overrides.trafficShapingConfig = std::move(tsc);
        });
    if (result != 0) {
      out_.printf("Config verification failed. Changes not applied.\r\n");
    }
    ld_check(result == 0);
  }
};

}}} // namespace facebook::logdevice::commands
