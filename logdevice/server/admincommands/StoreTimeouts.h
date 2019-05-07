/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <algorithm>
#include <array>
#include <iomanip>
#include <utility>
#include <vector>

#include <folly/stats/TimeseriesHistogram.h>

#include "logdevice/common/AdminCommandTable.h"
#include "logdevice/common/WorkerTimeoutStats.h"
#include "logdevice/common/request_util.h"
#include "logdevice/server/admincommands/AdminCommand.h"

namespace facebook { namespace logdevice { namespace commands {

class StoreTimeoutHistogram : public AdminCommand {
 public:
  using SummaryTable = AdminCommandTable<std::string, // unit
                                         int,         // worker id
                                         int,         // adjacent node id
                                         double,      // p50
                                         double,      // p75
                                         double,      // p95
                                         double,      // p99
                                         double,      // p99.9
                                         double       // p99.99
                                         >;
  static constexpr unsigned UnitColIdx = 0;
  static constexpr unsigned WorkerIdColIdx = 1;
  static constexpr unsigned AdjacentNodeIdColIdx = 2;
  static constexpr unsigned P50ColIdx = 3;
  static constexpr unsigned P75ColIdx = 4;
  static constexpr unsigned P95ColIdx = 5;
  static constexpr unsigned P99ColIdx = 6;
  static constexpr unsigned P999ColIdx = 7;
  static constexpr unsigned P9999ColIdx = 8;

  using EstimatesFromWorker = folly::Optional<
      std::tuple<std::array<WorkerTimeoutStats::Latency, 6>, worker_id_t, int>>;
  static constexpr unsigned EstimatesFieldIdx = 0;
  static constexpr unsigned WorkerIdFieldIdx = 1;
  static constexpr unsigned AdjacentNodeIdFieldIdx = 2;

  std::string getUsage() override {
    return "stats2 store_timeouts <worker>|all [node]"
           "[--summary] [--clear] [--json]";
  }

  void getOptions(boost::program_options::options_description& opts) override {
    // clang-format off
    opts.add_options()
        ("worker", boost::program_options::value<std::string>(&worker_))
        ("node", boost::program_options::value<int>(&node_))
        ("summary", boost::program_options::bool_switch(&summary_))
        ("clear", boost::program_options::bool_switch(&clear_))
        ("json", boost::program_options::bool_switch(&json_));
    // clang-format on
  }

  void getPositionalOptions(
      boost::program_options::positional_options_description& out_options)
      override {
    out_options.add("worker", 1);
    out_options.add("node", 2);
  }

  void run() override {
    if (clear_) {
      clearNodeEstimators();
      return;
    }

    summary_ |= json_;

    if (summary_) {
      printSummary();
      return;
    }

    for (const auto& estimates : getEstimatorsFromWorkers()) {
      printPercentiles(estimates);
    }
  }

 private:
  std::vector<EstimatesFromWorker> getEstimatorsFromWorkers() const {
    auto get_estimates = [this]() -> EstimatesFromWorker {
      Worker* worker = Worker::onThisThread();
      auto& timeout_stats = worker->getWorkerTimeoutStats();

      auto estimations = timeout_stats.getEstimations(
          WorkerTimeoutStats::Levels::TEN_SECONDS, node_);
      if (!estimations.hasValue()) {
        return {};
      }
      return std::make_tuple(estimations.value(), worker->idx_, node_);
    };

    if (worker_ == "all") {
      return run_on_worker_pool(
          server_->getProcessor(), WorkerType::GENERAL, get_estimates);
    }

    int worker_id = 0;
    try {
      worker_id = std::stoi(worker_);
    } catch (...) {
      return {};
    }

    std::vector<EstimatesFromWorker> estimators;
    estimators = run_on_workers(server_->getProcessor(),
                                std::vector<int>{worker_id},
                                WorkerType::GENERAL,
                                get_estimates);

    return estimators;
  }

  void printSummary() {
    SummaryTable table(!json_,
                       "Unit",
                       "WorkerId",
                       "AdjacentNodeId",
                       "p50",
                       "p75",
                       "p95",
                       "p99",
                       "p99.9",
                       "p99.99");

    auto get_estimates = [this]() -> std::vector<EstimatesFromWorker> {
      Worker* worker = Worker::onThisThread();
      auto& timeout_stats = worker->getWorkerTimeoutStats();
      std::vector<EstimatesFromWorker> result;
      result.reserve(timeout_stats.histograms_.size());
      for (auto& node_estimator : timeout_stats.histograms_) {
        auto estimate = timeout_stats.getEstimations(
            WorkerTimeoutStats::TEN_SECONDS, node_estimator.first);
        if (estimate.hasValue()) {
          result.emplace_back(std::make_tuple(
              estimate.value(), worker->idx_, node_estimator.first));
        } else {
          result.push_back(folly::none);
        }
      }

      return result;
    };

    const auto& results = run_on_worker_pool(
        server_->getProcessor(), WorkerType::GENERAL, get_estimates);

    for (const auto& estimates : results) {
      for (const auto& estimate : estimates) {
        addPercentilesRowToTable(estimate, table);
      }
    }

    if (json_) {
      table.printJson(out_);
    } else {
      table.print(out_);
    }
  }

  void clearNodeEstimators() const {
    run_on_all_workers(server_->getProcessor(), []() {
      Worker* worker = Worker::onThisThread();
      worker->getWorkerTimeoutStats().histograms_.clear();
      return 0;
    });
  }

  static void addPercentilesRowToTable(const EstimatesFromWorker& estimates,
                                       SummaryTable& table) {
    if (!estimates.hasValue()) {
      return;
    }

    const auto& quantiles = std::get<EstimatesFieldIdx>(*estimates);

    table.next()
        .template set<UnitColIdx>("ms")
        .template set<WorkerIdColIdx>(std::get<WorkerIdFieldIdx>(*estimates))
        .template set<AdjacentNodeIdColIdx>(
            std::get<AdjacentNodeIdFieldIdx>(*estimates))
        .template set<P50ColIdx>(quantiles[0])
        .template set<P75ColIdx>(quantiles[1])
        .template set<P95ColIdx>(quantiles[2])
        .template set<P99ColIdx>(quantiles[3])
        .template set<P999ColIdx>(quantiles[4])
        .template set<P9999ColIdx>(quantiles[5]);
  }

  void printPercentiles(const EstimatesFromWorker& estimates) {
    if (!estimates.hasValue()) {
      return;
    }
    std::ostringstream oss;
    const int node = std::get<2>(*estimates);
    oss << "worker_id: " << std::get<1>(*estimates).val() << '\n';
    oss << "node_id: " << (node == -1 ? "overall" : std::to_string(node))
        << '\n';

    for (int i = 0; i < WorkerTimeoutStats::kQuantiles.size(); ++i) {
      const double estimation = std::get<0>(*estimates)[i];
      oss << std::setw(7) << WorkerTimeoutStats::kQuantiles[i] << ": "
          << std::setw(0) << estimation << " ms\n";
    }
    out_.printf("%s", oss.str().c_str());
  }

 private:
  bool summary_{false};
  bool clear_{false};
  bool json_{false};
  std::string worker_;
  int node_{-1};
};

}}} // namespace facebook::logdevice::commands
