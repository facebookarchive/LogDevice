/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <algorithm>
#include <cctype>
#include <utility>
#include <vector>

#include "logdevice/common/AdminCommandTable.h"
#include "logdevice/common/stats/PerShardHistograms.h"
#include "logdevice/common/stats/ServerHistograms.h"
#include "logdevice/server/admincommands/AdminCommand.h"
#include "logdevice/server/locallogstore/ShardedRocksDBLocalLogStore.h"

namespace facebook { namespace logdevice { namespace commands {

template <typename... UniqueColTypes>
class StatsHistogramBase : public AdminCommand {
 public:
  using Hist = HistogramInterface;
  using HistTuple = std::tuple<std::string, Hist*, UniqueColTypes...>;
  using SummaryTable = AdminCommandTable<std::string, // name
                                         UniqueColTypes...,
                                         std::string, // unit
                                         double,      // min
                                         double,      // p50
                                         double,      // p75
                                         double,      // p95
                                         double,      // p99
                                         double,      // p99.99
                                         double,      // max
                                         int,         // count
                                         double       // mean
                                         >;
  static constexpr unsigned NameColIdx = 0;
  static constexpr unsigned UnitColIdx = 1 + sizeof...(UniqueColTypes);
  static constexpr unsigned MinColIdx = 2 + sizeof...(UniqueColTypes);
  static constexpr unsigned P50ColIdx = 3 + sizeof...(UniqueColTypes);
  static constexpr unsigned P75ColIdx = 4 + sizeof...(UniqueColTypes);
  static constexpr unsigned P95ColIdx = 5 + sizeof...(UniqueColTypes);
  static constexpr unsigned P99ColIdx = 6 + sizeof...(UniqueColTypes);
  static constexpr unsigned P9999ColIdx = 7 + sizeof...(UniqueColTypes);
  static constexpr unsigned MaxColIdx = 8 + sizeof...(UniqueColTypes);
  static constexpr unsigned CountColIdx = 9 + sizeof...(UniqueColTypes);
  static constexpr unsigned MeanColIdx = 10 + sizeof...(UniqueColTypes);

  std::string getUsage() override {
    return "[--summary] [--clear] [--json]";
  }

  void getOptions(boost::program_options::options_description& opts) override {
    opts.add_options()(
        "summary", boost::program_options::bool_switch(&summary_))(
        "clear", boost::program_options::bool_switch(&clear_))(
        "json", boost::program_options::bool_switch(&json_));
  }

  void forEachHistogram(facebook::logdevice::Stats& stats,
                        std::function<void(HistTuple&)> f) {
    auto hists = findHistograms(stats);
    if (hists.empty()) {
      out_.printf("Could not find any histogram that "
                  "matches the filters.\r\n");
      return;
    }
    for (auto& hist : hists) {
      f(hist);
    }
  }

  template <typename... S>
  void execute(S&&... colNames) {
    auto statsh = server_->getParameters()->getStats();
    if (!statsh) {
      // Without stats, there is nothing we can do.
      return;
    }

    // --json implies --summary
    summary_ |= json_;

    if (clear_) {
      statsh->runForEach([&](facebook::logdevice::Stats& s) {
        forEachHistogram(s, [](HistTuple& t) { std::get<1>(t)->clear(); });
      });
      return;
    }

    auto agg = statsh->aggregate();

    if (summary_) {
      SummaryTable table(!json_,
                         "Name",
                         std::forward<S>(colNames)...,
                         "Unit",
                         "min",
                         "p50",
                         "p75",
                         "p95",
                         "p99",
                         "p99.99",
                         "max",
                         "count",
                         "mean");
      forEachHistogram(agg, [&](HistTuple& t) { printHistSummary(t, table); });
      json_ ? table.printJson(out_) : table.print(out_);
    } else {
      forEachHistogram(agg, [&](HistTuple& t) { printHist(t); });
    }
  }

  void printHistSummary(HistTuple& tuple, SummaryTable& table) {
    const std::string& name = std::get<0>(tuple);
    const Hist* hist = std::get<1>(tuple);

    auto to_string = [&](double data, bool prettify) {
      if (!prettify) {
        return folly::to<std::string>(data);
      }
      return hist->valueToString(data);
    };

    uint64_t count;
    int64_t sum;
    std::array<double, 7> pct_in = {0, .5, .75, .95, .99, .9999, 1};
    std::array<int64_t, 7> pct_out;
    hist->estimatePercentiles(
        &pct_in[0], pct_in.size(), &pct_out[0], &count, &sum);

    table.next()
        .template set<NameColIdx>(name)
        .template set<MinColIdx>(pct_out[0], to_string)
        .template set<P50ColIdx>(pct_out[1], to_string)
        .template set<P75ColIdx>(pct_out[2], to_string)
        .template set<P95ColIdx>(pct_out[3], to_string)
        .template set<P99ColIdx>(pct_out[4], to_string)
        .template set<P9999ColIdx>(pct_out[5], to_string)
        .template set<MaxColIdx>(pct_out[6], to_string)
        .template set<CountColIdx>(count);
    if (count > 0) {
      table.template set<MeanColIdx>(1. * sum / count, to_string);
    }
    table.template set<UnitColIdx>(hist->getUnitName());
    setUniqueCols(tuple, table);
  }

 protected:
  bool summary_{false};
  bool clear_{false};
  bool json_{false};

  virtual std::vector<HistTuple>
  findHistograms(facebook::logdevice::Stats& stats) = 0;

  virtual void printHist(HistTuple& tuple) = 0;

  virtual void setUniqueCols(HistTuple& /*tuple*/, SummaryTable& /*table*/) {}
};

using ShardedStatsHistogramBase = StatsHistogramBase</*shard*/ int>;
class StatsHistogram : public ShardedStatsHistogramBase {
 public:
  std::string getUsage() override {
    return "stats2 histogram <type>|all [shard] " +
        ShardedStatsHistogramBase::getUsage();
  }

  void getOptions(boost::program_options::options_description& opts) override {
    opts.add_options()(
        "type", boost::program_options::value<std::string>(&type_))(
        "shard", boost::program_options::value<shard_index_t>(&shard_));
    ShardedStatsHistogramBase::getOptions(opts);
  }

  void getPositionalOptions(
      boost::program_options::positional_options_description& out_options)
      override {
    out_options.add("type", 1);
    out_options.add("shard", 2);
    ShardedStatsHistogramBase::getPositionalOptions(out_options);
  }

  void run() override {
    if (type_.empty()) {
      out_.printf("Statistic name or \'all\' must be specified\r\n");
      return;
    }

    auto statsh = server_->getParameters()->getStats();
    if (!statsh) {
      return;
    }

    execute("Shard");
  }

 private:
  std::string type_;
  shard_index_t shard_{-1};

  // Find the list of histograms this command should act on based on the filters
  // passed to this admin command. Select all histograms if `type_` == "all", or
  // the specific histogram whose name is `type_`.
  std::vector<HistTuple>
  findHistograms(facebook::logdevice::Stats& stats) override {
    ld_check(stats.server_histograms);
    std::vector<HistTuple> hists;

    if (shard_ == -1) {
      // The user did not specify a shard, look for non sharded histograms.
      if (type_ == "all") {
        for (auto& i : stats.server_histograms->map()) {
          hists.push_back(HistTuple(i.first, i.second, -1));
        }
      } else {
        auto* hist = stats.server_histograms->find(type_);
        if (hist) {
          hists.push_back(HistTuple(type_, hist, -1));
        }
      }
    }

    if (!server_->getProcessor()->runningOnStorageNode()) {
      // This is not a storage node, don't look for sharded histograms.
      return hists;
    }

    ld_check(server_->getShardedLocalLogStore() != nullptr);
    shard_index_t shard_lo = 0;
    shard_index_t shard_hi =
        server_->getShardedLocalLogStore()->numShards() - 1;
    if (shard_ != -1) {
      if (shard_ < shard_lo || shard_ > shard_hi) {
        out_.printf("Shard index out or range\r\n");
        return hists;
      }
      shard_lo = shard_hi = shard_;
    }

    if (type_ == "all") {
      for (auto& i : stats.per_shard_histograms->map()) {
        for (int idx = shard_lo; idx <= shard_hi; ++idx) {
          hists.push_back(HistTuple(i.first, i.second->get(idx), idx));
        }
      }
    } else {
      auto* hist = stats.per_shard_histograms->find(type_);
      if (hist) {
        for (int idx = shard_lo; idx <= shard_hi; ++idx) {
          hists.push_back(HistTuple(type_, hist->get(idx), idx));
        }
      }
    }
    return hists;
  }

  void setUniqueCols(HistTuple& tuple, SummaryTable& table) override {
    shard_index_t shard_idx = std::get<2>(tuple);

    if (shard_idx >= 0) {
      table.set<1>(shard_idx);
    }
  }

  void printHist(HistTuple& tuple) override {
    std::ostringstream oss;
    std::get<1>(tuple)->print(oss);

    const std::string& name = std::get<0>(tuple);
    shard_index_t shard_idx = std::get<2>(tuple);

    if (shard_idx >= 0) {
      out_.printf("%s%sShard %d:\r\n%s\r\n",
                  type_ == "all" ? name.c_str() : "",
                  type_ == "all" ? " - " : "",
                  shard_idx,
                  oss.str().c_str());
    } else if (type_ != "all") {
      out_.printf("%s\r\n", oss.str().c_str());
    } else {
      out_.printf("%s:\r\n%s\r\n", name.c_str(), oss.str().c_str());
    }
  }
};

using TrafficShapingHistogramBase =
    StatsHistogramBase<std::string /*scope*/, std::string /*priority*/>;
class TrafficShapingHistogram : public TrafficShapingHistogramBase {
 public:
  enum class SelectionType { SINGLE, ALL, MERGE, INVALID };

  std::string getUsage() override {
    return "stats2 shaping "
           "[--scope=NAME|ALL|MERGED] "
           "[--priority=NAME|ALL|MERGE] " +
        TrafficShapingHistogramBase::getUsage();
  }

  void getOptions(boost::program_options::options_description& opts) override {
    opts.add_options()(
        "scope", boost::program_options::value<std::string>(&scope_))(
        "priority", boost::program_options::value<std::string>(&priority_));
    TrafficShapingHistogramBase::getOptions(opts);
  }

  void run() override {
    SCOPE_EXIT {
      aggregated_priority_stats_.clear();
      aggregated_flow_group_stats_.clear();
    };

    scope_lo_ = 0;
    scope_hi_ = NodeLocation::NUM_ALL_SCOPES - 1;
    scope_sel_type_ = validateSelection(
        scope_, NodeLocation::scopeNames(), scope_lo_, scope_hi_);
    if (scope_sel_type_ == SelectionType::INVALID) {
      return;
    }

    priority_lo_ = 0;
    priority_hi_ = asInt(Priority::NUM_PRIORITIES) - 1;
    priority_sel_type_ = validateSelection(
        priority_, PriorityMap::toName(), priority_lo_, priority_hi_);
    if (priority_sel_type_ == SelectionType::INVALID) {
      return;
    }

    execute("Scope", "Priority");
  }

 private:
  template <typename EnumMapT>
  SelectionType
  validateSelection(std::string& s, EnumMapT& map, int& lo, int& high) {
    std::transform(s.begin(), s.end(), s.begin(), ::toupper);
    if (s == "ALL") {
      return SelectionType::ALL;
    }

    if (s == "MERGE") {
      return SelectionType::MERGE;
    }

    int idx = lo;
    for (; idx <= high; ++idx) {
      if (s == map[idx]) {
        break;
      }
    }

    if (idx > high) {
      out_.printf("Index out or range\r\n");
      return SelectionType::INVALID;
    }

    lo = high = idx;
    return SelectionType::SINGLE;
  }

  std::vector<HistTuple>
  findHistograms(facebook::logdevice::Stats& stats) override {
    std::vector<HistTuple> hists;

    if (scope_sel_type_ == SelectionType::MERGE) {
      aggregated_flow_group_stats_.push_back(
          std::make_unique<PerFlowGroupStats>(stats.totalPerFlowGroupStats()));
      auto& all_flow_groups = aggregated_flow_group_stats_.back();

      if (priority_sel_type_ == SelectionType::MERGE) {
        aggregated_priority_stats_.push_back(
            std::make_unique<PerShapingPriorityStats>(
                all_flow_groups->totalPerShapingPriorityStats()));
        auto* h = aggregated_priority_stats_.back()->time_in_queue.get();

        hists.push_back(HistTuple("time_in_queue", h, "Merge", "Merge"));
      } else {
        for (int p_idx = priority_lo_; p_idx <= priority_hi_; ++p_idx) {
          const auto& p_name = PriorityMap::toName()[p_idx];
          auto* h = all_flow_groups->priorities[p_idx].time_in_queue.get();

          hists.push_back(HistTuple("time_in_queue", h, "Merge", p_name));
        }
      }
    } else {
      for (int s_idx = scope_lo_; s_idx <= scope_hi_; ++s_idx) {
        const auto& scope_name = NodeLocation::scopeNames()[s_idx];
        auto& flow_group_stats = stats.per_flow_group_stats[s_idx];

        if (priority_sel_type_ == SelectionType::MERGE) {
          aggregated_priority_stats_.push_back(
              std::make_unique<PerShapingPriorityStats>(
                  flow_group_stats.totalPerShapingPriorityStats()));
          auto* h = aggregated_priority_stats_.back()->time_in_queue.get();

          hists.push_back(HistTuple("time_in_queue", h, scope_name, "Merge"));
        } else {
          for (int p_idx = priority_lo_; p_idx <= priority_hi_; ++p_idx) {
            const auto& p_name = PriorityMap::toName()[p_idx];
            auto* h = flow_group_stats.priorities[p_idx].time_in_queue.get();

            hists.push_back(HistTuple("time_in_queue", h, scope_name, p_name));
          }
        }
      }
    }
    return hists;
  }

  void setUniqueCols(HistTuple& tuple, SummaryTable& table) override {
    table.set<1>(std::get<2>(tuple));
    table.set<2>(std::get<3>(tuple));
  }

  void printHist(HistTuple& tuple) override {
    std::string scope_string = scope_sel_type_ == SelectionType::MERGE
        ? "All Scopes"
        : "Scope " + std::get<2>(tuple);
    std::string priority_string = priority_sel_type_ == SelectionType::MERGE
        ? "All Priorities"
        : "Priority " + std::get<3>(tuple);

    std::ostringstream oss;
    std::get<1>(tuple)->print(oss);
    out_.printf("%s: %s: time_in_queue\r\n%s\r\n",
                scope_string.c_str(),
                priority_string.c_str(),
                oss.str().c_str());
  }

  std::string scope_ = "MERGE";
  std::string priority_ = "MERGE";
  int scope_lo_;
  int scope_hi_;
  SelectionType scope_sel_type_;
  int priority_lo_;
  int priority_hi_;
  SelectionType priority_sel_type_;
  std::vector<std::unique_ptr<PerShapingPriorityStats>>
      aggregated_priority_stats_;
  std::vector<std::unique_ptr<PerFlowGroupStats>> aggregated_flow_group_stats_;
};

}}} // namespace facebook::logdevice::commands
