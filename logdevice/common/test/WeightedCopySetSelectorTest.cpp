/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <logdevice/common/WeightedCopySetSelector.h>

#include <gtest/gtest.h>
#include <logdevice/common/FailureDomainNodeSet.h>
#include <logdevice/common/HashBasedSequencerLocator.h>

#include "logdevice/common/configuration/ServerConfig.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/common/test/CopySetSelectorTestUtil.h"
#include "logdevice/common/test/NodeSetTestUtil.h"

using namespace facebook::logdevice;
using namespace facebook::logdevice::NodeSetTestUtil;
using NodeStatus = NodeAvailabilityChecker::NodeStatus;
using S = NodeLocationScope;

const logid_t LOG_ID = logid_t(42);
const shard_size_t NUM_SHARDS = 1;

#define N0 ShardID(0, 0)
#define N1 ShardID(1, 0)
#define N2 ShardID(2, 0)
#define N3 ShardID(3, 0)
#define N4 ShardID(4, 0)
#define N5 ShardID(5, 0)
#define N6 ShardID(6, 0)
#define N7 ShardID(7, 0)
#define N8 ShardID(8, 0)
#define N9 ShardID(9, 0)
#define N10 ShardID(10, 0)
#define N11 ShardID(11, 0)
#define N12 ShardID(12, 0)
#define N13 ShardID(13, 0)
#define N14 ShardID(14, 0)
#define N15 ShardID(15, 0)
#define N16 ShardID(16, 0)

namespace {

class WeightedCopySetSelectorTest : public ::testing::Test {
 public:
  WeightedCopySetSelectorTest() {
    dbg::currentLevel = getLogLevelFromEnv().value_or(dbg::Level::INFO);
    rng_.seed(5982775905867530240ull, 9936607027721666560ull);
  }

  // Should be called after populating the list of nodes (using addNodes()) and
  // assigning replication_.
  void initIfNeeded();

  // Lazily creates a copyset selector for the given sequencer and local
  // node IDs. The returned selector's log ID may not be equal to the provided
  // log ID, but it'll hash into the same primary sequencer node; the assumption
  // is that WeightedCopySetSelector doesn't substantially use log ID in any
  // way other than getting the primary sequencer node from it.
  // @param sequencer_node
  //   Must be equal to either -1 or getPrimarySequencerNode(log).
  //   Just an optimization.
  WeightedCopySetSelector& getSelector(logid_t log = LOG_ID,
                                       node_index_t sequencer_node = -1,
                                       node_index_t my_node = -1,
                                       bool locality = false);

  // These check correctness of generated copyset, but not distribution.
  CopySetSelector::Result select(std::vector<ShardID>& out,
                                 WeightedCopySetSelector& selector);
  CopySetSelector::Result augment(std::vector<ShardID>& copyset,
                                  WeightedCopySetSelector& selector);

  CopySetSelector::Result select(std::vector<ShardID>& out) {
    return select(out, getSelector());
  }
  CopySetSelector::Result augment(std::vector<ShardID>& copyset) {
    return augment(copyset, getSelector());
  }

  node_index_t getPrimarySequencerNode(logid_t log);

  // @param fake_weight_adjustments
  //   If not empty, it's a "null" test: we'll use one set of weights for
  //   copyset selection and another for checking distribution;
  //   we expect the check to say that distribution doesn't match. The set of
  //   weights that copyset selector will use is in nodes_in_config_; the set of
  //   weights that we'll check against is the same but with
  //   fake_weight_adjustments added to it (see get_weight in implementation).
  // @param expected_locality_rate
  //   Defines the range [first, second]. Expect that the fraction of copysets
  //   that contain at least one local copy falls in this range.
  //   Only checked if locality is true and fake_weight_adjustments is empty.
  void testDistributionImpl(bool locality,
                            std::pair<double, double> expected_locality_rate,
                            std::map<ShardID, double> fake_weight_adjustments);

  // Calls testDistributionImpl() 4 times: with `locality` = true and false,
  // with or without `fake_weight_adjustments`.
  // @param expected_locality_rate
  //   "Locality rate" is defined as the ratio
  //   (number of copysets that contain at least one node from local domain) /
  //   (total number of copysets), considering only copysets generated with
  //   my_domain = sequencer_domain and locality_enabled = true.
  //   This parameter gives the range in which the locality rate is expected to
  //   fall. For most tests this range is determined empirically: run the test
  //   to get the locality rate, check that it looks reasonable, set the
  //   expected_locality_rate around that value.
  // @param fake_weight_adjustments
  //   Perturbation of weights that's expected to produce a significantly
  //   different distribution. Used for verifying the sensitivity of the
  //   statistical test.
  void testDistribution(std::pair<double, double> expected_locality_rate,
                        std::map<ShardID, double> fake_weight_adjustments);

  void checkCopyset(const std::vector<ShardID>& cs);
  void checkUnreplicatable(const std::vector<ShardID>& whitelist);

  // Add nodes with given weights and sequencer weights to config.
  // Add the first in_nodeset of them to nodeset.
  void addNodes(std::string location_string,
                std::vector<double> weights,
                std::vector<double> sequencer,
                int in_nodeset = std::numeric_limits<int>::max());

  // Same but all sequencer weights are equal to storage weights.
  void addNodes(std::string location_string,
                std::vector<double> weights,
                int in_nodeset = std::numeric_limits<int>::max()) {
    addNodes(location_string, weights, {}, in_nodeset);
  }

  configuration::Nodes nodes_in_config_;
  StorageSet nodeset_indices_; // in increasing order
  ReplicationProperty replication_;

  // Boring boilerplate.
  StatsHolder stats{StatsParams().setIsServer(true)};
  XorShift128PRNG rng_;
  EpochMetaData epoch_metadata_;
  std::shared_ptr<NodeSetState> nodeset_state_;
  std::unique_ptr<ServerConfig> anon_server_config_; // doesn't have my node ID
  // Only sequencer affinity matters.
  logsconfig::DefaultLogAttributes default_log_attrs_;
  // Blacklist nodes here.
  TestCopySetSelectorDeps deps_;

  // We need different copyset selectors for different getMyNodeID() in config
  // and different primary sequencer nodes. Normally the primary sequencer node
  // is equal to getMyNodeID() (i.e. copyset selector lives on sequencer), but
  // there are some exceptions: (a) primary sequencer node is unavailable,
  // (b) rebuilding.
  struct Selector {
    std::shared_ptr<ServerConfig> server_config;

    // The copyset selector that we're testing.
    std::unique_ptr<WeightedCopySetSelector> selector;
  };

  // <my node, sequencer node, locality> -> selector
  std::map<std::tuple<node_index_t, node_index_t, bool>, Selector> selectors_;

  // Used for checking validity of generated copysets.
  std::unique_ptr<FailureDomainNodeSet<size_t>> replication_checker_;
  size_t replication_checker_counter_ = 0;

  // If true, we'll pass `*chain_out = false` to select().
  bool test_disabling_chain_ = false;
};

} // namespace

void WeightedCopySetSelectorTest::addNodes(std::string location_string,
                                           std::vector<double> weights,
                                           std::vector<double> sequencer,
                                           int in_nodeset) {
  ld_check(!weights.empty());
  if (sequencer.size() != weights.size()) {
    ld_check(sequencer.empty());
    sequencer = weights;
  }
  for (size_t i = 0; i < weights.size(); ++i) {
    if (i < in_nodeset) {
      nodeset_indices_.push_back(
          ShardID((node_index_t)nodes_in_config_.size(), 0));
    }
    ::addNodes(&nodes_in_config_,
               1,
               NUM_SHARDS,
               location_string,
               weights[i],
               sequencer[i]);
  }
}

void WeightedCopySetSelectorTest::initIfNeeded() {
  if (nodeset_state_) {
    return;
  }
  // Need to initialize the stuff stuff shared among all selectors.
  nodeset_state_ = std::make_shared<NodeSetState>(
      nodeset_indices_, LOG_ID, NodeSetState::HealthCheck::DISABLED);
  epoch_metadata_ = EpochMetaData(nodeset_indices_, replication_);
  configuration::NodesConfig nodes_config(nodes_in_config_);
  anon_server_config_ =
      std::unique_ptr<ServerConfig>(ServerConfig::fromDataTest(
          "weighted_copyset_selector_test(anon)", std::move(nodes_config)));
  replication_checker_ = std::make_unique<FailureDomainNodeSet<size_t>>(
      nodeset_indices_,
      *anon_server_config_->getNodesConfigurationFromServerConfigSource(),
      replication_);
}

node_index_t WeightedCopySetSelectorTest::getPrimarySequencerNode(logid_t log) {
  initIfNeeded();
  return HashBasedSequencerLocator::getPrimarySequencerNode(
      log,
      *anon_server_config_->getNodesConfigurationFromServerConfigSource(),
      &default_log_attrs_);
}

WeightedCopySetSelector&
WeightedCopySetSelectorTest::getSelector(logid_t log,
                                         node_index_t sequencer_node,
                                         node_index_t my_node,
                                         bool locality) {
  initIfNeeded();

  if (sequencer_node == -1) {
    if (locality) {
      sequencer_node = getPrimarySequencerNode(log);
    } else {
      sequencer_node = 0;
    }
  }
  if (my_node == -1) {
    ld_check(!locality || deps_.isAvailable(ShardID(sequencer_node, 0)));
    my_node = sequencer_node;
  }

  auto key = std::make_tuple(my_node, sequencer_node, locality);
  if (selectors_.count(key)) {
    return *selectors_.at(key).selector;
  }

  if (!nodeset_state_) {
    // Need to initialize the stuff stuff shared among all selectors.
    nodeset_state_ = std::make_shared<NodeSetState>(
        nodeset_indices_, LOG_ID, NodeSetState::HealthCheck::DISABLED);
    epoch_metadata_ = EpochMetaData(nodeset_indices_, replication_);
    configuration::NodesConfig nodes_config(nodes_in_config_);
    anon_server_config_ =
        std::unique_ptr<ServerConfig>(ServerConfig::fromDataTest(
            "weighted_copyset_selector_test(anon)", std::move(nodes_config)));
    replication_checker_ = std::make_unique<FailureDomainNodeSet<size_t>>(
        nodeset_indices_,
        *anon_server_config_->getNodesConfigurationFromServerConfigSource(),
        replication_);
  }

  Selector& s = selectors_[key];
  configuration::NodesConfig nodes_config(nodes_in_config_);
  s.server_config = std::shared_ptr<ServerConfig>(ServerConfig::fromDataTest(
      "weighted_copyset_selector_test", std::move(nodes_config)));
  s.selector = std::make_unique<WeightedCopySetSelector>(
      log,
      epoch_metadata_,
      nodeset_state_,
      s.server_config->getNodesConfigurationFromServerConfigSource(),
      NodeID(my_node, 1),
      &default_log_attrs_,
      locality,
      &stats,
      rng_,
      /* print_bias_warnings */ true,
      &deps_);
  return *s.selector;
}

CopySetSelector::Result
WeightedCopySetSelectorTest::select(std::vector<ShardID>& out,
                                    WeightedCopySetSelector& selector) {
  std::vector<StoreChainLink> cs(replication_.getReplicationFactor());
  copyset_size_t cs_size;
  bool chain = !test_disabling_chain_;

  auto result = selector.select(0, cs.data(), &cs_size, &chain, nullptr, rng_);

  if (result == CopySetSelector::Result::FAILED) {
    // Check that there aren't enough available nodes to pick a copyset.
    checkUnreplicatable({});
    return result;
  }

  EXPECT_NE(CopySetSelector::Result::PARTIAL, result);
  EXPECT_EQ(replication_.getReplicationFactor(), cs_size);

  // Fill `out`, check that all nodes are available, check `chain`.
  out.clear();
  bool expect_chain = !test_disabling_chain_;
  for (StoreChainLink cl : cs) {
    ShardID shard = cl.destination;
    auto status = deps_.getNodeStatus(shard.asNodeID());
    EXPECT_NE(NodeStatus::NOT_AVAILABLE, status);
    expect_chain &= status == NodeStatus::AVAILABLE;
    out.push_back(shard);
  }
  EXPECT_EQ(expect_chain, chain);

  checkCopyset(out);

  return result;
}

CopySetSelector::Result
WeightedCopySetSelectorTest::augment(std::vector<ShardID>& copyset,
                                     WeightedCopySetSelector& selector) {
  auto existing_copyset = copyset;
  size_t replication_factor = replication_.getReplicationFactor();
  copyset.resize(existing_copyset.size() + replication_factor);
  copyset_size_t new_size;
  auto result = selector.augment(
      copyset.data(), existing_copyset.size(), &new_size, rng_);

  if (result == CopySetSelector::Result::FAILED) {
    // Check that there aren't enough available nodes to pick a copyset.
    // Existing part of copyset is considered available.
    checkUnreplicatable(existing_copyset);
    return result;
  }

  EXPECT_NE(CopySetSelector::Result::PARTIAL, result);
  EXPECT_GE(new_size, replication_factor);

  copyset.resize(new_size);

  // New copyset must be a superset of the existing one.
  for (ShardID n : existing_copyset) {
    EXPECT_EQ(1, std::count(copyset.begin(), copyset.end(), n));
  }
  // copyset[replication_factor:] must consist of pre-existing nodes.
  for (size_t i = replication_factor; i < copyset.size(); ++i) {
    EXPECT_EQ(
        1,
        std::count(
            existing_copyset.begin(), existing_copyset.end(), copyset[i]));
  }

  // The first replication_factor elements must form a valid copyset.
  auto subset = copyset;
  subset.resize(replication_factor);
  checkCopyset(subset);

  return result;
}

void WeightedCopySetSelectorTest::checkCopyset(const std::vector<ShardID>& cs) {
  // Check replication.
  ++replication_checker_counter_;

  if (folly::Random::rand32() % 10 == 0) {
    // A sanity check for the test itself.
    // Only triggered with 10% probability for performance.
    ld_check(!replication_checker_->canReplicate(replication_checker_counter_));
  }

  for (ShardID shard : cs) {
    replication_checker_->setShardAttribute(
        shard, replication_checker_counter_);
  }
  EXPECT_TRUE(replication_checker_->canReplicate(replication_checker_counter_));

  // Check that there are no duplicates.
  auto sorted = cs;
  std::sort(sorted.begin(), sorted.end());
  EXPECT_EQ(replication_.getReplicationFactor(),
            std::unique(sorted.begin(), sorted.end()) - sorted.begin());
}

void WeightedCopySetSelectorTest::checkUnreplicatable(
    const std::vector<ShardID>& /*whitelist*/) {
  // Ask FailureDomainNodeSet whether it's replicatable.
  ++replication_checker_counter_;

  EXPECT_FALSE(
      replication_checker_->canReplicate(replication_checker_counter_));
}

void WeightedCopySetSelectorTest::testDistribution(
    std::pair<double, double> expected_locality_rate,
    std::map<ShardID, double> fake_weight_adjustments) {
  ld_check(!fake_weight_adjustments.empty());
  {
    SCOPED_TRACE("Without locality");
    testDistributionImpl(false, expected_locality_rate, {});
  }
  {
    SCOPED_TRACE("Without locality, null test");
    testDistributionImpl(
        false, expected_locality_rate, fake_weight_adjustments);
  }
  {
    SCOPED_TRACE("With locality");
    testDistributionImpl(true, expected_locality_rate, {});
  }
  {
    SCOPED_TRACE("With locality, null test");
    testDistributionImpl(true, expected_locality_rate, fake_weight_adjustments);
  }
}

void WeightedCopySetSelectorTest::testDistributionImpl(
    bool locality,
    std::pair<double, double> expected_locality_rate,
    std::map<ShardID, double> fake_weight_adjustments) {
  stats.reset();
  selectors_.clear();

  auto get_weight = [&](ShardID n) {
    double w = nodes_in_config_.at(n.node()).getWritableStorageCapacity();
    if (fake_weight_adjustments.count(n)) {
      w += fake_weight_adjustments[n];
    }
    if (!deps_.isAvailable(n)) {
      w = 0;
    }
    return w;
  };

  // Emulate the messy weight adjustments that copyset selector does to
  // compensate for rack-unaware nodeset selection. It'll all be better with
  // weight-aware nodeset selector.

  auto rf = replication_.getDistinctReplicationFactors();
  size_t replication_factor = replication_.getReplicationFactor();
  NodeLocationScope secondary_scope = rf[0].first;
  std::unordered_map<node_index_t, std::string> node_domain;
  std::map<std::string, double> domain_total_weight;
  std::map<std::string, double> domain_nodeset_weight;
  for (size_t i = 0; i < nodes_in_config_.size(); ++i) {
    ShardID shard(i, 0);
    const configuration::Node& node = nodes_in_config_.at(i);
    double w = get_weight(shard);
    std::string domain = node.location->getDomain(secondary_scope, i);
    node_domain[i] = domain;
    domain_total_weight[domain] += w;
    if (std::binary_search(
            nodeset_indices_.begin(), nodeset_indices_.end(), shard)) {
      domain_nodeset_weight[domain] += w;
    }
  }

  std::unordered_map<ShardID, double, ShardID::Hash> effective_weights;
  double sum_weights = 0;
  for (size_t i = 0; i < nodes_in_config_.size(); ++i) {
    ShardID shard(i, 0);
    const configuration::Node& node = nodes_in_config_.at(i);
    double w = get_weight(shard);
    std::string domain = node.location->getDomain(secondary_scope, i);
    if (w > 0 &&
        std::binary_search(
            nodeset_indices_.begin(), nodeset_indices_.end(), shard)) {
      effective_weights[shard] =
          w * domain_total_weight.at(domain) / domain_nodeset_weight.at(domain);
      sum_weights += effective_weights[shard];
    }
  }

  // Select a lot of copysets and see if distribution matches.
  // The statistical test is mostly the same as in SamplingTest; see comments
  // in SamplingTest for explanation.

  // Number of copysets to select.
  // Adjust to balance between speed and sensistivity.
  const size_t iters = 3000 * nodeset_indices_.size();
  // Allow deviations up to this big.
  // Adjust to balance between false positives and false negatives.
  const double threshold = 9 * sqrt(nodeset_indices_.size() + .0);

  // How many copysets were selected with my_node == sequencer_node.
  // If locality is true, we expect most of such copysets to contain
  // at least one node from local domain.
  size_t local_copyset_attempts = 0;
  // How many of them contained any nodes from local domain.
  size_t local_copyset_successes = 0;

  std::unordered_map<ShardID, size_t, ShardID::Hash> count;
  std::vector<ShardID> cs;
  for (size_t it = 0; it < iters; ++it) {
    logid_t log(1 + rng_() % 1000000000);
    node_index_t sequencer_node = getPrimarySequencerNode(log);
    node_index_t my_node;

    // With 20% probability pick my node ID randomly. Otherwise use
    // my_node = sequencer_node.
    if (rng_() * 1. / RNG::max() < .2) {
      my_node = rng_() % nodes_in_config_.size();
    } else {
      my_node = sequencer_node;
    }
    // When locality is disabled, sequencer node and my node don't matter.
    // Just use a single copyset selector.
    if (!locality) {
      log = logid_t(42);
      sequencer_node = my_node = -1;
    }

    WeightedCopySetSelector& selector =
        getSelector(log, sequencer_node, my_node, locality);

    ASSERT_EQ(CopySetSelector::Result::SUCCESS, select(cs, selector));
    for (ShardID n : cs) {
      ++count[n];
    }

    if (locality && my_node == sequencer_node &&
        deps_.isAvailable(ShardID(sequencer_node, 0))) {
      const std::string& sequencer_domain = node_domain.at(sequencer_node);
      bool has_local_copies = false;
      for (ShardID n : cs) {
        if (node_domain.at(n.node()) == sequencer_domain) {
          has_local_copies = true;
          break;
        }
      }
      ++local_copyset_attempts;
      if (has_local_copies) {
        ++local_copyset_successes;
      }
    }
  }

  // If the copyset selector was supposed to prefer local domain, check that
  // it indeed did.
  if (locality && fake_weight_adjustments.empty()) {
    if (local_copyset_attempts == 0) {
      ADD_FAILURE();
    } else {
      double ratio = 1. * local_copyset_successes / local_copyset_attempts;
      ld_debug("Locality rate: %lf (%lu/%lu)",
               ratio,
               local_copyset_successes,
               local_copyset_attempts);
      EXPECT_GE(ratio, expected_locality_rate.first);
      EXPECT_LE(ratio, expected_locality_rate.second);
    }
  }

  // Check that not too many copysets were reported as biased.
  int64_t num_biased_reported = stats.get().copyset_biased;
  if (fake_weight_adjustments.empty()) {
    // It's expected that biased copysets are reported in only two cases:
    //  (a) When copyset selector notices that a non-blacklisted node is
    //      unavailable and blacklists it.
    size_t max_biased_per_selector = deps_.countUnavailable(nodeset_indices_);
    //  (b) When copyset selector notices that detaching local domain causes
    //      bias and stops doing it. This can happen once initially, then once
    //      for every blacklisted node.
    if (locality) {
      max_biased_per_selector += max_biased_per_selector + 1;
    }
    EXPECT_LE(num_biased_reported, max_biased_per_selector * selectors_.size());
  }

  double deviation = 0;
  for (auto it : count) {
    if (effective_weights[it.first] == 0) {
      if (it.second != 0) {
        // Picked some zero-weight node. The distribution is clearly wrong.
        if (fake_weight_adjustments.empty()) {
          ADD_FAILURE() << it.first.toString() << ' ' << it.second;
        }
        deviation += 1e20;
      }
      continue;
    }
    double p = effective_weights[it.first] / sum_weights * replication_factor;
    ld_check(p <= 1. + 1e-13);
    if (p >= 1. - 1e-13) {
      // Node is so heavy that it should be picked in all copysets.
      // However, sometimes the copyset selector would pick it a few times
      // before realizing that it's doing something wrong and correcting it
      // (in particular, it would stop detaching local domain if it sees bias).
      // So, tolerate a small amount of copysets not having this node.
      // This can't be handled by our statistical test because the expression
      // for `deviation` below would divide by zero.
      double difference = 1. * (iters - it.second) / iters;
      // Note that we can't directly use num_biased_reported as tolerance
      // because it can be smaller than the actual number of biased copysets.
      double tolerance = num_biased_reported ? 1e-2 : 0.;
      if (difference > tolerance) {
        if (fake_weight_adjustments.empty()) {
          ADD_FAILURE() << it.first.toString() << ' ' << it.second << ' '
                        << difference << ' ' << tolerance;
        }
        deviation += 1e20;
      }
      continue;
    }
    double var = p * (1 - p);
    deviation += pow(it.second - p * iters, 2) / (var * iters);
  }
  ld_check(deviation >= 0);

  if (fake_weight_adjustments.empty()) {
    if (deviation > threshold) {
      ADD_FAILURE()
          << "The observed distribution seems different from the expected one. "
             "Deviation "
          << deviation << " > threshold " << threshold;
      ld_info("Expected distribution: %s, got: %s",
              toString(effective_weights).c_str(),
              toString(count).c_str());
    }
  } else {
    EXPECT_GT(deviation, threshold);
  }

  // It's expected that biased copysets are reported in only two cases:
  //  (a) When copyset selector notices that a non-blacklisted node is
  //      unavailable and blacklists it.
  //  (b) When copyset selector notices that detaching local domain causes bias
  //      and stops doing it.
  size_t num_selectors_approx = locality ? selectors_.size() : 1;
  // (a)
  size_t max_biased_per_selector = deps_.countUnavailable(nodeset_indices_);
  if (locality) {
    // (b)
    max_biased_per_selector += max_biased_per_selector + 1;
  }
  EXPECT_LE(
      num_biased_reported, max_biased_per_selector * num_selectors_approx);

  // If it's much smaller or much greater than 1, there's probably
  // an opportunity to make this test faster by decreasing number of iterations.
  ld_info("Relative deviation: %f", deviation / threshold);
}

TEST_F(WeightedCopySetSelectorTest, Basic) {
  addNodes("rg0.dc0.cl0.ro0.rk0", {2, 2});
  addNodes("rg0.dc0.cl0.ro0.rk1", {3, 3});
  replication_ = ReplicationProperty({{S::RACK, 2}, {S::NODE, 4}});

  std::vector<ShardID> cs;
  EXPECT_EQ(CopySetSelector::Result::SUCCESS, select(cs));
  std::sort(cs.begin(), cs.end());
  EXPECT_EQ(nodeset_indices_, cs);

  EXPECT_EQ(CopySetSelector::Result::SUCCESS, augment(cs = {N1, N3}));
  std::sort(cs.begin(), cs.end());
  EXPECT_EQ(nodeset_indices_, cs);
}

TEST_F(WeightedCopySetSelectorTest, ReplicationScope) {
  addNodes("rg0.dc0.cl0.ro0.rk0", {2, 2});
  addNodes("rg0.dc0.cl0.ro0.rk1", {3, 3});
  addNodes("rg0.dc0.cl0.ro0.rk2", {7, 7, 7});
  // Replicate across 3 racks.
  replication_ = ReplicationProperty({{S::RACK, 3}});

  std::vector<ShardID> cs;
  EXPECT_EQ(CopySetSelector::Result::SUCCESS, select(cs));
  EXPECT_EQ(CopySetSelector::Result::SUCCESS, augment(cs = {N1, N3}));

  deps_.setNotAvailableNodes({N0, N1});
  EXPECT_EQ(CopySetSelector::Result::FAILED, select(cs));
  EXPECT_EQ(CopySetSelector::Result::SUCCESS, augment(cs = {N1, N3}));
  EXPECT_EQ(CopySetSelector::Result::FAILED, augment(cs = {N2, N4}));
  EXPECT_EQ(CopySetSelector::Result::FAILED, augment(cs = {N2}));
  EXPECT_EQ(CopySetSelector::Result::FAILED, augment(cs = {}));

  // Test stats.
  EXPECT_EQ(1, stats.get().copyset_selected);
  EXPECT_EQ(1, stats.get().copyset_biased);
  EXPECT_EQ(1, stats.get().copyset_selection_failed);
  EXPECT_EQ(1, stats.get().copyset_selection_attempts);
  EXPECT_EQ(2, stats.get().copyset_selected_rebuilding);
  EXPECT_EQ(0, stats.get().copyset_biased_rebuilding);
  EXPECT_EQ(3, stats.get().copyset_selection_failed_rebuilding);
  EXPECT_EQ(2, stats.get().copyset_selection_attempts_rebuilding);
}

TEST_F(WeightedCopySetSelectorTest, DistributionFlat) {
  addNodes("rg.dc.cl.ro.rk", {3, 2, 2, 2}, {1, 2, 2, 2});
  replication_ = ReplicationProperty({{S::NODE, 3}});
  // Check distribution. Also check that our check would fail if the actual
  // distribution was uniform instead of {3,2,2,2}.
  testDistribution({1, 1}, {{N0, -1}});
}

TEST_F(WeightedCopySetSelectorTest, DistributionCrossRack) {
  addNodes("rg.dc.cl.ro.rk0", {1, 1, 1, 1, 1, 1}, 3);
  addNodes("rg.dc.cl.ro.rk1", {1, 1, 1});
  addNodes("rg.dc.cl.ro.rk2", {1, 1, 1});

  replication_ = ReplicationProperty({{S::RACK, 2}, {S::NODE, 3}});
  testDistribution({1, 1}, {{N3, -1}, {N4, -1}, {N5, -1}});
}

TEST_F(WeightedCopySetSelectorTest, DistributionCrossRack2) {
  addNodes("rg.dc.cl.ro.rk0", {1, 1}, {2, 2});
  addNodes("rg.dc.cl.ro.rk1", {2, 2}, {1, 1});
  addNodes("rg.dc.cl.ro.rk2", {2, 2}, {1, 1});
  addNodes("rg.dc.cl.ro.rk3", {2, 2}, {1, 1});

  replication_ = ReplicationProperty({{S::RACK, 2}, {S::NODE, 3}});
  testDistribution({1, 1}, {{N2, -1}});
}

TEST_F(WeightedCopySetSelectorTest, DistributionDifferentWeightsInSameRack) {
  addNodes("rg.dc.cl.ro.rk0", {4, 3, 3, 2}, 3);
  addNodes("rg.dc.cl.ro.rk1", {3, 2, 2});
  addNodes("rg.dc.cl.ro.rk2", {3, 2, 2});

  replication_ = ReplicationProperty({{S::RACK, 2}, {S::NODE, 3}});
  testDistribution({1, 1}, {{N1, -1}});
}

TEST_F(WeightedCopySetSelectorTest, DistributionCrossRegion) {
  addNodes("rg0.dc.cl.ro.rk0", {4, 3});
  addNodes("rg0.dc.cl.ro.rk1", {3, 3});
  addNodes("rg0.dc.cl.ro.rk2", {3, 2});

  addNodes("rg1.dc.cl.ro.rk0", {7});
  addNodes("rg1.dc.cl.ro.rk1", {4});
  addNodes("rg1.dc.cl.ro.rk2", {2, 1});

  addNodes("rg2.dc.cl.ro.rk0", {3, 3, 2});
  addNodes("rg2.dc.cl.ro.rk1", {7});
  addNodes("rg2.dc.cl.ro.rk2", {5, 1});
  addNodes("rg2.dc.cl.ro.rk3", {1});

  replication_ = ReplicationProperty({{S::REGION, 2}, {S::RACK, 3}});
  testDistribution({.8, .9}, {{N16, 0.5}});
}

TEST_F(WeightedCopySetSelectorTest, DistributionBlacklisting) {
  addNodes("rg.dc.cl.ro.rk0", {1, 1, 1, 1});
  addNodes("rg.dc.cl.ro.rk1", {1, 1, 1});
  addNodes("rg.dc.cl.ro.rk2", {1, 1, 1});
  addNodes("rg.dc.cl.ro.rk3", {1, 1, 1});
  replication_ = ReplicationProperty({{S::RACK, 2}, {S::NODE, 3}});

  deps_.setNotAvailableNodes({N7, N8, N9, N12});
  testDistribution({.9, 1}, {{N0, .5}});

  // A shameless plug: check that if we blacklist too much we can't replicate.
  deps_.setNotAvailableNodes(
      {N1, N2, N3, N5, N6, N7, N8, N9, N10, N11, N12}); // all except 0 and 4
  std::vector<ShardID> cs;
  EXPECT_EQ(CopySetSelector::Result::FAILED, select(cs));
}

TEST_F(WeightedCopySetSelectorTest, DistributionSingleCopy) {
  addNodes("rg.dc.cl.ro.rk0", {4, 3}, {3, 3});
  addNodes("rg.dc.cl.ro.rk1", {2}, std::vector<double>({10}));
  addNodes("rg.dc.cl.ro.rk2", {5, 5}, {2, 9}, 1);

  replication_ = ReplicationProperty({{S::NODE, 1}});
  testDistribution({.2, .3}, {{N3, -2}});
}

TEST_F(WeightedCopySetSelectorTest, DistributionSingleCopyWeirdReplication) {
  addNodes("rg.dc.cl.ro.rk0", {4, 3}, {3, 3});
  addNodes("rg.dc.cl.ro.rk1", {2}, std::vector<double>({10}));
  addNodes("rg.dc.cl.ro.rk2", {5, 5}, {2, 9}, 1);

  // Same as {S::NODE, 1}, except that rack weights are rescaled so that the
  // weight of each rack is equal to its weight in config, not in nodeset.
  replication_ = ReplicationProperty({{S::RACK, 1}});

  // Unlike previous test, even the weight of the node that's not in the
  // nodeset makes a difference.
  testDistribution({.4, .5}, {{N4, -2}});
}

TEST_F(WeightedCopySetSelectorTest, DistributionDisagg) {
  // Two sequencer-only racks, 3 storage-only racks.
  addNodes("rg.dc.cl.ro.rk_seq0", {0, 0, 0}, {1, 1, 2});
  addNodes("rg.dc.cl.ro.rk_seq1", {0, 0}, {1, 0});
  addNodes("rg.dc.cl.ro.rk0", {2, 3, 2}, {0, 0, 0});
  addNodes("rg.dc.cl.ro.rk1", {3, 3}, {0, 0});
  addNodes("rg.dc.cl.ro.rk2", {3, 2, 1, 2}, {0, 0, 0, 0}, 3);

  replication_ = ReplicationProperty({{S::RACK, 2}, {S::NODE, 3}});
  // No locality possible.
  testDistribution({0, 0}, {{N6, -2}});
}

TEST_F(WeightedCopySetSelectorTest, DistributionPartialDisagg) {
  // Sequencer-only rack.
  addNodes("rg.dc.cl.ro.rk_seq", {0, 0}, {1, 1}, 1);
  // Normal rack.
  addNodes("rg.dc.cl.ro.rk0", {4, 3, 3, 2}, 3);
  // Storage-only rack.
  addNodes("rg.dc.cl.ro.rk1", {3, 2, 2}, {0, 0, 0});
  // Kinda mixed rack.
  addNodes("rg.dc.cl.ro.rk2", {3, 3, 2}, {0, 0, 1}, 2);

  replication_ = ReplicationProperty({{S::RACK, 2}, {S::NODE, 3}});
  testDistribution({.8, .9}, {{N4, -1}});
}

TEST_F(WeightedCopySetSelectorTest, Unblacklisting) {
  addNodes("rg.dc.cl.ro.rk0", {1, 1, 1});
  replication_ = ReplicationProperty({{S::NODE, 2}});
  std::vector<ShardID> cs;

  deps_.setNotAvailableNodes({N0});
  EXPECT_EQ(CopySetSelector::Result::SUCCESS, select(cs));
  std::sort(cs.begin(), cs.end());
  EXPECT_EQ(std::vector<ShardID>({N1, N2}), cs);
  EXPECT_EQ(CopySetSelector::Result::SUCCESS, augment(cs = {N2}));
  EXPECT_EQ(std::vector<ShardID>({N2, N1}), cs);

  deps_.setNotAvailableNodes({N1});
  EXPECT_EQ(CopySetSelector::Result::SUCCESS, select(cs));
  std::sort(cs.begin(), cs.end());
  EXPECT_EQ(std::vector<ShardID>({N0, N2}), cs);
  EXPECT_EQ(CopySetSelector::Result::SUCCESS, augment(cs = {N2}));
  EXPECT_EQ(std::vector<ShardID>({N2, N0}), cs);
}

TEST_F(WeightedCopySetSelectorTest, Augment) {
  addNodes("rg.dc.cl.ro.rk0", {1, 1, 1, 1});
  addNodes("rg.dc.cl.ro.rk1", {1, 1, 1});
  addNodes("rg.dc.cl.ro.rk2", {1, 1, 1});
  addNodes("rg.dc.cl.ro.rk3", {1, 1, 1});
  replication_ = ReplicationProperty({{S::RACK, 3}, {S::NODE, 4}});

  std::vector<ShardID> cs;
  EXPECT_EQ(CopySetSelector::Result::SUCCESS, augment(cs = {}));

  // Valid copysets are left unchanged...
  EXPECT_EQ(CopySetSelector::Result::SUCCESS, augment(cs = {N0, N4, N7, N10}));
  EXPECT_EQ(std::vector<ShardID>({N0, N4, N7, N10}), cs);
  EXPECT_EQ(CopySetSelector::Result::SUCCESS,
            augment(cs = {N0, N4, N7, N10, N1, N6}));
  EXPECT_EQ(std::vector<ShardID>({N0, N4, N7, N10, N1, N6}), cs);
  EXPECT_EQ(CopySetSelector::Result::SUCCESS, augment(cs = {N0, N4, N7, N6}));
  EXPECT_EQ(std::vector<ShardID>({N0, N4, N7, N6}), cs);

  // ... unless the first replication_factor nodes are not a valid copyset;
  // then the copyset is reorderd.
  EXPECT_EQ(
      CopySetSelector::Result::SUCCESS, augment(cs = {N0, N1, N2, N3, N4, N7}));
  EXPECT_EQ(std::vector<ShardID>({N0, N1, N4, N7, N2, N3}), cs);

  // All in one rack. Need 2 more copies.
  EXPECT_EQ(CopySetSelector::Result::SUCCESS, augment(cs = {N0, N1, N2, N3}));
  EXPECT_EQ(6, cs.size());
  EXPECT_EQ(N0, cs[0]);
  EXPECT_EQ(N1, cs[1]);
  EXPECT_EQ(N2, cs[4]);
  EXPECT_EQ(N3, cs[5]);

  // Two racks. Need 1 more copy.
  EXPECT_EQ(CopySetSelector::Result::SUCCESS, augment(cs = {N0, N1, N4, N5}));
  EXPECT_EQ(5, cs.size());
  EXPECT_EQ(N0, cs[0]);
  EXPECT_EQ(N1, cs[1]);
  EXPECT_EQ(N4, cs[2]);
  EXPECT_EQ(N5, cs[4]);
}

TEST_F(WeightedCopySetSelectorTest, NotEnoughOutsideNodes) {
  addNodes("rg.dc.cl.ro.rk0", {1});
  addNodes("rg.dc.cl.ro.rk1", {1});
  addNodes("rg.dc.cl.ro.rk2", {1});
  replication_ = ReplicationProperty({{S::RACK, 3}});

  auto& selector = getSelector(
      LOG_ID, /* sequencer_node */ -1, /* my_node */ 1, /* locality */ true);

  std::vector<ShardID> cs;
  EXPECT_EQ(CopySetSelector::Result::SUCCESS, select(cs, selector));
  deps_.setNotAvailableNodes({N0});
  for (int i = 0; i < 15; ++i) {
    // select() used to crash with probability 1/2 here.
    EXPECT_EQ(CopySetSelector::Result::FAILED, select(cs, selector));
  }
  deps_.setNotAvailableNodes({N0, N2});
  EXPECT_EQ(CopySetSelector::Result::FAILED, select(cs, selector));
  EXPECT_EQ(CopySetSelector::Result::FAILED, select(cs, selector));
  deps_.setNotAvailableNodes({});
  EXPECT_EQ(CopySetSelector::Result::SUCCESS, select(cs, selector));
}

TEST_F(WeightedCopySetSelectorTest, NotEnoughLocalNodes) {
  addNodes("rg.dc.cl.ro.rk0", {1});
  addNodes("rg.dc.cl.ro.rk1", {1});
  addNodes("rg.dc.cl.ro.rk2", {1});
  replication_ = ReplicationProperty({{S::RACK, 1}, {S::NODE, 3}});

  auto& selector = getSelector(
      LOG_ID, /* sequencer_node */ -1, /* my_node */ 1, /* locality */ true);

  std::vector<ShardID> cs;
  EXPECT_EQ(CopySetSelector::Result::SUCCESS, select(cs, selector));
}

TEST_F(WeightedCopySetSelectorTest, DisablingChainSending) {
  test_disabling_chain_ = true;
  addNodes("rg.dc.cl.ro.rk0", {1, 1, 1, 1, 1});
  replication_ = ReplicationProperty({{S::NODE, 3}});
  auto& selector = getSelector(LOG_ID);
  std::vector<ShardID> cs;
  EXPECT_EQ(CopySetSelector::Result::SUCCESS, select(cs, selector));
}
