/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <gtest/gtest.h>

#include <folly/Memory.h>
#include <queue>
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/LibeventTimer.h"
#include "logdevice/common/NodeSetAccessor.h"

#include "logdevice/common/configuration/LocalLogsConfig.h"
#include "logdevice/common/test/MockBackoffTimer.h"
#include "logdevice/common/test/NodeSetTestUtil.h"
#include "logdevice/common/util.h"

#include "logdevice/common/debug.h"

namespace facebook { namespace logdevice {

using namespace NodeSetTestUtil;

namespace {

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

class TestCopySetSelector : public CopySetSelector {
 public:
  TestCopySetSelector(const StorageSet& nodeset,
                      const std::shared_ptr<Configuration> cfg,
                      copyset_size_t replication,
                      NodeLocationScope sync_replication_scope)
      : failure_domain_(
            nodeset,
            cfg->serverConfig(),
            ReplicationProperty(replication, sync_replication_scope)),
        replication_(replication) {
    ld_check(replication_ > 0 && replication_ <= COPYSET_SIZE_MAX);
  }

  Result select(copyset_size_t /* unused */,
                StoreChainLink[] /* unused */,
                copyset_size_t* /* unused */,
                bool* /* unused */,
                State* /* unused */,
                RNG&, /* unused */
                bool /* retry, unused */) const override {
    return Result::FAILED;
  }

  Result augment(ShardID inout_copyset[],
                 copyset_size_t existing_copyset_size,
                 copyset_size_t* out_full_size,
                 RNG&,
                 bool /* retry, unused */) const override {
    ld_check(inout_copyset != nullptr);
    ld_check(out_full_size != nullptr);

    if (result_ == Result::FAILED) {
      std::fill(inout_copyset,
                inout_copyset + existing_copyset_size + replication_,
                ShardID());
      *out_full_size = std::numeric_limits<copyset_size_t>::max();
      return result_;
    }

    ld_check(copyset_.size() >= replication_);
    for (size_t i = 0; i < existing_copyset_size; ++i) {
      EXPECT_EQ(
          1, std::count(copyset_.begin(), copyset_.end(), inout_copyset[i]));
    }
    for (size_t i = replication_; i < copyset_.size(); ++i) {
      EXPECT_EQ(1,
                std::count(inout_copyset,
                           inout_copyset + existing_copyset_size,
                           copyset_[i]));
    }

    std::copy(copyset_.begin(), copyset_.end(), inout_copyset);
    *out_full_size = copyset_.size();

    checkCanReplicate(inout_copyset, replication_);

    return Result::SUCCESS;
  }

  Result augment(StoreChainLink[] /* unused */,
                 copyset_size_t /* unused */,
                 copyset_size_t* /* unused */,
                 bool /* unused */,
                 bool* /* unused */,
                 RNG& /* unused */,
                 bool /* unused */) const override {
    throw std::runtime_error("unimplemented");
  }

  copyset_size_t getReplicationFactor() const override {
    return replication_;
  }

  void setResult(StorageSet copyset, Result result = Result::SUCCESS) {
    copyset_ = std::move(copyset);
    result_ = result;
  }

 private:
  mutable FailureDomainNodeSet<bool> failure_domain_;
  const size_t replication_;
  StorageSet copyset_;
  Result result_{Result::FAILED};

  void checkCanReplicate(const ShardID copyset[],
                         copyset_size_t copyset_size) const {
    failure_domain_.resetAttributeCounters();
    for (size_t i = 0; i < copyset_size; ++i) {
      failure_domain_.setShardAttribute(copyset[i], true);
    }
    ASSERT_TRUE(failure_domain_.canReplicate(true));
  }
};

} // namespace

class NodeSetAccessorTest : public ::testing::Test {
 public:
  const logid_t LOG_ID{2};

  copyset_size_t replication_{3};
  copyset_size_t extras_{1};
  NodeLocationScope sync_replication_scope_{NodeLocationScope::NODE};
  StorageSetAccessor::Property property_{
      StorageSetAccessor::Property::FMAJORITY};
  StorageSet nodeset_{N0, N1, N2, N3, N4, N5, N6, N7};
  StorageSet required_nodes_;

  bool allow_success_if_all_accessed_{false};
  bool require_strict_waves_{false};

  std::chrono::milliseconds timeout_{std::chrono::milliseconds::zero()};
  bool job_timer_active_{false};

  // cluster config
  std::shared_ptr<Configuration> config_;

  std::unique_ptr<CopySetSelector> copyset_selector_;

  std::shared_ptr<const Configuration> getConfig() const {
    return config_;
  }

  void triggerJobTimer() {
    accessor_->onJobTimedout();
  }

  std::unique_ptr<StorageSetAccessor> accessor_;

  using SendResult = StorageSetAccessor::SendResult;
  using AccessResult = StorageSetAccessor::AccessResult;

  // control the result of sending to a node
  std::map<ShardID, StorageSetAccessor::SendResult> send_result_;

  std::set<ShardID> wave_shards_;
  Status final_status_{E::UNKNOWN};

  StorageSetAccessor::WaveInfo wave_info_;

  void setUp();

  StorageSet getNotSentNodes() const {
    return accessor_->getShardsInState(
        StorageSetAccessor::ShardState::NOT_SENT);
  }

  MockBackoffTimer* getWaveTimer() {
    return static_cast<MockBackoffTimer*>(accessor_->wave_timer_.get());
  }

  TestCopySetSelector* getCopySetSelector() {
    return static_cast<TestCopySetSelector*>(
        copyset_selector_ != nullptr ? copyset_selector_.get()
                                     : accessor_->copyset_selector_.get());
  }

  NodeSetState* getNodeSetState() {
    return accessor_->nodeset_state_.get();
  }
};

class MockedStorageSetAccessor : public StorageSetAccessor {
 public:
  explicit MockedStorageSetAccessor(NodeSetAccessorTest* test)
      : StorageSetAccessor(test->LOG_ID,
                           test->nodeset_,
                           test->config_->serverConfig(),
                           ReplicationProperty(test->replication_,
                                               test->sync_replication_scope_),
                           [test](ShardID shard, const WaveInfo& info) {
                             auto it = test->send_result_.find(shard);
                             ld_check(it != test->send_result_.end());
                             test->wave_shards_.insert(shard);
                             test->wave_info_ = info;
                             EXPECT_GT(info.wave, 0);
                             EXPECT_FALSE(info.wave_shards.empty());
                             EXPECT_GE(info.wave_shards.size(), info.offset);
                             EXPECT_EQ(shard, info.wave_shards[info.offset]);
                             return it->second;
                           },
                           [test](Status st) { test->final_status_ = st; },
                           test->property_,
                           test->timeout_),
        test_(test) {}

  std::unique_ptr<LibeventTimer>
  createJobTimer(std::function<void()> /*callback*/) override {
    return nullptr;
  }

  void cancelJobTimer() override {
    test_->job_timer_active_ = false;
  }
  void activateJobTimer() override {
    test_->job_timer_active_ = true;
  }

  std::unique_ptr<BackoffTimer>
  createWaveTimer(std::function<void()> callback) override {
    auto timer = std::make_unique<MockBackoffTimer>();
    timer->setCallback(callback);
    return std::move(timer);
  }

  std::unique_ptr<CopySetSelector>
  createCopySetSelector(logid_t,
                        const EpochMetaData&,
                        std::shared_ptr<NodeSetState>,
                        const std::shared_ptr<ServerConfig>&) override {
    // transfer the selector to the accessor instance
    return std::move(test_->copyset_selector_);
  }

  ShardAuthoritativeStatusMap& getShardAuthoritativeStatusMap() override {
    return empty_map_;
  }

 private:
  NodeSetAccessorTest* const test_;
  ShardAuthoritativeStatusMap empty_map_;
};

void NodeSetAccessorTest::setUp() {
  Configuration::Nodes nodes;
  addNodes(&nodes, 1, 1, {}, "rg0.dc0.cl0.ro0.rk0", 1);
  addNodes(&nodes, 1, 1, {}, "rg1.dc0.cl0.ro0.rk0", 1);
  addNodes(&nodes, 2, 1, {}, "rg1.dc0.cl0.ro0.rk1", 2);
  addNodes(&nodes, 1, 1, {}, "rg1.dc0.cl0.ro0.rk2", 1);
  addNodes(&nodes, 1, 1, {}, "rg1.dc0.cl0..", 1);
  addNodes(&nodes, 1, 1, {}, "rg2.dc0.cl0.ro0.rk0", 1);
  addNodes(&nodes, 1, 1, {}, "rg2.dc0.cl0.ro0.rk1", 1);
  addNodes(&nodes, 1, 1, {}, "....", 1);

  Configuration::NodesConfig nodes_config;
  const size_t nodeset_size = nodes.size();
  nodes_config.setNodes(std::move(nodes));

  auto logs_config = std::make_unique<configuration::LocalLogsConfig>();
  addLog(logs_config.get(), LOG_ID, replication_, extras_, nodeset_size, {});
  config_ = std::make_shared<Configuration>(
      ServerConfig::fromData("nodeset_accessor_test", std::move(nodes_config)),
      std::move(logs_config));

  accessor_.reset(new MockedStorageSetAccessor(this));
  accessor_->setExtras(extras_);

  if (!required_nodes_.empty()) {
    accessor_->setRequiredShards(required_nodes_);
  }

  if (allow_success_if_all_accessed_) {
    accessor_->successIfAllShardsAccessed();
  }

  if (require_strict_waves_) {
    accessor_->requireStrictWaves();
  }

  for (auto idx : nodeset_) {
    send_result_[idx] = StorageSetAccessor::SendResult::SUCCESS;
  }

  copyset_selector_ = std::unique_ptr<CopySetSelector>(new TestCopySetSelector(
      nodeset_, config_, replication_, sync_replication_scope_));
}

#define verifyWave(...)                                                 \
  do {                                                                  \
    ASSERT_EQ(std::set<ShardID>({__VA_ARGS__}), wave_shards_);          \
    ASSERT_EQ(wave_shards_.size(), wave_info_.wave_shards.size());      \
    ASSERT_TRUE(std::all_of(                                            \
        wave_info_.wave_shards.begin(),                                 \
        wave_info_.wave_shards.end(),                                   \
        [&](ShardID shard) { return wave_shards_.count(shard) > 0; })); \
    wave_shards_.clear();                                               \
  } while (0)

#define accessNodes(result, ...)                   \
  do {                                             \
    for (auto shard : StorageSet{__VA_ARGS__}) {   \
      accessor_->onShardAccessed(shard, (result)); \
    }                                              \
  } while (0)

#define accessNodesWithWave(wave, result, ...)             \
  do {                                                     \
    for (auto shard : StorageSet{__VA_ARGS__}) {           \
      accessor_->onShardAccessed(shard, (result), (wave)); \
    }                                                      \
  } while (0)

#define setSendResult(result, ...)                             \
  do {                                                         \
    for (auto shard : StorageSet{__VA_ARGS__}) {               \
      ASSERT_NE(send_result_.end(), send_result_.find(shard)); \
      send_result_[shard] = (result);                          \
    }                                                          \
  } while (0)

TEST_F(NodeSetAccessorTest, BasicFmajority) {
  replication_ = 3;
  extras_ = 1;
  sync_replication_scope_ = NodeLocationScope::REGION;
  property_ = StorageSetAccessor::Property::FMAJORITY;
  nodeset_ = StorageSet{N0, N1, N2, N3, N4, N5, N6, N7};

  setUp();
  accessor_->start();
  // the first wave should send to all nodes
  verifyWave(N0, N1, N2, N3, N4, N5, N6, N7);
  ASSERT_TRUE(getWaveTimer()->isActive());
  accessNodes(AccessResult::SUCCESS, N0, N1, N2, N3, N4);
  ASSERT_EQ(E::UNKNOWN, final_status_);
  accessNodes(AccessResult::SUCCESS, N5);
  // we got all regions except one
  ASSERT_EQ(E::OK, final_status_);
}

TEST_F(NodeSetAccessorTest, JobTimeout) {
  timeout_ = std::chrono::milliseconds{2000};
  setUp();
  accessor_->start();
  ASSERT_TRUE(job_timer_active_);
  ASSERT_EQ(E::UNKNOWN, final_status_);
  triggerJobTimer();
  ASSERT_EQ(E::TIMEDOUT, final_status_);
}

TEST_F(NodeSetAccessorTest, Failure) {
  replication_ = 3;
  extras_ = 1;
  sync_replication_scope_ = NodeLocationScope::NODE;
  property_ = StorageSetAccessor::Property::FMAJORITY;
  nodeset_ = StorageSet{N0, N1, N2, N3, N4, N5, N6, N7};

  setUp();
  accessor_->start();
  verifyWave(N0, N1, N2, N3, N4, N5, N6, N7);
  accessNodes(AccessResult::PERMANENT_ERROR, N1, N2);
  ASSERT_EQ(E::UNKNOWN, final_status_);
  accessNodes(AccessResult::PERMANENT_ERROR, N5);
  ASSERT_EQ(E::FAILED, final_status_);
}

TEST_F(NodeSetAccessorTest, SendFailureFirstWave) {
  replication_ = 3;
  extras_ = 1;
  sync_replication_scope_ = NodeLocationScope::REGION;
  property_ = StorageSetAccessor::Property::FMAJORITY;
  nodeset_ = StorageSet{N0, N1, N2, N3, N4, N5, N6, N7};

  setUp();
  setSendResult(SendResult::PERMANENT_ERROR, N0, N5, N7);
  accessor_->start();

  // should immediately fail since enough nodes failed to send permanently
  ASSERT_EQ(E::FAILED, final_status_);
  ASSERT_EQ(nullptr, getWaveTimer());
}

TEST_F(NodeSetAccessorTest, FailureBeforeFirstWave) {
  replication_ = 3;
  extras_ = 1;
  sync_replication_scope_ = NodeLocationScope::REGION;
  property_ = StorageSetAccessor::Property::REPLICATION;
  // all in the same region
  nodeset_ = StorageSet{N1, N2, N3, N4, N5};
  setUp();
  accessor_->start();
  // should immediately fail since not enough nodes
  ASSERT_EQ(E::FAILED, final_status_);
}

TEST_F(NodeSetAccessorTest, RetryNewWave) {
  replication_ = 3;
  extras_ = 1;
  sync_replication_scope_ = NodeLocationScope::NODE;
  property_ = StorageSetAccessor::Property::FMAJORITY;
  nodeset_ = StorageSet{N0, N3, N4, N5, N6, N7};

  setUp();
  // node 0
  setSendResult(SendResult::PERMANENT_ERROR, N0);
  accessor_->start();
  // should send to the rest of the nodes
  verifyWave(N0, N3, N4, N5, N6, N7);
  accessNodes(AccessResult::SUCCESS, N3, N6);
  accessNodes(AccessResult::TRANSIENT_ERROR, N4);
  accessNodes(AccessResult::PERMANENT_ERROR, N7);
  ASSERT_EQ(E::UNKNOWN, final_status_);

  // trigger the next wave
  getWaveTimer()->trigger();
  verifyWave(N4, N5);
  accessNodes(AccessResult::SUCCESS, N4);
  ASSERT_EQ(E::UNKNOWN, final_status_);
  accessNodes(AccessResult::SUCCESS, N5);
  ASSERT_EQ(E::OK, final_status_);
}

TEST_F(NodeSetAccessorTest, BasicReplication) {
  replication_ = 3;
  extras_ = 1;
  sync_replication_scope_ = NodeLocationScope::REGION;
  property_ = StorageSetAccessor::Property::REPLICATION;
  nodeset_ = StorageSet{N0, N1, N2, N3, N4, N5, N6, N7, N8};

  setUp();

  getCopySetSelector()->setResult({N0, N1, N2});
  accessor_->start();
  accessNodes(AccessResult::SUCCESS, N0, N1, N2);

  ASSERT_EQ(E::OK, final_status_);
}

TEST_F(NodeSetAccessorTest, BlacklistNodes) {
  replication_ = 3;
  extras_ = 0;
  sync_replication_scope_ = NodeLocationScope::REGION;
  property_ = StorageSetAccessor::Property::REPLICATION;
  nodeset_ = StorageSet{N0, N1, N2, N3, N4, N5, N6, N7};

  setUp();

  setSendResult(SendResult::PERMANENT_ERROR, N0);
  getCopySetSelector()->setResult({N0, N1, N2});
  accessor_->start();

  accessNodes(AccessResult::PERMANENT_ERROR, N1);
  // 0 and 1 should be disabled on the NodeSet
  ASSERT_NE(std::chrono::steady_clock::time_point::min(),
            getNodeSetState()->getNotAvailableUntil(N0));
  ASSERT_NE(std::chrono::steady_clock::time_point::min(),
            getNodeSetState()->getNotAvailableUntil(N1));
  ASSERT_EQ(std::chrono::steady_clock::time_point::min(),
            getNodeSetState()->getNotAvailableUntil(N5));

  accessNodes(AccessResult::SUCCESS, N2);
  ASSERT_EQ(E::UNKNOWN, final_status_);

  // trigger the next wave
  getCopySetSelector()->setResult({N2, N5, N7});
  getWaveTimer()->trigger();
  accessNodes(AccessResult::SUCCESS, N5, N7);
  ASSERT_EQ(E::OK, final_status_);
}

TEST_F(NodeSetAccessorTest, ExtraOnSelectionFail) {
  replication_ = 3;
  extras_ = 0;
  sync_replication_scope_ = NodeLocationScope::REGION;
  property_ = StorageSetAccessor::Property::REPLICATION;
  nodeset_ = StorageSet{N0, N1, N2, N3, N4, N5, N6, N7};

  setUp();
  getCopySetSelector()->setResult({}, CopySetSelector::Result::FAILED);
  accessor_->start();
  ASSERT_EQ(1, wave_shards_.size());
  wave_shards_.clear();
}

TEST_F(NodeSetAccessorTest, ExtraPreference) {
  replication_ = 2;
  extras_ = 2;
  sync_replication_scope_ = NodeLocationScope::NODE;
  property_ = StorageSetAccessor::Property::REPLICATION;
  nodeset_ = StorageSet{N1, N2, N3, N4, N5};

  setUp();

  getCopySetSelector()->setResult({N1, N2});
  accessor_->start();
  ASSERT_EQ(4, wave_shards_.size());
  wave_shards_.clear();

  auto not_sent = getNotSentNodes();
  ASSERT_EQ(1, not_sent.size());

  accessNodes(AccessResult::TRANSIENT_ERROR, N2);
  getCopySetSelector()->setResult({}, CopySetSelector::Result::FAILED);
  getWaveTimer()->trigger();

  // this wave must always contain: 1) the node that is not sent yet, and
  // 2) the node had transient error
  verifyWave(not_sent[0], N2);
}

TEST_F(NodeSetAccessorTest, AnyNode) {
  replication_ = 3;
  extras_ = 1;
  sync_replication_scope_ = NodeLocationScope::REGION;
  property_ = StorageSetAccessor::Property::ANY;
  nodeset_ = StorageSet{N0, N1, N2, N3, N4, N5, N6, N7};

  setUp();
  setSendResult(SendResult::PERMANENT_ERROR, N0);
  getCopySetSelector()->setResult({N0, N1, N2});
  accessor_->start();
  ASSERT_EQ(4, wave_shards_.size());
  accessNodes(AccessResult::PERMANENT_ERROR, N1);
  accessNodes(AccessResult::SUCCESS, N2);
  ASSERT_EQ(E::OK, final_status_);
}

TEST_F(NodeSetAccessorTest, AnyNodeFailed) {
  replication_ = 3;
  extras_ = 10;
  sync_replication_scope_ = NodeLocationScope::REGION;
  property_ = StorageSetAccessor::Property::ANY;
  nodeset_ = StorageSet{N0, N1, N2, N3, N4, N5, N6, N7};

  setUp();
  setSendResult(SendResult::PERMANENT_ERROR, N0);
  setSendResult(SendResult::PERMANENT_ERROR, N1);
  setSendResult(SendResult::PERMANENT_ERROR, N2);
  getCopySetSelector()->setResult({N2, N4, N7});
  accessor_->start();
  ASSERT_EQ(8, wave_shards_.size());
  wave_shards_.clear();
  accessNodes(AccessResult::PERMANENT_ERROR, N3);
  accessNodes(AccessResult::PERMANENT_ERROR, N4);
  accessNodes(AccessResult::PERMANENT_ERROR, N6);
  accessNodes(AccessResult::PERMANENT_ERROR, N7);
  ASSERT_EQ(E::UNKNOWN, final_status_);
  accessNodes(AccessResult::TRANSIENT_ERROR, N5);
  ASSERT_EQ(E::UNKNOWN, final_status_);
  getCopySetSelector()->setResult({}, CopySetSelector::Result::FAILED);
  getWaveTimer()->trigger();
  verifyWave(N5);
  accessNodes(AccessResult::PERMANENT_ERROR, N5);
  ASSERT_EQ(E::FAILED, final_status_);
}

TEST_F(NodeSetAccessorTest, BasicAuthoritativeStatusAwareFmajority) {
  replication_ = 3;
  extras_ = 1;
  sync_replication_scope_ = NodeLocationScope::REGION;
  property_ = StorageSetAccessor::Property::FMAJORITY;
  nodeset_ = StorageSet{N0, N1, N2, N3, N4, N5, N6, N7};

  setUp();
  accessor_->start();
  // the first wave should send to all nodes
  verifyWave(N0, N1, N2, N3, N4, N5, N6, N7);
  ASSERT_TRUE(getWaveTimer()->isActive());
  accessNodes(AccessResult::SUCCESS, N0, N1, N2, N3, N4);
  ASSERT_EQ(E::UNKNOWN, final_status_);
  // node 5 become authoritative empty
  accessor_->setShardAuthoritativeStatus(
      N5, AuthoritativeStatus::AUTHORITATIVE_EMPTY);
  // by ignoring node 5 we got all regions except one
  ASSERT_EQ(E::OK, final_status_);
}

TEST_F(NodeSetAccessorTest, NonAuthoritativeFmajority) {
  replication_ = 3;
  extras_ = 1;
  sync_replication_scope_ = NodeLocationScope::REGION;
  property_ = StorageSetAccessor::Property::FMAJORITY;
  nodeset_ = StorageSet{N0, N1, N2, N3, N4, N5, N6, N7};

  setUp();
  accessor_->start();
  // the first wave should send to all nodes
  verifyWave(N0, N1, N2, N3, N4, N5, N6, N7);
  ASSERT_TRUE(getWaveTimer()->isActive());
  accessNodes(AccessResult::SUCCESS, N0, N2, N4, N5, N7);
  ASSERT_EQ(E::UNKNOWN, final_status_);
  accessor_->setShardAuthoritativeStatus(
      N1, AuthoritativeStatus::UNDERREPLICATION);
  ASSERT_EQ(E::UNKNOWN, final_status_);
  accessor_->setShardAuthoritativeStatus(
      N3, AuthoritativeStatus::UNDERREPLICATION);
  ASSERT_EQ(E::UNKNOWN, final_status_);
  accessor_->setShardAuthoritativeStatus(
      N6, AuthoritativeStatus::UNDERREPLICATION);
  // we should have non-authoritative fmajority by now
  ASSERT_EQ(E::OK, final_status_);
}

// StorageSetAccessor should not complete with success if one of the required
// nodes is not successfully stored.
TEST_F(NodeSetAccessorTest, RequiredNodeBasic) {
  replication_ = 3;
  extras_ = 0;
  sync_replication_scope_ = NodeLocationScope::REGION;
  property_ = StorageSetAccessor::Property::REPLICATION;
  nodeset_ = StorageSet{N0, N1, N2, N3, N4, N5, N6, N7};
  required_nodes_ = {N2, N3, N4, N5};

  setUp();

  // w/ 4 required nodes in region 1, one node in other region should be
  // sufficient
  getCopySetSelector()->setResult({N2, N3, N7, N4, N5});
  accessor_->start();
  for (auto n : required_nodes_) {
    // each node in required_nodes_ must appear in the wave
    ASSERT_GT(wave_shards_.count(n), 0);
  }

  // node 7 should be in the wave as well
  ASSERT_GT(wave_shards_.count(N7), 0);
  ASSERT_EQ(5, wave_shards_.size());

  // {2, 3, 7} already satisfy the replication requirement, however
  // StorageSetAccessor should not conclude w/ success until all required
  // nodes are successfully accessed.
  accessNodes(AccessResult::SUCCESS, N2, N3, N7);
  ASSERT_EQ(E::UNKNOWN, final_status_);
  accessNodes(AccessResult::SUCCESS, N4);
  ASSERT_EQ(E::UNKNOWN, final_status_);
  accessNodes(AccessResult::SUCCESS, N5);
  ASSERT_EQ(E::OK, final_status_);
}

// StorageSetAccessor should complete with E::FAILED if one of the required
// node has permanent failure
TEST_F(NodeSetAccessorTest, RequiredNodeFailure) {
  replication_ = 3;
  extras_ = 2;
  sync_replication_scope_ = NodeLocationScope::REGION;
  property_ = StorageSetAccessor::Property::REPLICATION;
  nodeset_ = StorageSet{N0, N1, N2, N3, N4, N5, N6, N7};
  required_nodes_ = {N2};

  setUp();

  setSendResult(SendResult::PERMANENT_ERROR, N2);
  getCopySetSelector()->setResult({N2, N0, N7});
  accessor_->start();
  ASSERT_EQ(E::FAILED, final_status_);
}

TEST_F(NodeSetAccessorTest, SuccessIfAllNodeAccessed) {
  replication_ = 3;
  extras_ = 4;
  sync_replication_scope_ = NodeLocationScope::REGION;
  property_ = StorageSetAccessor::Property::REPLICATION;

  // all nodes are in region 1, while it requires REGION scope replication
  nodeset_ = StorageSet{N1, N2, N3, N4};
  allow_success_if_all_accessed_ = true;

  setUp();

  // copyset selection must not succeed
  getCopySetSelector()->setResult({}, CopySetSelector::Result::FAILED);
  accessor_->start();

  // should pick 4 extra nodes
  verifyWave(N1, N2, N3, N4);
  accessNodes(AccessResult::SUCCESS, N1, N2, N3, N4);
  ASSERT_EQ(E::OK, final_status_);
}

TEST_F(NodeSetAccessorTest, requireStrictWaves) {
  replication_ = 3;
  extras_ = 1;
  sync_replication_scope_ = NodeLocationScope::NODE;
  property_ = StorageSetAccessor::Property::FMAJORITY;
  nodeset_ = StorageSet{N0, N3, N4, N5, N6, N7};
  require_strict_waves_ = true;

  setUp();
  // node 0
  setSendResult(SendResult::PERMANENT_ERROR, N0);
  accessor_->start();
  // should send to the rest of the nodes
  verifyWave(N0, N3, N4, N5, N6, N7);
  // this is wave 1
  ASSERT_EQ(1, wave_info_.wave);

  accessNodesWithWave(/*wave*/ 1, AccessResult::SUCCESS, N3, N6, N7);
  accessNodesWithWave(/*wave*/ 1, AccessResult::TRANSIENT_ERROR, N4);
  ASSERT_EQ(E::UNKNOWN, final_status_);
  ASSERT_TRUE(getWaveTimer()->isActive());

  // trigger the next wave
  getWaveTimer()->trigger();

  // the next wave should still be sent to every nodes except
  // for 0, despite that 3, 6, 7 has already been successfully
  // accessed in previous waves
  verifyWave(N3, N4, N5, N6, N7);
  // this is wave 2
  ASSERT_EQ(2, wave_info_.wave);

  accessNodesWithWave(/*wave*/ 2, AccessResult::SUCCESS, N3, N4, N7);
  ASSERT_EQ(E::UNKNOWN, final_status_);

  // results from the previous wave should be ignored
  accessNodesWithWave(/*wave*/ 1, AccessResult::SUCCESS, N5);
  ASSERT_EQ(E::UNKNOWN, final_status_);

  // N5 succeeds in wave 2, operation complete
  accessNodesWithWave(/*wave*/ 2, AccessResult::SUCCESS, N5);
  ASSERT_EQ(E::OK, final_status_);
}

// similar to the one above, but with replicaiton property
TEST_F(NodeSetAccessorTest, requireStrictWavesReplication) {
  replication_ = 5;
  extras_ = 0;
  sync_replication_scope_ = NodeLocationScope::NODE;
  property_ = StorageSetAccessor::Property::REPLICATION;
  nodeset_ = StorageSet{N0, N1, N2, N3, N4, N5, N6, N7};
  require_strict_waves_ = true;

  setUp();

  getCopySetSelector()->setResult({N1, N2, N3, N4, N5});
  accessor_->start();

  verifyWave(N1, N2, N3, N4, N5);
  // this is wave 1
  ASSERT_EQ(1, wave_info_.wave);

  accessNodesWithWave(1, AccessResult::SUCCESS, N2, N3, N4, N5);
  ASSERT_EQ(E::UNKNOWN, final_status_);
  ASSERT_TRUE(getWaveTimer()->isActive());

  // we need a full copyset for the next wave
  getCopySetSelector()->setResult({N0, N1, N3, N5, N7});
  // trigger the next wave
  getWaveTimer()->trigger();

  verifyWave(N0, N1, N3, N5, N7);
  // this is wave 2
  ASSERT_EQ(2, wave_info_.wave);

  accessNodesWithWave(2, AccessResult::SUCCESS, N1, N3, N5, N7);
  ASSERT_EQ(E::UNKNOWN, final_status_);

  accessNodesWithWave(1, AccessResult::SUCCESS, N0);
  ASSERT_EQ(E::UNKNOWN, final_status_);
  ASSERT_TRUE(getWaveTimer()->isActive());

  accessNodesWithWave(2, AccessResult::SUCCESS, N0);
  ASSERT_EQ(E::OK, final_status_);
}

TEST_F(NodeSetAccessorTest, ReproT15460364) {
  replication_ = 1;
  extras_ = 0;
  sync_replication_scope_ = NodeLocationScope::REGION;
  property_ = StorageSetAccessor::Property::REPLICATION;
  nodeset_ = StorageSet{N0, N1, N2, N3, N4, N5, N6, N7};
  required_nodes_ = {N1, N2, N3, N4};
  require_strict_waves_ = true;

  setUp();

  getCopySetSelector()->setResult({N1, N2, N3, N4});
  accessor_->start();

  verifyWave(N1, N2, N3, N4);
  ASSERT_EQ(1, wave_info_.wave);
}

// test that nodeset selector should pick all available nodes in a
// single wave if copyset selection failed
TEST_F(NodeSetAccessorTest, StrictWavesPickCopySetFailed) {
  replication_ = 3;
  extras_ = 0;
  sync_replication_scope_ = NodeLocationScope::NODE;
  property_ = StorageSetAccessor::Property::REPLICATION;
  nodeset_ = StorageSet{N0, N3, N4, N5, N6, N7};
  require_strict_waves_ = true;

  setUp();
  getCopySetSelector()->setResult({}, CopySetSelector::Result::FAILED);
  accessor_->start();

  // should pick all available nodes
  ASSERT_EQ(6, wave_shards_.size());
  verifyWave(N0, N3, N4, N5, N6, N7);
}

TEST_F(NodeSetAccessorTest, ReproT22867933) {
  replication_ = 2;
  extras_ = 0;
  property_ = StorageSetAccessor::Property::REPLICATION;

  setUp();

  getCopySetSelector()->setResult({N0, N1});
  accessor_->start();
  verifyWave(N0, N1);

  accessNodes(AccessResult::SUCCESS, N0);
  getCopySetSelector()->setResult({}, CopySetSelector::Result::FAILED);
  getWaveTimer()->trigger();

  ASSERT_EQ(1, wave_shards_.size());
  ASSERT_EQ(1, wave_info_.wave_shards.size());
  EXPECT_NE(N0, *wave_shards_.begin());
  EXPECT_EQ(*wave_shards_.begin(), wave_info_.wave_shards[0]);
  wave_shards_.clear();
}

}} // namespace facebook::logdevice
