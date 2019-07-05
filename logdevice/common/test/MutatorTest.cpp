/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/Mutator.h"

#include <folly/Memory.h>
#include <folly/Optional.h>
#include <gtest/gtest.h>

#include "logdevice/common/CrossDomainCopySetSelector.h"
#include "logdevice/common/EpochMetaData.h"
#include "logdevice/common/LinearCopySetSelector.h"
#include "logdevice/common/Random.h"
#include "logdevice/common/Timer.h"
#include "logdevice/common/configuration/LocalLogsConfig.h"
#include "logdevice/common/test/MockBackoffTimer.h"
#include "logdevice/common/test/NodeSetTestUtil.h"
#include "logdevice/include/Record.h"

namespace facebook { namespace logdevice {

using namespace facebook::logdevice::NodeSetTestUtil;

namespace {

// Convenient shortcuts for writting ShardIDs.
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

class TestCopySetSelectorDeps;

class MutatorTest : public ::testing::Test {
 public:
  const logid_t LOG_ID{2};
  const esn_t ESN{333};
  const epoch_t EPOCH{3};
  const esn_t EPOCH_LNG{121};
  const epoch_t SEAL_EPOCH{22};
  const NodeID seq_node_{NodeID(1, 1)};
  const recovery_id_t recovery_id_{3333};

  bool hole_{false};

  std::shared_ptr<Configuration> config_;
  ReplicationProperty replication_{{NodeLocationScope::NODE, 3}};
  StorageSet shards_{N0, N1, N2, N3, N4, N5, N6, N7, N8};
  NodeID my_node_{NodeID(1)};

  STORE_Header store_header_;
  STORE_Extra store_extra_;
  Payload payload_;

  // store a raw pointer to the wave timer inside the NodeSetAccessor.
  // does not own it
  BackoffTimer* wave_timer_{nullptr};

  std::set<ShardID> amend_metadata_;
  std::set<ShardID> conflict_copies_;

  std::shared_ptr<NodeSetState> nodeset_state_;
  std::unique_ptr<TestCopySetSelectorDeps> deps_;
  std::unique_ptr<RNG> rng_wrapper_;

  // results
  Status mutation_status_{E::UNKNOWN};
  ShardID node_not_in_config_;

  std::unordered_map<NodeID, std::unique_ptr<Message>, NodeID::Hash> messages_;

  std::unordered_map<node_index_t, SocketCallback*> socket_callbacks_;

  std::unique_ptr<Mutator> mutator_;

 public:
  std::shared_ptr<Configuration> getConfig() const {
    return config_;
  }

  void initStoreHeaderAndExtras();
  void setUp();
  MUTATED_Header createMutatedHeader(uint32_t wave = 1,
                                     Status status = E::OK,
                                     Seal seal = Seal());
};

class TestCopySetSelectorDeps : public CopySetSelectorDependencies,
                                public NodeAvailabilityChecker {
 public:
  NodeStatus checkNode(NodeSetState*,
                       ShardID shard,
                       StoreChainLink* destination_out,
                       bool /*ignore_nodeset_state*/,
                       bool /*allow_unencrypted_conections*/) const override {
    *destination_out = {shard, ClientID()};
    return NodeAvailabilityChecker::NodeStatus::AVAILABLE;
  }

  const NodeAvailabilityChecker* getNodeAvailability() const override {
    return this;
  }
};

class MockLinearCopySetSelector : public LinearCopySetSelector {
 public:
  explicit MockLinearCopySetSelector(MutatorTest* test)
      : LinearCopySetSelector(test->replication_.getReplicationFactor(),
                              test->shards_,
                              test->nodeset_state_,
                              test->deps_.get()) {}
};

class MockedNodeSetAccessor : public StorageSetAccessor {
 public:
  explicit MockedNodeSetAccessor(
      MutatorTest* test,
      StorageSetAccessor::ShardAccessFunc node_access,
      StorageSetAccessor::CompletionFunc completion,
      StorageSetAccessor::Property property)
      : StorageSetAccessor(test->LOG_ID,
                           test->shards_,
                           test->config_->serverConfig()
                               ->getNodesConfigurationFromServerConfigSource(),
                           test->replication_,
                           node_access,
                           completion,
                           property),
        test_(test) {
    rng_ = test->rng_wrapper_.get();
  }

  std::unique_ptr<Timer>
  createJobTimer(std::function<void()> /*callback*/) override {
    return nullptr;
  }

  void cancelJobTimer() override {}
  void activateJobTimer() override {}

  std::unique_ptr<BackoffTimer>
  createWaveTimer(std::function<void()> callback) override {
    auto timer = std::make_unique<MockBackoffTimer>();
    timer->setCallback(callback);
    test_->wave_timer_ = timer.get();
    return std::move(timer);
  }

  std::unique_ptr<CopySetSelector> createCopySetSelector(
      logid_t log_id,
      const EpochMetaData& epoch_metadata,
      std::shared_ptr<NodeSetState> nodeset_state,
      const std::shared_ptr<const configuration::nodes::NodesConfiguration>&
          nodes_configuration) override {
    if (epoch_metadata.replication.toOldRepresentation()
            ->sync_replication_scope > NodeLocationScope::NODE) {
      return std::make_unique<CrossDomainCopySetSelector>(
          log_id,
          epoch_metadata.shards,
          nodeset_state,
          nodes_configuration,
          test_->my_node_,
          epoch_metadata.replication.toOldRepresentation()->replication_factor,
          epoch_metadata.replication.toOldRepresentation()
              ->sync_replication_scope,
          test_->deps_.get());
    }
    return std::make_unique<MockLinearCopySetSelector>(test_);
  }

  ShardAuthoritativeStatusMap& getShardAuthoritativeStatusMap() override {
    return empty_map_;
  }

 private:
  MutatorTest* const test_;
  ShardAuthoritativeStatusMap empty_map_;
};

class MockMutator : public Mutator {
 public:
  using MockSender = SenderTestProxy<MockMutator>;

  explicit MockMutator(MutatorTest* test)
      : Mutator(test->store_header_,
                test->store_extra_,
                test->payload_,
                test->shards_,
                test->replication_,
                test->amend_metadata_,
                test->conflict_copies_,
                nullptr),
        test_(test) {
    sender_ = std::make_unique<MockSender>(this);
    ld_check(test != nullptr);
  }

  bool canSendToImpl(const Address&, TrafficClass, BWAvailableCallback&) {
    return true;
  }

  int sendMessageImpl(std::unique_ptr<Message>&& msg,
                      const Address& addr,
                      BWAvailableCallback*,
                      SocketCallback* callback) {
    ld_check(!addr.isClientAddress());
    if (addr.isClientAddress()) {
      err = E::INTERNAL;
      return -1;
    }
    NodeID dest = addr.id_.node_;
    EXPECT_TRUE(dest.isNodeID());
    test_->socket_callbacks_[dest.index()] = callback;
    test_->messages_[dest] = std::move(msg);
    return 0;
  }

 protected:
  std::shared_ptr<const configuration::nodes::NodesConfiguration>
  getNodesConfiguration() const override {
    return test_->getConfig()
        ->serverConfig()
        ->getNodesConfigurationFromServerConfigSource();
  }

  std::unique_ptr<StorageSetAccessor> createStorageSetAccessor(
      logid_t /*log_id*/,
      EpochMetaData epoch_metadata_with_mutation_set,
      std::shared_ptr<const configuration::nodes::NodesConfiguration>,
      StorageSetAccessor::ShardAccessFunc node_access,
      StorageSetAccessor::CompletionFunc completion,
      StorageSetAccessor::Property property) override {
    EXPECT_EQ(test_->shards_, epoch_metadata_with_mutation_set.shards);
    EXPECT_EQ(test_->replication_.toString(),
              epoch_metadata_with_mutation_set.replication.toString());
    return std::make_unique<MockedNodeSetAccessor>(
        test_, node_access, completion, property);
  }

  AuthoritativeStatus
  getNodeAuthoritativeStatus(ShardID /* shard */) const override {
    return AuthoritativeStatus::FULLY_AUTHORITATIVE;
  }

  chrono_interval_t<std::chrono::milliseconds>
  getMutationTimeout() const override {
    return chrono_interval_t<std::chrono::milliseconds>{
        std::chrono::milliseconds(1), std::chrono::milliseconds(1)};
  }

  void finalize(Status status, ShardID node_not_in_config) override {
    test_->mutation_status_ = status;
    test_->node_not_in_config_ = node_not_in_config;
  }

 private:
  MutatorTest* const test_;
};

void MutatorTest::initStoreHeaderAndExtras() {
  store_header_ = STORE_Header{RecordID{ESN, EPOCH, LOG_ID},
                               328492394, // timestamp
                               EPOCH_LNG, // lng
                               0,         // wave to be overwritten by Mutator
                               STORE_Header::WRITTEN_BY_RECOVERY,
                               0,  // nsync
                               99, // copyset_offset to be overwrittn
                               99, // copyset_size to be overwrittn
                               0,  // timeout_ms
                               seq_node_};

  if (hole_) {
    store_header_.flags |= STORE_Header::HOLE;
  } else {
    // record
    store_header_.flags |= STORE_Header::OFFSET_WITHIN_EPOCH;
    payload_ = Payload("foo", 3);
  }

  store_extra_.recovery_id = recovery_id_;
  store_extra_.recovery_epoch = SEAL_EPOCH;
  OffsetMap offsets_within_epoch;
  offsets_within_epoch.setCounter(BYTE_OFFSET, 22334455);
  store_extra_.offsets_within_epoch =
      (hole_ ? OffsetMap() : offsets_within_epoch);
}

void MutatorTest::setUp() {
  dbg::assertOnData = true;

  // initialize the cluster config
  Configuration::Nodes nodes;
  addNodes(&nodes, 1, 2, "rg0.dc0.cl0.ro0.rk0", 1);
  addNodes(&nodes, 1, 2, "rg1.dc0.cl0.ro0.rk0", 1);
  addNodes(&nodes, 2, 2, "rg1.dc0.cl0.ro0.rk1", 2);
  addNodes(&nodes, 1, 2, "rg1.dc0.cl0.ro0.rk2", 1);
  addNodes(&nodes, 2, 2, "rg1.dc0.cl0..", 1);
  addNodes(&nodes, 1, 2, "rg2.dc0.cl0.ro0.rk0", 1);
  addNodes(&nodes, 1, 2, "rg2.dc0.cl0.ro0.rk1", 1);
  addNodes(&nodes, 2, 2, "....", 1);

  const size_t nodeset_size = nodes.size();
  Configuration::NodesConfig nodes_config(std::move(nodes));

  auto logs_config = std::make_shared<configuration::LocalLogsConfig>();
  addLog(logs_config.get(), LOG_ID, replication_, 0, nodeset_size, {});

  config_ = std::make_shared<Configuration>(
      ServerConfig::fromDataTest("mutator_test", std::move(nodes_config)),
      std::move(logs_config));

  nodeset_state_ = std::make_shared<NodeSetState>(
      shards_, LOG_ID, NodeSetState::HealthCheck::DISABLED);

  deps_ = std::make_unique<TestCopySetSelectorDeps>();
  rng_wrapper_ = std::make_unique<DefaultRNG>();
  initStoreHeaderAndExtras();

  mutator_ = std::make_unique<MockMutator>(this);
  ASSERT_NE(nullptr, mutator_);
}

MUTATED_Header MutatorTest::createMutatedHeader(uint32_t wave,
                                                Status status,
                                                Seal seal) {
  MUTATED_Header header;
  header.recovery_id = recovery_id_;
  header.rid = RecordID{ESN, EPOCH, LOG_ID};
  header.status = status;
  header.seal = seal;
  header.shard = 0;
  header.wave = wave;
  return header;
}

#define CHECK_STORE_MSG(_msg, _node, _wave)                                \
  do {                                                                     \
    STORE_Message* message = dynamic_cast<STORE_Message*>(&_msg);          \
    ASSERT_NE(nullptr, message);                                           \
    const auto& header = (message)->getHeader();                           \
    const auto& copyset = (message)->getCopyset();                         \
    ASSERT_EQ(store_header_.rid, header.rid);                              \
    ASSERT_EQ(store_header_.timestamp, header.timestamp);                  \
    ASSERT_EQ(store_header_.last_known_good, header.last_known_good);      \
    ASSERT_EQ(_wave, header.wave);                                         \
    ASSERT_EQ(header.copyset_size, copyset.size());                        \
    ASSERT_GT(copyset.size(), header.copyset_offset);                      \
    ASSERT_EQ(_node, copyset[header.copyset_offset].destination.node());   \
    STORE_flags_t expected_flags =                                         \
        STORE_Header::WRITTEN_BY_RECOVERY | STORE_Header::CHECKSUM_PARITY; \
    if (amend_metadata_.count(ShardID(_node, 0)) > 0) {                    \
      expected_flags |= STORE_Header::AMEND;                               \
    }                                                                      \
    if (hole_) {                                                           \
      expected_flags |= STORE_Header::HOLE;                                \
      const auto& ph = (message)->getPayloadHolder();                      \
      ASSERT_TRUE(ph == nullptr || ph->size() == 0);                       \
    } else {                                                               \
      expected_flags |= STORE_Header::OFFSET_WITHIN_EPOCH;                 \
    }                                                                      \
    ASSERT_EQ(expected_flags, header.flags);                               \
    ASSERT_EQ(store_extra_, (message)->getExtra());                        \
  } while (0)

// Tests restoring replication factor of a data record
TEST_F(MutatorTest, BasicReplication) {
  amend_metadata_ = std::set<ShardID>{N2};
  conflict_copies_ = std::set<ShardID>{};
  setUp();

  mutator_->start();
  // not done yet
  ASSERT_EQ(E::UNKNOWN, mutation_status_);
  ASSERT_EQ(3, messages_.size());

  ASSERT_EQ(1, messages_.count(NodeID(2)));

  // Must send amend to N2, and store to other nodes
  for (const auto& kv : messages_) {
    CHECK_STORE_MSG((*kv.second), kv.first.index(), 1);
  }

  for (const auto& kv : messages_) {
    mutator_->onMessageSent(
        ShardID(kv.first.index(), 0),
        E::OK,
        dynamic_cast<STORE_Message*>(kv.second.get())->getHeader());
  }
  ASSERT_EQ(E::UNKNOWN, mutation_status_);

  int stored = 0;
  for (const auto& kv : messages_) {
    mutator_->onStored(
        ShardID(kv.first.index(), 0), createMutatedHeader(1, E::OK));
    if (++stored < 3) {
      ASSERT_EQ(E::UNKNOWN, mutation_status_);
    }
  }

  ASSERT_EQ(E::OK, mutation_status_);
}

// a node that received a store sends back a reply with preempted status
TEST_F(MutatorTest, Preemption) {
  hole_ = true;
  replication_.assign(
      {{NodeLocationScope::NODE, 3}, {NodeLocationScope::REGION, 2}});
  amend_metadata_ = std::set<ShardID>{N5, N6};
  conflict_copies_ = std::set<ShardID>{N3, N4};
  setUp();

  mutator_->start();
  // not done yet
  ASSERT_EQ(E::UNKNOWN, mutation_status_);

  // should send to {3, 4, 5, 6} plus one more node
  ASSERT_EQ(5, messages_.size());
  for (auto s : StorageSet{N3, N4, N5, N6}) {
    ASSERT_EQ(1, messages_.count(NodeID(s.node())));
  }
  for (const auto& kv : messages_) {
    CHECK_STORE_MSG((*kv.second), kv.first.index(), 1);
  }
  for (const auto& kv : messages_) {
    mutator_->onMessageSent(
        ShardID(kv.first.index(), 0),
        E::OK,
        dynamic_cast<STORE_Message*>(kv.second.get())->getHeader());
  }
  ASSERT_EQ(E::UNKNOWN, mutation_status_);

  for (auto s : StorageSet{N3, N4, N5}) {
    mutator_->onStored(s, createMutatedHeader(1, E::OK));
  }
  // must wait for N6 to respond
  ASSERT_EQ(E::UNKNOWN, mutation_status_);
  Seal preempted_seal(epoch_t(SEAL_EPOCH.val_ + 5), NodeID(8));
  // N6 replied with E::PREEMPTED
  mutator_->onStored(N6, createMutatedHeader(1, E::PREEMPTED, preempted_seal));

  ASSERT_EQ(E::PREEMPTED, mutation_status_);
  ASSERT_EQ(preempted_seal, mutator_->getPreemptedSeal());
}

// test the scenario that a node is removed from the config and triggered
// the socket close callback
TEST_F(MutatorTest, NotInConfig) {
  hole_ = true;
  replication_.assign(
      {{NodeLocationScope::NODE, 3}, {NodeLocationScope::REGION, 2}});
  amend_metadata_ = std::set<ShardID>{N2};
  conflict_copies_ = std::set<ShardID>{N3, N4};
  setUp();

  mutator_->start();
  // not done yet
  ASSERT_EQ(E::UNKNOWN, mutation_status_);

  // should send to 4 nodes since 2, 3, 4 are in the same region
  ASSERT_EQ(4, messages_.size());
  for (auto s : StorageSet{N2, N3, N4}) {
    ASSERT_EQ(1, messages_.count(NodeID(s.node())));
  }
  for (const auto& kv : messages_) {
    CHECK_STORE_MSG((*kv.second), kv.first.index(), 1);
  }
  for (const auto& kv : messages_) {
    mutator_->onMessageSent(
        ShardID(kv.first.index(), 0),
        E::OK,
        dynamic_cast<STORE_Message*>(kv.second.get())->getHeader());
  }
  ASSERT_EQ(E::UNKNOWN, mutation_status_);
  for (auto s : StorageSet{N2, N3}) {
    mutator_->onStored(s, createMutatedHeader(1, E::OK));
  }
  // N4 disconnects with E::NOTINCONFIG
  auto* callback = socket_callbacks_[4];
  ASSERT_NE(nullptr, callback);
  (*callback)(E::NOTINCONFIG, Address(NodeID(4)));

  ASSERT_EQ(E::NOTINCONFIG, mutation_status_);
  ASSERT_EQ(N4, node_not_in_config_);
}

// recipients may timeout or report error status, Mutator should start a new
// wave
TEST_F(MutatorTest, MultipleWaves) {
  replication_.assign(
      {{NodeLocationScope::NODE, 3}, {NodeLocationScope::REGION, 2}});
  amend_metadata_ = std::set<ShardID>{N2};
  conflict_copies_ = std::set<ShardID>{N3};
  setUp();

  mutator_->start();
  // not done yet
  ASSERT_EQ(E::UNKNOWN, mutation_status_);

  ASSERT_EQ(3, messages_.size());
  for (auto s : StorageSet{N2, N3}) {
    ASSERT_EQ(1, messages_.count(NodeID(s.node())));
  }
  // wave 1
  for (const auto& kv : messages_) {
    CHECK_STORE_MSG((*kv.second), kv.first.index(), 1);
  }
  // sending to N2 failed, while other nodes successfully sent OK replies
  for (const auto& kv : messages_) {
    mutator_->onMessageSent(
        ShardID(kv.first.index(), 0),
        kv.first.index() == 2 ? E::NOBUFS : E::OK,
        dynamic_cast<STORE_Message*>(kv.second.get())->getHeader());
  }
  ASSERT_EQ(E::UNKNOWN, mutation_status_);
  for (const auto& kv : messages_) {
    if (kv.first.index() != 2) {
      mutator_->onStored(
          ShardID(kv.first.index(), 0), createMutatedHeader(1, E::OK));
    }
  }
  ASSERT_EQ(E::UNKNOWN, mutation_status_);
  ASSERT_NE(nullptr, wave_timer_);
  ASSERT_TRUE(wave_timer_->isActive());
  // clear messages for wave 1
  messages_.clear();

  // wave timer expires, a new wave is sent
  static_cast<MockBackoffTimer*>(wave_timer_)->trigger();

  // wave 2 should still be sent to 3 nodes, including N2, N3
  ASSERT_EQ(3, messages_.size());
  for (auto s : StorageSet{N2, N3}) {
    ASSERT_EQ(1, messages_.count(NodeID(s.node())));
  }
  // wave 2
  for (const auto& kv : messages_) {
    CHECK_STORE_MSG((*kv.second), kv.first.index(), /*wave*/ 2);
  }
  for (const auto& kv : messages_) {
    mutator_->onStored(
        ShardID(kv.first.index(), 0), createMutatedHeader(2, E::OK));
  }
  ASSERT_EQ(E::OK, mutation_status_);
}

} // namespace
}} // namespace facebook::logdevice
