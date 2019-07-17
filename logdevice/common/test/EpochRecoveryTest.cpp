/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/EpochRecovery.h"

#include <algorithm>

#include <gtest/gtest.h>

#include "logdevice/common/DataRecordOwnsPayload.h"
#include "logdevice/common/Mutator.h"
#include "logdevice/common/protocol/GAP_Message.h"
#include "logdevice/common/protocol/MUTATED_Message.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/stats/ServerHistograms.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/common/test/DigestTestUtil.h"
#include "logdevice/common/test/MockBackoffTimer.h"
#include "logdevice/common/test/MockTimer.h"
#include "logdevice/common/test/TestUtil.h"

#define N0 ShardID(0, 0)
#define N1 ShardID(1, 0)
#define N2 ShardID(2, 0)
#define N3 ShardID(3, 0)
#define N4 ShardID(4, 0)
#define N5 ShardID(5, 0)
#define N6 ShardID(6, 0)

using namespace facebook::logdevice;
using namespace facebook::logdevice::DigestTestUtil;

namespace {

using ERMState = EpochRecovery::State;
using NState = RecoveryNode::State;

class MockEpochRecoveryDependencies;

class EpochRecoveryTest : public ::testing::Test {
 public:
  dbg::Level log_level_ = dbg::Level::DEBUG;
  Alarm alarm_{DEFAULT_TEST_TIMEOUT};

  static constexpr logid_t LOG_ID{23333};
  bool tail_optimized_ = false;

  Settings settings_;
  UpdateableSettings<Settings> updateable_settings_;
  StatsHolder stats_;
  std::shared_ptr<UpdateableConfig> updateable_config_;

  NodeID my_node_{NodeID(0, 1)};
  std::unordered_map<node_index_t, std::string> node_locations_;

  std::unordered_map<
      NodeID,
      folly::IntrusiveList<SocketCallback, &SocketCallback::listHook_>,
      NodeID::Hash>
      on_close_cb_map_;

  read_stream_id_t rsid_{0};

  // epoch recovery instance to be tested
  std::unique_ptr<EpochRecovery> erm_;

  // epoch to be recovered
  epoch_t epoch_{23};
  // seal epoch of log recovery
  epoch_t seal_epoch_{27};

  // previous tail record before the epoch
  TailRecord prev_tail_{
      {LOG_ID,
       lsn(21, 7),
       3432,
       {BYTE_OFFSET_INVALID /* deprecated, use OffsetMap instead */},
       TailRecordHeader::CHECKSUM_PARITY,
       {}},
      OffsetMap({{BYTE_OFFSET, 237419}}),
      std::shared_ptr<PayloadHolder>()};

  StorageSet all_shards_{N0, N1, N2, N3, N4, N5, N6};

  // nodeset and replication property of the log
  StorageSet storage_set_{N1, N2, N3};
  ReplicationProperty rep_{{NodeLocationScope::NODE, 2}};

  // Shards that are in draining mode. they are fully authoritative and will be
  // digested, but not writable so should not be included in mutation set
  std::set<ShardID> draining_shards_;

  struct Result {
    Status st;
    Seal seal; // valid when preempted
    TailRecord tail;
  };

  // result of epoch recovery
  Result result_;
  // tail record that epoch recovery to set in epoch store
  TailRecord lce_tail_;
  bool finished_{false};

  explicit EpochRecoveryTest();
  ~EpochRecoveryTest() override {}

  void initConfig();
  void setUp();

  std::shared_ptr<const NodesConfiguration> getNodesConfiguration() const {
    return updateable_config_->getNodesConfiguration();
  }

  void checkRecoveryState(ERMState expect_state);
};

// a dummy mutator that does nothing
class DummyMutator : public Mutator {
 public:
  explicit DummyMutator(EpochRecoveryTest* test,
                        const STORE_Header& header,
                        const STORE_Extra& extra,
                        Payload payload,
                        StorageSet mutation_set,
                        ReplicationProperty replication,
                        std::set<ShardID> amend_metadata,
                        std::set<ShardID> conflict_copies,
                        EpochRecovery* epoch_recovery)
      : Mutator(header,
                extra,
                payload,
                std::move(mutation_set),
                std::move(replication),
                std::move(amend_metadata),
                std::move(conflict_copies),
                epoch_recovery),
        test_(test) {
    ld_check(test_ != nullptr);
  }

  void start() override {}

 private:
  EpochRecoveryTest* const test_;
};

class MockEpochRecoveryDependencies : public EpochRecoveryDependencies {
  using MockSender = SenderTestProxy<MockEpochRecoveryDependencies>;

 public:
  explicit MockEpochRecoveryDependencies(EpochRecoveryTest* test)
      : EpochRecoveryDependencies(/*driver=*/nullptr), test_(test) {
    ld_check(test_ != nullptr);
    sender_ = std::make_unique<MockSender>(this);
  }

  int sendMessageImpl(std::unique_ptr<Message>&&,
                      const Address&,
                      BWAvailableCallback*,
                      SocketCallback*) {
    // TODO T22417568: verify messages
    return 0;
  }

  bool canSendToImpl(const Address&, TrafficClass, BWAvailableCallback&) {
    return true;
  }

  epoch_t getSealEpoch() const override {
    return test_->seal_epoch_;
  }

  epoch_t getLogRecoveryNextEpoch() const override {
    // next epoch should be the next epoch ofseal epoch
    return epoch_t(test_->seal_epoch_.val_ + 1);
  }

  void onEpochRecovered(epoch_t epoch,
                        TailRecord epoch_tail,
                        Status status,
                        Seal seal) override {
    ASSERT_FALSE(test_->finished_);
    ASSERT_EQ(test_->epoch_, epoch);
    test_->result_.st = status;
    test_->result_.seal = seal;
    test_->result_.tail = epoch_tail;
    test_->finished_ = true;
  }

  void onShardRemovedFromConfig(ShardID) override {}

  bool canMutateShard(ShardID shard) const override {
    return test_->draining_shards_.count(shard) == 0;
  }

  NodeID getMyNodeID() const override {
    return test_->my_node_;
  }

  read_stream_id_t issueReadStreamID() override {
    return read_stream_id_t(++test_->rsid_.val_);
  }

  void noteMutationsCompleted(const EpochRecovery& erm) override {
    ASSERT_EQ(test_->erm_.get(), &erm);
  }

  std::unique_ptr<BackoffTimer>
  createBackoffTimer(const chrono_expbackoff_t<std::chrono::milliseconds>&,
                     std::function<void()> callback) override {
    auto timer = std::make_unique<MockBackoffTimer>();
    if (callback) {
      timer->setCallback(std::move(callback));
    }
    return std::move(timer);
  }

  std::unique_ptr<Timer> createTimer(std::function<void()> cb) override {
    auto timer = std::make_unique<MockTimer>();
    timer->setCallback(std::move(cb));
    return std::move(timer);
  }

  int registerOnSocketClosed(const Address& addr, SocketCallback& cb) override {
    test_->on_close_cb_map_[addr.asNodeID()].push_back(cb);
    return 0;
  }

  int setLastCleanEpoch(logid_t logid,
                        epoch_t lce,
                        const TailRecord& tail_record,
                        EpochStore::CompletionLCE) override {
    EXPECT_EQ(test_->LOG_ID, logid);
    EXPECT_EQ(test_->epoch_, lce);
    test_->lce_tail_ = tail_record;
    return 0;
  }

  std::unique_ptr<Mutator>
  createMutator(const STORE_Header& header,
                const STORE_Extra& extra,
                Payload payload,
                StorageSet mutation_set,
                ReplicationProperty replication,
                std::set<ShardID> amend_metadata,
                std::set<ShardID> conflict_copies,
                EpochRecovery* epoch_recovery) override {
    return std::make_unique<DummyMutator>(test_,
                                          header,
                                          extra,
                                          payload,
                                          std::move(mutation_set),
                                          std::move(replication),
                                          std::move(amend_metadata),
                                          std::move(conflict_copies),
                                          epoch_recovery);
  }

  const Settings& getSettings() const override {
    return test_->settings_;
  }

  StatsHolder* getStats() const override {
    return &test_->stats_;
  }

  logid_t getLogID() const override {
    return test_->LOG_ID;
  }

 private:
  EpochRecoveryTest* const test_;
};

constexpr logid_t EpochRecoveryTest::LOG_ID;

EpochRecoveryTest::EpochRecoveryTest()
    : settings_(create_default_settings<Settings>()),
      stats_(StatsParams().setIsServer(true)),
      updateable_config_(std::make_shared<UpdateableConfig>()) {}

void EpochRecoveryTest::initConfig() {
  // init cluster config
  configuration::Nodes nodes;
  for (ShardID shard : all_shards_) {
    Configuration::Node& node = nodes[shard.node()];
    node.address = Sockaddr("::1", folly::to<std::string>(4440 + shard.node()));
    node.generation = 1;
    node.addSequencerRole();
    node.addStorageRole();

    auto it = node_locations_.find(shard.node());
    if (it != node_locations_.end()) {
      NodeLocation loc;
      int rv = loc.fromDomainString(it->second);
      ASSERT_EQ(0, rv);
      node.location = std::move(loc);
    }
  }

  logsconfig::LogAttributes log_attrs;
  // we won't use these
  log_attrs.set_replicationFactor(2);
  Configuration::NodesConfig nodes_config(std::move(nodes));
  auto logs_config = std::make_shared<configuration::LocalLogsConfig>();
  logs_config->insert(boost::icl::right_open_interval<logid_t::raw_type>(
                          LOG_ID.val_, LOG_ID.val_ + 1),
                      "log",
                      log_attrs);

  // metadata stored on all nodes with max replication factor 3
  Configuration::MetaDataLogsConfig meta_config =
      createMetaDataLogsConfig(nodes_config, nodes_config.getNodes().size(), 3);

  updateable_config_->updateableServerConfig()->update(
      ServerConfig::fromDataTest(__FILE__, nodes_config, meta_config));
  updateable_config_->updateableNodesConfiguration()->update(
      updateable_config_->getNodesConfigurationFromServerConfigSource());
  updateable_config_->updateableLogsConfig()->update(std::move(logs_config));
}

void EpochRecoveryTest::setUp() {
  dbg::currentLevel = log_level_;
  initConfig();

  const EpochMetaData metadata(storage_set_, rep_);
  auto deps = std::make_unique<MockEpochRecoveryDependencies>(this);
  erm_ = std::make_unique<EpochRecovery>(LOG_ID,
                                         epoch_,
                                         metadata,
                                         getNodesConfiguration(),
                                         std::move(deps),
                                         tail_optimized_);
}

void EpochRecoveryTest::checkRecoveryState(ERMState expected_state) {
  auto state = erm_->getState();
  ASSERT_EQ(expected_state, state);
  const auto& rs = erm_->getRecoverySet();

  // further checking some internal invariants
  switch (state) {
    case ERMState::SEAL_OR_INACTIVE:
      ASSERT_EQ(ESN_INVALID, erm_->getDigestStart());
      ASSERT_FALSE(erm_->digestComplete());
      break;
    case ERMState::DIGEST:
      ASSERT_NE(ESN_INVALID, erm_->getDigestStart());
      ASSERT_TRUE(erm_->isActive());
      ASSERT_FALSE(erm_->digestComplete());
      ASSERT_EQ(0, erm_->mutationSetSize());
      break;
    case ERMState::MUTATION:
      ASSERT_TRUE(erm_->isActive());
      ASSERT_TRUE(erm_->digestComplete());
      ASSERT_NE(0, erm_->mutationSetSize());
      ASSERT_GT(erm_->getMutators().size(), 0);
      ASSERT_EQ(
          erm_->mutationSetSize(), rs.countShardsInState(NState::MUTATABLE));
      {
        // mutation set should not intersect w/ shards in draining
        auto mutation_set = rs.getNodesInState(NState::MUTATABLE);
        bool intersect = std::any_of(
            draining_shards_.begin(), draining_shards_.end(), [&](ShardID s) {
              return mutation_set.count(s) > 0;
            });
        ASSERT_FALSE(intersect);
      }
      break;
    case ERMState::CLEAN:
      ASSERT_TRUE(erm_->isActive());
      ASSERT_TRUE(erm_->digestComplete());
      ASSERT_NE(0, erm_->mutationSetSize());
      // all mutators should be completed
      ASSERT_EQ(0, erm_->getMutators().size());
      break;
    case ERMState::ADVANCE_LCE:
      ASSERT_TRUE(erm_->isActive());
      ASSERT_TRUE(erm_->digestComplete());
      ASSERT_NE(0, erm_->mutationSetSize());
      ASSERT_EQ(erm_->mutationSetSize(), rs.countShardsInState(NState::CLEAN));
      break;
    case ERMState::MAX:
      ASSERT_TRUE(false);
  }
}

#define ASSERT_NODE_STATE(_nstate, ...)                         \
  do {                                                          \
    auto set = erm_->getRecoverySet().getNodesInState(_nstate); \
    ASSERT_EQ(set, std::set<ShardID>({__VA_ARGS__}));           \
  } while (0)

std::unique_ptr<DataRecordOwnsPayload>
mockRecord(lsn_t lsn,
           uint64_t ts,
           size_t payload_size = 128,
           OffsetMap offsets = OffsetMap()) {
  return create_record(EpochRecoveryTest::LOG_ID,
                       lsn,
                       RecordType::NORMAL,
                       1,
                       std::chrono::milliseconds(ts),
                       payload_size,
                       std::move(offsets));
}

std::unique_ptr<DataRecordOwnsPayload> mockRecord(lsn_t lsn,
                                                  RecordType type,
                                                  uint32_t wave_or_seal_epoch,
                                                  uint64_t ts = 1) {
  return create_record(EpochRecoveryTest::LOG_ID,
                       lsn,
                       type,
                       wave_or_seal_epoch,
                       std::chrono::milliseconds(ts));
}

GAP_Header mockGap(ShardID shard,
                   lsn_t lo,
                   lsn_t hi,
                   read_stream_id_t rsid,
                   GapReason reason = GapReason::NO_RECORDS) {
  return GAP_Header{EpochRecoveryTest::LOG_ID,
                    rsid,
                    lo,
                    hi,
                    reason,
                    GAP_Header::DIGEST,
                    shard.shard()};
}

// a very basic scenario:
// - NodeSet {N1, N2, N3}, replication 2
// - esn 1 was fully replicated on N1 and N2 with local LNG == 1
// - N3 is not available and does not participated in recovery
// - epoch recovery will simply plug a bridge record in esn_t(2) to N1 and N2
TEST_F(EpochRecoveryTest, Basic) {
  setUp();
  OffsetMap om;
  om.setCounter(BYTE_OFFSET, 19);
  erm_->onSealed(N1, esn_t(1), esn_t(1), om, folly::none);
  ASSERT_FALSE(erm_->isActive());
  checkRecoveryState(ERMState::SEAL_OR_INACTIVE);
  erm_->activate(prev_tail_);
  ASSERT_TRUE(erm_->isActive());
  // no f-majority despite activated
  checkRecoveryState(ERMState::SEAL_OR_INACTIVE);
  ASSERT_NODE_STATE(NState::DIGESTING);
  ASSERT_NODE_STATE(NState::SEALING, N2, N3);
  ASSERT_NODE_STATE(NState::SEALED, N1);

  // with N2 reply, there is f-majority and erm should start digesting on
  // N1 and N2
  erm_->onSealed(N2, esn_t(1), esn_t(1), om, folly::none);
  checkRecoveryState(ERMState::DIGEST);
  ASSERT_NODE_STATE(NState::DIGESTING, N1, N2);
  ASSERT_NODE_STATE(NState::SEALING, N3);

  ASSERT_EQ(esn_t(1), erm_->getDigestStart());
  ASSERT_EQ(esn_t(1), erm_->getLastKnownGood());

  // START sent
  erm_->onMessageSent(N1, MessageType::START, E::OK, read_stream_id_t(1));
  erm_->onMessageSent(N2, MessageType::START, E::OK, read_stream_id_t(2));

  // STARTED recevied
  erm_->onDigestStreamStarted(N1, read_stream_id_t(1), lsn(epoch_, 1), E::OK);
  erm_->onDigestStreamStarted(N2, read_stream_id_t(2), lsn(epoch_, 1), E::OK);
  erm_->onDigestRecord(N1, read_stream_id_t(1), mockRecord(lsn(epoch_, 1), 9));
  erm_->onDigestRecord(N2, read_stream_id_t(2), mockRecord(lsn(epoch_, 1), 9));
  erm_->onDigestGap(
      N1,
      mockGap(N1, lsn(epoch_, 2), lsn(epoch_, ESN_MAX), read_stream_id_t(1)));
  erm_->onDigestGap(
      N2,
      mockGap(N2, lsn(epoch_, 2), lsn(epoch_, ESN_MAX), read_stream_id_t(2)));

  // N1, N2 became DIGESTED then MUTATABLE
  ASSERT_NODE_STATE(NState::MUTATABLE, N1, N2);
  ASSERT_NODE_STATE(NState::SEALING, N3);

  // we have incomplete f-majority in digest, grace period timer should be
  // started
  ASSERT_TRUE(erm_->getGracePeriodTimer()->isActive());
  checkRecoveryState(ERMState::DIGEST);
  static_cast<MockTimer*>(erm_->getGracePeriodTimer())->trigger();

  // begin mutation once grace period expires
  checkRecoveryState(ERMState::MUTATION);
  ASSERT_EQ(2, erm_->mutationSetSize());
  // the only Mutator is to insert bridge record
  ASSERT_EQ(1, erm_->getMutators().size());

  // examine the mutator, it should be for plugging the bridge record at
  // esn 2
  const auto& mutator = *erm_->getMutators().at(esn_t(2));
  // nothing to amend, no gap-copy conflict
  ASSERT_TRUE(mutator.getAmendSet().empty());
  ASSERT_TRUE(mutator.getConflictSet().empty());
  ASSERT_TRUE(mutator.getStoreHeader().flags &
              STORE_Header::WRITTEN_BY_RECOVERY);
  ASSERT_TRUE(mutator.getStoreHeader().flags & STORE_Header::HOLE);
  ASSERT_TRUE(mutator.getStoreHeader().flags & STORE_Header::BRIDGE);

  // mutator is done
  erm_->onMutationComplete(esn_t(2), E::OK, ShardID());
  ASSERT_EQ(0, erm_->getMutators().size());
  checkRecoveryState(ERMState::CLEAN);
  ASSERT_NODE_STATE(NState::CLEANING, N1, N2);
  ASSERT_NODE_STATE(NState::SEALING, N3);

  // CLEAN messages should be sent to N1, N2
  erm_->onMessageSent(N1, MessageType::CLEAN, E::OK);
  erm_->onMessageSent(N2, MessageType::CLEAN, E::OK);

  // CLEANED received
  erm_->onCleaned(N2, E::OK, Seal());
  checkRecoveryState(ERMState::CLEAN);
  erm_->onCleaned(N1, E::OK, Seal());

  // epoch is consistent, should be advaning LCE
  checkRecoveryState(ERMState::ADVANCE_LCE);
  ASSERT_NODE_STATE(NState::CLEAN, N1, N2);
  ASSERT_NODE_STATE(NState::SEALING, N3);

  // check tail record
  ASSERT_TRUE(lce_tail_.isValid());
  ASSERT_EQ(lsn(epoch_, 1), lce_tail_.header.lsn);
  ASSERT_EQ(9, lce_tail_.header.timestamp);
  ASSERT_FALSE(lce_tail_.containOffsetWithinEpoch());
  OffsetMap offsets_to_add;
  offsets_to_add.setCounter(BYTE_OFFSET, 19);
  OffsetMap expected_offsets =
      OffsetMap::mergeOffsets(prev_tail_.offsets_map_, offsets_to_add);
  ASSERT_EQ(expected_offsets, lce_tail_.offsets_map_);

  // reply from epoch store
  erm_->onLastCleanEpochUpdated(E::OK, epoch_, lce_tail_);

  // epoch recovery should get finished
  ASSERT_TRUE(finished_);
  ASSERT_EQ(E::OK, result_.st);
  ASSERT_TRUE(lce_tail_.sameContent(result_.tail));
}

// similar to the basic test, but node changes authoritative status
// causing state machine to be restarted
TEST_F(EpochRecoveryTest, RestartWhenAuthoritativeStatusChanges) {
  setUp();
  OffsetMap om;
  om.setCounter(BYTE_OFFSET, 19);
  erm_->onSealed(N1, esn_t(1), esn_t(1), om, folly::none);
  checkRecoveryState(ERMState::SEAL_OR_INACTIVE);
  erm_->activate(prev_tail_);
  erm_->onSealed(N2, esn_t(1), esn_t(1), om, folly::none);
  checkRecoveryState(ERMState::DIGEST);
  ASSERT_NODE_STATE(NState::DIGESTING, N1, N2);
  ASSERT_NODE_STATE(NState::SEALING, N3);
  erm_->onMessageSent(N1, MessageType::START, E::OK, read_stream_id_t(1));
  erm_->onMessageSent(N2, MessageType::START, E::OK, read_stream_id_t(2));
  erm_->onDigestStreamStarted(N1, read_stream_id_t(1), lsn(epoch_, 1), E::OK);
  erm_->onDigestStreamStarted(N2, read_stream_id_t(2), lsn(epoch_, 1), E::OK);
  erm_->onDigestRecord(N1, read_stream_id_t(1), mockRecord(lsn(epoch_, 1), 9));
  erm_->onDigestRecord(N2, read_stream_id_t(2), mockRecord(lsn(epoch_, 1), 9));
  erm_->onDigestGap(
      N1,
      mockGap(N1, lsn(epoch_, 2), lsn(epoch_, ESN_MAX), read_stream_id_t(1)));
  erm_->onDigestGap(
      N2,
      mockGap(N2, lsn(epoch_, 2), lsn(epoch_, ESN_MAX), read_stream_id_t(2)));

  // N1, N2 became DIGESTED then MUTATABLE
  ASSERT_NODE_STATE(NState::MUTATABLE, N1, N2);
  ASSERT_NODE_STATE(NState::SEALING, N3);
  ASSERT_TRUE(erm_->getGracePeriodTimer()->isActive());
  static_cast<MockTimer*>(erm_->getGracePeriodTimer())->trigger();
  checkRecoveryState(ERMState::MUTATION);
  // mutator is done
  erm_->onMutationComplete(esn_t(2), E::OK, ShardID());
  checkRecoveryState(ERMState::CLEAN);

  // N3's authoritative status changed
  erm_->setNodeAuthoritativeStatus(N3, AuthoritativeStatus::UNDERREPLICATION);
  // epoch recovery shouldn't be restarted since N3 is not part of the mutation
  // and cleaning set
  checkRecoveryState(ERMState::CLEAN);
  // change N3's status back to fully authoritative, shouldn't affect ERM
  // state either
  erm_->setNodeAuthoritativeStatus(
      N3, AuthoritativeStatus::FULLY_AUTHORITATIVE);
  checkRecoveryState(ERMState::CLEAN);

  // CLEAN messages should be sent to N1, N2
  erm_->onMessageSent(N1, MessageType::CLEAN, E::OK);
  erm_->onMessageSent(N2, MessageType::CLEAN, E::OK);
  erm_->onCleaned(N2, E::OK, Seal());
  erm_->onCleaned(N1, E::OK, Seal());
  // epoch is consistent, should be advaning LCE
  checkRecoveryState(ERMState::ADVANCE_LCE);
  ASSERT_NODE_STATE(NState::CLEAN, N1, N2);
  ASSERT_NODE_STATE(NState::SEALING, N3);
  // N1's authoritative status changed
  erm_->setNodeAuthoritativeStatus(N1, AuthoritativeStatus::UNDERREPLICATION);
  // epoch recovery should be restarted even in advance lce stage;
  // new state: N1: SEALING (U), N2: SEALED, N3: SEALING
  // epoch recovery machine should be in SEAL_OR_INACTIVE state since it doesn't
  // have f-majority
  ASSERT_NODE_STATE(NState::SEALING, N1, N3);
  ASSERT_NODE_STATE(NState::SEALED, N2);
  checkRecoveryState(ERMState::SEAL_OR_INACTIVE);
}

// a storage node sends a hole plug below LNG due to bugs or corruption,
// the tail record computed might be a hole plug EpochRecovery should handle
// that by reporting a dataloss.
TEST_F(EpochRecoveryTest, UnexpectedHolePlugBelowLNG) {
  // turn off data dependent assert to make test not crash
  dbg::assertOnData = false;
  storage_set_ = StorageSet({N1, N2, N3});
  rep_ = ReplicationProperty({{NodeLocationScope::NODE, 2}});

  setUp();
  OffsetMap om;
  om.setCounter(BYTE_OFFSET, 19);
  erm_->onSealed(N1, esn_t(1), esn_t(1), om, folly::none);
  erm_->activate(prev_tail_);
  erm_->onSealed(N2, esn_t(1), esn_t(1), om, folly::none);
  erm_->onMessageSent(N1, MessageType::START, E::OK, read_stream_id_t(1));
  erm_->onMessageSent(N2, MessageType::START, E::OK, read_stream_id_t(2));
  erm_->onDigestStreamStarted(N1, read_stream_id_t(1), lsn(epoch_, 1), E::OK);
  erm_->onDigestStreamStarted(N2, read_stream_id_t(2), lsn(epoch_, 1), E::OK);

  // N1 sends a unexpected hole plug at LNG
  erm_->onDigestRecord(
      N1,
      read_stream_id_t(1),
      mockRecord(lsn(epoch_, 1), RecordType::HOLE, /*recovery_epoch*/ 2));
  // N2 sends a normal record but with lower precedence
  erm_->onDigestRecord(N2, read_stream_id_t(2), mockRecord(lsn(epoch_, 1), 9));
  erm_->onDigestGap(
      N1,
      mockGap(N1, lsn(epoch_, 2), lsn(epoch_, ESN_MAX), read_stream_id_t(1)));
  erm_->onDigestGap(
      N2,
      mockGap(N2, lsn(epoch_, 2), lsn(epoch_, ESN_MAX), read_stream_id_t(2)));
  ASSERT_NODE_STATE(NState::MUTATABLE, N1, N2);
  ASSERT_NODE_STATE(NState::SEALING, N3);
  ASSERT_TRUE(erm_->getGracePeriodTimer()->isActive());
  static_cast<MockTimer*>(erm_->getGracePeriodTimer())->trigger();
  checkRecoveryState(ERMState::MUTATION);
  // mutator is done
  erm_->onMutationComplete(esn_t(2), E::OK, ShardID());
  checkRecoveryState(ERMState::CLEAN);

  // CLEAN messages should be sent to N1, N2
  erm_->onMessageSent(N1, MessageType::CLEAN, E::OK);
  erm_->onMessageSent(N2, MessageType::CLEAN, E::OK);
  erm_->onCleaned(N2, E::OK, Seal());
  erm_->onCleaned(N1, E::OK, Seal());
  // epoch is consistent, should be advaning LCE
  checkRecoveryState(ERMState::ADVANCE_LCE);

  // check tail record, it should indicate a dataloss gap
  ASSERT_TRUE(lce_tail_.isValid());
  ASSERT_EQ(lsn(epoch_, 1), lce_tail_.header.lsn);
  ASSERT_EQ(0, lce_tail_.header.timestamp);
  ASSERT_FALSE(lce_tail_.containOffsetWithinEpoch());
  ASSERT_TRUE(lce_tail_.header.flags & TailRecordHeader::GAP);

  // it should still maintain the byte offset though
  OffsetMap offsets_to_add;
  offsets_to_add.setCounter(BYTE_OFFSET, 19);
  OffsetMap expected_offsets =
      OffsetMap::mergeOffsets(prev_tail_.offsets_map_, offsets_to_add);
  ASSERT_EQ(expected_offsets, lce_tail_.offsets_map_);

  // reply from epoch store
  erm_->onLastCleanEpochUpdated(E::OK, epoch_, lce_tail_);

  // epoch recovery should get finished
  ASSERT_TRUE(finished_);
  ASSERT_EQ(E::OK, result_.st);
  ASSERT_TRUE(lce_tail_.sameContent(result_.tail));
}

TEST_F(EpochRecoveryTest, MutationSetShouldNotContainDrainingNodes) {
  // N2 is draining and cannot store copies, but is able to participate
  // in digest
  storage_set_ = {N1, N2, N3};
  rep_.assign({{NodeLocationScope::NODE, 2}});
  draining_shards_ = {N2};

  setUp();
  OffsetMap om;
  om.setCounter(BYTE_OFFSET, 19);
  // N3 will be absent in the beginning, causing recovery to get stuck
  // even if it does have f-majority (authoritative_incomplete) digest
  erm_->onSealed(N1, esn_t(0), esn_t(1), om, folly::none);
  checkRecoveryState(ERMState::SEAL_OR_INACTIVE);
  // with N2 reply, there is f-majority and erm should start digesting on
  // N1 and N2
  erm_->onSealed(N2, esn_t(0), esn_t(1), om, folly::none);
  erm_->activate(prev_tail_);

  checkRecoveryState(ERMState::DIGEST);
  ASSERT_NODE_STATE(NState::DIGESTING, N1, N2);
  ASSERT_NODE_STATE(NState::SEALING, N3);

  ASSERT_EQ(esn_t(1), erm_->getDigestStart());
  ASSERT_EQ(esn_t(0), erm_->getLastKnownGood());

  // START sent
  erm_->onMessageSent(N1, MessageType::START, E::OK, read_stream_id_t(1));
  erm_->onMessageSent(N2, MessageType::START, E::OK, read_stream_id_t(2));

  // STARTED recevied
  erm_->onDigestStreamStarted(N1, read_stream_id_t(1), lsn(epoch_, 0), E::OK);
  erm_->onDigestStreamStarted(N2, read_stream_id_t(2), lsn(epoch_, 0), E::OK);

  // N1 and N2 sent different stuff: N1 sent a hole while N2 sent a record of
  // higer precedence
  erm_->onDigestRecord(
      N1,
      read_stream_id_t(1),
      mockRecord(lsn(epoch_, 1), RecordType::HOLE, /*recovery_epoch*/ 24));
  erm_->onDigestRecord(
      N2,
      read_stream_id_t(2),
      mockRecord(lsn(epoch_, 1), RecordType::MUTATED, /*recovery_epoch*/ 26));
  erm_->onDigestGap(
      N1,
      mockGap(N1, lsn(epoch_, 2), lsn(epoch_, ESN_MAX), read_stream_id_t(1)));
  erm_->onDigestGap(
      N2,
      mockGap(N2, lsn(epoch_, 2), lsn(epoch_, ESN_MAX), read_stream_id_t(2)));

  // N1, N2 became DIGESTED, however, since N2 is in draining, only N1 can be
  // MUTATABLE
  ASSERT_NODE_STATE(NState::MUTATABLE, N1);
  ASSERT_NODE_STATE(NState::DIGESTED, N2);
  ASSERT_NODE_STATE(NState::SEALING, N3);

  // we have incomplete f-majority in digest, however, we do not have enough
  // nodes in mutation set to mutate copies, we should keep waiting for N3 and
  // should _not_ start the grace period timer
  ASSERT_FALSE(erm_->getGracePeriodTimer()->isActive());
  checkRecoveryState(ERMState::DIGEST);

  // finally N3 finished sealing and became SEALED
  erm_->onSealed(N3, esn_t(0), esn_t(1), om, folly::none);
  // should start digesting on N3
  erm_->onMessageSent(N3, MessageType::START, E::OK, read_stream_id_t(3));
  erm_->onDigestStreamStarted(N3, read_stream_id_t(3), lsn(epoch_, 0), E::OK);

  erm_->onDigestGap(
      N3,
      mockGap(N3, lsn(epoch_, 2), lsn(epoch_, ESN_MAX), read_stream_id_t(3)));

  // N3 will become MUTATABLE
  ASSERT_NODE_STATE(NState::MUTATABLE, N1, N3);
  ASSERT_NODE_STATE(NState::DIGESTED, N2);

  // mutation stage should be started
  checkRecoveryState(ERMState::MUTATION);
  ASSERT_EQ(2, erm_->mutationSetSize());

  // two mutators, one for re-replicating esn 1, and the other for bridge record
  // at esn 2
  ASSERT_EQ(2, erm_->getMutators().size());

  {
    const auto& mutator = *erm_->getMutators().at(esn_t(1));
    // we need to resolve conflict on N1: hole -> record, and
    // place a copy in N3
    ASSERT_EQ(StorageSet({N1, N3}), mutator.getMutationSet());
    ASSERT_EQ(std::set<ShardID>({N1}), mutator.getConflictSet());
    ASSERT_TRUE(mutator.getAmendSet().empty());

    ASSERT_TRUE(mutator.getStoreHeader().flags &
                STORE_Header::WRITTEN_BY_RECOVERY);
    ASSERT_FALSE(mutator.getStoreHeader().flags & STORE_Header::HOLE);
    ASSERT_FALSE(mutator.getStoreHeader().flags & STORE_Header::BRIDGE);
  }

  // esn 2 should be the bridge record
  {
    const auto& mutator = *erm_->getMutators().at(esn_t(2));
    ASSERT_TRUE(mutator.getAmendSet().empty());
    ASSERT_TRUE(mutator.getConflictSet().empty());
    ASSERT_EQ(StorageSet({N1, N3}), mutator.getMutationSet());
    ASSERT_TRUE(mutator.getStoreHeader().flags &
                STORE_Header::WRITTEN_BY_RECOVERY);
    ASSERT_TRUE(mutator.getStoreHeader().flags & STORE_Header::HOLE);
    ASSERT_TRUE(mutator.getStoreHeader().flags & STORE_Header::BRIDGE);
  }

  // mutator is done
  erm_->onMutationComplete(esn_t(2), E::OK, ShardID());
  erm_->onMutationComplete(esn_t(1), E::OK, ShardID());
  ASSERT_EQ(0, erm_->getMutators().size());
  checkRecoveryState(ERMState::CLEAN);
  ASSERT_NODE_STATE(NState::CLEANING, N1, N3);
  ASSERT_NODE_STATE(NState::DIGESTED, N2);

  // CLEAN messages should be sent to N1, N3
  erm_->onMessageSent(N1, MessageType::CLEAN, E::OK);
  erm_->onMessageSent(N3, MessageType::CLEAN, E::OK);

  // CLEANED received
  erm_->onCleaned(N3, E::OK, Seal());
  checkRecoveryState(ERMState::CLEAN);
  erm_->onCleaned(N1, E::OK, Seal());

  // epoch is consistent, should be advaning LCE
  checkRecoveryState(ERMState::ADVANCE_LCE);
  ASSERT_NODE_STATE(NState::CLEAN, N1, N3);
  ASSERT_NODE_STATE(NState::DIGESTED, N2);

  // check tail record
  ASSERT_TRUE(lce_tail_.isValid());
  ASSERT_EQ(lsn(epoch_, 1), lce_tail_.header.lsn);

  // reply from epoch store
  erm_->onLastCleanEpochUpdated(E::OK, epoch_, lce_tail_);

  // epoch recovery should get finished
  ASSERT_TRUE(finished_);
  ASSERT_EQ(E::OK, result_.st);
  ASSERT_TRUE(lce_tail_.sameContent(result_.tail));
}

//
// NodeSet {N1, N2}, replication 2

// Digest will receive record in this sequence
//
//      (1,1) (1,2)  (1,3) (1,4)
// ------------------------------
// N1  |  x     x
// N2  |  x    o(3)          x
//
// each record will have payload size of 10
// sealed message for N1: lng_: 1 epoch_size: 40 (which is inaccurate)
// sealed message for N2: lng_: 1 epoch_size: 40 (which is inaccurate)
//
// in the end state, we should have the correct byteoffset in the tail
// which is previous_tail_record.byteoffset + 20, only (1,1) and (1,4) should
// be counted in the byteoffset.
TEST_F(EpochRecoveryTest, correctByteOffset) {
  storage_set_ = StorageSet({N1, N2});
  setUp();
  OffsetMap om;
  om.setCounter(BYTE_OFFSET, 40);
  erm_->onSealed(N1, esn_t(1), esn_t(1), om, folly::none);
  ASSERT_FALSE(erm_->isActive());
  checkRecoveryState(ERMState::SEAL_OR_INACTIVE);
  erm_->activate(prev_tail_);
  // no f-majority despite activated
  checkRecoveryState(ERMState::SEAL_OR_INACTIVE);
  ASSERT_NODE_STATE(NState::DIGESTING);
  ASSERT_NODE_STATE(NState::SEALING, N2);
  ASSERT_NODE_STATE(NState::SEALED, N1);

  // with N2 reply, there is f-majority and erm should start digesting on
  // N1 and N2
  erm_->onSealed(N2, esn_t(1), esn_t(1), om, folly::none);
  checkRecoveryState(ERMState::DIGEST);
  ASSERT_NODE_STATE(NState::DIGESTING, N1, N2);

  ASSERT_EQ(esn_t(1), erm_->getDigestStart());
  ASSERT_EQ(esn_t(1), erm_->getLastKnownGood());

  // START sent
  erm_->onMessageSent(N1, MessageType::START, E::OK, read_stream_id_t(1));
  erm_->onMessageSent(N2, MessageType::START, E::OK, read_stream_id_t(2));

  // STARTED recevied
  erm_->onDigestStreamStarted(N1, read_stream_id_t(1), lsn(epoch_, 1), E::OK);
  erm_->onDigestStreamStarted(N2, read_stream_id_t(2), lsn(epoch_, 1), E::OK);
  // esn(1)
  erm_->onDigestRecord(
      N1,
      read_stream_id_t(1),
      mockRecord(lsn(epoch_, 1), 9, 10, OffsetMap({{BYTE_OFFSET, 10}})));
  erm_->onDigestRecord(
      N2,
      read_stream_id_t(2),
      mockRecord(lsn(epoch_, 1), 9, 10, OffsetMap({{BYTE_OFFSET, 10}})));

  // esn(2)
  erm_->onDigestRecord(
      N1,
      read_stream_id_t(1),
      mockRecord(lsn(epoch_, 2), 9, 10, OffsetMap({{BYTE_OFFSET, 20}})));
  erm_->onDigestRecord(
      N2,
      read_stream_id_t(2),
      mockRecord(lsn(epoch_, 2), RecordType::HOLE, /*recovery_epoch*/ 3));

  // esn(4)
  erm_->onDigestRecord(
      N2,
      read_stream_id_t(2),
      mockRecord(lsn(epoch_, 4), 9, 10, OffsetMap({{BYTE_OFFSET, 40}})));

  erm_->onDigestGap(
      N1,
      mockGap(N1, lsn(epoch_, 3), lsn(epoch_, ESN_MAX), read_stream_id_t(1)));
  erm_->onDigestGap(
      N2,
      mockGap(N2, lsn(epoch_, 5), lsn(epoch_, ESN_MAX), read_stream_id_t(2)));

  ASSERT_NODE_STATE(NState::MUTATABLE, N1, N2);
  checkRecoveryState(ERMState::MUTATION);

  // mutator is done
  erm_->onMutationComplete(esn_t(2), E::OK, ShardID());
  erm_->onMutationComplete(esn_t(3), E::OK, ShardID());
  erm_->onMutationComplete(esn_t(4), E::OK, ShardID());
  erm_->onMutationComplete(esn_t(5), E::OK, ShardID());
  checkRecoveryState(ERMState::CLEAN);

  // CLEAN messages should be sent to N1, N2
  erm_->onMessageSent(N1, MessageType::CLEAN, E::OK);
  erm_->onMessageSent(N2, MessageType::CLEAN, E::OK);
  erm_->onCleaned(N2, E::OK, Seal());
  erm_->onCleaned(N1, E::OK, Seal());

  // epoch is consistent, should be advaning LCE
  checkRecoveryState(ERMState::ADVANCE_LCE);
  ASSERT_NODE_STATE(NState::CLEAN, N1, N2);

  // check tail record
  ASSERT_TRUE(lce_tail_.isValid());
  ASSERT_EQ(lsn(epoch_, 4), lce_tail_.header.lsn);
  ASSERT_EQ(9, lce_tail_.header.timestamp);
  ASSERT_FALSE(lce_tail_.containOffsetWithinEpoch());
  // each record will have payload size of 10, and only (1, 1) and (1, 4) will
  // appear in the final recovered log, so only 20 bytes in this epoch,
  // also shouldn't use the inaccurate epoch_size from sealed message, which is
  // 40 in this case
  OffsetMap offsets_to_add;
  offsets_to_add.setCounter(BYTE_OFFSET, 20);
  OffsetMap expected_offsets =
      OffsetMap::mergeOffsets(prev_tail_.offsets_map_, offsets_to_add);
  ASSERT_EQ(expected_offsets, lce_tail_.offsets_map_);
}

} // anonymous namespace
