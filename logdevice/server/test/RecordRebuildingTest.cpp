/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <folly/Random.h>
#include <gtest/gtest.h>

#include "logdevice/common/FlowGroup.h"
#include "logdevice/common/LocalLogStoreRecordFormat.h"
#include "logdevice/common/NodeSetState.h"
#include "logdevice/common/protocol/STORE_Message.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/server/RecordRebuildingStore.h"

using namespace facebook::logdevice;

// Shortcuts for writing ShardIDs
#define N10 ShardID(10, 0)
#define N20 ShardID(20, 0)
#define N30 ShardID(30, 0)
#define N40 ShardID(40, 0)
#define N50 ShardID(50, 0)
#define N60 ShardID(60, 0)
#define N70 ShardID(70, 0)
#define N80 ShardID(80, 0)
#define N90 ShardID(90, 0)

namespace facebook { namespace logdevice {

// This can't be in anonymous namespace because it's a friend of SocketCallback
// and BWAvailableCallback.
class RecordRebuildingMockSocket {
 public:
  explicit RecordRebuildingMockSocket(NodeID n, std::unique_ptr<FlowGroup> fgp)
      : flow_group(std::move(fgp)), node_id(n) {}

  void simulateClose() {
    while (!flow_group->priorityq_.empty()) {
      auto& cb = flow_group->priorityq_.front();
      cb.deactivate();
      cb.cancelled(E::PEER_CLOSED);
    }

    auto cbs_moved = std::move(close_cbs);
    while (!cbs_moved.empty()) {
      auto& cb = cbs_moved.front();
      cbs_moved.pop_front();
      cb(E::PEER_CLOSED, Address(node_id));
    }
  }

  void simulateBandwidthAvailable() {
    std::mutex mu;
    while (!flow_group->priorityq_.empty()) {
      auto& cb = flow_group->priorityq_.front();
      cb.deactivate();
      cb(*flow_group, mu);
    }
  }

  folly::IntrusiveList<SocketCallback, &SocketCallback::listHook_> close_cbs;

  // For the purposes of this test, flow_group is just a list of
  // BWAvailableCallback's.
  std::unique_ptr<FlowGroup> flow_group{nullptr};
  const NodeID node_id;

  // Set this to E::CBREGISTERED to simulate traffic shaping; callback will be
  // added to bandwidth_cbs. Set to something like E::UNROUTABLE to simulate
  // just being unable to send.
  Status status = E::OK;
};

namespace {

const logid_t LOG_ID(8008);
const lsn_t LSN(123);
const uint64_t TIMESTAMP(1234567890ul);
const esn_t LNG(332233);
const log_rebuilding_id_t REBUILDING_ID(11);
const ServerInstanceId
    SERVER_INSTANCE_ID(std::chrono::duration_cast<std::chrono::milliseconds>(
                           std::chrono::system_clock::now().time_since_epoch())
                           .count());

class MockNodeAvailabilityChecker : public NodeAvailabilityChecker {
 public:
  NodeStatus checkNode(NodeSetState* nodeset_state,
                       ShardID shard,
                       StoreChainLink* destination_out,
                       bool ignore_nodeset_state,
                       bool /*allow_unencrypted_connections*/) const override {
    ++const_cast<std::map<ShardID, int>&>(requests_seen)[shard];
    if (!ignore_nodeset_state) {
      const auto now = std::chrono::steady_clock::now();
      auto not_available_until =
          nodeset_state->checkNotAvailableUntil(shard, now);
      if (not_available_until != std::chrono::steady_clock::time_point::min()) {
        return NodeStatus::NOT_AVAILABLE;
      }
    }
    destination_out->destination = shard;
    return NodeStatus::AVAILABLE;
  }

  std::map<ShardID, int> requests_seen;
};

class TestRecordRebuildingStore : public RecordRebuildingStore,
                                  public RecordRebuildingOwner {
 public:
  using MockSender = SenderTestProxy<TestRecordRebuildingStore>;

  struct CopysetPick {
    std::vector<ShardID> existing_part;
    std::vector<ShardID> new_part;
  };

  TestRecordRebuildingStore(RawRecord record,
                            std::shared_ptr<ReplicationScheme> replication,
                            std::initializer_list<ShardID> rebuilding_set,
                            node_index_t my_node_index,
                            const NodeAvailabilityChecker* node_availability)
      : RecordRebuildingStore(/*block_id=*/0,
                              /*shard=*/0,
                              std::move(record),
                              this /* owner */,
                              replication,
                              nullptr /* scratch_payload_holder */,
                              node_availability),
        my_node_index_(my_node_index),
        settings_(create_default_settings<Settings>()),
        rebuildingSettings_(create_default_settings<RebuildingSettings>()) {
    sender_ = std::make_unique<MockSender>(this);
    for (ShardID s : rebuilding_set) {
      rebuilding_set_.shards.emplace(
          s, RebuildingNodeInfo(RebuildingMode::RESTORE));
    }
  }

  const RebuildingCopyset& amendRecipients() const {
    return amendRecipients_;
  }

  void setDirty(ShardID s, DataClass dc) {
    auto shard_kv = rebuilding_set_.shards.find(s);
    ld_check(shard_kv != rebuilding_set_.shards.end());
    auto& dc_time_ranges = shard_kv->second.dc_dirty_ranges[dc];
    auto dirty_start =
        RecordTimestamp(std::chrono::milliseconds(TIMESTAMP - 1000));
    auto dirty_end =
        RecordTimestamp(std::chrono::milliseconds(TIMESTAMP + 1000));
    dc_time_ranges.insert(RecordTimeInterval(dirty_start, dirty_end));
  }

  void fireRetryTimer() {
    EXPECT_TRUE(retry_timer_active_);
    retry_timer_active_ = false;
    onRetryTimeout();
  }

  void fireStoreTimer() {
    EXPECT_TRUE(store_timer_active_);
    store_timer_active_ = false;
    onStoreTimeout();
  }

  bool canSendToImpl(const Address&, TrafficClass, BWAvailableCallback&) {
    return true;
  }

  int sendMessageImpl(std::unique_ptr<Message>&& msg,
                      const Address& addr,
                      BWAvailableCallback* bw_cb,
                      SocketCallback* close_cb) {
    ld_check(!addr.isClientAddress());
    ld_check(close_cb != nullptr);

    NodeID nid = addr.id_.node_;

    auto& sock = getSocket(nid.index());
    if (sock.status == E::CBREGISTERED) {
      ld_check(bw_cb != nullptr);
      sock.flow_group->push(*bw_cb, Priority::BACKGROUND);
    }
    if (sock.status == E::OK) {
      sock.close_cbs.push_back(*close_cb);
      messages_sent_.emplace_back(std::move(msg), nid);
    } else {
      messages_sent_.emplace_back(nullptr, nid);
    }

    err = sock.status;
    return sock.status == E::OK ? 0 : -1;
  }

  RecordRebuildingMockSocket& getSocket(node_index_t n) {
    if (!sockets_.count(n)) {
      sockets_.emplace(
          std::piecewise_construct,
          std::forward_as_tuple(n),
          std::forward_as_tuple(
              NodeID(n, n * 2),
              std::make_unique<FlowGroup>(
                  std::make_unique<NwShapingFlowGroupDeps>(nullptr))));
    }
    return sockets_.at(n);
  }

  RebuildingSet rebuilding_set_;
  node_index_t my_node_index_;

  std::queue<CopysetPick> copysets_to_pick_;

  bool retry_timer_active_ = false;
  bool store_timer_active_ = false;
  bool complete_ = false;
  bool deferred_complete_ = false;

  std::unique_ptr<AmendSelfStorageTask> amend_self_task_;
  std::vector<std::pair<std::unique_ptr<Message>, NodeID>> messages_sent_;

  Settings settings_;
  UpdateableSettings<RebuildingSettings> rebuildingSettings_;

  std::map<node_index_t, RecordRebuildingMockSocket> sockets_;

  std::set<node_index_t> removed_from_config_;

 protected:
  const RebuildingSet& getRebuildingSet() const override {
    return rebuilding_set_;
  }
  lsn_t getRestartVersion() const override {
    return 11;
  }
  lsn_t getRebuildingVersion() const override {
    return 11;
  }
  log_rebuilding_id_t getLogRebuildingId() const override {
    return REBUILDING_ID;
  }
  node_index_t getMyNodeIndex() const override {
    return my_node_index_;
  }
  ServerInstanceId getServerInstanceId() const override {
    return SERVER_INSTANCE_ID;
  }
  logid_t getLogID() const override {
    return LOG_ID;
  }
  bool isStorageShardInConfig(ShardID shard) const override {
    if (removed_from_config_.count(shard.node())) {
      return false;
    }
    return shard.node() < 100;
  }
  const Settings& getSettings() const override {
    return settings_;
  }
  UpdateableSettings<RebuildingSettings>
  getRebuildingSettings() const override {
    return rebuildingSettings_;
  }
  int pickCopysetImpl() override {
    if (copysets_to_pick_.empty()) {
      return -1;
    }
    auto cs = copysets_to_pick_.front();
    copysets_to_pick_.pop();
    std::vector<ShardID> copyset(newCopyset_.begin(), newCopyset_.end());
    EXPECT_EQ(cs.existing_part, copyset);
    for (ShardID i : cs.new_part) {
      newCopyset_.push_back(i);
    }
    return 0;
  }
  void activateRetryTimer() override {
    retry_timer_active_ = true;
  }
  void resetRetryTimer() override {
    retry_timer_active_ = false;
  }
  bool isStoreTimerActive() override {
    return store_timer_active_;
  }
  void activateStoreTimer() override {
    store_timer_active_ = true;
  }
  void resetStoreTimer() override {
    store_timer_active_ = false;
  }
  void putAmendSelfTask(std::unique_ptr<AmendSelfStorageTask> task) override {
    EXPECT_EQ(nullptr, amend_self_task_);
    amend_self_task_ = std::move(task);
  }
  void deferredComplete() override {
    ASSERT_FALSE(complete_);
    complete_ = true;
    deferred_complete_ = true;
  }

  void
  onAllStoresReceived(lsn_t lsn,
                      std::unique_ptr<FlushTokenMap> flushTokenMap) override {
    ASSERT_FALSE(complete_);
    complete_ = true;
  }
  void onCopysetInvalid(lsn_t lsn) override {
    ADD_FAILURE();
  }
  void
  onAllAmendsReceived(lsn_t lsn,
                      std::unique_ptr<FlushTokenMap> flushTokenMap) override {
    ADD_FAILURE();
  }
};

class RecordRebuildingStoreTest
    : public ::testing::Test,
      public ::testing::WithParamInterface<bool /*rebuild-store-durability*/> {
 public:
  RecordRebuildingStoreTest() {
    replication_ = std::make_shared<ReplicationScheme>();
    replication_->epoch_metadata.replication =
        ReplicationProperty({{NodeLocationScope::NODE, 3}});
    setStorageSet({N10, N20, N30, N40, N50, N60, N70, N80, N90});

    dbg::assertOnData = true;
  }

  void setStorageSet(StorageSet shards) {
    replication_->epoch_metadata.setShards(shards);
    replication_->nodeset_state = std::make_shared<NodeSetState>(
        shards, LOG_ID, NodeSetState::HealthCheck::DISABLED);
  }

  static RawRecord createRecord(uint32_t wave, std::vector<ShardID> copyset) {
    STORE_Header h = {
        RecordID(LSN, LOG_ID),
        TIMESTAMP,
        LNG,
        wave,
        STORE_Header::BUFFERED_WRITER_BLOB | STORE_Header::CHECKSUM_PARITY,
        0,
        0,
        (copyset_size_t)copyset.size(),
        0,
        NodeID()};
    std::vector<StoreChainLink> cs(copyset.size());
    std::transform(copyset.begin(), copyset.end(), cs.begin(), [](ShardID s) {
      return (StoreChainLink){s, ClientID()};
    });
    std::string buf;
    LocalLogStoreRecordFormat::formRecordHeader(
        h,
        cs.data(),
        &buf,
        false /* shard_in_copyset */,
        std::map<KeyType, std::string>());
    buf += "payload";
    void* blob = malloc(buf.size());
    memcpy(blob, &buf[0], buf.size());
    Slice s = Slice(blob, buf.size());
    return RawRecord(LSN, s, true);
  }

  static STORED_Header createStoredHeader(uint32_t wave,
                                          Status status = E::OK) {
    return {RecordID(LSN, LOG_ID),
            wave,
            status,
            NodeID(),
            STORED_Header::REBUILDING};
  }

  static STORE_Header createStoreHeader(uint32_t wave) {
    // RecordRebuildingStore only looks at wave and flags.
    return {
        RecordID(LSN, LOG_ID),
        0,        // timestamp
        esn_t(0), // lng
        wave,
        STORE_Header::REBUILDING,
        0,        // nsync
        0,        // copyset_offset
        0,        // copyset_size
        0,        // timeout_ms
        NodeID(), // sequencer_node_id
    };
  }

  static void checkStoreMessage(const Message* m0, uint32_t wave, bool amend) {
    auto m = dynamic_cast<const STORE_Message*>(m0);
    ASSERT_NE(nullptr, m);
    EXPECT_EQ(RecordID(LSN, LOG_ID), m->getHeader().rid);
    EXPECT_EQ(TIMESTAMP, m->getHeader().timestamp);
    EXPECT_EQ(LNG, m->getHeader().last_known_good);
    EXPECT_EQ(wave, m->getHeader().wave);
    EXPECT_TRUE(m->getHeader().flags & STORE_Header::REBUILDING);
    EXPECT_EQ(amend, !!(m->getHeader().flags & STORE_Header::AMEND));
    const PayloadHolder* payload = m->getPayloadHolder();
    if (amend) {
      ASSERT_EQ(nullptr, payload);
    } else {
      ASSERT_NE(nullptr, payload);
      ASSERT_NE(
          nullptr, const_cast<PayloadHolder*>(payload)->getPayload().data());
    }
  }

  MockNodeAvailabilityChecker node_availability_;
  std::shared_ptr<ReplicationScheme> replication_;
};

} // anonymous namespace

TEST_F(RecordRebuildingStoreTest, Basic) {
  // Copyset is {40, 30, 20}, 30 is rebuilding, I am 20.
  TestRecordRebuildingStore r(createRecord(321, {N30, N20, N40}),
                              replication_,
                              {N30},
                              20,
                              &node_availability_);
  // Re-replicate to 50.
  r.copysets_to_pick_.push({{N20, N40}, {N50}});

  r.start();
  EXPECT_EQ(0, r.copysets_to_pick_.size());
  ASSERT_EQ(1, r.messages_sent_.size());
  EXPECT_EQ(NodeID(50, 0), r.messages_sent_[0].second);
  std::map<ShardID, int> expected_availability_requests = {{N50, 1}};
  EXPECT_EQ(expected_availability_requests, node_availability_.requests_seen);
  checkStoreMessage(r.messages_sent_[0].first.get(), 321, false);
  r.messages_sent_.clear();
  r.onStored(createStoredHeader(321), N50, LSN_INVALID, 0, REBUILDING_ID);

  ASSERT_EQ(0, r.messages_sent_.size());
  ASSERT_EQ(nullptr, r.amend_self_task_);
  EXPECT_TRUE(r.complete_);
}

TEST_F(RecordRebuildingStoreTest, TimeoutsAndFailures) {
  replication_->epoch_metadata.replication =
      ReplicationProperty({{NodeLocationScope::NODE, 5}});
  TestRecordRebuildingStore r(createRecord(321, {N10, N20, N30, N40, N50}),
                              replication_,
                              {N10, N20},
                              30,
                              &node_availability_);
  r.copysets_to_pick_.push({{N30, N40, N50}, {N60, N70}});

  r.start();
  EXPECT_EQ(1, r.messages_sent_.size()); // sent one STORE
  EXPECT_EQ(NodeID(70, 0), r.messages_sent_[0].second);

  // retry timer fired
  r.messages_sent_.clear();
  EXPECT_FALSE(r.retry_timer_active_);
  r.fireStoreTimer();
  ASSERT_EQ(1, r.messages_sent_.size()); // retried STORE
  EXPECT_EQ(NodeID(70, 0), r.messages_sent_[0].second);

  // STORE succeeded
  r.messages_sent_.clear();
  r.onStored(createStoredHeader(321), N70, LSN_INVALID, 0, REBUILDING_ID);
  EXPECT_EQ(1, r.messages_sent_.size()); // sent second store
  EXPECT_EQ(NodeID(60, 0), r.messages_sent_[0].second);
  EXPECT_FALSE(r.retry_timer_active_);
  EXPECT_TRUE(r.store_timer_active_);

  // STORE failed
  r.messages_sent_.clear();
  r.onStored(
      createStoredHeader(321, E::DISABLED), N60, LSN_INVALID, 0, REBUILDING_ID);
  EXPECT_EQ(0, r.messages_sent_.size()); // waiting for retry timer

  // STORED for previous copy caught up, should be ignored
  r.messages_sent_.clear();
  r.onStored(
      createStoredHeader(321, E::DISABLED), N70, LSN_INVALID, 0, REBUILDING_ID);
  EXPECT_EQ(0, r.messages_sent_.size()); // still waiting for retry timer

  // retry timer fired
  r.messages_sent_.clear();
  r.fireRetryTimer();
  ASSERT_EQ(1, r.messages_sent_.size()); // retried STORE
  EXPECT_EQ(NodeID(60, 0), r.messages_sent_[0].second);

  // STORE succeeded
  r.messages_sent_.clear();
  r.onStored(createStoredHeader(321), N60, LSN_INVALID, 0, REBUILDING_ID);

  ASSERT_EQ(nullptr, r.amend_self_task_);
  EXPECT_TRUE(r.complete_);
}

TEST_F(RecordRebuildingStoreTest, Extras) {
  TestRecordRebuildingStore r(createRecord(321, {N10, N20, N30, N40, N50, N60}),
                              replication_,
                              {N30},
                              50,
                              &node_availability_);
  r.copysets_to_pick_.push({{N50}, {N30, N70}});

  r.start();
  EXPECT_EQ(0, r.copysets_to_pick_.size());
  ASSERT_EQ(1, r.messages_sent_.size());
  EXPECT_EQ(NodeID(70, 0), r.messages_sent_[0].second);

  r.messages_sent_.clear();
  r.onStored(createStoredHeader(321), N70, LSN_INVALID, 0, REBUILDING_ID);
  ASSERT_EQ(1, r.messages_sent_.size());
  EXPECT_EQ(NodeID(30, 0), r.messages_sent_[0].second);
  ASSERT_EQ(nullptr, r.amend_self_task_);

  r.messages_sent_.clear();
  r.onStored(createStoredHeader(321), N30, LSN_INVALID, 0, REBUILDING_ID);
  ASSERT_EQ(0, r.messages_sent_.size());

  EXPECT_TRUE(r.complete_);
}

TEST_F(RecordRebuildingStoreTest, InstaFail) {
  // Malformed record.
  TestRecordRebuildingStore r(RawRecord(LSN, Slice(), true),
                              replication_,
                              {N30},
                              20,
                              &node_availability_);
  ASSERT_FALSE(r.deferred_complete_);
  r.start();
  ASSERT_TRUE(r.deferred_complete_);
}

TEST_F(RecordRebuildingStoreTest, PickFailures) {
  TestRecordRebuildingStore r(createRecord(321, {N10, N20, N30}),
                              replication_,
                              {N10, N30},
                              20,
                              &node_availability_);

  // Fail to pick copyset.
  r.start();
  ASSERT_EQ(0, r.messages_sent_.size());
  ASSERT_EQ(nullptr, r.amend_self_task_);
  EXPECT_FALSE(r.store_timer_active_);

  // After timeout, pick copyset and send a STORE.
  r.copysets_to_pick_.push({{N20}, {N40, N50}});
  r.fireRetryTimer();
  ASSERT_EQ(0, r.copysets_to_pick_.size());
  ASSERT_EQ(1, r.messages_sent_.size());
  EXPECT_EQ(NodeID(50, 0), r.messages_sent_[0].second);

  // STORED ok, send another STORE.
  r.copysets_to_pick_.push({{N20}, {N60, N70}}); // won't be picked yet
  r.messages_sent_.clear();
  r.onStored(createStoredHeader(321), N50, LSN_INVALID, 0, REBUILDING_ID);
  ASSERT_EQ(1, r.copysets_to_pick_.size());
  ASSERT_EQ(1, r.messages_sent_.size());
  EXPECT_EQ(NodeID(40, 0), r.messages_sent_[0].second);

  // STORED with NOSPC, pick another copyset and send a new STORE.
  r.messages_sent_.clear();
  r.onStored(createStoredHeader(321, Status::NOSPC),
             N40,
             LSN_INVALID,
             0,
             REBUILDING_ID);
  ASSERT_EQ(NodeSetState::NodeSetState::NotAvailableReason::NO_SPC,
            replication_->nodeset_state->getNotAvailableReason(N40));
  ASSERT_EQ(0, r.copysets_to_pick_.size());
  ASSERT_EQ(1, r.messages_sent_.size());
  EXPECT_EQ(NodeID(70, 0), r.messages_sent_[0].second);

  // Time out waiting for STORED, fail to resend STORE.
  r.copysets_to_pick_.push({{N20}, {N50, N60}}); // won't be picked yet
  replication_->nodeset_state->setNotAvailableUntil(
      N70,
      std::chrono::steady_clock::now() + std::chrono::hours(100),
      NodeSetState::NodeSetState::NotAvailableReason::NO_SPC);
  r.messages_sent_.clear();
  r.fireStoreTimer();
  ASSERT_EQ(1, r.copysets_to_pick_.size());
  EXPECT_EQ(2, node_availability_.requests_seen[N70]);
  ASSERT_EQ(0, r.messages_sent_.size());

  // After timeout, pick another copyset and send STORE.
  r.fireRetryTimer();
  ASSERT_EQ(0, r.copysets_to_pick_.size());
  ASSERT_EQ(1, r.messages_sent_.size());
  EXPECT_EQ(NodeID(60, 0), r.messages_sent_[0].second);

  // STORED ok, send next STORE.
  r.messages_sent_.clear();
  r.onStored(createStoredHeader(321), N60, LSN_INVALID, 0, REBUILDING_ID);
  ASSERT_EQ(1, r.messages_sent_.size());
  EXPECT_EQ(NodeID(50, 0), r.messages_sent_[0].second);
  ASSERT_EQ(nullptr, r.amend_self_task_);

  // STORED ok, amend self.
  r.messages_sent_.clear();
  r.onStored(createStoredHeader(321), N50, LSN_INVALID, 0, REBUILDING_ID);
  ASSERT_EQ(0, r.messages_sent_.size());

  EXPECT_TRUE(r.complete_);
}

TEST_F(RecordRebuildingStoreTest, VariousFailures) {
  replication_->epoch_metadata.replication =
      ReplicationProperty({{NodeLocationScope::NODE, 5}});
  TestRecordRebuildingStore r(createRecord(21, {N10, N20, N30, N40, N50}),
                              replication_,
                              {N10, N40},
                              20,
                              &node_availability_);

  // Pick copyset, send first STORE. Sending postponed by traffic shaping.
  r.copysets_to_pick_.push({{N20, N30, N50}, {N60, N70}});
  r.getSocket(70).status = E::CBREGISTERED;
  r.start();
  ASSERT_EQ(0, r.copysets_to_pick_.size());
  ASSERT_EQ(1, r.messages_sent_.size());
  EXPECT_EQ(NodeID(70, 0), r.messages_sent_[0].second);
  r.messages_sent_.clear();
  EXPECT_FALSE(r.retry_timer_active_);
  EXPECT_FALSE(r.store_timer_active_);

  // Traffic shaping lets the message through.
  r.getSocket(70).status = E::OK;
  r.getSocket(70).simulateBandwidthAvailable();
  ASSERT_EQ(1, r.messages_sent_.size());
  EXPECT_EQ(NodeID(70, 0), r.messages_sent_[0].second);
  r.messages_sent_.clear();
  EXPECT_FALSE(r.retry_timer_active_);
  EXPECT_TRUE(r.store_timer_active_);

  // Sending failed. Start retry timer.
  r.onStoreSent(E::PEER_CLOSED, createStoreHeader(21), N70, 11, 1);
  ASSERT_EQ(0, r.messages_sent_.size());
  EXPECT_TRUE(r.retry_timer_active_);
  // Having store timer running here is unnecessary, but that's how
  // RecordRebuildingStore is implemented now.
  EXPECT_TRUE(r.store_timer_active_);

  // Store timer fired. Resend store.
  r.fireStoreTimer();
  ASSERT_EQ(1, r.messages_sent_.size());
  EXPECT_EQ(NodeID(70, 0), r.messages_sent_[0].second);
  r.messages_sent_.clear();
  // Having retry timer running here is unnecessary, but currently
  // RecordRebuildingStore doesn't bother cancelling it in this situation.
  EXPECT_TRUE(r.retry_timer_active_);
  EXPECT_TRUE(r.store_timer_active_);

  // Retry timer fired. Do nothing because store is already in flight.
  r.fireRetryTimer();
  ASSERT_EQ(0, r.messages_sent_.size());
  EXPECT_FALSE(r.retry_timer_active_);
  EXPECT_TRUE(r.store_timer_active_);

  // Connection closed. Start retry timer.
  r.getSocket(70).simulateClose();
  ASSERT_EQ(0, r.messages_sent_.size());
  EXPECT_TRUE(r.retry_timer_active_);
  // Once again RecordRebuildingStore didn't bother to cancel a timer. Meh.
  EXPECT_TRUE(r.store_timer_active_);

  // Retry timer fired. Re-sending the message fails.
  r.getSocket(70).status = E::PEER_CLOSED;
  r.fireRetryTimer();
  ASSERT_EQ(1, r.messages_sent_.size());
  EXPECT_EQ(NodeID(70, 0), r.messages_sent_[0].second);
  r.messages_sent_.clear();
  EXPECT_TRUE(r.retry_timer_active_);
  EXPECT_TRUE(r.store_timer_active_);

  // Retry timer failed again. Re-sending delayed by traffic shaping.
  r.getSocket(70).status = E::CBREGISTERED;
  r.fireRetryTimer();
  ASSERT_EQ(1, r.messages_sent_.size());
  EXPECT_EQ(NodeID(70, 0), r.messages_sent_[0].second);
  r.messages_sent_.clear();
  EXPECT_FALSE(r.retry_timer_active_);
  EXPECT_TRUE(r.store_timer_active_);

  // Store timer fired. Node was removed from config. Start new wave.
  // RecordRebuildingStore must be extremely annoyed by now.
  r.removed_from_config_.insert(70);
  r.copysets_to_pick_.push({{N20, N30, N50}, {N60, N80}});
  r.fireStoreTimer();
  ASSERT_EQ(0, r.copysets_to_pick_.size());
  ASSERT_EQ(1, r.messages_sent_.size());
  EXPECT_EQ(NodeID(80, 0), r.messages_sent_[0].second);
  r.messages_sent_.clear();
  EXPECT_FALSE(r.retry_timer_active_);
  EXPECT_TRUE(r.store_timer_active_);

  // Store succeeded. Send next STORE.
  r.onStored(createStoredHeader(21), N80, 11, 2, REBUILDING_ID);
  ASSERT_EQ(1, r.messages_sent_.size());
  EXPECT_EQ(NodeID(60, 0), r.messages_sent_[0].second);
  r.messages_sent_.clear();
  EXPECT_FALSE(r.retry_timer_active_);
  EXPECT_TRUE(r.store_timer_active_);

  // Timed out. Re-send.
  r.fireStoreTimer();
  ASSERT_EQ(1, r.messages_sent_.size());
  EXPECT_EQ(NodeID(60, 0), r.messages_sent_[0].second);
  r.messages_sent_.clear();
  EXPECT_FALSE(r.retry_timer_active_);
  EXPECT_TRUE(r.store_timer_active_);

  // Store succeeded.
  r.onStored(createStoredHeader(21), N60, 11, 2, REBUILDING_ID);
  EXPECT_FALSE(r.store_timer_active_);

  // RecordRebuildingStore is glad that it's finally over.
  EXPECT_TRUE(r.complete_);
}

TEST_F(RecordRebuildingStoreTest, Drained) {
  // Copyset is {40, 30, 20}, 20 is rebuilding in relocate mode, I am 20.
  replication_->relocate_local_records = true;
  TestRecordRebuildingStore r(createRecord(321, {N30, N20, N40}),
                              replication_,
                              {N20},
                              20,
                              &node_availability_);
  // Re-replicate to 50.
  r.copysets_to_pick_.push({{N30, N40}, {N50}});

  r.start();
  EXPECT_EQ(0, r.copysets_to_pick_.size());
  ASSERT_EQ(1, r.messages_sent_.size());
  EXPECT_EQ(NodeID(50, 0), r.messages_sent_[0].second);
  std::map<ShardID, int> expected_availability_requests = {{N50, 1}};
  EXPECT_EQ(expected_availability_requests, node_availability_.requests_seen);
  checkStoreMessage(r.messages_sent_[0].first.get(), 321, false);
  r.messages_sent_.clear();
  r.onStored(createStoredHeader(321), N50, LSN_INVALID, 0, REBUILDING_ID);

  ASSERT_EQ(0, r.messages_sent_.size());
  ASSERT_EQ(nullptr, r.amend_self_task_);
  EXPECT_TRUE(r.complete_);
}

TEST_F(RecordRebuildingStoreTest, Dirty) {
  // Copyset is {30, 20, 40}, 30 and 20 are dirty and rebuilding in
  // restore mode, I am 20. This is a mini-rebuilding where N20 didn't
  // lose this record.
  TestRecordRebuildingStore r(createRecord(321, {N30, N20, N40}),
                              replication_,
                              {N30, N20},
                              20,
                              &node_availability_);
  r.setDirty(N30, DataClass::APPEND);
  r.setDirty(N20, DataClass::APPEND);
  // Re-replicate N30's copy to N50.
  r.copysets_to_pick_.push({{N20, N40}, {N50}});

  r.start();
  EXPECT_EQ(0, r.copysets_to_pick_.size());
  ASSERT_EQ(1, r.messages_sent_.size());
  EXPECT_EQ(NodeID(50, 0), r.messages_sent_[0].second);
  std::map<ShardID, int> expected_availability_requests = {{N50, 1}};
  RebuildingCopyset expected_amends = {N40, N20};
  EXPECT_EQ(expected_availability_requests, node_availability_.requests_seen);
  ASSERT_EQ(r.amendRecipients(), expected_amends);
  checkStoreMessage(r.messages_sent_[0].first.get(), 321, false);
  r.messages_sent_.clear();
  r.onStored(createStoredHeader(321), N50, LSN_INVALID, 0, REBUILDING_ID);

  ASSERT_EQ(0, r.messages_sent_.size());
  ASSERT_EQ(nullptr, r.amend_self_task_);
  EXPECT_TRUE(r.complete_);
}

TEST_F(RecordRebuildingStoreTest, PickFailures2) {
  replication_->epoch_metadata.replication =
      ReplicationProperty({{NodeLocationScope::NODE, 3}});
  TestRecordRebuildingStore r(createRecord(21, {N10, N20, N30}),
                              replication_,
                              {N10},
                              20,
                              &node_availability_);

  // Pick copyset, send first STORE.
  r.copysets_to_pick_.push({{N20, N30}, {N40}});
  r.start();
  ASSERT_EQ(0, r.copysets_to_pick_.size());
  ASSERT_EQ(1, r.messages_sent_.size());
  EXPECT_EQ(NodeID(40, 0), r.messages_sent_[0].second);
  r.messages_sent_.clear();
  EXPECT_FALSE(r.retry_timer_active_);
  EXPECT_TRUE(r.store_timer_active_);

  // NOSPC reply. Repick copyset.
  r.copysets_to_pick_.push({{N20, N30}, {N50}});
  r.onStored(createStoredHeader(21, Status::NOSPC),
             N40,
             LSN_INVALID,
             0,
             REBUILDING_ID);
  EXPECT_EQ(NodeSetState::NotAvailableReason::NO_SPC,
            replication_->nodeset_state->getNotAvailableReason(N40));
  ASSERT_EQ(0, r.copysets_to_pick_.size());
  ASSERT_EQ(1, r.messages_sent_.size());
  EXPECT_EQ(NodeID(50, 0), r.messages_sent_[0].second);
  r.messages_sent_.clear();
  EXPECT_FALSE(r.retry_timer_active_);
  EXPECT_TRUE(r.store_timer_active_);

  // Store succeeded. All stores done.
  r.onStored(createStoredHeader(21), N50, 11, 2, REBUILDING_ID);
  ASSERT_EQ(0, r.messages_sent_.size());
  EXPECT_FALSE(r.retry_timer_active_);
  EXPECT_FALSE(r.store_timer_active_);
  EXPECT_TRUE(r.complete_);

  auto amend = r.getRecordRebuildingAmendState();
  ASSERT_NE(nullptr, amend);
  EXPECT_EQ(LSN, amend->lsn_);
  EXPECT_EQ(replication_, amend->replication_); // compare pointers
  EXPECT_EQ(copyset_t({N20, N30, N50}), amend->newCopyset_);
  EXPECT_EQ(copyset_t({N30, N20}), amend->amendRecipients_);
  EXPECT_EQ(2, amend->rebuildingWave_);
}

}} // namespace facebook::logdevice
