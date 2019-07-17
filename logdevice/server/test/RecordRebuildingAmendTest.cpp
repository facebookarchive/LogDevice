/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/RecordRebuildingAmend.h"

#include <folly/Random.h>
#include <gtest/gtest.h>

#include "logdevice/common/FlowGroup.h"
#include "logdevice/common/LocalLogStoreRecordFormat.h"
#include "logdevice/common/protocol/STORE_Message.h"
#include "logdevice/common/test/TestUtil.h"

using namespace facebook::logdevice;

// Shortcuts for writing ShardIDs
#define N0 ShardID(0, 0)
#define N10 ShardID(10, 0)
#define N20 ShardID(20, 0)
#define N30 ShardID(30, 0)
#define N40 ShardID(40, 0)
#define N50 ShardID(50, 0)
#define N60 ShardID(60, 0)
#define N70 ShardID(70, 0)
#define N80 ShardID(80, 0)

namespace facebook { namespace logdevice {

// This can't be in anonymous namespace because it's a friend of SocketCallback
// and BWAvailableCallback.
class RecordRebuildingAmendMockSocket {
 public:
  explicit RecordRebuildingAmendMockSocket(NodeID n,
                                           std::unique_ptr<FlowGroup> fgp)
      : node_id(n), flow_group(std::move(fgp)) {}

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

  const NodeID node_id;

  folly::IntrusiveList<SocketCallback, &SocketCallback::listHook_> close_cbs;

  // For the purposes of this test, flow_group is just a list of
  // BWAvailableCallback's.
  std::unique_ptr<FlowGroup> flow_group{nullptr};

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
                       bool /*unused*/) const override {
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

#define ASSERT_MAP_EQ(map1, map2)                 \
  {                                               \
    ASSERT_EQ(map1.size(), map2.size());          \
    FlushTokenMap::const_iterator i, j;           \
    for (auto& entry : map1) {                    \
      ASSERT_EQ(1, map2.count(entry.first));      \
      ASSERT_EQ(entry.second, map2[entry.first]); \
    }                                             \
  }

class TestRecordRebuildingAmend : public RecordRebuildingAmend,
                                  public RecordRebuildingOwner {
 public:
  using MockSender = SenderTestProxy<TestRecordRebuildingAmend>;

  TestRecordRebuildingAmend(
      std::shared_ptr<ReplicationScheme> replication,
      STORE_Header storeHeader,
      LocalLogStoreRecordFormat::flags_t flags,
      copyset_t newCopyset,
      copyset_t amendRecipients,
      node_index_t my_node_index,
      uint32_t rebuildingWave,
      const MockNodeAvailabilityChecker* node_availability)
      : RecordRebuildingAmend(LSN,
                              /*shard=*/0,
                              this /* owner */,
                              replication,
                              storeHeader,
                              flags,
                              newCopyset,
                              amendRecipients,
                              rebuildingWave,
                              node_availability),
        my_node_index_(my_node_index),
        settings_(create_default_settings<Settings>()) {
    sender_ = std::make_unique<MockSender>(this);
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

  RecordRebuildingAmendMockSocket& getSocket(node_index_t n) {
    if (!sockets_.count(n)) {
      sockets_.emplace(
          std::piecewise_construct,
          std::forward_as_tuple(n),
          std::forward_as_tuple(
              NodeID(n),
              std::make_unique<FlowGroup>(
                  std::make_unique<NwShapingFlowGroupDeps>(nullptr))));
    }
    return sockets_.at(n);
  }

  FlushTokenMap getFlushTokenMap() {
    FlushTokenMap flushTokenMap;
    for (int i = 0; i < stages_.size(); i++) {
      for (auto& r : stages_[i].recipients) {
        auto key = std::make_pair(r.shard_.node(), r.server_instance_id);
        flushTokenMap.emplace(key, r.flush_token);
      }
    }
    return flushTokenMap;
  }

  node_index_t my_node_index_;

  std::set<node_index_t> fail_to_send_;

  bool retry_timer_active_ = false;
  bool store_timer_active_ = false;
  bool complete_ = false;
  bool deferred_complete_ = false;
  bool store_complete_ = false;

  std::unique_ptr<AmendSelfStorageTask> amend_self_task_;
  std::vector<std::pair<std::unique_ptr<Message>, NodeID>> messages_sent_;

  Settings settings_;
  UpdateableSettings<RebuildingSettings> rebuildingSettings_;
  RebuildingSet rebuilding_set_;

  std::map<node_index_t, RecordRebuildingAmendMockSocket> sockets_;

  std::set<node_index_t> removed_from_config_;

 protected:
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
  const RebuildingSet& getRebuildingSet() const override {
    return rebuilding_set_;
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
  void activateStoreTimer() override {
    store_timer_active_ = true;
  }
  void resetStoreTimer() override {
    store_timer_active_ = false;
  }
  void activateRetryTimer() override {
    EXPECT_FALSE(retry_timer_active_);
    retry_timer_active_ = true;
  }
  void resetRetryTimer() override {
    retry_timer_active_ = false;
  }
  bool retryTimerIsActive() {
    return retry_timer_active_;
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
    ADD_FAILURE();
  }
  void onCopysetInvalid(lsn_t lsn) override {
    ADD_FAILURE();
  }
  void
  onAllAmendsReceived(lsn_t lsn,
                      std::unique_ptr<FlushTokenMap> flushTokenMap) override {
    ASSERT_FALSE(complete_);
    complete_ = true;
  }
};

class RecordRebuildingAmendTest : public ::testing::Test {
 public:
  RecordRebuildingAmendTest() {
    replication_ = std::make_shared<ReplicationScheme>();
    replication_->epoch_metadata.replication =
        ReplicationProperty({{NodeLocationScope::NODE, 3}});
    setStorageSet({N10, N20, N30, N40, N50, N60, N70});

    dbg::assertOnData = true;
  }

  void setStorageSet(StorageSet storage_set) {
    replication_->epoch_metadata.shards = storage_set;
    replication_->nodeset_state = std::make_shared<NodeSetState>(
        storage_set, LOG_ID, NodeSetState::HealthCheck::DISABLED);
  }

  STORE_Header createStoreHeader(uint32_t wave) {
    return {RecordID(LSN, LOG_ID),
            TIMESTAMP,
            LNG,
            wave,
            (STORE_Header::BUFFERED_WRITER_BLOB |
             STORE_Header::CHECKSUM_PARITY | STORE_Header::REBUILDING),
            0,
            0,
            (copyset_size_t)copyset.size(),
            0,
            NodeID()};
  }

  static STORED_Header createStoredHeader(uint32_t wave,
                                          Status status = E::OK) {
    return {RecordID(LSN, LOG_ID),
            wave,
            status,
            NodeID(),
            STORED_Header::REBUILDING};
  }

  static void checkAmendMessage(const Message* m0, uint32_t wave) {
    auto m = dynamic_cast<const STORE_Message*>(m0);
    ASSERT_NE(nullptr, m);
    EXPECT_EQ(RecordID(LSN, LOG_ID), m->getHeader().rid);
    EXPECT_EQ(TIMESTAMP, m->getHeader().timestamp);
    EXPECT_EQ(LNG, m->getHeader().last_known_good);
    EXPECT_EQ(wave, m->getHeader().wave);
    EXPECT_TRUE(m->getHeader().flags & STORE_Header::REBUILDING);
    EXPECT_TRUE(m->getHeader().flags & STORE_Header::AMEND);
    EXPECT_FALSE(m->getHeader().flags & STORE_Header::DRAINED);
    const PayloadHolder* payload = m->getPayloadHolder();
    ASSERT_EQ(nullptr, payload);
  }

  copyset_t copyset;
  MockNodeAvailabilityChecker node_availability_;
  std::shared_ptr<ReplicationScheme> replication_;
};

} // anonymous namespace

TEST_F(RecordRebuildingAmendTest, Basic) {
  // Copyset is {40, 30, 20}, 30 is rebuilding, I am 20, Store went to 50
  copyset.push_back(N40);
  copyset.push_back(N20);
  copyset.push_back(N50);

  TestRecordRebuildingAmend r(replication_,
                              createStoreHeader(321),
                              0 /* flags */,
                              copyset,
                              {N40, N20},
                              20,
                              1,
                              &node_availability_);

  r.start();
  ASSERT_EQ(1, r.messages_sent_.size());
  EXPECT_EQ(NodeID(40), r.messages_sent_[0].second);

  std::map<ShardID, int> expected_availability_requests = {{N40, 1}};
  EXPECT_EQ(expected_availability_requests, node_availability_.requests_seen);
  checkAmendMessage(r.messages_sent_[0].first.get(), 321);
  r.onStored(createStoredHeader(321),
             N40,
             LSN_INVALID,
             0,
             REBUILDING_ID,
             SERVER_INSTANCE_ID,
             2345);

  ASSERT_NE(nullptr, r.amend_self_task_);
  EXPECT_EQ(LOG_ID, r.amend_self_task_->logid);
  EXPECT_EQ(LSN, r.amend_self_task_->lsn);
  EXPECT_EQ(11, r.amend_self_task_->restartVersion);
  EXPECT_FALSE(r.complete_);

  r.onAmendedSelf(E::OK, 3456);
  EXPECT_TRUE(r.complete_);
  FlushTokenMap map2 = {
      {{20, SERVER_INSTANCE_ID}, 3456}, {{40, SERVER_INSTANCE_ID}, 2345}};
  ASSERT_MAP_EQ(r.getFlushTokenMap(), map2);
}

TEST_F(RecordRebuildingAmendTest, StaleStoreReject) {
  // Copyset is {40, 30, 20}, 30 is rebuilding, I am 20, Store went to 50
  copyset.push_back(N40);
  copyset.push_back(N20);
  copyset.push_back(N50);

  TestRecordRebuildingAmend r(replication_,
                              createStoreHeader(321),
                              0 /* flags */,
                              copyset,
                              {N40, N20},
                              20,
                              3,
                              &node_availability_);

  // Even before a flush from 50 arrives, we get a stale stored reply
  // and it should be rejected
  r.onStored(createStoredHeader(321),
             N50,
             LSN_INVALID,
             1 /*rebuilding wave*/,
             REBUILDING_ID,
             SERVER_INSTANCE_ID,
             2345);

  r.start();
  ASSERT_EQ(1, r.messages_sent_.size());
  EXPECT_EQ(NodeID(40), r.messages_sent_[0].second);

  std::map<ShardID, int> expected_availability_requests = {{N40, 1}};
  EXPECT_EQ(expected_availability_requests, node_availability_.requests_seen);
  checkAmendMessage(r.messages_sent_[0].first.get(), 321);
  r.onStored(createStoredHeader(321),
             N40,
             LSN_INVALID,
             0,
             REBUILDING_ID,
             SERVER_INSTANCE_ID,
             2345);

  EXPECT_EQ(LOG_ID, r.amend_self_task_->logid);
  EXPECT_EQ(LSN, r.amend_self_task_->lsn);
  EXPECT_EQ(11, r.amend_self_task_->restartVersion);
  EXPECT_FALSE(r.complete_);

  r.onAmendedSelf(E::OK, 3456);
  EXPECT_TRUE(r.complete_);
  FlushTokenMap map2 = {
      {{20, SERVER_INSTANCE_ID}, 3456}, {{40, SERVER_INSTANCE_ID}, 2345}};
  ASSERT_MAP_EQ(r.getFlushTokenMap(), map2);
}

TEST_F(RecordRebuildingAmendTest, NoRemoteNodesToAmend) {
  // Copyset is {40, 30, 20}, 40, 30 are rebuilding,
  // I am 20, Store went to 50, 60
  copyset.push_back(N20);
  copyset.push_back(N60);
  copyset.push_back(N50);

  TestRecordRebuildingAmend r(replication_,
                              createStoreHeader(321),
                              0 /* flags */,
                              copyset,
                              {N20},
                              20,
                              1,
                              &node_availability_);

  r.start();
  EXPECT_EQ(LOG_ID, r.amend_self_task_->logid);
  EXPECT_EQ(LSN, r.amend_self_task_->lsn);
  EXPECT_EQ(11, r.amend_self_task_->restartVersion);
  EXPECT_FALSE(r.complete_);

  r.onAmendedSelf(E::OK, 3456);
  EXPECT_TRUE(r.complete_);
  FlushTokenMap map2 = {{{20, SERVER_INSTANCE_ID}, 3456}};
  ASSERT_MAP_EQ(r.getFlushTokenMap(), map2);
}

TEST_F(RecordRebuildingAmendTest, TimeoutsAndFailures) {
  replication_->epoch_metadata.replication =
      ReplicationProperty({{NodeLocationScope::NODE, 5}});
  copyset.push_back(N30);
  copyset.push_back(N40);
  copyset.push_back(N50);
  copyset.push_back(N60);
  copyset.push_back(N70);

  TestRecordRebuildingAmend r(replication_,
                              createStoreHeader(321),
                              0 /* flags */,
                              copyset,
                              {N30, N40, N50},
                              30,
                              1,
                              &node_availability_);

  r.start();
  EXPECT_EQ(NodeID(50), r.messages_sent_[0].second);
  EXPECT_EQ(NodeID(40), r.messages_sent_[1].second);
  EXPECT_EQ(2, r.messages_sent_.size());
  ASSERT_EQ(nullptr, r.amend_self_task_);
  EXPECT_TRUE(r.store_timer_active_);

  // first amend failed
  r.messages_sent_.clear();
  r.onStored(createStoredHeader(321, E::DISABLED),
             N50,
             LSN_INVALID,
             0,
             REBUILDING_ID,
             SERVER_INSTANCE_ID,
             0);
  EXPECT_EQ(0, r.messages_sent_.size()); // waiting for retry timer
  EXPECT_TRUE(r.retry_timer_active_);

  // second AMEND succeeded
  r.messages_sent_.clear();
  r.onStored(createStoredHeader(321),
             N40,
             LSN_INVALID,
             0,
             REBUILDING_ID,
             SERVER_INSTANCE_ID,
             1234);
  EXPECT_EQ(0, r.messages_sent_.size()); // still waiting for retry timer

  // retry timer fired
  r.messages_sent_.clear();
  r.fireRetryTimer();
  ASSERT_EQ(1, r.messages_sent_.size()); // retried first AMEND
  EXPECT_EQ(NodeID(50), r.messages_sent_[0].second);
  EXPECT_FALSE(r.retry_timer_active_);

  // store timer fired
  r.messages_sent_.clear();
  r.fireStoreTimer();
  ASSERT_EQ(1, r.messages_sent_.size()); // retried first AMEND again
  EXPECT_EQ(NodeID(50), r.messages_sent_[0].second);
  EXPECT_FALSE(r.retry_timer_active_);
  EXPECT_TRUE(r.store_timer_active_);

  // first AMEND succeeded
  r.messages_sent_.clear();
  r.onStored(createStoredHeader(321),
             N50,
             LSN_INVALID,
             0,
             REBUILDING_ID,
             SERVER_INSTANCE_ID,
             3000);
  EXPECT_EQ(0, r.messages_sent_.size()); // no more communication needed
  EXPECT_FALSE(r.retry_timer_active_);
  EXPECT_FALSE(r.store_timer_active_);

  // AmendSelfStorageTask failed
  r.amend_self_task_ = nullptr;
  r.onAmendedSelf(E::FAILED);
  ASSERT_EQ(nullptr, r.amend_self_task_); // waiting for retry timer
  EXPECT_TRUE(r.retry_timer_active_);
  EXPECT_FALSE(r.store_timer_active_);

  // retry timer fired
  r.fireRetryTimer();
  ASSERT_NE(nullptr, r.amend_self_task_); // retried amending self

  // AmendSelfStorageTask failed again just in case
  r.amend_self_task_ = nullptr;
  r.onAmendedSelf(E::DISABLED);
  ASSERT_EQ(nullptr, r.amend_self_task_); // waiting for retry timer

  // retry timer fired
  r.fireRetryTimer();
  ASSERT_NE(nullptr, r.amend_self_task_); // retried amending self

  // AmendSelfStorageTask succeeded
  r.onAmendedSelf(E::OK, 2222);

  // done, finally
  EXPECT_TRUE(r.complete_);
  FlushTokenMap map2 = {{{30, SERVER_INSTANCE_ID}, 2222},
                        {{40, SERVER_INSTANCE_ID}, 1234},
                        {{50, SERVER_INSTANCE_ID}, 3000}};
  ASSERT_MAP_EQ(r.getFlushTokenMap(), map2);
}

TEST_F(RecordRebuildingAmendTest, Drained) {
  // Copyset is {40, 30, 20}, 20 is rebuilding in relocate mode,
  // I am 20, Store went to 50.
  replication_->relocate_local_records = true;
  copyset.push_back(N40);
  copyset.push_back(N30);
  copyset.push_back(N50);

  TestRecordRebuildingAmend r(replication_,
                              createStoreHeader(321),
                              0 /* flags */,
                              copyset,
                              {N40, N30, N20},
                              20,
                              1,
                              &node_availability_);

  r.start();
  ASSERT_EQ(2, r.messages_sent_.size());
  EXPECT_EQ(NodeID(30), r.messages_sent_[0].second);
  EXPECT_EQ(NodeID(40), r.messages_sent_[1].second);

  std::map<ShardID, int> expected_availability_requests = {{N40, 1}, {N30, 1}};
  EXPECT_EQ(expected_availability_requests, node_availability_.requests_seen);
  checkAmendMessage(r.messages_sent_[0].first.get(), 321);
  r.onStored(createStoredHeader(321),
             N40,
             LSN_INVALID,
             0,
             REBUILDING_ID,
             SERVER_INSTANCE_ID,
             2345);
  checkAmendMessage(r.messages_sent_[1].first.get(), 321);
  r.onStored(createStoredHeader(321),
             N30,
             LSN_INVALID,
             0,
             REBUILDING_ID,
             SERVER_INSTANCE_ID,
             4567);

  ASSERT_NE(nullptr, r.amend_self_task_);
  EXPECT_EQ(LOG_ID, r.amend_self_task_->logid);
  EXPECT_EQ(LSN, r.amend_self_task_->lsn);
  EXPECT_EQ(11, r.amend_self_task_->restartVersion);
  EXPECT_FALSE(r.complete_);

  const WriteOp* write_op;
  ASSERT_EQ(1, r.amend_self_task_->getWriteOps(&write_op, 1));
  const PutWriteOp* put_op = static_cast<const PutWriteOp*>(write_op);
  LocalLogStoreRecordFormat::flags_t flags;
  ASSERT_EQ(
      0, LocalLogStoreRecordFormat::parseFlags(put_op->record_header, &flags));
  ASSERT_TRUE(flags & LocalLogStoreRecordFormat::FLAG_DRAINED);

  r.onAmendedSelf(E::OK, 3456);
  EXPECT_TRUE(r.complete_);
  FlushTokenMap map2 = {{{20, SERVER_INSTANCE_ID}, 3456},
                        {{30, SERVER_INSTANCE_ID}, 4567},
                        {{40, SERVER_INSTANCE_ID}, 2345}};
  ASSERT_MAP_EQ(r.getFlushTokenMap(), map2);
}

TEST_F(RecordRebuildingAmendTest, Dirty) {
  // Copyset is {40, 30, 20}, 30 and 20 ard dirty and rebuilding in
  // restore mode, I am 20, Store went to 50.
  copyset.push_back(N20);
  copyset.push_back(N40);
  copyset.push_back(N50);

  TestRecordRebuildingAmend r(replication_,
                              createStoreHeader(321),
                              0 /* flags */,
                              copyset,
                              {N40, N20},
                              20,
                              1,
                              &node_availability_);

  r.start();
  ASSERT_EQ(1, r.messages_sent_.size());
  EXPECT_EQ(NodeID(40), r.messages_sent_[0].second);

  std::map<ShardID, int> expected_availability_requests = {{N40, 1}};
  EXPECT_EQ(expected_availability_requests, node_availability_.requests_seen);
  checkAmendMessage(r.messages_sent_[0].first.get(), 321);
  r.onStored(createStoredHeader(321),
             N40,
             LSN_INVALID,
             0,
             REBUILDING_ID,
             SERVER_INSTANCE_ID,
             2345);

  ASSERT_NE(nullptr, r.amend_self_task_);
  EXPECT_EQ(LOG_ID, r.amend_self_task_->logid);
  EXPECT_EQ(LSN, r.amend_self_task_->lsn);
  EXPECT_EQ(11, r.amend_self_task_->restartVersion);
  EXPECT_FALSE(r.complete_);

  const WriteOp* write_op;
  ASSERT_EQ(1, r.amend_self_task_->getWriteOps(&write_op, 1));
  const PutWriteOp* put_op = static_cast<const PutWriteOp*>(write_op);
  LocalLogStoreRecordFormat::flags_t flags;
  ASSERT_EQ(
      0, LocalLogStoreRecordFormat::parseFlags(put_op->record_header, &flags));
  ASSERT_FALSE(flags & LocalLogStoreRecordFormat::FLAG_DRAINED);

  r.onAmendedSelf(E::OK, 3456);
  EXPECT_TRUE(r.complete_);
  FlushTokenMap map2 = {
      {{20, SERVER_INSTANCE_ID}, 3456}, {{40, SERVER_INSTANCE_ID}, 2345}};
  ASSERT_MAP_EQ(r.getFlushTokenMap(), map2);
}
}} // namespace facebook::logdevice
