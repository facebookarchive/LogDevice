/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/Appender.h"

#include <unordered_map>

#include <folly/Memory.h>
#include <gtest/gtest.h>
#include <opentracing/mocktracer/in_memory_recorder.h>
#include <opentracing/mocktracer/tracer.h>

#include "logdevice/common/ExponentialBackoffAdaptiveVariable.h"
#include "logdevice/common/LinearCopySetSelector.h"
#include "logdevice/common/NoopTraceLogger.h"
#include "logdevice/common/PassThroughCopySetManager.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/LocalLogsConfig.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/protocol/APPENDED_Message.h"
#include "logdevice/common/protocol/DELETE_Message.h"
#include "logdevice/common/protocol/RELEASE_Message.h"
#include "logdevice/common/protocol/STORED_Message.h"
#include "logdevice/common/protocol/STORE_Message.h"
#include "logdevice/common/test/CopySetSelectorTestUtil.h"
#include "logdevice/common/test/TestUtil.h"

namespace facebook { namespace logdevice {

/**
 * @file Unit tests for Appender, the state machine for storing a record on
 *       a sufficient number of storage nodes.
 */

// Convenient shortcuts for writting ShardIDs.
#define N0S0 ShardID(0, 0)
#define N1S0 ShardID(1, 0)
#define N2S0 ShardID(2, 0)
#define N3S0 ShardID(3, 0)
#define N4S0 ShardID(4, 0)
#define N5S0 ShardID(5, 0)
#define N6S0 ShardID(6, 0)
#define N7S0 ShardID(7, 0)
#define N8S0 ShardID(8, 0)
// Convenient shortcuts for writting NodeIDs.
#define N0 NodeID(0, 0)
#define N1 NodeID(1, 0)
#define N2 NodeID(2, 0)
#define N3 NodeID(3, 0)
#define N4 NodeID(4, 0)
#define N5 NodeID(5, 0)
#define N6 NodeID(6, 0)
#define N7 NodeID(7, 0)
#define N8 NodeID(8, 0)

static const logid_t LOG_ID(1111);
static const lsn_t LSN = compose_lsn(epoch_t{1}, esn_t{42});

template <typename T>
using message_map_t =
    std::unordered_map<ShardID, std::unique_ptr<T>, ShardID::Hash>;

class AppenderTest : public ::testing::Test {
 private:
  class MockNodeSetState;
  friend class MockNodeSetState;

  class MockAppender;
  friend class MockAppender;

  class TestCopySetSelector;
  friend class TestCopySetSelector;
  class TestCopySetSelectorDeps;

 public:
  class MockAppendRequest;
  friend class MockAppendRequest;

  AppenderTest()
      : activeAppenders_(100), settings_(create_default_settings<Settings>()) {
    settings_.disable_chain_sending = true;
    dbg::assertOnData = true;
  }

  ~AppenderTest() override {
    // Without this, Appender asserts about aborting during shutdown if it
    // hasn't already been reaped, but not all tests feel like bringing the
    // Appender to completion
    is_accepting_work_ = false;
  }

  // ShardIDs in the storage set.
  std::vector<ShardID>
      shards_{N0S0, N1S0, N2S0, N3S0, N4S0, N5S0, N6S0, N7S0, N8S0};
  std::set<NodeID> dead_nodes_;
  UpdateableConfig config_;
  AppenderMap activeAppenders_;
  Settings settings_;
  std::shared_ptr<CopySetManager> copyset_manager_;
  std::unique_ptr<TestCopySetSelectorDeps> deps_;

  // Used by tests to determine what the next calls to checkNodeSet() should
  // return.
  Status nodeset_status_{E::OK};

  copyset_size_t replication_{3};
  copyset_size_t extras_{2};
  copyset_size_t synced_{0};

  // Used by tests to determine what the next calls to
  // bytesPendingLimitReached() should return.
  bool bytes_pending_limit_reached_{false};

  // Used by tests to determine what the next calls to isAcceptingWork() should
  // return.
  bool is_accepting_work_{true};

  // Keep track of the state of the Appender's timers.
  // Note that instead of mocking the timers directly, this test suite will call
  // the onTimeout() method of Appender directly.
  // These values are only used to assert that the timers have been activated
  // through calls to activateStoreTimer() and activateRetryTimer(), and
  bool store_timer_active_{false};
  bool retry_timer_active_{false};

  // Keep track of which nodes are not available following Appender calling
  // setNotAvailableUntil(). Used by checkNotAvailableUntil() to inform the
  // Appender of which nodes are not available. Can be modified directly by
  // tests to simulate a node not being available and check that it is not
  // selected by pickDestinations().
  std::unordered_map<ShardID, NodeSetState::NotAvailableReason, ShardID::Hash>
      not_available_nodes_;

  // Values that will be returned by getGraylistedNodes in the availability
  // checker.
  std::unordered_set<node_index_t> graylisted_nodes_;

  // Used to mock LinearCopySetSelector::Iterator.
  int first_candidate_idx_{0};

  // Keep track of received STORE/DELETE/RELEASE messages.
  // Tests can use several macros to check the content of these maps for a given
  // NodeID.
  message_map_t<STORE_Message> store_msgs_;
  message_map_t<DELETE_Message> delete_msgs_;
  message_map_t<RELEASE_Message> release_msgs_;

  // Store the APPENDED header sent by Appender (if any).
  // Tests can use CHECK_APPENDED() and CHECK_APPENDED_PREEMPTED() to check the
  // content of this value.
  folly::Optional<APPENDED_Header> reply_;

  // indicate if the appender is for draining
  bool draining_{false};

  // Indicate if the Appender already retired (set by retireAppender()).
  bool retired_{false};

  // Store the information regarding whether or not the Sequencer was preempted.
  // Set by noteAppenderPreempted() and read by checkIfPreempted().
  epoch_t preempted_epoch_{EPOCH_INVALID};
  NodeID preempted_by_;

  // Value passed in noteAppenderReaped() if called while the Sequencer has been
  // preempted (preempted_epoch_ > lsn_to_epoch(LSN)).
  epoch_t last_released_epoch_{1};

  MockAppender* appender_;

  // Update `config_`. Should be called at least once before start().
  void updateConfig();

  // Start the Appender.
  void start();

  void startWithMockE2ETracer(std::shared_ptr<opentracing::Tracer>);

  // takes advantage of friend declaration in STORE_Message to obtain the header
  const STORE_Header& getHeader(const STORE_Message* msg);

  // takes advantage of friend declaration in STORE_Message to obtain the header
  const STORE_Extra& getExtra(const STORE_Message* msg) const {
    return msg->extra_;
  }

  // call Appender::onTimeout().
  void triggerTimeout();

  // Trigger the on socket close callback for `nid`.
  void triggerOnSocketClosed(ShardID shard);

  // Methods that wrap macro-based functionality,
  // in order to enable calling said functionality from a Worker
  // via access to an AppenderTest object
  void checkStoreMsg(uint32_t expected_wave, std::set<ShardID> shards);
  void checkStoreMsgAndTriggerOnSent(Status status,
                                     uint32_t expected_wave,
                                     std::set<ShardID> shards);
  void checkNoStoreMsg();
  void onStoredSent(Status status, uint32_t wave, std::set<ShardID> shards);
  void checkAppended(Status status);
  void checkDeleteMsg(std::set<ShardID> shards);
  void checkReleaseMsg(std::set<ShardID> shards);
  void checkRetired() {
    ASSERT_TRUE(retired_);
  }
  void checkReplyHasNoValue() {
    ASSERT_FALSE(reply_.hasValue());
  }

 private:
  // Map a NodeID to a list of SocketCallback. (this mocks some logic in
  // Recipient which registers a socket callback to the socket by calling
  // registerOnSocketClosed()). Tests can call triggerOnSocketClosed(ShardID) to
  // simulate the callback being called for a NodeID.
  std::unordered_map<
      NodeID,
      folly::IntrusiveList<SocketCallback, &SocketCallback::listHook_>,
      NodeID::Hash>
      on_close_cb_map_;

  // Used by getNextCandidate().
  int num_candidates_seen_{0};
};

class AppenderTest::TestCopySetSelectorDeps
    : public logdevice::TestCopySetSelectorDeps {
 public:
  explicit TestCopySetSelectorDeps(AppenderTest* test) : test_(test) {}

  NodeStatus checkNode(NodeSetState* nodeset_state,
                       ShardID shard,
                       StoreChainLink* destination_out,
                       bool ignore_nodeset_state,
                       bool allow_unencrypted_connections) const override {
    return NodeAvailabilityChecker::checkNode(nodeset_state,
                                              shard,
                                              destination_out,
                                              ignore_nodeset_state,
                                              allow_unencrypted_connections);
  }

  void setConnectionError(NodeID node, Status error) {
    node_status_[node].connection_state_ = error;
  }

 private:
  int checkConnection(NodeID nid,
                      ClientID* our_name_at_peer,
                      bool) const override {
    auto it = node_status_.find(nid);
    if (it != node_status_.end() &&
        it->second.connection_state_ != Status::OK) {
      err = it->second.connection_state_;
      LOG(INFO) << "check connection error: " << nid.toString() << " " << err;
      return -1;
    }

    if (it == node_status_.end()) {
      *our_name_at_peer = ClientID::MIN;
    } else {
      *our_name_at_peer = it->second.client_id_;
    }
    LOG(INFO) << "check connection: " << our_name_at_peer->toString();
    return 0;
  }

  const std::unordered_set<node_index_t>& getGraylistedNodes() const override {
    return test_->graylisted_nodes_;
  }

  std::chrono::steady_clock::time_point
  checkNotAvailableUntil(NodeSetState*,
                         ShardID shard,
                         std::chrono::steady_clock::time_point) const override {
    if (test_->not_available_nodes_.find(shard) !=
        test_->not_available_nodes_.end()) {
      return std::chrono::steady_clock::time_point::max();
    } else {
      return std::chrono::steady_clock::time_point::min();
    }
  }

  bool checkFailureDetector(node_index_t /*index*/) const override {
    return true;
  }

  int connect(NodeID /*nid*/, bool /*allow_unencrypted*/) const override {
    // Called by when checkNode() sees a node that's not
    // connected. Ignored.
    return 0;
  }

  const NodeAvailabilityChecker* getNodeAvailability() const override {
    return this;
  }

  std::shared_ptr<const configuration::nodes::NodesConfiguration>
  getNodesConfiguration() const override {
    return test_->config_.getNodesConfiguration();
  }

 private:
  AppenderTest* test_;
};

// a linear copyset selector for testing
class AppenderTest::TestCopySetSelector : public LinearCopySetSelector {
 public:
  explicit TestCopySetSelector(AppenderTest* test,
                               std::shared_ptr<NodeSetState> nodeset_state,
                               CopySetSelectorDependencies* deps)
      : LinearCopySetSelector(test->replication_,
                              test->shards_,
                              nodeset_state,
                              deps),
        test_(test) {}

 private:
  void resetCandidates(const StorageSet& /*nodeset*/,
                       LinearCopySetSelector::Iterator* /*it*/) const override {
    test_->num_candidates_seen_ = 0;
  }

  ShardID
  getNextCandidate(LinearCopySetSelector::Iterator* /*it*/) const override {
    if (test_->num_candidates_seen_ == test_->shards_.size()) {
      return ShardID();
    }
    ld_check(test_->num_candidates_seen_ < test_->shards_.size());
    const int idx = test_->first_candidate_idx_ + test_->num_candidates_seen_;
    ++test_->num_candidates_seen_;
    return test_->shards_[idx % test_->shards_.size()];
  }

  AppenderTest* test_;
};

class AppenderTest::MockNodeSetState : public NodeSetState {
 public:
  MockNodeSetState(AppenderTest* test,
                   const StorageSet& shards,
                   logid_t log_id,
                   HealthCheck healthCheck)
      : NodeSetState(shards, log_id, healthCheck), test_(test) {}

  nodeset_ssize_t
  numNotAvailableShards(NotAvailableReason reason) const override {
    ld_check(reason < NotAvailableReason::Count);

    nodeset_ssize_t res = 0;
    for (auto& n : test_->not_available_nodes_) {
      if (n.second == reason) {
        res++;
      }
    }

    return res;
  }

  void resetGrayList(GrayListResetReason /* unused */) override {
    for (auto it = test_->not_available_nodes_.begin();
         it != test_->not_available_nodes_.end();) {
      if (it->second == NotAvailableReason::SLOW) {
        it = test_->not_available_nodes_.erase(it);
      } else {
        ++it;
      }
    }
  }

  const Settings* getSettings() const override {
    return &test_->settings_;
  }

  const std::shared_ptr<const NodesConfiguration>
  getNodesConfiguration() const override {
    return test_->config_.getNodesConfiguration();
  }

  void postHealthCheckRequest(ShardID /* unused */,
                              bool /* unused */) override {}

  NodeSetState::NotAvailableReason
  getNotAvailableReason(ShardID shard) const override {
    if (test_->not_available_nodes_.find(shard) !=
        test_->not_available_nodes_.end()) {
      return test_->not_available_nodes_[shard];
    }
    return NodeSetState::NotAvailableReason::NONE;
  }

  std::chrono::steady_clock::time_point
  getDeadline(NotAvailableReason /* unused */) const override {
    return std::chrono::steady_clock::now() + std::chrono::milliseconds(100);
  }

  double getSpaceBasedRetentionThreshold() const override {
    return 0;
  }

  bool shouldPerformSpaceBasedRetention() const override {
    return true;
  }

  double getGrayListThreshold() const override {
    return 0.5;
  }

 private:
  AppenderTest* test_;
};

class AppenderTest::MockAppender : public Appender {
 public:
  using MockSender = SenderTestProxy<MockAppender>;

  explicit MockAppender(AppenderTest* test,
                        std::chrono::milliseconds client_timeout,
                        request_id_t append_request_id)
      : Appender(
            Worker::onThisThread(false),
            std::make_shared<NoopTraceLogger>(UpdateableConfig::createEmpty()),
            client_timeout,
            append_request_id,
            STORE_flags_t(0),
            LOG_ID,
            PayloadHolder(MockAppender::dummyPayload, PayloadHolder::UNOWNED),
            epoch_t(0),
            500),
        stats_(StatsParams().setIsServer(true)),
        test_(test) {
    sender_ = std::make_unique<MockSender>(this);
  }

  explicit MockAppender(AppenderTest* test,
                        std::chrono::milliseconds client_timeout,
                        request_id_t append_request_id,
                        std::shared_ptr<opentracing::Tracer> e2e_tracer)
      : Appender(
            Worker::onThisThread(false),
            std::make_shared<NoopTraceLogger>(UpdateableConfig::createEmpty()),
            client_timeout,
            append_request_id,
            STORE_flags_t(0),
            LOG_ID,
            PayloadHolder(MockAppender::dummyPayload, PayloadHolder::UNOWNED),
            epoch_t(0),
            500,
            e2e_tracer),
        stats_(StatsParams().setIsServer(true)),
        test_(test) {
    sender_ = std::make_unique<MockSender>(this);
  }

  void sendReleases(const ShardID* dests,
                    size_t ndests,
                    const RecordID& rid,
                    ReleaseType release_type) override {
    for (const ShardID* dest = dests; dest < dests + ndests; ++dest) {
      auto release_msg = std::make_unique<RELEASE_Message>(
          RELEASE_Header({rid, release_type, dest->shard()}));

      int rv = sender_->sendMessage(std::move(release_msg), dest->asNodeID());
      if (rv != 0) {
        RATELIMIT_ERROR(std::chrono::seconds(10),
                        10,
                        "Failed to send a RELEASE for record %s to %s: %s. ",
                        rid.toString().c_str(),
                        dest->toString().c_str(),
                        error_description(err));
      }
    }
  }

  void checkWorkerThread() override {}

  void initStoreTimer() override {}
  void initRetryTimer() override {}
  void cancelStoreTimer() override {
    test_->store_timer_active_ = false;
  }
  bool storeTimerIsActive() override {
    return test_->store_timer_active_;
  }
  void cancelRetryTimer() override {
    test_->retry_timer_active_ = false;
  }
  void activateRetryTimer() override {
    test_->retry_timer_active_ = true;
  }
  void activateStoreTimer(std::chrono::milliseconds) override {
    test_->store_timer_active_ = true;
  }
  bool retryTimerIsActive() override {
    return test_->retry_timer_active_;
  }
  bool isNodeAlive(NodeID node) override {
    // if we can't find the node in dead_nodes_, it is alive.
    return test_->dead_nodes_.find(node) == test_->dead_nodes_.end();
  }
  lsn_t getLastKnownGood() const override {
    return compose_lsn(epoch_t{1}, esn_t{1});
  }

  NodeLocationScope getCurrentBiggestReplicationScope() const override {
    return NodeLocationScope::NODE;
  }

  copyset_size_t getExtras() const override {
    return test_->extras_;
  }

  std::shared_ptr<CopySetManager> getCopySetManager() const override {
    return test_->copyset_manager_;
  }

  const Settings& getSettings() const override {
    return test_->settings_;
  }
  copyset_size_t getSynced() const override {
    return test_->synced_;
  }
  int link() override {
    return test_->activeAppenders_.map.insert(*this);
  }

  const std::shared_ptr<Configuration> getClusterConfig() const override {
    return test_->config_.get();
  }

  NodeID getMyNodeID() const override {
    return NodeID(10, 20);
  }

  template <typename T>
  void onMessageReceived(std::unique_ptr<Message>& msg,
                         message_map_t<T>& map,
                         ShardID shard) {
    T* tmp = dynamic_cast<T*>(msg.get());
    ASSERT_TRUE(tmp);
    // The map should not already have a message. Tests are supposed to check
    // each received message at each step.
    ASSERT_EQ(map.end(), map.find(shard));
    map[shard] = std::unique_ptr<T>(tmp);
    msg.release();
  }

  bool canSendToImpl(const Address&, TrafficClass, BWAvailableCallback&) {
    return true;
  }

  int sendMessageImpl(std::unique_ptr<Message>&& msg,
                      const Address& addr,
                      BWAvailableCallback*,
                      SocketCallback*) {
    ld_check(!addr.isClientAddress());
    if (addr.isClientAddress()) {
      err = E::INTERNAL;
      return -1;
    }

    // used in e2e tracing test when we want to create spans for all stores
    if (msg->type_ == MessageType::STORE && e2e_tracer_) {
      ShardID to = ShardID(addr.id_.node_.index(), 0);
      std::pair<uint32_t, ShardID> current_info(1, to);

      std::shared_ptr<opentracing::Span> span = e2e_tracer_->StartSpan("STORE");
      all_store_spans_[current_info] = span;
    }

    NodeID nid = addr.id_.node_;
    switch (msg->type_) {
      case MessageType::STORE:
        onMessageReceived<STORE_Message>(
            msg, test_->store_msgs_, ShardID(nid.index(), 0));
        break;
      case MessageType::DELETE:
        onMessageReceived<DELETE_Message>(
            msg, test_->delete_msgs_, ShardID(nid.index(), 0));
        break;
      case MessageType::RELEASE:
        onMessageReceived<RELEASE_Message>(
            msg, test_->release_msgs_, ShardID(nid.index(), 0));
        break;
      default:
        ld_check(false);
    }

    return 0;
  }

  std::string describeConnection(const Address& addr) const override {
    return addr.toString();
  }

  bool bytesPendingLimitReached() const override {
    return test_->bytes_pending_limit_reached_;
  }

  bool isAcceptingWork() const override {
    return test_->is_accepting_work_;
  }

  NodeID checkIfPreempted(epoch_t epoch) override {
    if (test_->preempted_epoch_ != EPOCH_INVALID &&
        epoch <= test_->preempted_epoch_) {
      return test_->preempted_by_;
    }
    return NodeID();
  }

  bool isDraining() const override {
    return test_->draining_;
  }

  void retireAppender(Status /*st*/,
                      lsn_t /*lsn*/,
                      Appender::Reaper& /*reaper*/) override {
    // retireAppender should be called only once.
    EXPECT_FALSE(test_->retired_);
    test_->retired_ = true;
  }

  void noteAppenderPreempted(epoch_t epoch, NodeID preempted_by) override {
    EXPECT_EQ(EPOCH_INVALID, test_->preempted_epoch_);
    EXPECT_FALSE(test_->preempted_by_.isNodeID());
    test_->preempted_epoch_ = epoch;
    test_->preempted_by_ = preempted_by;
    EXPECT_TRUE(test_->preempted_by_.isNodeID());
  }

  bool checkNodeSet() const override {
    err = test_->nodeset_status_;
    return test_->nodeset_status_ == E::OK;
  }

  bool noteAppenderReaped(Appender::FullyReplicated replicated,
                          lsn_t reaped_lsn,
                          std::shared_ptr<TailRecord> tail_record,
                          epoch_t* last_released_epoch_out,
                          bool* lng_changed_out) override {
    if (lsn_to_epoch(reaped_lsn) <= test_->preempted_epoch_) {
      err = E::PREEMPTED;
      *lng_changed_out = false;
      return false;
    }

    if (replicated != Appender::FullyReplicated::YES) {
      err = E::ABORTED;
      *lng_changed_out = false;
      return false;
    }

    *last_released_epoch_out = test_->last_released_epoch_;
    *lng_changed_out = true;
    return true;
  }

  void
  setNotAvailableUntil(ShardID shard,
                       std::chrono::steady_clock::time_point /*until_time*/,
                       NodeSetState::NotAvailableReason reason) override {
    test_->not_available_nodes_[shard] = reason;
  }

  StatsHolder* getStats() override {
    return &stats_;
  }

  int registerOnSocketClosed(NodeID nid, SocketCallback& cb) override {
    test_->on_close_cb_map_[nid].push_back(cb);
    return 0;
  }

  void replyToAppendRequest(APPENDED_Header& replyhdr) override {
    ASSERT_FALSE(test_->reply_.hasValue());
    test_->reply_ = replyhdr;
  }

  void schedulePeriodicReleases() override {}

  bool epochMetaDataAvailable(epoch_t /*epoch*/) const override {
    return true;
  }

  // Exposes `nodes_stored_amendable_', as a std::multiset for easy comparison
  // regardless of the underlying set representation used by Appender.
  std::multiset<ShardID> getNodesStoredAmendable() {
    return std::multiset<ShardID>(
        nodes_stored_amendable_.begin(), nodes_stored_amendable_.end());
  }

 private:
  StatsHolder stats_;
  AppenderTest* test_;
  static Payload dummyPayload; // something to pass to Apppender constructor,
                               // not used by the test
};

void AppenderTest::start() {
  // Appender will delete itself when reaped.
  appender_ = new MockAppender(this, std::chrono::seconds{1}, request_id_t{1});
  appender_->start(nullptr, LSN);
}

// Start the Appender providing a tracer object to be used in e2e tracing
void AppenderTest::startWithMockE2ETracer(
    std::shared_ptr<opentracing::Tracer> tracer) {
  appender_ =
      new MockAppender(this, std::chrono::seconds{1}, request_id_t{1}, tracer);
  appender_->appender_span_ = tracer->StartSpan("APPENDER");
  appender_->start(nullptr, LSN);
}

void AppenderTest::updateConfig() {
  configuration::Nodes nodes;
  for (ShardID shard : shards_) {
    node_index_t nid = shard.node();
    ld_check(!nodes.count(nid));
    Configuration::Node& node = nodes[nid];
    node.address = Sockaddr("::1", folly::to<std::string>(4440 + nid));
    node.generation = 1;
    node.addStorageRole();
    node.addSequencerRole();
  }

  logsconfig::LogAttributes log_attrs;
  log_attrs.set_replicationFactor(replication_);

  Configuration::NodesConfig nodes_config(nodes);
  auto logs_config = std::make_unique<configuration::LocalLogsConfig>();
  logs_config->insert(boost::icl::right_open_interval<logid_t::raw_type>(
                          LOG_ID.val_, LOG_ID.val_ + 1),
                      "mylog",
                      log_attrs);

  // metadata stored on all nodes with max replication factor 3
  Configuration::MetaDataLogsConfig meta_config =
      createMetaDataLogsConfig(nodes_config, nodes_config.getNodes().size(), 3);

  std::shared_ptr<ServerConfig> server_cfg =
      ServerConfig::fromDataTest(__FILE__, nodes_config, meta_config);
  config_.updateableServerConfig()->update(server_cfg);
  config_.updateableNodesConfiguration()->update(
      server_cfg->getNodesConfigurationFromServerConfigSource());
  config_.updateableLogsConfig()->update(std::move(logs_config));

  StorageSet shards;
  for (const auto& it : nodes) {
    for (shard_index_t s = 0; s < it.second.getNumShards(); ++s) {
      shards.push_back(ShardID(it.first, s));
    }
  }
  std::sort(shards.begin(), shards.end());
  auto nodeset_state = std::make_shared<MockNodeSetState>(
      this, shards, LOG_ID, NodeSetState::HealthCheck::DISABLED);

  deps_ = std::make_unique<TestCopySetSelectorDeps>(this);
  std::unique_ptr<CopySetSelector> copyset_selector(
      new TestCopySetSelector(this, nodeset_state, deps_.get()));
  copyset_manager_.reset(new PassThroughCopySetManager(
      std::move(copyset_selector), nodeset_state));
  copyset_manager_->disableCopySetShuffling();
}

const STORE_Header& AppenderTest::getHeader(const STORE_Message* msg) {
  return msg->header_;
}

void AppenderTest::triggerTimeout() {
  ASSERT_TRUE(retry_timer_active_ || store_timer_active_);
  retry_timer_active_ = false;
  store_timer_active_ = false;
  appender_->onTimeout();
}

void AppenderTest::triggerOnSocketClosed(ShardID shard) {
  auto it = on_close_cb_map_.find(shard.asNodeID());
  ASSERT_NE(on_close_cb_map_.end(), it);
  auto& cb_list = it->second;
  while (!cb_list.empty()) {
    auto& cb = cb_list.front();
    cb_list.pop_front();
    cb(E::PEER_CLOSED, Address(shard.asNodeID()));
  }
}

// Helper function for creating a STORED_Header instance.
STORED_Header storedHeader(uint32_t wave,
                           Status status,
                           NodeID redirect = NodeID(),
                           STORED_flags_t flags = 0) {
  STORED_Header hdr;
  hdr.rid = {lsn_to_esn(LSN), lsn_to_epoch(LSN), LOG_ID};
  hdr.wave = wave;
  hdr.status = status;
  hdr.redirect = redirect;
  hdr.flags = flags;
  hdr.shard = 0;
  return hdr;
}

// Check that all the nodes `nids` have received a STORE message for wave
// `expected_wave`.
#define CHECK_STORE_MSG(expected_wave, ...)                                    \
  {                                                                            \
    for (auto shard : {__VA_ARGS__}) {                                         \
      auto it = store_msgs_.find(shard);                                       \
      ASSERT_NE(store_msgs_.end(), it);                                        \
      ASSERT_EQ(expected_wave, getHeader(it->second.get()).wave);              \
      ASSERT_EQ(lsn_to_epoch(LSN), getHeader(it->second.get()).rid.epoch);     \
      ASSERT_EQ(lsn_to_esn(LSN), getHeader(it->second.get()).rid.esn);         \
      ASSERT_EQ(LOG_ID, getHeader(it->second.get()).rid.logid);                \
      ASSERT_EQ(                                                               \
          draining_,                                                           \
          (bool)(getHeader(it->second.get()).flags & STORE_Header::DRAINING)); \
      store_msgs_.erase(it);                                                   \
    }                                                                          \
  }

void AppenderTest::checkStoreMsg(uint32_t expected_wave,
                                 std::set<ShardID> shards) {
  for (auto shard : shards) {
    auto it = store_msgs_.find(shard);
    ASSERT_NE(store_msgs_.end(), it);
    ASSERT_EQ(expected_wave, getHeader(it->second.get()).wave);
    ASSERT_EQ(lsn_to_epoch(LSN), getHeader(it->second.get()).rid.epoch);
    ASSERT_EQ(lsn_to_esn(LSN), getHeader(it->second.get()).rid.esn);
    ASSERT_EQ(LOG_ID, getHeader(it->second.get()).rid.logid);
    ASSERT_EQ(
        draining_,
        (bool)(getHeader(it->second.get()).flags & STORE_Header::DRAINING));
    store_msgs_.erase(it);
  }
}

// Same as CHECK_STORE_MSG but also call Appender::onCopySent for the STORE
// messages.
#define CHECK_STORE_MSG_AND_TRIGGER_ON_SENT(status, expected_wave, ...)        \
  {                                                                            \
    for (auto shard : {__VA_ARGS__}) {                                         \
      auto it = store_msgs_.find(shard);                                       \
      ASSERT_NE(store_msgs_.end(), it) << shard.toString();                    \
      ASSERT_EQ(expected_wave, getHeader(it->second.get()).wave);              \
      ASSERT_EQ(                                                               \
          draining_,                                                           \
          (bool)(getHeader(it->second.get()).flags & STORE_Header::DRAINING)); \
      appender_->onCopySent(status, shard, getHeader(it->second.get()));       \
      store_msgs_.erase(it);                                                   \
    }                                                                          \
  }

void AppenderTest::checkStoreMsgAndTriggerOnSent(Status status,
                                                 uint32_t expected_wave,
                                                 std::set<ShardID> shards) {
  for (auto shard : shards) {
    auto it = store_msgs_.find(shard);
    ASSERT_NE(store_msgs_.end(), it);
    ASSERT_EQ(expected_wave, getHeader(it->second.get()).wave);
    ASSERT_EQ(
        draining_,
        (bool)(getHeader(it->second.get()).flags & STORE_Header::DRAINING));
    appender_->onCopySent(status, shard, getHeader(it->second.get()));
    store_msgs_.erase(it);
  }
}

#define CHECK_NO_STORE_MSG() ASSERT_TRUE(store_msgs_.empty())

void AppenderTest::checkNoStoreMsg() {
  ASSERT_TRUE(store_msgs_.empty());
}

// Check that all/only the given shards have received a DELETE message.
#define CHECK_DELETE_MSG(...)                                          \
  {                                                                    \
    for (auto shard : {__VA_ARGS__}) {                                 \
      auto it = delete_msgs_.find(shard);                              \
      ASSERT_NE(delete_msgs_.end(), it);                               \
      ASSERT_EQ(lsn_to_epoch(LSN), it->second->getHeader().rid.epoch); \
      ASSERT_EQ(lsn_to_esn(LSN), it->second->getHeader().rid.esn);     \
      ASSERT_EQ(LOG_ID, it->second->getHeader().rid.logid);            \
      delete_msgs_.erase(it);                                          \
    }                                                                  \
    ASSERT_TRUE(delete_msgs_.empty());                                 \
  }

void AppenderTest::checkDeleteMsg(std::set<ShardID> shards) {
  for (auto shard : shards) {
    auto it = delete_msgs_.find(shard);
    ASSERT_NE(delete_msgs_.end(), it);
    ASSERT_EQ(lsn_to_epoch(LSN), it->second->getHeader().rid.epoch);
    ASSERT_EQ(lsn_to_esn(LSN), it->second->getHeader().rid.esn);
    ASSERT_EQ(LOG_ID, it->second->getHeader().rid.logid);
    delete_msgs_.erase(it);
  }
  ASSERT_TRUE(delete_msgs_.empty());
}

#define CHECK_NO_DELETE_MSG() ASSERT_TRUE(delete_msgs_.empty())

// Check that all/only the given shards have received a RELEASE message for wave
// `wave`.
#define CHECK_RELEASE_MSG(...)             \
  {                                        \
    for (auto shard : {__VA_ARGS__}) {     \
      auto it = release_msgs_.find(shard); \
      ASSERT_NE(release_msgs_.end(), it);  \
      release_msgs_.erase(it);             \
    }                                      \
    ASSERT_TRUE(release_msgs_.empty());    \
  }

void AppenderTest::checkReleaseMsg(std::set<ShardID> shards) {
  for (auto shard : shards) {
    auto it = release_msgs_.find(shard);
    ASSERT_NE(release_msgs_.end(), it);
    release_msgs_.erase(it);
  }
  ASSERT_TRUE(release_msgs_.empty());
}

#define CHECK_NO_RELEASE_MSG() ASSERT_TRUE(release_msgs_.empty())

// Verify that APPENDED was sent by Appender with status `expected_status`.
#define CHECK_APPENDED(expected_status)         \
  {                                             \
    ASSERT_TRUE(reply_.hasValue());             \
    ASSERT_EQ(request_id_t{1}, reply_->rqid);   \
    ASSERT_EQ(expected_status, reply_->status); \
    ASSERT_EQ(LSN, reply_->lsn);                \
    ASSERT_FALSE(reply_->redirect.isNodeID());  \
  }

void AppenderTest::checkAppended(Status expected_status) {
  ASSERT_TRUE(reply_.hasValue());
  ASSERT_EQ(request_id_t{1}, reply_->rqid);
  ASSERT_EQ(expected_status, reply_->status);
  ASSERT_EQ(LSN, reply_->lsn);
  ASSERT_FALSE(reply_->redirect.isNodeID());
}

// Verify that APPENDED was sent by Appender with E::PREEMPTED and redirect
// set to `expected_redirect`.
#define CHECK_APPENDED_PREEMPTED(expected_redirect) \
  {                                                 \
    ASSERT_TRUE(reply_.hasValue());                 \
    ASSERT_EQ(request_id_t{1}, reply_->rqid);       \
    ASSERT_EQ(E::PREEMPTED, reply_->status);        \
    ASSERT_TRUE(reply_->redirect.isNodeID());       \
    ASSERT_EQ(expected_redirect, reply_->redirect); \
  }

// Reply with a STORED message with status `status` for wave `wave`.
#define ON_STORED_SENT(status, wave, ...)                    \
  {                                                          \
    for (auto shard : {__VA_ARGS__}) {                       \
      appender_->onReply(storedHeader(wave, status), shard); \
    }                                                        \
  }

void AppenderTest::onStoredSent(Status status,
                                uint32_t wave,
                                std::set<ShardID> shards) {
  for (auto shard : shards) {
    appender_->onReply(storedHeader(wave, status), shard);
  }
}

// Reply with a STORED message with E::PREEMPTED.
#define ON_STORED_SENT_PREEMPTED(preempted_by, wave, ...)         \
  {                                                               \
    for (auto shard : {__VA_ARGS__}) {                            \
      appender_->onReply(                                         \
          storedHeader(wave, E::PREEMPTED, preempted_by), shard); \
    }                                                             \
  }

// Check that Appender called setNotAvailableUntil() for the given node id.
#define CHECK_NOT_AVAILABLE(shard, reason)                                   \
  {                                                                          \
    ASSERT_NE(not_available_nodes_.end(), not_available_nodes_.find(shard)); \
    ASSERT_EQ(NodeSetState::NotAvailableReason::reason,                      \
              not_available_nodes_[shard]);                                  \
  }

// Mark several node ids not available.
#define SET_NOT_AVAILABLE(reason, ...)                                        \
  {                                                                           \
    for (auto shard : {__VA_ARGS__}) {                                        \
      not_available_nodes_[shard] = NodeSetState::NotAvailableReason::reason; \
    }                                                                         \
  }

// Mark several node ids available.
#define SET_AVAILABLE(...)               \
  {                                      \
    for (auto shard : {__VA_ARGS__}) {   \
      not_available_nodes_.erase(shard); \
    }                                    \
  }

TEST_F(AppenderTest, Simple) {
  updateConfig();
  first_candidate_idx_ = 0;
  start();
  CHECK_STORE_MSG(1, N4S0);
  CHECK_STORE_MSG_AND_TRIGGER_ON_SENT(E::OK, 1, N0S0, N1S0, N2S0, N3S0);
  CHECK_NO_STORE_MSG();
  ON_STORED_SENT(E::OK, 1, N0S0, N1S0, N3S0);
  CHECK_APPENDED(E::OK);
  ASSERT_TRUE(retired_);
  CHECK_DELETE_MSG(N2S0, N4S0);
  Appender::Reaper()(appender_);
  CHECK_RELEASE_MSG(N0S0, N1S0, N3S0);
}

// A first wave is sent but we reach a point where we failed to store on too
// many nodes in the recipient set such that we know we should start a new wave
// immediately. During the first wave, Appender received a negative reply from a
// node with E::NOSPC. We check that this node is discarded by pickDestinations
// for the new wave. We also check that a stale reply for wave 1 is discarded
// when waiting for replies for wave 2.
TEST_F(AppenderTest, NotEnoughRepliesExpectedTryNewWave) {
  updateConfig();
  first_candidate_idx_ = 0;

  start();

  // The Appender sent a STORE message to {0, 1, 2, 3, 4}
  // STORE could be sent to 4 nodes.
  CHECK_STORE_MSG_AND_TRIGGER_ON_SENT(E::OK, 1, N0S0, N1S0, N2S0, N4S0);
  // The STORE message could not be sent to one recipient.
  CHECK_STORE_MSG_AND_TRIGGER_ON_SENT(E::TIMEDOUT, 1, N3S0);
  CHECK_NO_STORE_MSG();
  // One recipient successfully stored its copy.
  ON_STORED_SENT(E::OK, 1, N0S0);
  // Two recipients reply with an error.
  ON_STORED_SENT(E::NOSPC, 1, N4S0);
  ON_STORED_SENT(E::FAILED, 1, N2S0);

  // At this point, Appender should give up this wave because 3 out of 5 nodes
  // could not store a copy of the record, meaning that there is not enough
  // nodes left to fully replicate it. The retry timer is activated.

  // Check that Appender notified NodeSetState that N4S0 has no space.
  CHECK_NOT_AVAILABLE(N4S0, NO_SPC);

  // The candidate iterator will propose N4S0 first but pickDestinations should
  // discard it.
  first_candidate_idx_ = 4;
  // Retry timer is triggered.
  triggerTimeout();
  // STORE could be sent to 5 nodes with wave 2.
  CHECK_STORE_MSG_AND_TRIGGER_ON_SENT(E::OK, 2, N5S0, N6S0, N7S0, N8S0, N0S0);
  CHECK_NO_STORE_MSG();

  // 2 recipients reply.
  ON_STORED_SENT(E::OK, 2, N8S0, N0S0);
  // 1 recipient replies for the previous wave. This should not cause APPENDED
  // to be sent.
  ON_STORED_SENT(E::OK, 1, N1S0);
  ASSERT_FALSE(reply_.hasValue());
  // A third recipient replies.
  ON_STORED_SENT(E::OK, 2, N7S0);

  // Check that we got an APPENDED message.
  CHECK_APPENDED(E::OK);

  // We should have retired.
  ASSERT_TRUE(retired_);
  // Reap the appender.
  Appender::Reaper()(appender_);

  // Check that the Appender sent RELEASE messages to the 3 nodes.
  CHECK_RELEASE_MSG(N8S0, N0S0, N7S0);
}

// Check that if the store timeout expires, a new wave is started.
TEST_F(AppenderTest, StoreTimeoutTryNewWave) {
  updateConfig();
  first_candidate_idx_ = 0;
  start();
  CHECK_STORE_MSG_AND_TRIGGER_ON_SENT(E::OK, 1, N0S0, N1S0, N2S0, N3S0, N4S0);
  CHECK_NO_STORE_MSG();

  // 2 nodes reply in time but the store timeout triggers.
  ON_STORED_SENT(E::OK, 1, N0S0, N1S0);
  first_candidate_idx_ = 5;
  triggerTimeout();

  CHECK_STORE_MSG_AND_TRIGGER_ON_SENT(E::OK, 2, N5S0, N6S0, N7S0, N8S0, N0S0);
  CHECK_NO_STORE_MSG();
  // STORED is received from previous wave and discarded.
  ON_STORED_SENT(E::OK, 1, N4S0, N3S0, N2S0);
  ASSERT_FALSE(reply_.hasValue());
  // 3 nodes reply in time this time.
  ON_STORED_SENT(E::OK, 2, N7S0, N5S0, N6S0);
  CHECK_APPENDED(E::OK);
  CHECK_DELETE_MSG(N8S0, N0S0);
  ASSERT_TRUE(retired_);
  Appender::Reaper()(appender_);
  CHECK_RELEASE_MSG(N7S0, N5S0, N6S0);
}

// If all receiving nodes respond negatively, none will be greylisted.
TEST_F(AppenderTest, StoreTimeoutRelaxedGraylisting) {
  updateConfig();
  first_candidate_idx_ = 0;
  start();
  CHECK_STORE_MSG_AND_TRIGGER_ON_SENT(E::OK, 1, N0S0, N1S0, N2S0, N3S0, N4S0);
  CHECK_NO_STORE_MSG();

  // None of the nodes replies in time (before store timeout triggers).
  triggerTimeout();

  CHECK_STORE_MSG_AND_TRIGGER_ON_SENT(E::OK, 2, N0S0, N1S0, N2S0, N3S0, N4S0);
  CHECK_NO_STORE_MSG();
  // STORED is received from previous wave and discarded.
  ON_STORED_SENT(E::OK, 1, N4S0, N3S0, N2S0, N1S0, N0S0);
  ASSERT_FALSE(reply_.hasValue());
  // 3 nodes reply on time.
  ON_STORED_SENT(E::OK, 2, N0S0, N1S0, N2S0);
  CHECK_APPENDED(E::OK);
  CHECK_DELETE_MSG(N3S0, N4S0);
  ASSERT_TRUE(retired_);
  Appender::Reaper()(appender_);
  CHECK_RELEASE_MSG(N0S0, N1S0, N2S0);
}

// Check that a recipient is discarded by pickDestinations because the socket
// for it reached its buffer limit.
TEST_F(AppenderTest, SocketBufferLimit) {
  updateConfig();
  first_candidate_idx_ = 0;
  // Simulate the socket for N1S0, N2S0 having reached its buffer limits.
  // It should not be picked by pickDestination.
  for (auto shard : {N1S0, N2S0}) {
    deps_->setConnectionError(shard.asNodeID(), E::NOBUFS);
  }
  start();
  CHECK_STORE_MSG_AND_TRIGGER_ON_SENT(E::OK, 1, N0S0, N3S0, N4S0, N5S0, N6S0);
  CHECK_NO_STORE_MSG();
  ON_STORED_SENT(E::OK, 1, N0S0, N5S0, N6S0);
  CHECK_APPENDED(E::OK);
  CHECK_DELETE_MSG(N3S0, N4S0);
  ASSERT_TRUE(retired_);
  Appender::Reaper()(appender_);
  CHECK_RELEASE_MSG(N0S0, N5S0, N6S0);
}

// Test chain sending.
TEST_F(AppenderTest, ChainSending) {
  settings_.disable_chain_sending = false;
  updateConfig();
  first_candidate_idx_ = 0;
  start();

  // A store message should have been sent to N0S0 only.
  CHECK_STORE_MSG_AND_TRIGGER_ON_SENT(E::OK, 1, N0S0);
  CHECK_NO_STORE_MSG();

  // 3 nodes reply.
  ON_STORED_SENT(E::OK, 1, N0S0, N1S0, N3S0);
  CHECK_APPENDED(E::OK);
  CHECK_DELETE_MSG(N2S0, N4S0);
  ASSERT_TRUE(retired_);
  Appender::Reaper()(appender_);
  CHECK_RELEASE_MSG(N0S0, N1S0, N3S0);
}

// Test chain sending failures. Even though we are notified we cannot send
// STORE to only one node, we should still start another wave immediately as
// this other node was supposed to forward to others.
TEST_F(AppenderTest, ChainSendingFail) {
  settings_.disable_chain_sending = false;
  extras_ = 1;
  updateConfig();
  first_candidate_idx_ = 0;
  start();

  // A store message should have been sent to N0S0 only. However, the socket for
  // that node is out of evbuffer space.
  CHECK_STORE_MSG_AND_TRIGGER_ON_SENT(E::NOBUFS, 1, N0S0);
  CHECK_NO_STORE_MSG();

  // Since this is a chain sending, one node not reachable should be enough for
  // deciding to immediately retry a new wave after the retry timer triggers.
  ASSERT_FALSE(store_timer_active_);
  ASSERT_TRUE(retry_timer_active_);

  // Retry timer triggers, we start another wave.
  first_candidate_idx_ = 2;
  triggerTimeout();

  // Second wave, use chain sending again and N2S0 is the first recipient.
  CHECK_STORE_MSG_AND_TRIGGER_ON_SENT(E::OK, 2, N2S0);
  CHECK_NO_STORE_MSG();
  ON_STORED_SENT(E::OK, 2, N2S0, N3S0, N4S0);
  CHECK_APPENDED(E::OK);
  CHECK_DELETE_MSG(N5S0);
  ASSERT_TRUE(retired_);
  Appender::Reaper()(appender_);
  CHECK_RELEASE_MSG(N2S0, N3S0, N4S0);
}

// One of the storage nodes in the chain fails to forward the STORE to the next
// link. We should start another wave immediately when this happens.
TEST_F(AppenderTest, ChainSendingForwardingFailure) {
  extras_ = 0;
  settings_.disable_chain_sending = false;
  updateConfig();
  first_candidate_idx_ = 0;
  start();

  // The chain link is N0S0 -> N1S0 -> N2S0
  CHECK_STORE_MSG_AND_TRIGGER_ON_SENT(E::OK, 1, N0S0);
  CHECK_NO_STORE_MSG();

  // N1S0 sends STORED(E::OK)
  ON_STORED_SENT(E::OK, 1, N1S0);

  // N0S0 sends STORED(E::OK)
  ON_STORED_SENT(E::OK, 1, N0S0);

  // N1S0 notifies that it could not forward to N2S0, we should start another
  // wave immediately.
  ON_STORED_SENT(E::FORWARD, 1, N1S0);

  // The retry timer is activated so that we retry another wave immediately.
  ASSERT_FALSE(store_timer_active_);
  ASSERT_TRUE(retry_timer_active_);
  // The new wave has the following chain link: N2S0 -> N3S0 -> N4S0
  first_candidate_idx_ = 2;
  triggerTimeout();

  // Second wave, use chain sending again and N2S0 is the first recipient.
  CHECK_STORE_MSG_AND_TRIGGER_ON_SENT(E::OK, 2, N2S0);
  CHECK_NO_STORE_MSG();
  // 3 nodes reply.
  ON_STORED_SENT(E::OK, 2, N2S0, N3S0, N4S0);
  CHECK_APPENDED(E::OK);
  ASSERT_TRUE(retired_);
  Appender::Reaper()(appender_);
  CHECK_RELEASE_MSG(N2S0, N3S0, N4S0);
}

// We fail to send STORE to the first node in the chain. This should immediately
// trigger another wave.
TEST_F(AppenderTest, ChainSendingFailToSendFirstNode) {
  extras_ = 0;
  settings_.disable_chain_sending = false;
  updateConfig();
  first_candidate_idx_ = 0;
  start();

  // The chain link is N0S0 -> N1S0 -> N2S0. But we cannot send to the first
  // recipient.
  CHECK_STORE_MSG_AND_TRIGGER_ON_SENT(E::NOBUFS, 1, N0S0);
  CHECK_NO_STORE_MSG();

  // The retry timer is activated so that we retry another wave immediately.
  ASSERT_FALSE(store_timer_active_);
  ASSERT_TRUE(retry_timer_active_);
  // The new wave has the following chain link: N2S0 -> N3S0 -> N4S0
  first_candidate_idx_ = 2;
  triggerTimeout();

  // Second wave, use chain sending again and N2S0 is the first recipient.
  CHECK_STORE_MSG_AND_TRIGGER_ON_SENT(E::OK, 2, N2S0);
  CHECK_NO_STORE_MSG();
  ON_STORED_SENT(E::OK, 2, N2S0, N3S0, N4S0);
  CHECK_APPENDED(E::OK);
  ASSERT_TRUE(retired_);
  Appender::Reaper()(appender_);
  CHECK_RELEASE_MSG(N2S0, N3S0, N4S0);
}

// Check that Appender will not fail if the number of recipients available is
// smaller than R + E but still greater than R.
TEST_F(AppenderTest, NotEnoughExtras) {
  updateConfig();
  first_candidate_idx_ = 0;

  // Simulate 6 nodes being unavailable.
  // There are still `replication` recipients available so the Appender should
  // try to start a wave anyway.
  SET_NOT_AVAILABLE(OVERLOADED, N3S0, N4S0, N8S0);
  SET_NOT_AVAILABLE(NO_SPC, N5S0);
  SET_NOT_AVAILABLE(UNROUTABLE, N0S0, N6S0);
  start();

  // N1S0, N2S0, N7S0 are still available and should have been picked by
  // pickDestinations().
  CHECK_STORE_MSG_AND_TRIGGER_ON_SENT(E::OK, 1, N1S0, N2S0, N7S0);
  CHECK_NO_STORE_MSG();
  ON_STORED_SENT(E::OK, 1, N1S0, N2S0, N7S0);
  CHECK_APPENDED(E::OK);
  // There are no nodes to send DELETE to.
  CHECK_NO_DELETE_MSG();
  ASSERT_TRUE(retired_);
  Appender::Reaper()(appender_);
  CHECK_RELEASE_MSG(N1S0, N2S0, N7S0);
}

// Check that Appender will try again after the store timeout if it fails to
// send a complete wave because not enough destinations are available.
TEST_F(AppenderTest, NotEnoughDestinationsRetry) {
  updateConfig();
  first_candidate_idx_ = 0;

  // Simulate 7 nodes being unavailable.
  // There are less than `replication` recipients available so the Appender
  // cannot start a wave immediately and will trigger the store timeout.
  SET_NOT_AVAILABLE(OVERLOADED, N3S0, N4S0, N8S0);
  SET_NOT_AVAILABLE(NO_SPC, N5S0, N7S0);
  SET_NOT_AVAILABLE(UNROUTABLE, N0S0, N6S0);
  start();

  CHECK_NO_STORE_MSG();
  CHECK_NO_DELETE_MSG();
  CHECK_NO_RELEASE_MSG();

  // In addition to N1S0, N2S0, N3S0 and N5S0 are available. The next wave will
  // have 1 extra.
  // Note: the current implementation causes the next wave to be wave 3.
  SET_AVAILABLE(N3S0, N5S0);
  triggerTimeout();

  CHECK_STORE_MSG_AND_TRIGGER_ON_SENT(E::OK, 3, N1S0, N2S0, N3S0, N5S0);
  CHECK_NO_STORE_MSG();
  ON_STORED_SENT(E::OK, 3, N5S0, N2S0, N1S0);
  CHECK_APPENDED(E::OK);
  // DELETE is sent to N3S0
  CHECK_DELETE_MSG(N3S0);
  ASSERT_TRUE(retired_);
  Appender::Reaper()(appender_);
  CHECK_RELEASE_MSG(N1S0, N2S0, N5S0);
}

TEST_F(AppenderTest, PreemptedSimple) {
  updateConfig();
  first_candidate_idx_ = 0;
  start();

  CHECK_STORE_MSG_AND_TRIGGER_ON_SENT(E::OK, 1, N0S0, N1S0, N2S0, N3S0, N4S0);
  CHECK_NO_STORE_MSG();

  // N2S0, N4S0 reply.
  ON_STORED_SENT(E::OK, 1, N2S0, N4S0);

  // N1S0 replies with E::PREEMPTED (sequencer N8 sealed the epoch).
  ON_STORED_SENT_PREEMPTED(N8, 1, N1S0);

  // APPENDED with E::PREEMPTED should have been sent.
  CHECK_APPENDED_PREEMPTED(N8)

  // We should have retired early.
  ASSERT_TRUE(retired_);
  // Reap the appender.
  Appender::Reaper()(appender_);

  CHECK_NO_RELEASE_MSG();
}

TEST_F(AppenderTest, PreemptedByDeadNode) {
  updateConfig();
  first_candidate_idx_ = 0;
  start();

  CHECK_STORE_MSG_AND_TRIGGER_ON_SENT(E::OK, 1, N0S0, N1S0, N2S0, N3S0, N4S0);
  CHECK_NO_STORE_MSG();

  // N2S0, N4S0 reply.
  ON_STORED_SENT(E::OK, 1, N2S0, N4S0);

  // pretend N8 is dead
  dead_nodes_.insert(N8);
  // N1S0 replies with E::PREEMPTED (sequencer N8 sealed the epoch).
  ON_STORED_SENT_PREEMPTED(N8, 1, N1S0);

  // APPENDED with E::PREEMPTED should have been sent.
  CHECK_APPENDED_PREEMPTED(N8);
  ASSERT_TRUE(reply_->flags & APPENDED_Header::REDIRECT_NOT_ALIVE);

  // We should have retired early.
  ASSERT_TRUE(retired_);
  // Reap the appender.
  Appender::Reaper()(appender_);

  CHECK_NO_RELEASE_MSG();
}

// Check that STORED with status E::REBUILDING or E::NOSPC makes appender
// update node state in NodeSetState.
TEST_F(AppenderTest, NodeSetStateTransientErrors) {
  updateConfig();
  start();

  CHECK_STORE_MSG_AND_TRIGGER_ON_SENT(E::OK, 1, N0S0, N1S0, N2S0, N3S0, N4S0);

  ON_STORED_SENT(E::OK, 1, N2S0, N3S0);
  ON_STORED_SENT(E::DISABLED, 1, N0S0);
  CHECK_NOT_AVAILABLE(N0S0, STORE_DISABLED);
  ON_STORED_SENT(E::NOSPC, 1, N1S0);
  CHECK_NOT_AVAILABLE(N1S0, NO_SPC);
  ASSERT_FALSE(reply_.hasValue());
  ASSERT_FALSE(retired_);
  ON_STORED_SENT(E::OK, 1, N4S0);
  CHECK_APPENDED(E::OK);
  ASSERT_TRUE(retired_);
  Appender::Reaper()(appender_);
  CHECK_RELEASE_MSG(N2S0, N3S0, N4S0);
}

// Test that `nodes_stored_amendable_' is updated correctly as we get STORED
// replies
TEST_F(AppenderTest, NodesStoredAmendable) {
  shards_ = {N0S0, N1S0};
  replication_ = 2;
  extras_ = 0;
  updateConfig();
  start();

  CHECK_STORE_MSG_AND_TRIGGER_ON_SENT(E::OK, 1, N0S0, N1S0);
  // FAILED reply should not affect the set
  appender_->onReply(storedHeader(1, E::FAILED, NodeID(), 0 /* flags */), N0S0);
  triggerTimeout();
  ASSERT_EQ((std::multiset<ShardID>{}), appender_->getNodesStoredAmendable());

  CHECK_STORE_MSG_AND_TRIGGER_ON_SENT(E::OK, 2, N0S0, N1S0);
  // OK should trigger addition to the set
  appender_->onReply(storedHeader(2, E::OK, NodeID(), 0 /* flags */), N0S0);
  triggerTimeout();
  ASSERT_EQ(
      (std::multiset<ShardID>{N0S0}), appender_->getNodesStoredAmendable());

  CHECK_STORE_MSG_AND_TRIGGER_ON_SENT(E::OK, 3, N0S0, N1S0);
  // Repeat should be a no-op
  appender_->onReply(storedHeader(3, E::OK, NodeID(), 0 /* flags */), N0S0);
  triggerTimeout();
  ASSERT_EQ(
      (std::multiset<ShardID>{N0S0}), appender_->getNodesStoredAmendable());

  CHECK_STORE_MSG_AND_TRIGGER_ON_SENT(E::OK, 4, N0S0, N1S0);
  // Other node can be added to the set
  appender_->onReply(storedHeader(4, E::OK, NodeID(), 0 /* flags */), N1S0);
  triggerTimeout();
  ASSERT_EQ((std::multiset<ShardID>{N0S0, N1S0}),
            appender_->getNodesStoredAmendable());
}

// If the first wave fails but someone nodes send STORED,
// subsequent waves should amend those copies and avoid resending the payload
TEST_F(AppenderTest, SubsequentWavesAmendDirect) {
  shards_ = {N0S0, N1S0, N2S0, N3S0};
  replication_ = 3;
  extras_ = 0;
  updateConfig();

  // Instruct TestCopySetSelector to issue {N0S0, N1S0, N2S0} as the copyset
  first_candidate_idx_ = 0;
  start();
  CHECK_STORE_MSG_AND_TRIGGER_ON_SENT(E::OK, 1, N0S0, N1S0, N2S0);
  appender_->onReply(storedHeader(1, E::OK, NodeID(), 0 /* flags */), N0S0);
  appender_->onReply(storedHeader(1, E::FAILED, NodeID(), 0 /* flags */), N1S0);
  appender_->onReply(storedHeader(1, E::FAILED, NodeID(), 0 /* flags */), N2S0);

  // Issue {N3S0, N0S0, N1S0} for the second wave.  N0S0 should get a STORE with
  // the AMEND flag set.
  first_candidate_idx_ = 3;
  triggerTimeout();
  auto STORE_has_amend_flag = [&](ShardID shard) -> bool {
    auto it = store_msgs_.find(shard);
    if (it == store_msgs_.end()) {
      ADD_FAILURE();
      return false;
    }
    return getHeader(it->second.get()).flags & STORE_Header::AMEND;
  };
  ASSERT_TRUE(STORE_has_amend_flag(N0S0))
      << "N0S0 succeeded on first wave, should have amended";
  ASSERT_FALSE(STORE_has_amend_flag(N1S0))
      << "N1S0 didn't succeed on first wave, should not have amended";
  ASSERT_FALSE(STORE_has_amend_flag(N3S0))
      << "N3S0 failed on first wave, should not have amended";
}

// When chain-sending, amends have to be handled delicately. If any nodes in
// the chain need the payload, we have to transmit it; cannot just send
// amends.
TEST_F(AppenderTest, SubsequentWavesAmendChained) {
  shards_ = {N0S0, N1S0, N2S0, N3S0, N4S0};
  replication_ = 5;
  extras_ = 0;
  settings_.disable_chain_sending = false;
  updateConfig();

  start();
  {
    ASSERT_EQ(1, store_msgs_.size());
    auto it = store_msgs_.find(N0S0);
    ASSERT_NE(store_msgs_.end(), it);
    ASSERT_FALSE(getHeader(it->second.get()).flags & STORE_Header::AMEND)
        << "first STORE wave should never have AMEND flag set";
    ASSERT_GE(getExtra(it->second.get()).first_amendable_offset,
              getHeader(it->second.get()).copyset_size)
        << "first STORE wave should have first_amendable_offset >= r "
           "(effectively infinite)";
  }
  CHECK_STORE_MSG_AND_TRIGGER_ON_SENT(E::OK, 1, N0S0);
  appender_->onReply(storedHeader(1, E::OK, NodeID(), 0 /* flags */), N0S0);
  // N1 didn't reply
  appender_->onReply(storedHeader(1, E::OK, NodeID(), 0 /* flags */), N2S0);
  appender_->onReply(storedHeader(1, E::OK, NodeID(), 0 /* flags */), N3S0);
  appender_->onReply(storedHeader(1, E::FAILED, NodeID(), 0 /* flags */), N4S0);

  triggerTimeout();
  // Because N4S0 failed to store, the second wave is not amendable at all (N4S0
  // needs the payload and is last in the chain)
  {
    ASSERT_EQ(1, store_msgs_.size());
    auto it = store_msgs_.find(N0S0);
    ASSERT_NE(store_msgs_.end(), it);
    ASSERT_FALSE(getHeader(it->second.get()).flags & STORE_Header::AMEND)
        << "second STORE wave should not have AMEND flag set";
    ASSERT_GE(getExtra(it->second.get()).first_amendable_offset,
              getHeader(it->second.get()).copyset_size)
        << "second STORE wave should have first_amendable_offset >= r "
           "(effectively infinite)";
  }
  CHECK_STORE_MSG_AND_TRIGGER_ON_SENT(E::OK, 2, N0S0);
  appender_->onReply(storedHeader(1, E::FAILED, NodeID(), 0 /* flags */), N0S0);
  appender_->onReply(storedHeader(1, E::FAILED, NodeID(), 0 /* flags */), N1S0);
  appender_->onReply(storedHeader(1, E::FAILED, NodeID(), 0 /* flags */), N2S0);
  appender_->onReply(storedHeader(1, E::FAILED, NodeID(), 0 /* flags */), N3S0);
  appender_->onReply(storedHeader(1, E::FAILED, NodeID(), 0 /* flags */), N4S0);

  // If we rotate the copyset to be N4S0 N0S0* N1S0 N2S0* N3S0* where asterisks
  // indicates nodes that are amendable, `first_amendable_offset' in the STORE
  // should become 3; N1S0 should drop the payload when forwarding to N2S0.
  first_candidate_idx_ = 4;
  triggerTimeout();
  {
    ASSERT_EQ(1, store_msgs_.size());
    auto it = store_msgs_.find(N4S0);
    ASSERT_NE(store_msgs_.end(), it);
    ASSERT_FALSE(getHeader(it->second.get()).flags & STORE_Header::AMEND)
        << "third STORE wave should not have AMEND flag set";
    ASSERT_EQ(3, getExtra(it->second.get()).first_amendable_offset)
        << "third STORE wave should allow amending N2S0 and N3S0";
  }
  CHECK_STORE_MSG_AND_TRIGGER_ON_SENT(E::OK, 3, N4S0);
  // Now let the remainining two storage nodes become amendable so the last
  // wave is all amendable
  appender_->onReply(storedHeader(1, E::FAILED, NodeID(), 0 /* flags */), N0S0);
  appender_->onReply(storedHeader(1, E::OK, NodeID(), 0 /* flags */), N1S0);
  appender_->onReply(storedHeader(1, E::OK, NodeID(), 0 /* flags */), N2S0);
  appender_->onReply(storedHeader(1, E::OK, NodeID(), 0 /* flags */), N3S0);
  appender_->onReply(storedHeader(1, E::OK, NodeID(), 0 /* flags */), N4S0);

  first_candidate_idx_ = 0;
  triggerTimeout();
  {
    // Appender::decideAmends() disables chaining when we're mostly amending
    ASSERT_EQ(replication_, store_msgs_.size());
    for (const auto& entry : store_msgs_) {
      ASSERT_FALSE(getHeader(entry.second.get()).flags & STORE_Header::CHAIN);
      ASSERT_TRUE(getHeader(entry.second.get()).flags & STORE_Header::AMEND);
    }
  }
  CHECK_STORE_MSG_AND_TRIGGER_ON_SENT(E::OK, 4, N0S0, N1S0, N2S0, N3S0, N4S0);
}

// test the scenario that Appender for draining may receive preemptions from
// storage node indicating the STOREs were preempted by soft seals only.
// Appender should retry a new wave with DRAINING flags
TEST_F(AppenderTest, SoftPreemptedWhenDraining) {
  replication_ = 3;
  extras_ = 1;
  updateConfig();
  first_candidate_idx_ = 0;
  start();

  CHECK_STORE_MSG_AND_TRIGGER_ON_SENT(E::OK, 1, N0S0, N1S0, N2S0, N3S0);
  CHECK_NO_STORE_MSG();

  /// sequencer starts draining appenders
  draining_ = true;

  // N1S0 replies E::OK
  ON_STORED_SENT(E::OK, 1, N1S0);

  // however, N2S0 was soft preempted
  const STORED_flags_t SOFT(STORED_Header::PREMPTED_BY_SOFT_SEAL_ONLY);
  appender_->onReply(storedHeader(1, E::PREEMPTED, N8, SOFT), N2S0);

  // sequencer should NOT be preempted, no reply should been sent
  EXPECT_EQ(EPOCH_INVALID, preempted_epoch_);
  EXPECT_FALSE(preempted_by_.isNodeID());
  ASSERT_FALSE(reply_.hasValue());

  // since the appender may still complete the wave, no new wave should
  // be started
  CHECK_NO_STORE_MSG();

  // now N3S0 is also soft preempted
  appender_->onReply(storedHeader(1, E::PREEMPTED, N8, SOFT), N3S0);
  // sequencer should NOT be preempted either
  EXPECT_EQ(EPOCH_INVALID, preempted_epoch_);
  EXPECT_FALSE(preempted_by_.isNodeID());
  ASSERT_FALSE(reply_.hasValue());

  // there is no hope to finish the wave, the Appender should retry
  // for a new wave
  ASSERT_TRUE(retry_timer_active_);
  ASSERT_FALSE(store_timer_active_);
  first_candidate_idx_ = 3;
  triggerTimeout();

  // this checks the DRAINING flag in STORE header
  CHECK_STORE_MSG_AND_TRIGGER_ON_SENT(E::OK, 2, N3S0, N4S0, N5S0, N6S0);
  CHECK_NO_STORE_MSG();

  // simulate N4S0, N5S0, N6S0 replying
  ON_STORED_SENT(E::OK, 2, N4S0, N5S0, N6S0);
  // We have `replication` copies. We should have sent APPENDED.
  CHECK_APPENDED(E::OK);
  // we should have retired.
  ASSERT_TRUE(retired_);
  // simulate N3S0 replying
  ON_STORED_SENT(E::OK, 2, N3S0);
  // Reap the appender.
  Appender::Reaper()(appender_);
  // Check that the Appender sent RELEASE messages to the 3 nodes.
  CHECK_RELEASE_MSG(N4S0, N5S0, N6S0);
}

// similar to the previous test, but test appender in draining can still be
// preempted by normal seals
TEST_F(AppenderTest, PremptedByNormalSealWhenDraining) {
  replication_ = 3;
  extras_ = 1;
  updateConfig();
  first_candidate_idx_ = 0;
  start();

  CHECK_STORE_MSG_AND_TRIGGER_ON_SENT(E::OK, 1, N0S0, N1S0, N2S0, N3S0);
  CHECK_NO_STORE_MSG();
  draining_ = true;
  ON_STORED_SENT(E::OK, 1, N1S0);
  // however, N2S0 was soft preempted
  const STORED_flags_t SOFT(STORED_Header::PREMPTED_BY_SOFT_SEAL_ONLY);
  appender_->onReply(storedHeader(1, E::PREEMPTED, N8, SOFT), N2S0);
  appender_->onReply(storedHeader(1, E::PREEMPTED, N8, SOFT), N3S0);
  EXPECT_EQ(EPOCH_INVALID, preempted_epoch_);
  EXPECT_FALSE(preempted_by_.isNodeID());
  ASSERT_FALSE(reply_.hasValue());
  ASSERT_TRUE(retry_timer_active_);
  ASSERT_FALSE(store_timer_active_);
  first_candidate_idx_ = 3;
  triggerTimeout();
  CHECK_STORE_MSG_AND_TRIGGER_ON_SENT(E::OK, 2, N3S0, N4S0, N5S0, N6S0);
  CHECK_NO_STORE_MSG();
  // in this case, N3S0 sents a NORMAL preemption
  appender_->onReply(storedHeader(2, E::PREEMPTED, N8, 0), N3S0);
  EXPECT_EQ(lsn_to_epoch(LSN), preempted_epoch_);
  EXPECT_EQ(N8, preempted_by_);
  // APPENDED with E::PREEMPTED should have been sent.
  CHECK_APPENDED_PREEMPTED(N8)
  // We should have retired early.
  ASSERT_TRUE(retired_);
  Appender::Reaper()(appender_);
  // No release message should have been sent.
  CHECK_NO_RELEASE_MSG();
}

TEST_F(AppenderTest, E2ETracing) {
  // check that spans corresponding to store messages are created
  shards_ = {N0S0, N1S0, N2S0, N3S0, N4S0};
  replication_ = 3;
  extras_ = 0;
  updateConfig();
  first_candidate_idx_ = 0;

  auto recorder = new opentracing::mocktracer::InMemoryRecorder{};
  opentracing::mocktracer::MockTracerOptions tracer_options;
  tracer_options.recorder.reset(recorder);
  auto tracer = std::make_shared<opentracing::mocktracer::MockTracer>(
      opentracing::mocktracer::MockTracerOptions{std::move(tracer_options)});

  // send store implementation should create spans for all STOREs sent
  startWithMockE2ETracer(tracer);

  CHECK_STORE_MSG_AND_TRIGGER_ON_SENT(E::OK, 1, N0S0, N1S0, N2S0);
  // when stored is sent both store and stored spans should be finished
  ON_STORED_SENT(E::OK, 1, N0S0, N1S0, N2S0);

  ASSERT_EQ(recorder->spans().size(), replication_ * 2);

  for (unsigned int i = 0; i < replication_; i += 2) {
    auto stored_parent_span_id = recorder->spans().at(i).references[0].span_id;
    auto store_span_id = recorder->spans().at(i + 1).span_context.span_id;
    ASSERT_EQ(stored_parent_span_id, store_span_id);
  }
}

Payload AppenderTest::MockAppender::dummyPayload("test", sizeof("test"));

}} // namespace facebook::logdevice
