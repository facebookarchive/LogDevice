/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/protocol/APPEND_Message.h"

#include <folly/Memory.h>
#include <folly/Optional.h>
#include <gtest/gtest.h>
#include <opentracing/mocktracer/in_memory_recorder.h>
#include <opentracing/mocktracer/tracer.h>

#include "logdevice/common/AppenderPrep.h"
#include "logdevice/common/Checksum.h"
#include "logdevice/common/EpochSequencer.h"
#include "logdevice/common/MetaDataLogWriter.h"
#include "logdevice/common/NoopTraceLogger.h"
#include "logdevice/common/PermissionChecker.h"
#include "logdevice/common/RateLimiter.h"
#include "logdevice/common/Sequencer.h"
#include "logdevice/common/SequencerLocator.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/test/TestUtil.h"

namespace facebook { namespace logdevice {

class MockAppenderPrep;
class APPEND_MessageTest;

namespace {

class MockEpochSequencer : public EpochSequencer {
 public:
  MockEpochSequencer(APPEND_MessageTest* test,
                     logid_t log_id,
                     epoch_t epoch,
                     std::unique_ptr<EpochMetaData> metadata,
                     EpochSequencerImmutableOptions immutable_options,
                     Sequencer* parent)
      : EpochSequencer(log_id,
                       epoch,
                       std::move(metadata),
                       immutable_options,
                       parent),
        test_(test) {}

  const Settings& getSettings() const override;

 private:
  APPEND_MessageTest* const test_;
};

class MockSequencer : public Sequencer {
 public:
  explicit MockSequencer(APPEND_MessageTest* test,
                         logid_t logid,
                         UpdateableSettings<Settings> settings)
      : Sequencer(logid, settings, /*stats=*/nullptr), test_(test) {}

  std::shared_ptr<Configuration> getClusterConfig() const override;

  void startGetTrimPointRequest() override {}

  std::shared_ptr<EpochSequencer>
  createEpochSequencer(epoch_t epoch,
                       std::unique_ptr<EpochMetaData> metadata) override;

  // skip log recovery
  int startRecovery(std::chrono::milliseconds /*delay*/) override {
    return 0;
  }

  void schedulePeriodicReleases() override {}

  void startPeriodicReleasesBroadcast() override {}

  void getHistoricalMetaData(GetHistoricalMetaDataMode /* unused */) override {}

 private:
  APPEND_MessageTest* const test_;
};
} // namespace

class APPEND_MessageTest : public ::testing::Test {
 public:
  static logid_t TEST_LOG;

  explicit APPEND_MessageTest()
      : settings_(create_default_settings<Settings>()) {
    // build a Configuration object and use it to initialize a Sequencer
    Configuration::Node node;
    node.address = Sockaddr("127.0.0.1", "20034");
    node.gossip_address = Sockaddr("127.0.0.1", "20035");
    node.generation = 1;
    node.addStorageRole();
    Configuration::NodesConfig nodes({{0, std::move(node)}});

    logsconfig::LogAttributes log_attrs;
    log_attrs.set_replicationFactor(1);
    auto logs_config = std::make_unique<configuration::LocalLogsConfig>();
    logs_config->insert(boost::icl::right_open_interval<logid_t::raw_type>(
                            TEST_LOG.val_, TEST_LOG.val_ + 1),
                        "test_log",
                        log_attrs);

    // Disabling auto log provisioning
    configuration::MetaDataLogsConfig ml_config;
    ml_config.sequencers_provision_epoch_store = false;
    ml_config.sequencers_write_metadata_logs = false;
    config_ = std::make_shared<Configuration>(
        ServerConfig::fromDataTest(__FILE__, nodes, std::move(ml_config)),
        std::move(logs_config));

    settings_.enable_sticky_copysets = true;

    sequencer_ = std::make_shared<MockSequencer>(
        this, TEST_LOG, UpdateableSettings<Settings>(settings_));

    setSequencerEpoch(current_epoch_);
    principal_ =
        PrincipalIdentity("", std::make_pair("USER", "TestCredentials"));
    dbg::assertOnData = true;
  }

  void setSequencerEpoch(epoch_t epoch) {
    auto metadata = std::make_unique<EpochMetaData>(
        StorageSet{ShardID(0, 0)},
        ReplicationProperty({{NodeLocationScope::NODE, 1}}));
    metadata->h.epoch = metadata->h.effective_since = epoch;

    sequencer_->startActivation([](logid_t) { return 0; });
    sequencer_->completeActivationWithMetaData(
        epoch, config_, std::move(metadata));

    current_epoch_ = epoch;
  }

  void advanceEpoch() {
    ++current_epoch_.val_;
    setSequencerEpoch(current_epoch_);
  }

  std::shared_ptr<MockAppenderPrep>
  create(logid_t log_id,
         APPEND_flags_t flags = APPEND_flags_t(APPEND_Header::CHECKSUM_PARITY),
         epoch_t seen_epoch = EPOCH_INVALID);

  std::shared_ptr<MockAppenderPrep> createWithMockE2ETracing(
      logid_t log_id,
      std::shared_ptr<opentracing::Tracer> e2e_tracer = nullptr,
      std::unique_ptr<opentracing::Span> append_msg_recv_span = nullptr,
      APPEND_flags_t flags = APPEND_flags_t(APPEND_Header::CHECKSUM_PARITY),
      epoch_t seen_epoch = EPOCH_INVALID);

  std::shared_ptr<Sequencer> sequencer_;
  std::shared_ptr<Configuration> config_;
  epoch_t current_epoch_{1};
  Settings settings_;
  PrincipalIdentity principal_;
};
logid_t APPEND_MessageTest::TEST_LOG(1);

class MockSequencerLocator : public SequencerLocator {
 public:
  explicit MockSequencerLocator(MockAppenderPrep& appender_prep)
      : SequencerLocator(), appender_prep_(appender_prep) {}

  int locateSequencer(
      logid_t logid,
      Completion cf,
      const ServerConfig::SequencersConfig* sequencers = nullptr) override;
  bool isAllowedToCache() const override {
    return false;
  }

 private:
  MockAppenderPrep& appender_prep_;
};

class DummyPermissionChecker : public PermissionChecker {
  void isAllowed(ACTION /* unused */,
                 const PrincipalIdentity& /* unused */,
                 logid_t /* unused */,
                 callback_func_t cb) const override {
    cb(PermissionCheckStatus::ALLOWED);
  }
  PermissionCheckerType getPermissionCheckerType() const override {
    return PermissionCheckerType::CONFIG;
  }
};

class MockAppenderPrep : public AppenderPrep {
 public:
  explicit MockAppenderPrep(APPEND_MessageTest* owner)
      : AppenderPrep(PayloadHolder()),
        owner_(owner),
        locator_(*this),
        permission_checker_(std::make_shared<DummyPermissionChecker>(
            DummyPermissionChecker())) {}

  void setAlive(std::initializer_list<NodeID> nodes) {
    for (NodeID node : nodes) {
      alive_[node.index()] = true;
    }
  }
  void setBoycotted(std::initializer_list<NodeID> nodes) {
    for (NodeID node : nodes) {
      boycotted_[node.index()] = true;
    }
  }
  void setSequencer(logid_t log_id, NodeID node_id) {
    sequencer_nodes_[log_id] = node_id;
  }

  std::shared_ptr<opentracing::Span> getAppenderSpan() {
    return appender_span_;
  }

  std::shared_ptr<Sequencer> sequencer_;
  RateLimiter* limiter_{nullptr};
  bool can_activate_{true};
  NodeID my_node_id_;
  // if set, next call to runAppender() returns this value instead of proxying
  // it to Sequencer
  folly::Optional<Status> next_status_;
  bool activate_no_op_{false};
  // cluster nodes that have zero sequencer weight (i.e. don't run sequencers)
  std::set<node_index_t> seq_weight_zero_;

  std::vector<std::unique_ptr<Appender>> buffered_appenders_;
  std::vector<Appender*> running_appenders_;
  mutable std::vector<std::pair<Status, NodeID>> results_;

  void setPrincipal(PrincipalIdentity principal) {
    owner_->principal_ = principal;
  }

  void setAcceptWork(bool acceptWork) {
    accept_work = acceptWork;
  }

  int getSequencerNode(logid_t log_id, NodeID* node_out) const {
    auto it = sequencer_nodes_.find(log_id);
    if (it == sequencer_nodes_.end()) {
      return -1;
    }

    *node_out = it->second;
    return 0;
  }

 protected:
  std::shared_ptr<Sequencer> findSequencer(logid_t) const override {
    if (!sequencer_) {
      err = E::NOSEQUENCER;
    }
    return sequencer_;
  }
  NodeID getMyNodeID() const override {
    return my_node_id_;
  }
  bool isAlive(NodeID node) const override {
    auto it = alive_.find(node.index());
    return it != alive_.end() && it->second;
  }
  bool isBoycotted(NodeID node) const override {
    auto it = boycotted_.find(node.index());
    return it != boycotted_.end() && it->second;
  }

  bool isIsolated() const override {
    return false;
  }

  bool nodeInConfig(NodeID node) const override {
    return seq_weight_zero_.find(node.index()) == seq_weight_zero_.end();
  }
  int activateSequencer(logid_t /*log_id*/,
                        std::shared_ptr<Sequencer>& sequencer,
                        Sequencer::ActivationPred pred) override {
    if (limiter_ && !limiter_->isAllowed()) {
      err = E::TOOMANY;
      return -1;
    }
    if (sequencer && pred && !pred(*sequencer)) {
      err = E::ABORTED;
      return -1;
    }
    if (!activate_no_op_) {
      owner_->advanceEpoch();
    }
    return 0;
  }
  int bufferAppender(logid_t /*log_id*/,
                     std::unique_ptr<Appender>& appender) override {
    buffered_appenders_.push_back(std::move(appender));
    return 0;
  }
  bool checkNodeSet(const Sequencer&) const override {
    return true;
  }
  bool hasBufferedAppenders(logid_t) const override {
    return false;
  }
  RunAppenderStatus runAppender(Sequencer& s, Appender& appender) override {
    if (running_appenders_.size() == 0 ||
        running_appenders_.back() != &appender) {
      // add the Appender only once to the list
      running_appenders_.push_back(&appender);
    }

    if (next_status_.hasValue()) {
      err = next_status_.value();
      next_status_.clear();
      return err != E::OK ? RunAppenderStatus::ERROR_DELETE
                          : RunAppenderStatus::SUCCESS_KEEP;
    }
    return s.runAppender(&appender);
  }
  void updateNoRedirectUntil(Sequencer& sequencer) override {
    sequencer.setNoRedirectUntil(std::chrono::steady_clock::duration::max());
  }
  bool canActivateSequencers() const override {
    return can_activate_;
  }
  bool isAcceptingWork() const override {
    return accept_work;
  }
  void sendError(Appender*, Status st) const override {
    results_.push_back(std::make_pair(st, NodeID()));
  }
  void sendRedirect(Appender*, NodeID node, Status st) const override {
    results_.push_back(std::make_pair(st, node));
  }
  StatsHolder* stats() const override {
    return nullptr;
  }
  const Settings& getSettings() const override {
    return owner_->settings_;
  }
  const PrincipalIdentity* getPrincipal() override {
    return &owner_->principal_;
  }
  void isAllowed(std::shared_ptr<PermissionChecker> permission_checker,
                 const PrincipalIdentity& principal,
                 callback_func_t cb) override {
    if (permission_checker) {
      for (auto identity : principal.identities) {
        if (identity.second == "TestCredentials") {
          std::cout << "ALLOWED: " << identity.second;
          cb(PermissionCheckStatus::ALLOWED);
          return;
        }
      }
    }
    std::cout << "DENIED ";
    cb(PermissionCheckStatus::DENIED);
  }

  std::shared_ptr<PermissionChecker> getPermissionChecker() override {
    return permission_checker_;
  }

  SequencerLocator& getSequencerLocator() override {
    return locator_;
  }

 private:
  APPEND_MessageTest* owner_;
  std::unordered_map<node_index_t, bool> alive_;
  std::unordered_map<node_index_t, bool> boycotted_;
  std::unordered_map<logid_t, NodeID, logid_t::Hash> sequencer_nodes_;
  bool accept_work{true};
  MockSequencerLocator locator_;
  std::shared_ptr<PermissionChecker> permission_checker_;
};

int MockSequencerLocator::locateSequencer(
    logid_t logid,
    Completion cf,
    const configuration::SequencersConfig*) {
  NodeID node_out;
  if (appender_prep_.getSequencerNode(logid, &node_out) == 0) {
    cf(E::OK, logid, node_out);
  } else {
    cf(E::NOTFOUND, logid, NodeID());
  }
  return 0;
}

namespace {
std::shared_ptr<Configuration> MockSequencer::getClusterConfig() const {
  return test_->config_;
}

std::shared_ptr<EpochSequencer>
MockSequencer::createEpochSequencer(epoch_t epoch,
                                    std::unique_ptr<EpochMetaData> metadata) {
  EpochSequencerImmutableOptions opts;
  opts.window_size = 128;
  return std::make_shared<MockEpochSequencer>(
      test_, getLogID(), epoch, std::move(metadata), opts, this);
}

const Settings& MockEpochSequencer::getSettings() const {
  return test_->settings_;
}

} // namespace

class APPEND_MessageTest_MockAppender : public Appender {
 public:
  explicit APPEND_MessageTest_MockAppender(
      epoch_t seen_epoch,
      STORE_flags_t flags = STORE_flags_t(0))
      : Appender(
            nullptr,
            std::make_shared<NoopTraceLogger>(UpdateableConfig::createEmpty()),
            std::chrono::seconds(1),
            request_id_t(1),
            passthru_flags(flags),
            APPEND_MessageTest::TEST_LOG,
            PayloadHolder(dummyPayload, PayloadHolder::UNOWNED),
            seen_epoch,
            50) {}

  explicit APPEND_MessageTest_MockAppender(
      STORE_flags_t flags = STORE_flags_t(0))
      : APPEND_MessageTest_MockAppender(epoch_t(0), flags) {}

  explicit APPEND_MessageTest_MockAppender(
      const MockAppenderPrep& prep,
      STORE_flags_t flags = STORE_flags_t(0))
      : APPEND_MessageTest_MockAppender(prep.getSeen(), flags) {}

  explicit APPEND_MessageTest_MockAppender(
      const MockAppenderPrep& prep,
      PayloadHolder ph,
      STORE_flags_t flags = STORE_flags_t(0))
      : Appender(
            nullptr,
            std::make_shared<NoopTraceLogger>(UpdateableConfig::createEmpty()),
            std::chrono::seconds(1),
            request_id_t(1),
            passthru_flags(flags),
            APPEND_MessageTest::TEST_LOG,
            std::move(ph),
            prep.getSeen(),
            50) {}
  explicit APPEND_MessageTest_MockAppender(
      const MockAppenderPrep& prep,
      std::shared_ptr<opentracing::Tracer> e2e_tracer,
      std::shared_ptr<opentracing::Span> appender_span)
      : Appender(
            nullptr,
            std::make_shared<NoopTraceLogger>(UpdateableConfig::createEmpty()),
            std::chrono::seconds(1),
            request_id_t(1),
            passthru_flags(STORE_flags_t(0)),
            APPEND_MessageTest::TEST_LOG,
            PayloadHolder(dummyPayload, PayloadHolder::UNOWNED),
            prep.getSeen(),
            50,
            e2e_tracer,
            appender_span) {}

  int start(std::shared_ptr<EpochSequencer> /*epoch_sequencer*/,
            lsn_t /*lsn*/) override {
    delete this;
    return 0;
  }

 private:
  static Payload dummyPayload;

  // Passthru flags from AppenderPrep to Appender
  STORE_flags_t passthru_flags(STORE_flags_t flags) {
    return flags &
        (APPEND_Header::CHECKSUM | APPEND_Header::CHECKSUM_64BIT |
         APPEND_Header::CHECKSUM_PARITY | APPEND_Header::BUFFERED_WRITER_BLOB);
  }
};
using MockAppender = APPEND_MessageTest_MockAppender;
Payload MockAppender::dummyPayload("mirko", strlen("mirko"));

std::shared_ptr<MockAppenderPrep>
APPEND_MessageTest::create(logid_t log_id,
                           APPEND_flags_t flags,
                           epoch_t seen_epoch) {
  APPEND_Header header{};
  header.logid = log_id;
  header.seen = seen_epoch;
  header.flags = flags;

  auto prep = std::make_shared<MockAppenderPrep>(this);
  AppendAttributes attrs;
  attrs.optional_keys[KeyType::FINDKEY] = "abcdefgh";
  prep->setAppendMessage(header, LSN_INVALID, ClientID(1), attrs);
  prep->sequencer_ = sequencer_;
  prep->my_node_id_ = NodeID(0, 1);
  return prep;
}

std::shared_ptr<MockAppenderPrep> APPEND_MessageTest::createWithMockE2ETracing(
    logid_t log_id,
    std::shared_ptr<opentracing::Tracer> tracer,
    std::unique_ptr<opentracing::Span> append_msg_recv_span,
    APPEND_flags_t flags,
    epoch_t seen_epoch) {
  APPEND_Header header{};
  header.logid = log_id;
  header.seen = seen_epoch;
  header.flags = flags;

  auto prep = std::make_shared<MockAppenderPrep>(this);

  AppendAttributes attrs;
  attrs.optional_keys[KeyType::FINDKEY] = "abcdefgh";
  prep->setAppendMessage(header,
                         LSN_INVALID,
                         ClientID(1),
                         attrs,
                         tracer,
                         std::move(append_msg_recv_span));
  prep->sequencer_ = sequencer_;
  prep->my_node_id_ = NodeID(0, 1);

  return prep;
}

#define ASSERT_RUNNING(prep, arg)                                       \
  do {                                                                  \
    ASSERT_EQ((std::vector<Appender*>(arg)), prep->running_appenders_); \
    prep->running_appenders_.clear();                                   \
  } while (0)

#define ASSERT_BUFFERED(prep, arg)                                          \
  do {                                                                      \
    std::vector<Appender*> actual;                                          \
    for (const std::unique_ptr<Appender>& a_ : prep->buffered_appenders_) { \
      actual.push_back(a_.get());                                           \
    }                                                                       \
    ASSERT_EQ((std::vector<Appender*>(arg)), actual);                       \
    prep->buffered_appenders_.clear();                                      \
  } while (0)

#define ASSERT_RESULTS(prep, ...)                                          \
  do {                                                                     \
    auto& v = prep->results_;                                              \
    ASSERT_EQ((std::vector<std::pair<Status, NodeID>>({__VA_ARGS__})), v); \
    v.clear();                                                             \
  } while (0)

TEST_F(APPEND_MessageTest, Basic) {
  const logid_t log1(1), log2(2);
  const NodeID N1(0, 1), N2(1, 1);

  {
    std::unique_ptr<Appender> a(new MockAppender);
    Appender* raw = a.get();
    auto prep = create(log1);
    prep->my_node_id_ = N1;
    prep->sequencer_ = nullptr;
    prep->setSequencer(log1, N1);
    prep->setAlive({N1, N2});

    // this append should be handled by N1
    prep->execute(std::move(a));
    ASSERT_BUFFERED(prep, {raw});
  }
  {
    std::unique_ptr<Appender> a(new MockAppender);
    auto prep = create(log1);
    prep->my_node_id_ = N1;
    prep->setSequencer(log1, N2);
    prep->setAlive({N1, N2});

    // sequencer on N1 is expected to send a redirect to N2
    prep->execute(std::move(a));
    ASSERT_RESULTS(prep, std::make_pair(E::REDIRECTED, N2));
  }
  {
    std::unique_ptr<Appender> a(new MockAppender);
    Appender* raw = a.get();
    auto prep = create(log2);
    prep->my_node_id_ = N1;
    prep->setSequencer(log2, N2);
    prep->setAlive({N1});

    // even though N2 is responsible for log1, N1 should handle it because
    // N2 is dead
    prep->execute(std::move(a));
    ASSERT_RUNNING(prep, {raw});
  }
  {
    std::unique_ptr<Appender> a(new MockAppender);
    Appender* raw = a.get();
    auto prep = create(log2);
    prep->my_node_id_ = N1;
    prep->setSequencer(log2, N2);
    prep->setAlive({N1, N2});
    prep->setBoycotted({N2});

    // even though N2 is responsible for log1, N1 should handle it because
    // N2 is boycotted
    prep->execute(std::move(a));
    ASSERT_RUNNING(prep, {raw});
  }
}

// Tests that an append with NO_REDIRECT flag is processed even though another
// node runs sequencers for a log.
TEST_F(APPEND_MessageTest, NO_REDIRECT) {
  const logid_t log(1);
  const NodeID N1(0, 1), N2(1, 1);

  STORE_flags_t flags =
      APPEND_Header::NO_REDIRECT | APPEND_Header::CHECKSUM_PARITY;
  std::unique_ptr<Appender> appender(new MockAppender(flags));
  Appender* raw = appender.get();
  auto prep = create(log, flags);
  prep->my_node_id_ = N1;
  prep->setSequencer(log, N2);
  prep->setAlive({N1, N2});

  // this append should be handled by N1
  prep->execute(std::move(appender));
  ASSERT_RUNNING(prep, {raw});
}

// Verifies that preemption redirects are correctly handled.
TEST_F(APPEND_MessageTest, PreemptionRedirects) {
  const logid_t log(1);
  const NodeID N1(0, 1), N2(1, 1);

  // Let's assume that sequencer on this node was preempted by N2.
  sequencer_->notePreempted(EPOCH_MAX, N2);

  {
    std::unique_ptr<Appender> appender(new MockAppender);
    auto prep = create(log);
    prep->my_node_id_ = N1;
    prep->setSequencer(log, N1);
    prep->setAlive({N1, N2});

    // N1 is alive and should handle `log', but it was preempted by N2.
    prep->execute(std::move(appender));
    ASSERT_RESULTS(prep, std::make_pair(E::PREEMPTED, N2));
  }
  {
    std::unique_ptr<Appender> appender(new MockAppender);
    Appender* raw = appender.get();
    auto prep = create(log);
    prep->my_node_id_ = N1;
    prep->setSequencer(log, N1);
    prep->setAlive({N1});

    // N1 was preempted by N2, but N2 is dead. Make sure that N1 reactivates
    // the sequencer and buffers the appender.
    prep->execute(std::move(appender));
    ASSERT_BUFFERED(prep, {raw});
  }
  {
    std::unique_ptr<Appender> appender(new MockAppender);
    Appender* raw = appender.get();
    auto prep = create(log);
    prep->my_node_id_ = N1;
    prep->setSequencer(log, N1);
    prep->setAlive({N1, N2});
    prep->setBoycotted({N2});

    // N1 was preempted by N2, but N2 is boycotted. Make sure that N1
    // reactivates the sequencer and buffers the appender.
    prep->execute(std::move(appender));
    ASSERT_BUFFERED(prep, {raw});
  }
}

// Tests that, after receiving an append with NO_REDIRECT, sequencer node
// executes further appends even if it doesn't normally handle the log.
TEST_F(APPEND_MessageTest, NoRedirectUntil) {
  const logid_t log(1);
  const NodeID N1(0, 1), N2(1, 1);

  {
    // issue an append to N1 with NO_REDIRECT flag set
    STORE_flags_t flags =
        APPEND_Header::NO_REDIRECT | APPEND_Header::CHECKSUM_PARITY;
    std::unique_ptr<Appender> appender(new MockAppender(flags));
    Appender* raw = appender.get();
    auto prep = create(log, flags);
    prep->my_node_id_ = N1;
    prep->setSequencer(log, N1);
    prep->setAlive({N1, N2});

    prep->execute(std::move(appender));
    ASSERT_RUNNING(prep, {raw});
  }
  {
    // Send another one to N1. Assert that it processes it even though N2 runs
    // a most recent sequencer for the log.
    std::unique_ptr<Appender> appender(new MockAppender);
    Appender* raw = appender.get();
    auto prep = create(log);
    prep->my_node_id_ = N1;
    prep->setSequencer(log, N2);
    prep->setAlive({N1, N2});

    prep->execute(std::move(appender));
    ASSERT_RUNNING(prep, {raw});
  }
}

// Verifies that APPEND messages with `seen' greater than sequencer's current
// epoch cause a reactivation.
TEST_F(APPEND_MessageTest, SeenEpoch) {
  const logid_t log(1);
  const NodeID N1(0, 1);

  setSequencerEpoch(epoch_t(2));

  STORE_flags_t flags = APPEND_Header::CHECKSUM_PARITY;
  auto prep = create(log,
                     flags,
                     /* seen epoch */ epoch_t(3));
  std::unique_ptr<Appender> appender(new MockAppender(*prep, flags));
  Appender* raw = appender.get();
  prep->my_node_id_ = N1;
  prep->setSequencer(log, N1);
  prep->setAlive({N1});

  prep->execute(std::move(appender));
  ASSERT_BUFFERED(prep, {raw});
}

// In this test, two APPEND messages that would cause reactivation (greater
// `seen' epoch) execute at the same time (i.e. both get E::STALE). Only one
// should reactivate.
TEST_F(APPEND_MessageTest, ConcurrentReactivations) {
  const logid_t log(1);
  const NodeID N1(0, 1);

  setSequencerEpoch(epoch_t(2));

  STORE_flags_t flags = APPEND_Header::CHECKSUM_PARITY;
  auto prep1 = create(log,
                      flags,
                      /* seen epoch */ epoch_t(3));
  prep1->my_node_id_ = N1;
  prep1->next_status_ = E::STALE;
  prep1->activate_no_op_ = true; // don't cause actual reactivation
  prep1->setSequencer(log, N1);
  prep1->setAlive({N1});

  std::unique_ptr<Appender> a1(new MockAppender(*prep1, flags));
  Appender* r1 = a1.get();
  prep1->execute(std::move(a1));
  ASSERT_BUFFERED(prep1, {r1});

  flags = 16;
  auto prep2 = create(log, flags, /* seen epoch */ epoch_t(3));
  prep2->my_node_id_ = N1;
  prep2->next_status_ = E::STALE;
  prep2->setSequencer(log, N1);
  prep2->setAlive({N1});

  // reactivation, triggered by the first APPEND, finished after prep2 got
  // E::STALE
  ASSERT_EQ(2, current_epoch_.val_);
  advanceEpoch();

  // prep2 should be executed instead of causing another reactivation
  std::unique_ptr<Appender> a2(new MockAppender(*prep2, flags));
  Appender* r2 = a2.get();
  prep2->execute(std::move(a2));
  ASSERT_RUNNING(prep2, {r2});

  ASSERT_EQ(3, current_epoch_.val_);
}

// Tests that APPEND fails if processing it would cause the reactivation limit
// to be exceeded on the sequencer node.
TEST_F(APPEND_MessageTest, ReactivationLimit) {
  // only allow one reactivation
  RateLimiter limiter(1e-3, 1e-4);

  const logid_t log(1);
  const NodeID N1(0, 1), N2(1, 1);

  setSequencerEpoch(epoch_t(2));
  sequencer_->notePreempted(epoch_t(2), N2);

  STORE_flags_t flags =
      APPEND_Header::REACTIVATE_IF_PREEMPTED | APPEND_Header::CHECKSUM_PARITY;
  auto create_prep = [&]() {
    auto prep = create(log, flags);
    prep->limiter_ = &limiter;
    prep->my_node_id_ = N1;
    prep->setSequencer(log, N1);
    prep->setAlive({N1, N2});
    return prep;
  };

  // Send an append with REACTIVATE_IF_PREEMPTED to N1, forcing it to
  // reactivate even though it was preempted.
  {
    auto prep = create_prep();
    std::unique_ptr<Appender> appender(new MockAppender(flags));
    Appender* raw = appender.get();
    prep->execute(std::move(appender));
    ASSERT_BUFFERED(prep, {raw});
  }

  // Send another APPEND to the same node. This request should fail since
  // node would reactivate the sequencer, but the limit has been reached.
  {
    auto prep = create_prep();
    std::unique_ptr<Appender> appender(new MockAppender(flags));
    sequencer_->notePreempted(epoch_t(3), N2);
    prep->execute(std::move(appender));
    ASSERT_RESULTS(prep, std::make_pair(E::NOSEQUENCER, NodeID()));
  }
}

// Tests that APPEND fails if the node is not already running a sequencer and
// lazy bringup is disabled.
TEST_F(APPEND_MessageTest, LazyBringupDisabled) {
  const logid_t log(1);
  const NodeID N1(0, 1);

  auto prep = create(log);
  std::unique_ptr<Appender> appender(new MockAppender(*prep));
  prep->my_node_id_ = N1;
  prep->sequencer_ = nullptr; // sequencer doesn't exist yet
  prep->can_activate_ = false;
  prep->setSequencer(log, N1);
  prep->setAlive({N1});

  prep->execute(std::move(appender));
  ASSERT_RESULTS(prep, std::make_pair(E::NOSEQUENCER, NodeID()));
}

// Verifies that nodes will reject appends if the sequencer node can't be
// determined (e.g. all nodes are unavailable).
TEST_F(APPEND_MessageTest, SequencerUnknown) {
  const logid_t log(1);
  const NodeID N1(0, 1), N2(1, 1), N3(2, 1);

  auto prep = create(log);
  std::unique_ptr<Appender> appender(new MockAppender(*prep));
  prep->my_node_id_ = NodeID(0, 1);

  prep->execute(std::move(appender));
  ASSERT_RESULTS(prep, std::make_pair(E::NOTREADY, NodeID()));
}

// Tests that a node with sequencer weight set to zero rejects appends with
// E::NOTREADY, indicating that the client should select a different node.
TEST_F(APPEND_MessageTest, WeightZero) {
  const logid_t log(1);
  const NodeID N0(0, 1);

  auto p1 = create(log);
  std::unique_ptr<Appender> a1(new MockAppender(*p1));
  Appender* r1 = a1.get();
  p1->my_node_id_ = N0;
  p1->setSequencer(log, N0);
  p1->setAlive({N0});

  p1->execute(std::move(a1));
  ASSERT_RUNNING(p1, {r1});

  auto p2 = create(log);
  std::unique_ptr<Appender> a2(new MockAppender(*p2));
  p2->my_node_id_ = N0;
  p2->setSequencer(log, N0);
  p2->setAlive({N0});
  p2->seq_weight_zero_.insert(N0.index());

  p2->execute(std::move(a2));
  ASSERT_RESULTS(p2, std::make_pair(E::NOTREADY, NodeID()));
}

// Tests that if isAllowed returns false, execute will fail with E::ACCESS
TEST_F(APPEND_MessageTest, isNotAllowed) {
  const logid_t log(1);
  const NodeID N0(0, 1);

  auto prep = create(log);
  prep->setPrincipal(
      PrincipalIdentity("", std::make_pair("USER", "NotAllowed")));
  std::unique_ptr<Appender> a(new MockAppender(*prep));
  prep->my_node_id_ = N0;
  prep->setSequencer(log, N0);
  prep->setAlive({N0});

  prep->execute(std::move(a));
  ASSERT_RESULTS(prep, std::make_pair(E::ACCESS, NodeID()));
}

// Tests that if isAacceptingWork returns false,
// execute will fail with E::SHUTDOWN
TEST_F(APPEND_MessageTest, isNotAcceptingWork) {
  const logid_t log(1);
  const NodeID N0(0, 1);

  auto prep = create(log);
  std::unique_ptr<Appender> a(new MockAppender(*prep));
  prep->my_node_id_ = N0;
  prep->setSequencer(log, N0);
  prep->setAlive({N0});
  prep->setAcceptWork(false);

  prep->execute(std::move(a));
  ASSERT_RESULTS(prep, std::make_pair(E::SHUTDOWN, NodeID()));
}

// Verifies the correctness of the execute function when the payload has
// a checksum associated with it
TEST_F(APPEND_MessageTest, CorruptionBeforeDataStore) {
  const logid_t log(1);
  const NodeID N0(0, 1);
  uint32_t flags = 0;
  uint64_t corruptedBits = 0xA5A5A5A5A5A5A5A5;

  char data[128];

  // Verfies that the functionality of APPEND_Message::execute works as expected
  // with 32 and 64 bit checksums embedded in the payload
  {
    size_t new_size = sizeof(data) + 4;
    char* new_data = (char*)malloc(new_size);

    checksum_bytes(Slice(data, sizeof(data)), 32, new_data);
    memcpy(new_data + 4, data, sizeof(data));
    PayloadHolder ph(new_data, new_size);

    uint32_t flags2 = flags | APPEND_Header::CHECKSUM;
    auto prep = create(log, flags2);
    std::unique_ptr<Appender> a(new MockAppender(*prep, std::move(ph), flags2));
    prep->my_node_id_ = N0;
    prep->sequencer_ = nullptr;
    prep->setSequencer(log, N0);
    prep->setAlive({N0});

    prep->execute(std::move(a));
  }
  {
    size_t new_size = sizeof(data) + 8;
    char* new_data = (char*)malloc(new_size);

    checksum_bytes(Slice(data, sizeof(data)), 64, new_data);
    memcpy(new_data + 8, data, sizeof(data));
    PayloadHolder ph(new_data, new_size);

    uint32_t flags2 = flags | APPEND_Header::CHECKSUM |
        APPEND_Header::CHECKSUM_64BIT | APPEND_Header::CHECKSUM_PARITY;
    auto prep = create(log, flags2);
    std::unique_ptr<Appender> a(new MockAppender(*prep, std::move(ph), flags2));
    prep->my_node_id_ = N0;
    prep->sequencer_ = nullptr;
    prep->setSequencer(log, N0);
    prep->setAlive({N0});

    prep->execute(std::move(a));
  }
  // Verifies that when the data or the checksum bits are invalid for 32 bit
  // checksums, a E::BADPAYLOAD error will be sent to the client
  {
    size_t new_size = sizeof(data) + 4;
    char* new_data = (char*)malloc(new_size);

    // Copy currupted checksum bits into the data for the payload
    memcpy(new_data, &corruptedBits, 4);
    memcpy(new_data + 4, data, sizeof(data));
    PayloadHolder ph(new_data, new_size);

    uint32_t flags2 = flags | APPEND_Header::CHECKSUM;
    auto prep = create(log, flags2);
    std::unique_ptr<Appender> a(new MockAppender(*prep, std::move(ph), flags2));
    prep->my_node_id_ = N0;
    prep->sequencer_ = nullptr;
    prep->setSequencer(log, N0);
    prep->setAlive({N0});

    prep->execute(std::move(a));
    ASSERT_RESULTS(prep, std::make_pair(E::BADPAYLOAD, NodeID()));
  }
  // Verifies that when the data or the checksum bits are invalid for 64 bit
  // checksums, a E::BADPAYLOAD error will be sent to the client
  {
    size_t new_size = sizeof(data) + 8;
    char* new_data = (char*)malloc(new_size);

    // Copy currupted checksum bits into the data for the payload
    memcpy(new_data, &corruptedBits, 8);
    memcpy(new_data + 8, data, sizeof(data));
    PayloadHolder ph(new_data, new_size);

    uint32_t flags2 = flags | APPEND_Header::CHECKSUM |
        APPEND_Header::CHECKSUM_64BIT | APPEND_Header::CHECKSUM_PARITY;
    auto prep = create(log, flags2);
    std::unique_ptr<Appender> a(new MockAppender(*prep, std::move(ph), flags2));
    prep->my_node_id_ = N0;
    prep->sequencer_ = nullptr;
    prep->setSequencer(log, N0);
    prep->setAlive({N0});

    prep->execute(std::move(a));
    ASSERT_RESULTS(prep, std::make_pair(E::BADPAYLOAD, NodeID()));
  }
}

TEST_F(APPEND_MessageTest, E2ETracing) {
  // simple test to check that spans are created and all works as expected
  const logid_t log(1);
  const NodeID N0(0, 1);

  auto recorder = new opentracing::mocktracer::InMemoryRecorder{};
  opentracing::mocktracer::MockTracerOptions tracer_options;
  tracer_options.recorder.reset(recorder);
  auto tracer = std::make_shared<opentracing::mocktracer::MockTracer>(
      opentracing::mocktracer::MockTracerOptions{std::move(tracer_options)});

  // create a span to be used as a span coming from client side
  auto span = tracer->StartSpan("Mock_APPEND_Message_received");

  {
    auto prep = createWithMockE2ETracing(log, tracer, std::move(span));

    // the appender span created on the appender prep is passed to the appender
    // constructor
    auto appender_span = prep->getAppenderSpan();
    std::unique_ptr<Appender> a(new MockAppender(*prep, tracer, appender_span));

    // simple configuration for test purposes
    prep->my_node_id_ = N0;
    prep->setSequencer(log, N0);
    prep->setAlive({N0});

    prep->execute(std::move(a));
  }

  // so far the execution should have created span
  // one span is created in this test
  ASSERT_TRUE(recorder->spans().size() > 1);

  // the appender span is the last one to finish
  auto appender_span_reference_span_id =
      recorder->spans().back().references[0].span_id;
  auto appender_span_reference_type =
      recorder->spans().back().references[0].reference_type;

  // the span id of the append message receive (the first span created)
  auto append_recv_span_id = recorder->spans().front().span_context.span_id;

  ASSERT_EQ(appender_span_reference_span_id, append_recv_span_id);
  ASSERT_EQ(appender_span_reference_type,
            opentracing::SpanReferenceType::FollowsFromRef);
}
}} // namespace facebook::logdevice
