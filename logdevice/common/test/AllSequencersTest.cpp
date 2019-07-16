/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/AllSequencers.h"

#include <chrono>
#include <mutex>
#include <thread>

#include <folly/Random.h>
#include <gtest/gtest.h>

#include "logdevice/common/Appender.h"
#include "logdevice/common/EpochSequencer.h"
#include "logdevice/common/LogRecoveryRequest.h"
#include "logdevice/common/NoopTraceLogger.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Sequencer.h"
#include "logdevice/common/Timer.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/plugin/CommonBuiltinPlugins.h"
#include "logdevice/common/request_util.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/common/test/TestUtil.h"

using namespace facebook::logdevice;

namespace {

class MockAllSequencers;
struct MockProcessor;

class AllSequencersTest : public ::testing::Test {
 public:
  dbg::Level log_level_ = dbg::Level::DEBUG;
  Alarm alarm_{DEFAULT_TEST_TIMEOUT};

  Settings settings_;
  UpdateableSettings<Settings> updateable_settings_;
  StatsHolder stats_;
  std::shared_ptr<UpdateableConfig> updateable_config_;
  std::unique_ptr<MockProcessor> processor_;
  std::unique_ptr<AllSequencers> all_seqs_;

  struct LogState {
    std::atomic<size_t> metadata_requests{0};
    std::atomic<size_t> log_recoveries{0};
    std::atomic<size_t> draining_completions{0};
    std::atomic<size_t> epoch_store_nonempty_checks{0};
  };

  // number of times it requests metadata from epoch store
  std::unordered_map<logid_t, LogState, logid_t::Hash> logs_state_;
  // if false, requesting metadata from epoch store will fail synchronously
  std::atomic<bool> metadata_req_ok_{true};
  // when epoch store is found empty and we check if metadata log is too,
  // this determines the result
  std::atomic<bool> metadata_log_empty_{true};
  // when the above is true, this is the epoch that will be returned as result
  // from the subsequent update operation to epoch store
  std::atomic<epoch_t> empty_epoch_store_update_result_{epoch_t(777)};

  explicit AllSequencersTest();

  void setUp();
  std::unique_ptr<EpochMetaData> genMetaData(epoch_t epoch) {
    return std::make_unique<EpochMetaData>(
        StorageSet{ShardID(1, 0)},
        ReplicationProperty(1, NodeLocationScope::NODE),
        epoch_t(epoch.val() + 1),
        epoch);
  }
  std::shared_ptr<Configuration> getConfig() const {
    return updateable_config_->get();
  }

  std::shared_ptr<const configuration::nodes::NodesConfiguration>
  getNodesConfiguration() const {
    return updateable_config_->getNodesConfiguration();
  }

  size_t getMetaDataRequests(logid_t log) {
    return logs_state_.at(log).metadata_requests;
  }

  size_t getLogRecoveries(logid_t log) {
    return logs_state_.at(log).log_recoveries;
  }

  size_t getDrainingCompletions(logid_t log) {
    return logs_state_.at(log).draining_completions;
  }
};

class MockSequencer : public Sequencer {
 public:
  explicit MockSequencer(AllSequencersTest* test, logid_t logid)
      : Sequencer(logid,
                  test->updateable_settings_,
                  &test->stats_,
                  test->all_seqs_.get()),
        test_(test) {}

  ~MockSequencer() override {}

  std::shared_ptr<Configuration> getClusterConfig() const override {
    return test_->getConfig();
  }

  std::shared_ptr<const configuration::nodes::NodesConfiguration>
  getNodesConfiguration() const {
    return test_->getNodesConfiguration();
  }

  void startGetTrimPointRequest() override {}

  int startRecovery(std::chrono::milliseconds /*delay*/ =
                        std::chrono::milliseconds::zero()) override {
    ++test_->logs_state_.at(getLogID()).log_recoveries;
    return 0;
  }

  void startDrainingTimer(epoch_t /*draining*/) override {}

  void finalizeDraining(DrainedAction action) override {
    switch (action) {
      case Sequencer::DrainedAction::NONE:
        break;
      case Sequencer::DrainedAction::GRACEFUL_COMPLETION:
        ++test_->logs_state_.at(getLogID()).draining_completions;
        break;
      case Sequencer::DrainedAction::RECOVERY:
        ++test_->logs_state_.at(getLogID()).log_recoveries;
        break;
    }
  }

  void getHistoricalMetaData(GetHistoricalMetaDataMode /* unused */) override {}

  void schedulePeriodicReleases() override {}

  void startPeriodicReleasesBroadcast() override {}

 private:
  AllSequencersTest* const test_;
};

struct MockProcessor : public Processor {
  explicit MockProcessor(AllSequencersTest* test)
      : Processor(test->updateable_config_,
                  std::make_shared<NoopTraceLogger>(test->updateable_config_),
                  test->updateable_settings_,
                  &test->stats_,
                  std::make_shared<PluginRegistry>(
                      createAugmentedCommonBuiltinPluginVector<>())) {}

  ~MockProcessor() override {}

  bool isNodeIsolated() const override {
    return isNodeIsolated_;
  }

  bool isNodeIsolated_{false};
};

class MockAllSequencers : public AllSequencers {
 public:
  explicit MockAllSequencers(AllSequencersTest* test, MockProcessor* processor)
      : AllSequencers(processor,
                      test->updateable_config_,
                      test->updateable_settings_),
        test_(test) {}

  ~MockAllSequencers() override {}

  std::shared_ptr<Sequencer>
  createSequencer(logid_t logid,
                  UpdateableSettings<Settings> /*settings*/) override {
    return std::make_shared<MockSequencer>(test_, logid);
  }

  std::shared_ptr<Sequencer>
  getMetaDataLogSequencer(logid_t /*datalog_id*/) override {
    return nullptr;
  }

  void notifyMetaDataLogWriterOnActivation(Sequencer* /*seq*/,
                                           epoch_t /*epoch*/,
                                           bool /*bypass_recovery*/) override {}

  int getEpochMetaData(
      logid_t logid,
      const std::string& /* activation_reason */,
      std::shared_ptr<Configuration> /*cfg*/,
      std::shared_ptr<const configuration::nodes::NodesConfiguration>,
      folly::Optional<epoch_t> /*acceptable_activation_epoch*/,
      bool /*provision_if_empty*/,
      std::shared_ptr<EpochMetaData> /* new_metadata */) override {
    ++test_->logs_state_.at(logid).metadata_requests;
    return test_->metadata_req_ok_ ? 0 : -1;
  }

  StatsHolder* getStats() const override {
    return &test_->stats_;
  }

  void notifyWorkerActivationCompletion(logid_t /*logid*/,
                                        Status /*st*/) override {}

  void
  startMetadataLogEmptyCheck(logid_t logid,
                             const std::string& activation_reason) override {
    onMetadataLogEmptyCheckResult(
        test_->metadata_log_empty_ ? E::NOTFOUND : E::NOTEMPTY,
        logid,
        activation_reason);
  }

  void startEpochStoreNonemptyCheck(logid_t logid) override {
    ++test_->logs_state_.at(logid).epoch_store_nonempty_checks;
  }

  void onEpochMetaDataFromEpochStore(
      Status st,
      logid_t logid,
      const std::string& activation_reason,
      std::unique_ptr<EpochMetaData> info,
      std::unique_ptr<EpochStoreMetaProperties> meta_props) override {
    if (st == E::EMPTY) {
      startMetadataLogEmptyCheck(logid, activation_reason);
    } else {
      AllSequencers::onEpochMetaDataFromEpochStore(
          st, logid, activation_reason, std::move(info), std::move(meta_props));
    }
  }

  void
  onMetadataLogEmptyCheckResult(Status st,
                                logid_t logid,
                                const std::string& activation_reason) override {
    if (st == E::NOTFOUND) {
      onEpochMetaDataFromEpochStore(
          E::OK,
          logid,
          activation_reason,
          test_->genMetaData(test_->empty_epoch_store_update_result_.load()),
          nullptr);
    } else {
      AllSequencers::onMetadataLogEmptyCheckResult(
          st, logid, activation_reason);
    }
  }

 private:
  AllSequencersTest* const test_;
};

AllSequencersTest::AllSequencersTest()
    : settings_(create_default_settings<Settings>()),
      stats_(StatsParams().setIsServer(true)) {}

void AllSequencersTest::setUp() {
  dbg::currentLevel = log_level_;
  auto config = std::make_shared<UpdateableConfig>(
      Configuration::fromJsonFile(TEST_CONFIG_FILE("sequencer_test.conf")));
  updateable_config_ = config;
  updateable_settings_ = UpdateableSettings<Settings>(settings_);

  std::shared_ptr<Configuration> cfg = updateable_config_->get();
  std::shared_ptr<configuration::LocalLogsConfig> logs_config =
      cfg->localLogsConfig();
  configuration::LocalLogsConfig::Iterator it;
  for (it = logs_config->logsBegin(); it != logs_config->logsEnd(); ++it) {
    logid_t logid(it->first);
    logs_state_[logid];
  }

  processor_ = std::make_unique<MockProcessor>(this);
  all_seqs_ = std::make_unique<MockAllSequencers>(this, processor_.get());
}

TEST_F(AllSequencersTest, ActivateSequencers) {
  setUp();
  for (uint64_t i = 1; i <= 100; ++i) {
    const logid_t logid(i);
    // initially sequencer is not found
    ASSERT_EQ(nullptr, all_seqs_->findSequencer(logid));
    ASSERT_EQ(0, getMetaDataRequests(logid));
    auto pred = [](const Sequencer&) { return true; };
    // activate sequencer for log 1
    int rv = all_seqs_->activateSequencer(logid, "test", pred);
    ASSERT_EQ(0, rv);
    ASSERT_EQ(1, getMetaDataRequests(logid));

    // recovery should not even started
    ASSERT_EQ(0, getLogRecoveries(logid));
    // sequencer should be created and in ACTIVATING state
    auto seq = all_seqs_->findSequencer(logid);
    ASSERT_NE(nullptr, seq);
    ASSERT_EQ(Sequencer::State::ACTIVATING, seq->getState());

    // while in ACTIVATING, sequencer cannot be activated, and E::INPROGRESS
    // will be returned
    rv = all_seqs_->activateSequencer(logid, "test", pred);
    ASSERT_EQ(-1, rv);
    ASSERT_EQ(E::INPROGRESS, err);
    // reactivate sequencer returns 0 according to the public function def
    rv = all_seqs_->reactivateSequencer(logid, "test");
    ASSERT_EQ(0, rv);
    ASSERT_EQ(1, getMetaDataRequests(logid));
    // simulate epoch metadata is gotten from epoch store
    all_seqs_->onEpochMetaDataFromEpochStore(
        E::OK, logid, "test", genMetaData(epoch_t(2)), nullptr);
    // sequencer should be successfully activated to ACTIVE state
    ASSERT_EQ(Sequencer::State::ACTIVE, seq->getState());
    // recovery should have been started
    ASSERT_EQ(1, getLogRecoveries(logid));
    // sequencer should be running in epoch 2
    ASSERT_EQ(epoch_t(2), seq->getCurrentEpoch());
    rv = all_seqs_->activateSequencerIfNotActive(logid, "test");
    ASSERT_EQ(-1, rv);
    ASSERT_EQ(E::EXISTS, err);

    // try to reactivate the sequencer, should be successful this time
    rv = all_seqs_->reactivateSequencer(logid, "test");
    ASSERT_EQ(0, rv);
    ASSERT_EQ(2, getMetaDataRequests(logid));
    ASSERT_EQ(Sequencer::State::ACTIVATING, seq->getState());
    all_seqs_->onEpochMetaDataFromEpochStore(
        E::OK, logid, "test", genMetaData(epoch_t(6)), nullptr);
    ASSERT_EQ(Sequencer::State::ACTIVE, seq->getState());
    // epoch 2 should be gracefully drained
    ASSERT_EQ(1, getLogRecoveries(logid));
    ASSERT_EQ(1, getDrainingCompletions(logid));
    ASSERT_EQ(epoch_t(6), seq->getCurrentEpoch());
  }
}

TEST_F(AllSequencersTest, ActivateErrors) {
  setUp();
  auto pred = [](const Sequencer&) { return true; };
  // activate a sequencer not in config
  int rv = all_seqs_->activateSequencer(logid_t(77998), "test", pred);
  ASSERT_EQ(-1, rv);
  ASSERT_EQ(E::NOTFOUND, err);

  const logid_t logid(1);
  ASSERT_EQ(nullptr, all_seqs_->findSequencer(logid));
  ASSERT_EQ(0, getMetaDataRequests(logid));

  // failed the metadata request
  metadata_req_ok_ = false;
  rv = all_seqs_->activateSequencer(logid, "test", pred);
  ASSERT_EQ(-1, rv);
  ASSERT_EQ(E::FAILED, err);
  ASSERT_EQ(1, getMetaDataRequests(logid));
  // however, the sequencer object should be created and in UNAVAILABLE state
  auto seq = all_seqs_->findSequencer(logid);
  ASSERT_NE(nullptr, seq);
  ASSERT_EQ(Sequencer::State::UNAVAILABLE, seq->getState());
  // do it again, this time fails asynchonously
  metadata_req_ok_ = true;
  rv = all_seqs_->activateSequencer(logid, "test", pred);
  ASSERT_EQ(0, rv);
  ASSERT_EQ(2, getMetaDataRequests(logid));
  // simulate that we lost the race in epoch store
  all_seqs_->onEpochMetaDataFromEpochStore(
      E::AGAIN, logid, "test", nullptr, nullptr);
  ASSERT_EQ(Sequencer::State::UNAVAILABLE, seq->getState());
  ASSERT_EQ(0, getLogRecoveries(logid));
  ASSERT_EQ(0, getDrainingCompletions(logid));
  // perform a successful activation to epoch 3
  rv = all_seqs_->activateSequencer(logid, "test", pred);
  ASSERT_EQ(0, rv);
  ASSERT_EQ(3, getMetaDataRequests(logid));
  all_seqs_->onEpochMetaDataFromEpochStore(
      E::OK, logid, "test", genMetaData(epoch_t(3)), nullptr);
  // sequencer should be successfully activated to ACTIVE state
  ASSERT_EQ(Sequencer::State::ACTIVE, seq->getState());
  ASSERT_EQ(epoch_t(3), seq->getCurrentEpoch());
  ASSERT_EQ(1, getLogRecoveries(logid));

  // simualte node is isolated
  processor_->isNodeIsolated_ = true;
  rv = all_seqs_->activateSequencer(logid, "test", pred);
  ASSERT_EQ(-1, rv);
  ASSERT_EQ(E::ISOLATED, err);
  processor_->isNodeIsolated_ = false;

  // failed synchonrously with pred
  rv = all_seqs_->activateSequencer(
      logid, "test", [](const Sequencer&) { return false; });
  ASSERT_EQ(-1, rv);
  ASSERT_EQ(E::ABORTED, err);
  // In Seqeuncer::startActivation(), pred is evaluated **before** calling
  // the metadata function
  ASSERT_EQ(3, getMetaDataRequests(logid));
  // sequencer will be put back in ACTIVE though
  ASSERT_EQ(Sequencer::State::ACTIVE, seq->getState());
  ASSERT_EQ(epoch_t(3), seq->getCurrentEpoch());
  // reactivate again but encounted permanent error
  rv = all_seqs_->activateSequencer(logid, "test", pred);
  ASSERT_EQ(0, rv);
  ASSERT_EQ(4, getMetaDataRequests(logid));
  // simulate that we lost the race in epoch store
  all_seqs_->onEpochMetaDataFromEpochStore(
      E::TOOBIG, logid, "test", nullptr, nullptr);
  ASSERT_EQ(Sequencer::State::PERMANENT_ERROR, seq->getState());
  ASSERT_EQ(1, getLogRecoveries(logid));
  ASSERT_EQ(0, getDrainingCompletions(logid));
  // activate again will get E::SYSLIMIT
  rv = all_seqs_->activateSequencer(logid, "test", pred);
  ASSERT_EQ(-1, rv);
  ASSERT_EQ(E::SYSLIMIT, err);
  rv = all_seqs_->reactivateSequencer(logid, "test");
  ASSERT_EQ(-1, rv);
  ASSERT_EQ(E::SYSLIMIT, err);
}

TEST_F(AllSequencersTest, ActivateWithEpochStoreWiped) {
  setUp();
  for (int i : {1, 2}) {
    auto pred = [](const Sequencer&) { return true; };
    const logid_t logid(i);
    ASSERT_EQ(nullptr, all_seqs_->findSequencer(logid));
    ASSERT_EQ(0, getMetaDataRequests(logid));

    metadata_req_ok_ = true;
    int rv = all_seqs_->activateSequencer(logid, "test", pred);
    ASSERT_EQ(0, rv);
    ASSERT_EQ(1, getMetaDataRequests(logid));
    // the sequencer object should be created and in ACTIVATING state
    auto seq = all_seqs_->findSequencer(logid);
    ASSERT_NE(nullptr, seq);
    ASSERT_EQ(Sequencer::State::ACTIVATING, seq->getState());

    // simulate metadata log also being empty for log 1, but not log 2
    metadata_log_empty_.store(i == 1);

    // simulate that the epoch store is empty, which should start check of
    // whether metadata log is empty as well
    all_seqs_->onEpochMetaDataFromEpochStore(
        E::EMPTY, logid, "test", nullptr, nullptr);
    auto stats = stats_.aggregate();
    if (i == 1) {
      // both epoch store and metadata log were empty - all good!
      ASSERT_EQ(Sequencer::State::ACTIVE, seq->getState());
      ASSERT_EQ(
          empty_epoch_store_update_result_.load(), seq->getCurrentEpoch());
      ASSERT_EQ(stats.sequencer_activation_failed_metadata_inconsistency, 0);
      ASSERT_EQ(0, logs_state_.at(logid).epoch_store_nonempty_checks.load());
    } else {
      // inconsistency between epoch store and metadata log!
      ASSERT_EQ(Sequencer::State::UNAVAILABLE, seq->getState());
      ASSERT_EQ(stats.sequencer_activation_failed_metadata_inconsistency, 1);
      ASSERT_EQ(1, logs_state_.at(logid).epoch_store_nonempty_checks.load());
    }
  }
}

TEST_F(AllSequencersTest, ParallelActivations) {
  settings_.reactivation_limit = RATE_UNLIMITED;
  setUp();
  const logid_t logid(1);
  std::atomic<epoch_t::raw_type> epoch{1};
  std::atomic<size_t> in_progress{0};
  std::vector<std::thread> threads;
  for (int i = 0; i < 128; ++i) {
    threads.emplace_back([&]() {
      auto pred = [](const Sequencer&) { return true; };
      for (int j = 0; j < 10; ++j) {
        int rv = all_seqs_->activateSequencer(logid, "test", pred);
        EXPECT_TRUE(rv == 0 || err == E::INPROGRESS);
        /* sleep override */
        std::this_thread::sleep_for(
            std::chrono::milliseconds(folly::Random::rand64(10) + 1));
        if (rv == 0) {
          all_seqs_->onEpochMetaDataFromEpochStore(
              E::OK, logid, "test", genMetaData(epoch_t(epoch++)), nullptr);
        } else {
          in_progress++;
        }
      }
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
  auto seq = all_seqs_->findSequencer(logid);
  ld_info("Current Sequencer epoch %u, metadata request %lu, log recoveries "
          "started %lu, draining completion started %lu, error "
          "E::INPROGRESS %lu.",
          seq->getCurrentEpoch().val_,
          getMetaDataRequests(logid),
          getLogRecoveries(logid),
          getDrainingCompletions(logid),
          in_progress.load());
}

} // anonymous namespace
