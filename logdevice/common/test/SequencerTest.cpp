/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/Sequencer.h"

#include <chrono>
#include <mutex>
#include <queue>
#include <thread>

#include <gtest/gtest.h>

#include "logdevice/common/Appender.h"
#include "logdevice/common/EpochSequencer.h"
#include "logdevice/common/LogRecoveryRequest.h"
#include "logdevice/common/MetaDataLogWriter.h"
#include "logdevice/common/NoopTraceLogger.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Timer.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/protocol/STORE_Message.h"
#include "logdevice/common/request_util.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/common/test/TestUtil.h"

using namespace facebook::logdevice;

namespace {

class MockEpochSequencer;
class MockAppender;

class SequencerTest : public ::testing::Test {
 public:
  dbg::Level log_level_ = dbg::Level::DEBUG;
  Alarm alarm_{DEFAULT_TEST_TIMEOUT};

  static constexpr logid_t LOG_ID{1};
  int window_size_{128};
  esn_t esn_max_{ESN_MAX};

  Settings settings_;
  UpdateableSettings<Settings> updateable_settings_;
  StatsHolder stats_;
  std::shared_ptr<UpdateableConfig> updateable_config_;
  std::shared_ptr<Configuration> original_config_;

  std::shared_ptr<Processor> processor_;
  std::shared_ptr<Sequencer> sequencer_;

  bool with_processor_{false};
  std::atomic<epoch_t::raw_type> draining_timer_epoch_{EPOCH_INVALID.val_};

  std::mutex mutex_;
  std::vector<Sequencer::DrainedAction> drained_actions_;
  std::queue<MockAppender*> appenders_;

  folly::Optional<epoch_t> request_epoch_reading_metadata_;

  explicit SequencerTest();
  ~SequencerTest() override;

  void setUp();
  std::unique_ptr<MockAppender> createAppender(size_t size = 0);

  int getMetaData() {
    return 0;
  }

  std::unique_ptr<EpochMetaData>
  genMetaData(epoch_t epoch, folly::Optional<epoch_t> since = folly::none) {
    return std::make_unique<EpochMetaData>(
        StorageSet{ShardID(1, 0)},
        ReplicationProperty(1, NodeLocationScope::NODE),
        epoch,
        since.value_or(epoch));
  }

  std::shared_ptr<const EpochMetaDataMap::Map>
  genMetaDataMap(std::set<epoch_t::raw_type> since_epochs) {
    EpochMetaDataMap::Map map;
    for (const auto e : since_epochs) {
      map.insert(std::make_pair(epoch_t(e), *genMetaData(epoch_t(e))));
    }
    return std::make_shared<const EpochMetaDataMap::Map>(std::move(map));
  }

  std::shared_ptr<const EpochMetaDataMap>
  genEpochMetaDataMap(std::set<epoch_t::raw_type> since_epochs, epoch_t until) {
    return EpochMetaDataMap::create(
        genMetaDataMap(std::move(since_epochs)), until);
  }

  TailRecord genTailRecord(lsn_t tail_lsn, bool include_payload = false) {
    TailRecordHeader::flags_t flags =
        (include_payload ? TailRecordHeader::HAS_PAYLOAD : 0);
    flags |= TailRecordHeader::CHECKSUM_PARITY;
    void* payload_flat = malloc(20);
    std::strncpy((char*)payload_flat, "Tail Record Test.", 20);
    return TailRecord(
        TailRecordHeader{
            LOG_ID,
            tail_lsn,
            tail_lsn,              // ts and byteoffset same as lsn
            {BYTE_OFFSET_INVALID}, // deprecated, use OffsetMap instead
            flags,
            {}},
        OffsetMap({{BYTE_OFFSET, tail_lsn}}),
        include_payload ? std::make_shared<PayloadHolder>(payload_flat, 20)
                        : nullptr);
  }

  std::shared_ptr<Configuration> getConfig() const {
    return updateable_config_->get();
  }

  std::shared_ptr<const configuration::nodes::NodesConfiguration>
  getNodesConfiguration() const {
    return updateable_config_->getNodesConfiguration();
  }

  ActivateResult completeActivation(int epoch) {
    return sequencer_->completeActivationWithMetaData(
        epoch_t(epoch), getConfig(), genMetaData(epoch_t(epoch)));
  }

  void drainingTimerExpired(epoch_t draining_epoch) {
    draining_timer_epoch_.store(EPOCH_INVALID.val_);
    sequencer_->noteDrainingCompleted(draining_epoch, Status::TIMEDOUT);
  }

  void checkHistoricalMetaDataRequestEpoch(epoch_t epoch) {
    ASSERT_TRUE(request_epoch_reading_metadata_.hasValue());
    ASSERT_EQ(epoch, request_epoch_reading_metadata_.value());
    request_epoch_reading_metadata_.clear();
  }

  void noHistoricalMetaDataRequested() {
    ASSERT_FALSE(request_epoch_reading_metadata_.hasValue());
  }

  void removeLogFromConfig();
  void restoreConfig();

  MockEpochSequencer* getDrainingEpochSequencer();
  MockEpochSequencer* getCurrentEpochSequencer();
};

class MockEpochSequencer : public EpochSequencer {
 public:
  MockEpochSequencer(SequencerTest* test,
                     logid_t log_id,
                     epoch_t epoch,
                     std::unique_ptr<EpochMetaData> metadata,
                     const EpochSequencerImmutableOptions& immutable_options,
                     Sequencer* parent)
      : EpochSequencer(log_id,
                       epoch,
                       std::move(metadata),
                       immutable_options,
                       parent),
        test_(test) {}

  ~MockEpochSequencer() override {
    ld_debug("epoch sequencer of epoch %u destroyed.", getEpoch().val_);
  }

  const Settings& getSettings() const override {
    return test_->settings_;
  }

  // we manually abort appenders in this test
  void abortAppenders() override {}

  bool shouldAbort() const {
    return abort_.load();
  }
  void retireAppenders(esn_t start, esn_t end, bool abort);

 private:
  SequencerTest* const test_;
  // if true, appenders will be aborted on retire
  std::atomic<bool> abort_{false};
};

class MockAppender : public Appender {
 public:
  using MockSender = SenderTestProxy<MockAppender>;

  explicit MockAppender(SequencerTest* test, size_t size)
      : Appender(
            /*worker*/ Worker::onThisThread(false),
            /*tracelogger*/ nullptr,
            /*client_timeout*/ std::chrono::milliseconds(0),
            /* append_request_id= */ request_id_t(0),
            STORE_flags_t(0),
            test->LOG_ID,
            PayloadHolder(MockAppender::dummyPayload, PayloadHolder::UNOWNED),
            epoch_t(0),
            size),
        test_(test) {}

  ~MockAppender() override {
    epoch_sequencer_.reset();
  }

  int start(std::shared_ptr<EpochSequencer> epoch_sequencer,
            lsn_t lsn) override {
    // hold the reference to the epoch sequencer object
    epoch_sequencer_ = std::move(epoch_sequencer);
    lsn_ = lsn;
    {
      std::lock_guard<std::mutex> lock(test_->mutex_);
      test_->appenders_.push(this);
    }
    return 0;
  }

  lsn_t getLSN() const {
    return lsn_;
  }

  const Settings& getSettings() const override {
    return test_->settings_;
  }

  void onReaped() override {
    epoch_t last_released_epoch;
    bool lng_changed;
    MockEpochSequencer* mseq =
        dynamic_cast<MockEpochSequencer*>(epoch_sequencer_.get());
    mseq->noteAppenderReaped(
        (mseq->shouldAbort() ? Appender::FullyReplicated::NO
                             : Appender::FullyReplicated::YES),
        getLSN(),
        std::make_shared<TailRecord>(
            TailRecordHeader{
                getLogID(),
                getLSN(),
                0,
                {BYTE_OFFSET_INVALID /* deprecated, OffsetMap used instead */},
                TailRecordHeader::OFFSET_WITHIN_EPOCH,
                {}},
            OffsetMap::fromLegacy(0),
            std::shared_ptr<PayloadHolder>()),
        &last_released_epoch,
        &lng_changed);
  }

 private:
  static Payload dummyPayload;
  std::shared_ptr<EpochSequencer> epoch_sequencer_;
  SequencerTest* const test_;
  lsn_t lsn_{LSN_INVALID};
};

class MockSequencer : public Sequencer {
 public:
  explicit MockSequencer(SequencerTest* test)
      : Sequencer(test->LOG_ID, test->updateable_settings_, &test->stats_),
        test_(test) {}

  ~MockSequencer() override {}

  std::shared_ptr<Configuration> getClusterConfig() const override {
    return test_->getConfig();
  }

  std::shared_ptr<const configuration::nodes::NodesConfiguration>
  getNodesConfiguration() const override {
    return test_->getConfig()
        ->serverConfig()
        ->getNodesConfigurationFromServerConfigSource();
  }

  void startGetTrimPointRequest() override {}

  std::shared_ptr<EpochSequencer>
  createEpochSequencer(epoch_t epoch,
                       std::unique_ptr<EpochMetaData> metadata) override {
    EpochSequencerImmutableOptions opts;
    opts.window_size = test_->window_size_;
    opts.esn_max = test_->esn_max_;
    return std::make_shared<MockEpochSequencer>(
        test_, test_->LOG_ID, epoch, std::move(metadata), opts, this);
  }

  void startDrainingTimer(epoch_t draining) override {
    auto prev = test_->draining_timer_epoch_.exchange(draining.val_);
    ASSERT_LE(prev, draining.val_);
  }

  void finalizeDraining(DrainedAction action) override {
    std::lock_guard<std::mutex> lock(test_->mutex_);
    test_->drained_actions_.push_back(action);
  }

  int sendReleases(lsn_t /*lsn*/,
                   ReleaseType /*release_type*/,
                   const SendReleasesPred& /*pred*/ = nullptr) override {
    // TODO: test to verify that releases are sent
    return 0;
  }

  void schedulePeriodicReleases() override {}

  void startPeriodicReleasesBroadcast() override {}

  void processRedirectedRecords() override {}

  void getHistoricalMetaData(GetHistoricalMetaDataMode mode) override {
    if (mode == GetHistoricalMetaDataMode::IMMEDIATE) {
      test_->request_epoch_reading_metadata_.assign(getCurrentEpoch());
    }
  }

 private:
  SequencerTest* const test_;
};

constexpr logid_t SequencerTest::LOG_ID;
Payload MockAppender::dummyPayload("payload", 8);

void MockEpochSequencer::retireAppenders(esn_t start, esn_t end, bool abort) {
  ASSERT_LE(start, end);
  abort_.store(abort);
  for (esn_t::raw_type e = start.val_;; ++e) {
    MockAppender::Reaper reaper;
    retireAppender(E::OK, compose_lsn(getEpoch(), esn_t(e)), reaper);
    if (e == end.val_) {
      break;
    }
  }
}

SequencerTest::SequencerTest()
    : settings_(create_default_settings<Settings>()),
      stats_(StatsParams().setIsServer(true)) {}

SequencerTest::~SequencerTest() {
  while (!appenders_.empty()) {
    delete appenders_.front();
    appenders_.pop();
  }
}

MockEpochSequencer* SequencerTest::getDrainingEpochSequencer() {
  auto eps = sequencer_->getEpochSequencers();
  MockEpochSequencer* draining_seq =
      dynamic_cast<MockEpochSequencer*>(eps.second.get());
  EXPECT_NE(nullptr, draining_seq);
  return draining_seq;
}

MockEpochSequencer* SequencerTest::getCurrentEpochSequencer() {
  auto eps = sequencer_->getEpochSequencers();
  MockEpochSequencer* current_seq =
      dynamic_cast<MockEpochSequencer*>(eps.first.get());
  EXPECT_NE(nullptr, current_seq);
  return current_seq;
}

void SequencerTest::setUp() {
  dbg::currentLevel = log_level_;
  auto config = std::make_shared<UpdateableConfig>(
      Configuration::fromJsonFile(TEST_CONFIG_FILE("sequencer_test.conf")));

  // turn on byte offsets
  settings_.byte_offsets = true;
  if (with_processor_) {
    processor_ = make_test_processor(settings_, config, &stats_, NodeID(1, 1));
    ASSERT_NE(nullptr, processor_.get());
  }

  updateable_config_ = config;
  updateable_settings_ = UpdateableSettings<Settings>(settings_);
  sequencer_ = std::make_shared<MockSequencer>(this);
}

void SequencerTest::removeLogFromConfig() {
  const logid_t dummy_log(993248043);
  ld_check(dummy_log != LOG_ID);

  auto config = updateable_config_->get();
  original_config_ = config;
  auto logs_config_changed = config->localLogsConfig()->copy();
  auto local_logs_config_changed =
      checked_downcast<configuration::LocalLogsConfig*>(
          logs_config_changed.get());
  auto& logs = const_cast<logsconfig::LogMap&>(
      checked_downcast<configuration::LocalLogsConfig*>(
          logs_config_changed.get())
          ->getLogMap());
  auto log_in_directory = logs.begin()->second;
  auto rv = local_logs_config_changed->replaceLogGroup(
      log_in_directory.getFullyQualifiedName(),
      log_in_directory.log_group->withRange(
          logid_range_t({dummy_log, dummy_log})));
  ld_check(rv);
  auto new_config = std::make_shared<Configuration>(
      config->serverConfig(), std::move(logs_config_changed));
  ld_check_eq(0,
              updateable_config_->updateableLogsConfig()->update(
                  new_config->logsConfig()));
  ld_check_eq(0,
              updateable_config_->updateableServerConfig()->update(
                  new_config->serverConfig()));
}

void SequencerTest::restoreConfig() {
  if (original_config_ != nullptr) {
    ld_check_eq(0,
                updateable_config_->updateableLogsConfig()->update(
                    original_config_->logsConfig()));
    ld_check_eq(0,
                updateable_config_->updateableServerConfig()->update(
                    original_config_->serverConfig()));
  }
}

std::unique_ptr<MockAppender> SequencerTest::createAppender(size_t size) {
  return std::make_unique<MockAppender>(this, size);
}

#define CHECK_EPOCHS(cur, drn)                                               \
  do {                                                                       \
    auto seqs = sequencer_->getEpochSequencers();                            \
    ASSERT_EQ(seqs.first ? seqs.first->getEpoch() : EPOCH_INVALID, (cur));   \
    ASSERT_EQ(seqs.second ? seqs.second->getEpoch() : EPOCH_INVALID, (drn)); \
  } while (0)

lsn_t lsn(epoch_t::raw_type epoch, esn_t::raw_type esn) {
  return compose_lsn(epoch_t(epoch), esn_t(esn));
}

TEST_F(SequencerTest, InitialState) {
  setUp();

  // initial state of the sequencer
  ASSERT_EQ(Sequencer::State::UNAVAILABLE, sequencer_->getState());
  ASSERT_EQ(LOG_ID, sequencer_->getLogID());

  ASSERT_EQ(LSN_INVALID, sequencer_->getNextLSN());
  ASSERT_EQ(0, sequencer_->getNumAppendsInFlight());
  ASSERT_EQ(LSN_INVALID, sequencer_->getLastReleased());
  ASSERT_EQ(EPOCH_INVALID, sequencer_->getCurrentEpoch());

  CHECK_EPOCHS(EPOCH_INVALID, EPOCH_INVALID);

  ASSERT_EQ(
      NodeID(), sequencer_->checkIfPreempted(sequencer_->getCurrentEpoch()));
  ASSERT_FALSE(sequencer_->isPreempted());
  ASSERT_EQ(Seal(), sequencer_->getSealRecord());
  ASSERT_FALSE(sequencer_->isRecoveryComplete());

  auto appender = createAppender();
  ASSERT_EQ(
      RunAppenderStatus::ERROR_DELETE, sequencer_->runAppender(appender.get()));
  ASSERT_EQ(E::NOSEQUENCER, err);

  // TODO 7467469: byte offset stuff
}

TEST_F(SequencerTest, ActivationEmptyEpochs) {
  // remove the reactivation limit
  settings_.reactivation_limit = RATE_UNLIMITED;
  setUp();

  ASSERT_EQ(Sequencer::State::UNAVAILABLE, sequencer_->getState());
  int rv =
      sequencer_->startActivation([this](logid_t) { return getMetaData(); });
  ASSERT_EQ(0, rv);
  ASSERT_EQ(Sequencer::State::ACTIVATING, sequencer_->getState());
  CHECK_EPOCHS(EPOCH_INVALID, EPOCH_INVALID);
  // activate to epoch 2
  auto result = completeActivation(2);

  // no draining happened
  ASSERT_EQ(ActivateResult::RECOVERY, result);
  CHECK_EPOCHS(epoch_t(2), EPOCH_INVALID);
  ASSERT_EQ(epoch_t(2), sequencer_->getCurrentEpoch());
  ASSERT_EQ(Sequencer::State::ACTIVE, sequencer_->getState());

  // activate once again
  rv = sequencer_->startActivation([this](logid_t) { return getMetaData(); });
  ASSERT_EQ(0, rv);
  ASSERT_EQ(Sequencer::State::ACTIVATING, sequencer_->getState());
  // activate to epoch 5
  result = completeActivation(5);
  ASSERT_EQ(ActivateResult::GRACEFUL_DRAINING, result);
  // although we started draining for epoch 2, it should synchronously finished
  // with the completeActivation call
  CHECK_EPOCHS(/*current*/ epoch_t(5), /*draining*/ EPOCH_INVALID);
  ASSERT_EQ(epoch_t(5), sequencer_->getCurrentEpoch());
  ASSERT_EQ(Sequencer::State::ACTIVE, sequencer_->getState());
  // should not start draining timer in such case
  ASSERT_EQ(EPOCH_INVALID.val_, draining_timer_epoch_.load());

  // continue activating more empty epochs
  for (int e = 6; e < 200; ++e) {
    rv = sequencer_->startActivation([this](logid_t) { return getMetaData(); });
    ASSERT_EQ(0, rv);
    ASSERT_EQ(Sequencer::State::ACTIVATING, sequencer_->getState());
    result = completeActivation(e);
    ASSERT_EQ(ActivateResult::GRACEFUL_DRAINING, result);
    ASSERT_EQ(epoch_t(e), sequencer_->getCurrentEpoch());
    ASSERT_EQ(Sequencer::State::ACTIVE, sequencer_->getState());
    ASSERT_EQ(EPOCH_INVALID.val_, draining_timer_epoch_.load());
  }
}

TEST_F(SequencerTest, ActivationAndDraining) {
  setUp();
  int rv =
      sequencer_->startActivation([this](logid_t) { return getMetaData(); });
  ASSERT_EQ(0, rv);
  ASSERT_EQ(Sequencer::State::ACTIVATING, sequencer_->getState());
  CHECK_EPOCHS(EPOCH_INVALID, EPOCH_INVALID);
  // Appends should fail with E::INPROGRESS while activating for the first time
  auto appender = createAppender();
  ASSERT_EQ(
      RunAppenderStatus::ERROR_DELETE, sequencer_->runAppender(appender.get()));
  ASSERT_EQ(E::INPROGRESS, err);

  // first activate to epoch 3
  auto result = completeActivation(3);
  ASSERT_EQ(ActivateResult::RECOVERY, result);
  CHECK_EPOCHS(epoch_t(3), EPOCH_INVALID);
  ASSERT_EQ(epoch_t(3), sequencer_->getCurrentEpoch());
  ASSERT_EQ(Sequencer::State::ACTIVE, sequencer_->getState());
  // should be succesfull taking appends in epoch 3
  appender = createAppender();
  ASSERT_EQ(
      RunAppenderStatus::SUCCESS_KEEP, sequencer_->runAppender(appender.get()));
  // Appender must be in the epoch 3
  ASSERT_EQ(epoch_t(3), lsn_to_epoch(appender->getLSN()));
  // ownership passed to sequencer
  appender.release();

  // activate once again to epoch 5
  rv = sequencer_->startActivation([this](logid_t) { return getMetaData(); });
  ASSERT_EQ(0, rv);
  ASSERT_EQ(Sequencer::State::ACTIVATING, sequencer_->getState());

  // while activating, sequencer should still be able to process appends in
  // epoch 3
  for (int i = 0; i < 5; ++i) {
    appender = createAppender();
    ASSERT_EQ(RunAppenderStatus::SUCCESS_KEEP,
              sequencer_->runAppender(appender.get()));
    ASSERT_EQ(epoch_t(3), lsn_to_epoch(appender->getLSN()));
    appender.release();
  }
  ASSERT_EQ(Sequencer::State::ACTIVATING, sequencer_->getState());

  result = completeActivation(5);
  ASSERT_EQ(ActivateResult::GRACEFUL_DRAINING, result);
  // should start draing for epoch 3
  CHECK_EPOCHS(/*current*/ epoch_t(5), /*draining*/ epoch_t(3));
  // draining timer for epoch 3 must have been restarted
  ASSERT_EQ(3, draining_timer_epoch_.load());
  // further inspect the state for EpochSequnecer in epoch 3
  auto* draining_seq = getDrainingEpochSequencer();
  ASSERT_NE(nullptr, draining_seq);

  ASSERT_EQ(EpochSequencer::State::DRAINING, draining_seq->getState());
  // 6 appends accepted
  ASSERT_EQ(6, draining_seq->getNumAppendsInFlight());
  ASSERT_EQ(epoch_t(5), sequencer_->getCurrentEpoch());
  ASSERT_EQ(Sequencer::State::ACTIVE, sequencer_->getState());

  // new appends will be in epoch 5 instead
  appender = createAppender();
  ASSERT_EQ(
      RunAppenderStatus::SUCCESS_KEEP, sequencer_->runAppender(appender.get()));
  ASSERT_EQ(epoch_t(5), lsn_to_epoch(appender->getLSN()));
  appender.release();

  // retire first 5 appenders
  draining_seq->retireAppenders(esn_t(1), esn_t(5), false);
  ASSERT_EQ(EpochSequencer::State::DRAINING, draining_seq->getState());
  CHECK_EPOCHS(/*current*/ epoch_t(5), /*draining*/ epoch_t(3));
  ASSERT_TRUE(drained_actions_.empty());

  // retire the last appender
  draining_seq->retireAppenders(esn_t(6), esn_t(6), false);
  ASSERT_EQ(EpochSequencer::State::QUIESCENT, draining_seq->getState());
  ASSERT_EQ(lsn(3, 6), draining_seq->getLastKnownGood());
  // epoch 3 should have been evicted
  CHECK_EPOCHS(/*current*/ epoch_t(5), /*draining*/ EPOCH_INVALID);
  ASSERT_EQ(1, drained_actions_.size());
  ASSERT_EQ(
      Sequencer::DrainedAction::GRACEFUL_COMPLETION, drained_actions_.back());
}

TEST_F(SequencerTest, DrainedStaleEpoch) {
  setUp();
  // activate to epoch 3, take an append, then activate to epoch 5
  sequencer_->startActivation([this](logid_t) { return getMetaData(); });
  completeActivation(3);
  ASSERT_EQ(Sequencer::State::ACTIVE, sequencer_->getState());
  auto appender = createAppender();
  ASSERT_EQ(
      RunAppenderStatus::SUCCESS_KEEP, sequencer_->runAppender(appender.get()));
  appender.release();
  int rv =
      sequencer_->startActivation([this](logid_t) { return getMetaData(); });
  ASSERT_EQ(0, rv);
  auto result = completeActivation(5);
  // draining for epoch 3 started
  ASSERT_EQ(ActivateResult::GRACEFUL_DRAINING, result);
  auto eps = sequencer_->getEpochSequencers();
  MockEpochSequencer* draining_seq_3 = getDrainingEpochSequencer();
  ASSERT_NE(nullptr, draining_seq_3);
  ASSERT_EQ(epoch_t(3), draining_seq_3->getEpoch());
  ASSERT_EQ(EpochSequencer::State::DRAINING, draining_seq_3->getState());
  // take an append in epoch 5
  appender = createAppender();
  ASSERT_EQ(
      RunAppenderStatus::SUCCESS_KEEP, sequencer_->runAppender(appender.get()));
  appender.release();
  // activate to epoch 6
  rv = sequencer_->startActivation([this](logid_t) { return getMetaData(); });
  ASSERT_EQ(0, rv);
  result = completeActivation(6);
  CHECK_EPOCHS(/*current*/ epoch_t(6), /*draining*/ epoch_t(5));
  ASSERT_EQ(ActivateResult::GRACEFUL_DRAINING, result);
  MockEpochSequencer* draining_seq_5 = getDrainingEpochSequencer();
  ASSERT_NE(nullptr, draining_seq_5);
  ASSERT_EQ(epoch_t(5), draining_seq_5->getEpoch());
  ASSERT_EQ(EpochSequencer::State::DRAINING, draining_seq_5->getState());
  // no drained action yet
  ASSERT_TRUE(drained_actions_.empty());
  // we should abort draining and begin destruction of epoch 3
  ASSERT_EQ(EpochSequencer::State::DYING, draining_seq_3->getState());
  // complete draining for epoch 3 but sequencer won't perform any action
  draining_seq_3->retireAppenders(esn_t(1), esn_t(1), false);
  ASSERT_EQ(EpochSequencer::State::QUIESCENT, draining_seq_3->getState());
  ASSERT_EQ(lsn(3, 1), draining_seq_3->getLastKnownGood());
  sequencer_->noteDrainingCompleted(epoch_t(3), E::OK);
  ASSERT_EQ(1, drained_actions_.size());
  ASSERT_EQ(Sequencer::DrainedAction::NONE, drained_actions_.back());

  // abort epoch 5 resulting an unsuccessful drain, expect a new action of
  // RECOVERY rather than GRACEFUL_COMPLETION
  draining_seq_5->retireAppenders(esn_t(1), esn_t(1), /*abort*/ true);
  ASSERT_EQ(EpochSequencer::State::QUIESCENT, draining_seq_5->getState());
  ASSERT_EQ(2, drained_actions_.size());
  ASSERT_EQ(Sequencer::DrainedAction::RECOVERY, drained_actions_.back());
}

TEST_F(SequencerTest, DrainingTimedout) {
  setUp();
  // activate to epoch 3, take an append, then activate to epoch 5
  sequencer_->startActivation([this](logid_t) { return getMetaData(); });
  completeActivation(3);
  ASSERT_EQ(Sequencer::State::ACTIVE, sequencer_->getState());
  auto appender = createAppender();
  ASSERT_EQ(
      RunAppenderStatus::SUCCESS_KEEP, sequencer_->runAppender(appender.get()));
  appender.release();
  int rv =
      sequencer_->startActivation([this](logid_t) { return getMetaData(); });
  ASSERT_EQ(0, rv);
  auto result = completeActivation(5);
  CHECK_EPOCHS(/*current*/ epoch_t(5), /*draining*/ epoch_t(3));
  // draining for epoch 3 started, a timer should started
  ASSERT_EQ(ActivateResult::GRACEFUL_DRAINING, result);
  ASSERT_EQ(3, draining_timer_epoch_.load());
  auto eps = sequencer_->getEpochSequencers();
  MockEpochSequencer* draining_seq_3 = getDrainingEpochSequencer();
  ASSERT_EQ(EpochSequencer::State::DRAINING, draining_seq_3->getState());
  // simulate draining timer fired for epoch 3
  drainingTimerExpired(epoch_t(3));
  // epoch 3 should be evicted and its epoch sequencer will start destruction
  CHECK_EPOCHS(/*current*/ epoch_t(5), /*draining*/ EPOCH_INVALID);
  ASSERT_EQ(EpochSequencer::State::DYING, draining_seq_3->getState());
  ASSERT_EQ(1, drained_actions_.size());

  // recovery was deferred, but must be scheduled for now as draining is aborted
  ASSERT_EQ(Sequencer::DrainedAction::RECOVERY, drained_actions_.back());
  draining_seq_3->retireAppenders(esn_t(1), esn_t(1), false);
  ASSERT_EQ(EpochSequencer::State::QUIESCENT, draining_seq_3->getState());
}

TEST_F(SequencerTest, ActivationLimit) {
  // only allows 5 reactivations per 10 seconds
  settings_.reactivation_limit =
      rate_limit_t(4, std::chrono::milliseconds(10000));
  setUp();
  for (int e = 1; e <= 6; ++e) {
    int rv =
        sequencer_->startActivation([this](logid_t) { return getMetaData(); });
    if (e < 6) {
      ASSERT_EQ(0, rv);
      ASSERT_EQ(Sequencer::State::ACTIVATING, sequencer_->getState());
      auto result = completeActivation(e);
      if (e == 1) {
        // no draining for the first epoch
        ASSERT_EQ(ActivateResult::RECOVERY, result);
      } else {
        ASSERT_EQ(ActivateResult::GRACEFUL_DRAINING, result);
      }
      ASSERT_EQ(epoch_t(e), sequencer_->getCurrentEpoch());
      ASSERT_EQ(Sequencer::State::ACTIVE, sequencer_->getState());
    } else { // activate for the sixth time
      ASSERT_EQ(-1, rv);
      ASSERT_EQ(E::TOOMANY, err);
      // sequencer shold stay in ACTIVE state after activation failures
      ASSERT_EQ(Sequencer::State::ACTIVE, sequencer_->getState());
    }
  }
}

TEST_F(SequencerTest, ActivationInProgess) {
  setUp();
  int rv;
  rv = sequencer_->startActivation([this](logid_t) { return getMetaData(); });
  ASSERT_EQ(0, rv);
  ASSERT_EQ(Sequencer::State::ACTIVATING, sequencer_->getState());
  // attempt to activate while sequencer is still in ACTIVATING
  rv = sequencer_->startActivation([this](logid_t) { return getMetaData(); });
  ASSERT_EQ(-1, rv);
  ASSERT_EQ(Sequencer::State::ACTIVATING, sequencer_->getState());
  ASSERT_EQ(E::INPROGRESS, err);
}

// test that activation failure can move the sequencer back to the original
// state
TEST_F(SequencerTest, ActivationFailures) {
  settings_.reactivation_limit = RATE_UNLIMITED;
  setUp();
  int rv;
  ASSERT_EQ(Sequencer::State::UNAVAILABLE, sequencer_->getState());
  rv = sequencer_->startActivation([](logid_t) { return -1; });
  ASSERT_EQ(-1, rv);
  ASSERT_EQ(E::FAILED, err);
  // sequencer should be put back in unavailable state
  ASSERT_EQ(Sequencer::State::UNAVAILABLE, sequencer_->getState());
  // perform a successful activation to get the sequencer into active state
  sequencer_->startActivation([this](logid_t) { return getMetaData(); });
  completeActivation(3);
  ASSERT_EQ(Sequencer::State::ACTIVE, sequencer_->getState());
  // fail activation again
  rv = sequencer_->startActivation([this](logid_t) { return getMetaData(); });
  ASSERT_EQ(0, rv);
  ASSERT_EQ(Sequencer::State::ACTIVATING, sequencer_->getState());
  sequencer_->onActivationFailed();
  ASSERT_EQ(Sequencer::State::ACTIVE, sequencer_->getState());
  // made the sequencer as preempted
  sequencer_->notePreempted(epoch_t(5), NodeID(2, 1));
  ASSERT_EQ(Sequencer::State::PREEMPTED, sequencer_->getState());
  rv = sequencer_->startActivation([this](logid_t) { return getMetaData(); });
  ASSERT_EQ(0, rv);
  ASSERT_EQ(Sequencer::State::ACTIVATING, sequencer_->getState());
  sequencer_->onActivationFailed();
  // sequencer should go back to preempted state
  ASSERT_EQ(Sequencer::State::PREEMPTED, sequencer_->getState());

  // finally sequencer activation should fail on PERMANENT_ERROR state
  sequencer_->onPermanentError();
  ASSERT_EQ(Sequencer::State::PERMANENT_ERROR, sequencer_->getState());
  ASSERT_EQ(EPOCH_INVALID, sequencer_->getCurrentEpoch());
  ASSERT_EQ(LSN_INVALID, sequencer_->getNextLSN());
  CHECK_EPOCHS(EPOCH_INVALID, EPOCH_INVALID);
  rv = sequencer_->startActivation([this](logid_t) { return getMetaData(); });
  ASSERT_EQ(-1, rv);
  ASSERT_EQ(E::SYSLIMIT, err);
  // should stay in error state
  ASSERT_EQ(Sequencer::State::PERMANENT_ERROR, sequencer_->getState());
  sequencer_->onActivationFailed();
  ASSERT_EQ(Sequencer::State::PERMANENT_ERROR, sequencer_->getState());
}

TEST_F(SequencerTest, FirstActivationPreempted) {
  settings_.reactivation_limit = RATE_UNLIMITED;
  setUp();
  ASSERT_EQ(Sequencer::State::UNAVAILABLE, sequencer_->getState());
  sequencer_->startActivation([this](logid_t) { return getMetaData(); });
  // however activation was preempted by another node due to a race
  sequencer_->notePreempted(epoch_t(2), NodeID(3, 1));
  ASSERT_TRUE(sequencer_->isPreempted());
  sequencer_->onActivationFailed();
  // sequencer should go to preempted state
  ASSERT_EQ(Sequencer::State::PREEMPTED, sequencer_->getState());
  ASSERT_EQ(NodeID(3, 1), sequencer_->checkIfPreempted(epoch_t(2)));
}

TEST_F(SequencerTest, PreemptionSimple) {
  settings_.reactivation_limit = RATE_UNLIMITED;
  setUp();
  sequencer_->startActivation([this](logid_t) { return getMetaData(); });
  ASSERT_EQ(Sequencer::State::ACTIVATING, sequencer_->getState());
  completeActivation(3);
  // preempted by a seal of epoch 5
  const NodeID preemptor(2, 1);
  sequencer_->notePreempted(epoch_t(5), preemptor);
  ASSERT_EQ(Sequencer::State::PREEMPTED, sequencer_->getState());
  ASSERT_TRUE(sequencer_->isPreempted());
  ASSERT_EQ(Seal(epoch_t(5), preemptor), sequencer_->getSealRecord());
  ASSERT_EQ(preemptor, sequencer_->checkIfPreempted(epoch_t(4)));
  ASSERT_EQ(preemptor, sequencer_->checkIfPreempted(epoch_t(5)));
  ASSERT_EQ(NodeID(), sequencer_->checkIfPreempted(epoch_t(6)));

  sequencer_->startActivation([this](logid_t) { return getMetaData(); });
  ASSERT_EQ(Sequencer::State::ACTIVATING, sequencer_->getState());
  ASSERT_TRUE(sequencer_->isPreempted());
  sequencer_->notePreempted(epoch_t(7), preemptor);
  // won't set state to PREEMPTED while activating
  ASSERT_EQ(Sequencer::State::ACTIVATING, sequencer_->getState());
  completeActivation(9);
  ASSERT_EQ(Sequencer::State::ACTIVE, sequencer_->getState());
  // no longer considered as preempted
  ASSERT_FALSE(sequencer_->isPreempted());
}

TEST_F(SequencerTest, LastReleased) {
  settings_.reactivation_limit = RATE_UNLIMITED;
  setUp();

  ASSERT_EQ(LSN_INVALID, sequencer_->getLastReleased());
  // activated to epoch 3
  sequencer_->startActivation([this](logid_t) { return getMetaData(); });
  completeActivation(3);
  // last released stays the same as recovery is not finished
  ASSERT_EQ(LSN_INVALID, sequencer_->getLastReleased());
  ASSERT_FALSE(sequencer_->isRecoveryComplete());

  epoch_t last_released_epoch{EPOCH_MAX};
  // simulate some appender get reaped in epoch 3
  bool release_changed =
      sequencer_->noteAppenderReaped(lsn(3, 1), &last_released_epoch);
  ASSERT_EQ(LSN_INVALID, sequencer_->getLastReleased());
  ASSERT_FALSE(release_changed);
  ASSERT_EQ(EPOCH_INVALID, last_released_epoch);
  ASSERT_EQ(E::STALE, err);

  // now retire some appenders in epoch 3 for real
  const size_t num_appenders{23};
  for (int i = 0; i < num_appenders; ++i) {
    auto appender = createAppender();
    ASSERT_EQ(RunAppenderStatus::SUCCESS_KEEP,
              sequencer_->runAppender(appender.get()));
    appender.release();
  }

  MockEpochSequencer* cur = getCurrentEpochSequencer();
  cur->retireAppenders(esn_t(1), esn_t(23), /*abort*/ false);
  ASSERT_EQ(lsn(3, 23), cur->getLastKnownGood());
  ASSERT_EQ(LSN_INVALID, sequencer_->getLastReleased());

  // recovery finished for next_epoch 3, last released should get advanced
  sequencer_->onRecoveryCompleted(
      E::OK, epoch_t(3), genTailRecord(lsn(2, 97)), nullptr);
  ASSERT_EQ(lsn(3, 23), sequencer_->getLastReleased());
  ASSERT_EQ(lsn(3, 23), sequencer_->getTailRecord()->header.lsn);

  // keep appending to the epoch, last released should advance with lng
  for (int i = 0; i < 5; ++i) {
    auto appender = createAppender();
    ASSERT_EQ(RunAppenderStatus::SUCCESS_KEEP,
              sequencer_->runAppender(appender.get()));
    ASSERT_EQ(lsn(3, 24 + i), appender->getLSN());
    appender.release();
  }
  cur->retireAppenders(esn_t(24), esn_t(28), /*abort*/ false);
  ASSERT_EQ(lsn(3, 28), cur->getLastKnownGood());
  ASSERT_EQ(lsn(3, 28), sequencer_->getLastReleased());
}

// aborted appenders should not increase last released for the epoch
TEST_F(SequencerTest, LastReleased2) {
  setUp();
  sequencer_->startActivation([this](logid_t) { return getMetaData(); });
  completeActivation(3);
  sequencer_->onRecoveryCompleted(
      E::OK, epoch_t(3), genTailRecord(lsn(2, 97)), nullptr);
  ASSERT_EQ(lsn(3, 0), sequencer_->getLastReleased());
  ASSERT_EQ(lsn(2, 97), sequencer_->getTailRecord()->header.lsn);
  for (int i = 0; i < 10; ++i) {
    auto appender = createAppender();
    ASSERT_EQ(RunAppenderStatus::SUCCESS_KEEP,
              sequencer_->runAppender(appender.get()));
    ASSERT_EQ(lsn(3, 1 + i), appender->getLSN());
    appender.release();
  }
  getCurrentEpochSequencer()->retireAppenders(esn_t(1), esn_t(4), false);
  ASSERT_EQ(lsn(3, 4), getCurrentEpochSequencer()->getLastKnownGood());
  ASSERT_EQ(lsn(3, 4), sequencer_->getLastReleased());
  // abort the next 3 appenders
  getCurrentEpochSequencer()->retireAppenders(esn_t(5), esn_t(7), true);
  ASSERT_EQ(lsn(3, 4), getCurrentEpochSequencer()->getLastKnownGood());
  // last released shouldn't get increased
  ASSERT_EQ(lsn(3, 4), sequencer_->getLastReleased());
  // retire the next 3 appenders normally, however, last released for the epoch
  // should stay
  getCurrentEpochSequencer()->retireAppenders(esn_t(8), esn_t(10), false);
  ASSERT_EQ(lsn(3, 4), getCurrentEpochSequencer()->getLastKnownGood());
  ASSERT_EQ(lsn(3, 4), sequencer_->getLastReleased());
}

// draining appends can advance last released while recovery is not completed
// but cannot if recovery of new epoch has finished
TEST_F(SequencerTest, LastReleased3) {
  setUp();
  sequencer_->startActivation([this](logid_t) { return getMetaData(); });
  completeActivation(3);
  sequencer_->onRecoveryCompleted(
      E::OK, epoch_t(3), genTailRecord(lsn(2, 97)), nullptr);
  ASSERT_EQ(lsn(3, 0), sequencer_->getLastReleased());
  ASSERT_EQ(lsn(2, 97), sequencer_->getTailRecord()->header.lsn);
  // insert 10 appends
  for (int i = 0; i < 10; ++i) {
    auto appender = createAppender();
    ASSERT_EQ(RunAppenderStatus::SUCCESS_KEEP,
              sequencer_->runAppender(appender.get()));
    ASSERT_EQ(lsn(3, 1 + i), appender->getLSN());
    appender.release();
  }
  // activate sequencer to epoch 5
  sequencer_->startActivation([this](logid_t) { return getMetaData(); });
  completeActivation(5);
  ASSERT_EQ(epoch_t(5), sequencer_->getCurrentEpoch());
  // epoch 3 should be in draining
  MockEpochSequencer* draining_seq_3 = getDrainingEpochSequencer();
  ASSERT_EQ(epoch_t(3), draining_seq_3->getEpoch());
  ASSERT_EQ(EpochSequencer::State::DRAINING, draining_seq_3->getState());

  draining_seq_3->retireAppenders(esn_t(1), esn_t(5), false);
  // last released should get increased
  ASSERT_EQ(lsn(3, 5), sequencer_->getLastReleased());
  // log recovery with next_epoch 5 finished
  sequencer_->onRecoveryCompleted(
      E::OK, epoch_t(5), genTailRecord(lsn(3, 5)), nullptr);
  ASSERT_EQ(lsn(5, 0), sequencer_->getLastReleased());
  ASSERT_EQ(lsn(3, 5), sequencer_->getTailRecord()->header.lsn);
  draining_seq_3->retireAppenders(esn_t(6), esn_t(10), false);
  ASSERT_EQ(EpochSequencer::State::QUIESCENT, draining_seq_3->getState());
  // last released should _not_ get increased
  ASSERT_EQ(lsn(5, 0), sequencer_->getLastReleased());

  // take a closer look
  epoch_t last_released_epoch{EPOCH_MAX};
  bool release_changed =
      sequencer_->noteAppenderReaped(lsn(3, 11), &last_released_epoch);
  ASSERT_FALSE(release_changed);
  ASSERT_EQ(lsn(5, 0), sequencer_->getLastReleased());
  ASSERT_EQ(epoch_t(5), last_released_epoch);
  ASSERT_EQ(E::STALE, err);
  auto appender = createAppender();
  ASSERT_EQ(
      RunAppenderStatus::SUCCESS_KEEP, sequencer_->runAppender(appender.get()));
  ASSERT_EQ(lsn(5, 1), appender->getLSN());
  appender.release();
  getCurrentEpochSequencer()->retireAppenders(esn_t(1), esn_t(1), false);
  ASSERT_EQ(lsn(5, 1), sequencer_->getLastReleased());
}

TEST_F(SequencerTest, MaxTotalAppendersSizeHard) {
  settings_.num_workers = 1;
  settings_.max_total_appenders_size_hard = 10000;
  with_processor_ = true;
  setUp();
  auto test = [&]() -> int {
    int rv =
        sequencer_->startActivation([this](logid_t) { return getMetaData(); });
    EXPECT_EQ(0, rv);
    completeActivation(3);
    EXPECT_EQ(epoch_t(3), sequencer_->getCurrentEpoch());
    EXPECT_EQ(Sequencer::State::ACTIVE, sequencer_->getState());
    sequencer_->onRecoveryCompleted(
        E::OK, epoch_t(3), genTailRecord(lsn(0, 0)), nullptr);
    // no released record
    EXPECT_EQ(LSN_INVALID, sequencer_->getTailRecord()->header.lsn);
    // should be succesfull taking appends in epoch 3
    auto appender = createAppender(5000);
    EXPECT_EQ(RunAppenderStatus::SUCCESS_KEEP,
              sequencer_->runAppender(appender.get()));
    EXPECT_EQ(epoch_t(3), lsn_to_epoch(appender->getLSN()));
    appender.release();
    appender = createAppender(4000);
    EXPECT_EQ(RunAppenderStatus::SUCCESS_KEEP,
              sequencer_->runAppender(appender.get()));
    appender.release();
    appender = createAppender(1001);
    EXPECT_EQ(RunAppenderStatus::ERROR_DELETE,
              sequencer_->runAppender(appender.get()));
    EXPECT_EQ(E::TEMPLIMIT, err);
    // retire the first appender, should have the quota again
    getCurrentEpochSequencer()->retireAppenders(esn_t(1), esn_t(1), false);
    // we have to completely delete it
    delete appenders_.front();
    appenders_.pop();
    EXPECT_EQ(lsn(3, 1), sequencer_->getLastReleased());
    appender = createAppender(4001);
    EXPECT_EQ(RunAppenderStatus::SUCCESS_KEEP,
              sequencer_->runAppender(appender.get()));
    appender.release();
    return 0;
  };

  run_on_worker(processor_.get(), /*worker_id=*/0, test);
}

TEST_F(SequencerTest, LogRemoval) {
  settings_.reactivation_limit = RATE_UNLIMITED;
  setUp();
  sequencer_->startActivation([this](logid_t) { return getMetaData(); });
  completeActivation(3);
  ASSERT_EQ(Sequencer::State::ACTIVE, sequencer_->getState());
  auto appender = createAppender();
  ASSERT_EQ(
      RunAppenderStatus::SUCCESS_KEEP, sequencer_->runAppender(appender.get()));
  appender.release();

  // remove the log from config
  removeLogFromConfig();
  sequencer_->noteConfigurationChanged(
      getConfig(), getNodesConfiguration(), true);
  // sequencer should transition to unavailable state and evict all epochs
  ASSERT_EQ(Sequencer::State::UNAVAILABLE, sequencer_->getState());
  ASSERT_EQ(EPOCH_INVALID, sequencer_->getCurrentEpoch());
  auto seqs = sequencer_->getEpochSequencers();
  ASSERT_EQ(nullptr, seqs.first);
  ASSERT_EQ(nullptr, seqs.second);
  appender = createAppender();
  // append should fail with E::NOSEQUENCER
  ASSERT_EQ(
      RunAppenderStatus::ERROR_DELETE, sequencer_->runAppender(appender.get()));
  ASSERT_EQ(E::NOSEQUENCER, err);
  sequencer_->onRecoveryCompleted(
      E::OK, epoch_t(3), genTailRecord(lsn(1, 97)), nullptr);
  ASSERT_EQ(LSN_INVALID, sequencer_->getLastReleased());
  // no tail record as sequencer is in uavailable state
  ASSERT_EQ(nullptr, sequencer_->getTailRecord());
  auto result = completeActivation(4);
  ASSERT_EQ(ActivateResult::FAILED, result);

  // put the log back, sequencer should be useful again after activation
  restoreConfig();
  int rv =
      sequencer_->startActivation([this](logid_t) { return getMetaData(); });
  ASSERT_EQ(0, rv);
  result = completeActivation(5);
  ASSERT_EQ(ActivateResult::RECOVERY, result);
  ASSERT_EQ(Sequencer::State::ACTIVE, sequencer_->getState());
  ASSERT_EQ(epoch_t(5), sequencer_->getCurrentEpoch());
  appender = createAppender();
  ASSERT_EQ(
      RunAppenderStatus::SUCCESS_KEEP, sequencer_->runAppender(appender.get()));
  EXPECT_EQ(epoch_t(5), lsn_to_epoch(appender->getLSN()));
  appender.release();
  getCurrentEpochSequencer()->retireAppenders(esn_t(1), esn_t(1), false);
  sequencer_->onRecoveryCompleted(
      E::OK, epoch_t(5), genTailRecord(lsn(1, 97)), nullptr);
  ASSERT_EQ(lsn(5, 1), sequencer_->getLastReleased());
  ASSERT_EQ(lsn(5, 1), sequencer_->getTailRecord()->header.lsn);
}

TEST_F(SequencerTest, RecoveryCompleteAfterUnavailableAndPreemption) {
  setUp();
  sequencer_->startActivation([this](logid_t) { return getMetaData(); });
  completeActivation(3);

  // sequencer become unavailable because seq_weight becomes 0
  sequencer_->noteConfigurationChanged(
      getConfig(), getNodesConfiguration(), false);
  ASSERT_EQ(Sequencer::State::UNAVAILABLE, sequencer_->getState());

  const NodeID preemptor(2, 1);
  sequencer_->notePreempted(epoch_t(5), preemptor);
  sequencer_->onRecoveryCompleted(
      E::PREEMPTED, epoch_t(3), TailRecord(), nullptr);
}

TEST_F(SequencerTest, RecoveryCompleteAfterUnavailableAndReactivation) {
  setUp();
  sequencer_->startActivation([this](logid_t) { return getMetaData(); });
  completeActivation(3);

  // sequencer become unavailable because seq_weight becomes 0
  sequencer_->noteConfigurationChanged(
      getConfig(), getNodesConfiguration(), false);
  ASSERT_EQ(Sequencer::State::UNAVAILABLE, sequencer_->getState());

  // sequencer reactivates itself
  sequencer_->startActivation([this](logid_t) { return getMetaData(); });
  sequencer_->onRecoveryCompleted(
      E::PREEMPTED, epoch_t(3), TailRecord(), nullptr);
}

TEST_F(SequencerTest, HistoricalMetadataRequest) {
  settings_.reactivation_limit = RATE_UNLIMITED;
  setUp();
  ASSERT_EQ(Sequencer::State::UNAVAILABLE, sequencer_->getState());
  sequencer_->startActivation([this](logid_t) { return getMetaData(); });
  sequencer_->completeActivationWithMetaData(
      epoch_t(5), getConfig(), genMetaData(epoch_t(5), epoch_t(3)));
  checkHistoricalMetaDataRequestEpoch(epoch_t(5));
  bool rv = sequencer_->onHistoricalMetaData(
      E::OK, epoch_t(5), genMetaDataMap({1, 2}));
  // retry not needed
  ASSERT_FALSE(rv);
  auto expected_map = genEpochMetaDataMap({1, 2, 3}, epoch_t(5));
  ASSERT_EQ(*expected_map, *sequencer_->getMetaDataMap());
}

TEST_F(SequencerTest, HistoricalMetadataSinceOne) {
  settings_.reactivation_limit = RATE_UNLIMITED;
  setUp();
  ASSERT_EQ(Sequencer::State::UNAVAILABLE, sequencer_->getState());
  sequencer_->startActivation([this](logid_t) { return getMetaData(); });
  // got epoch 5 metadata effective since 1
  sequencer_->completeActivationWithMetaData(
      epoch_t(5), getConfig(), genMetaData(epoch_t(5), EPOCH_MIN));
  noHistoricalMetaDataRequested();
}

// test optimizations that do not require read metadata logs
TEST_F(SequencerTest, HistoricalMetadataRequest2) {
  settings_.reactivation_limit = RATE_UNLIMITED;
  setUp();
  sequencer_->startActivation([this](logid_t) { return getMetaData(); });
  sequencer_->completeActivationWithMetaData(
      epoch_t(5), getConfig(), genMetaData(epoch_t(5), epoch_t(3)));
  checkHistoricalMetaDataRequestEpoch(epoch_t(5));
  bool rv = sequencer_->onHistoricalMetaData(
      E::OK, epoch_t(5), genMetaDataMap({1, 2}));
  // retry not needed
  ASSERT_FALSE(rv);
  // sequencer activates with the same metadata in epoch 10
  sequencer_->startActivation([this](logid_t) { return getMetaData(); });
  sequencer_->completeActivationWithMetaData(
      epoch_t(10), getConfig(), genMetaData(epoch_t(10), epoch_t(3)));
  noHistoricalMetaDataRequested();
  auto expected_map = genEpochMetaDataMap({1, 2, 3}, epoch_t(10));
  ASSERT_EQ(*expected_map, *sequencer_->getMetaDataMap());
  // sequencer activates to epoch 15 with a new metadata with effective since 11
  sequencer_->startActivation([this](logid_t) { return getMetaData(); });
  sequencer_->completeActivationWithMetaData(
      epoch_t(15), getConfig(), genMetaData(epoch_t(15), epoch_t(11)));
  noHistoricalMetaDataRequested();
  expected_map = genEpochMetaDataMap({1, 2, 3, 11}, epoch_t(15));
  ASSERT_EQ(*expected_map, *sequencer_->getMetaDataMap());
}

// TODO: more multi-threaded tests, tests for various race conditions

} // anonymous namespace
