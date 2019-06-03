/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/EpochSequencer.h"

#include <chrono>
#include <mutex>
#include <thread>

#include <folly/Random.h>
#include <gtest/gtest.h>

#include "event2/buffer.h"
#include "logdevice/common/Appender.h"
#include "logdevice/common/MetaDataLogWriter.h"
#include "logdevice/common/NoopTraceLogger.h"
#include "logdevice/common/PassThroughCopySetManager.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Sequencer.h"
#include "logdevice/common/TailRecord.h"
#include "logdevice/common/Timer.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/libevent/compat.h"
#include "logdevice/common/protocol/STORE_Message.h"
#include "logdevice/common/request_util.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/common/test/TestUtil.h"

using namespace facebook::logdevice;

namespace {

class MockEpochSequencer;
class MockAppender;

class EpochSequencerTest : public ::testing::Test {
 public:
  explicit EpochSequencerTest();

  ~EpochSequencerTest() override {
    es_.reset();
    sequencers_.update(nullptr);
    processor_.reset();
    checkStats();
  }

  dbg::Level log_level_ = dbg::Level::INFO;
  Alarm alarm_{DEFAULT_TEST_TIMEOUT};

  static constexpr logid_t LOG_ID{1};
  static constexpr epoch_t EPOCH{23};
  int window_size_ = 256;
  bool tail_optimized_ = true;
  esn_t esn_max_ = ESN_MAX;
  size_t max_appender_age_ms_ = 3;

  int num_workers_ = 16;

  size_t payload_size_ = 200;

  std::atomic<bool> suspend_retire_{false};

  std::atomic<lsn_t> max_lsn_accepted_{LSN_INVALID};

  Settings settings_;
  StatsHolder stats_;
  std::shared_ptr<UpdateableConfig> updateable_config_;

  std::shared_ptr<CopySetManager> copyset_manager_;

  std::shared_ptr<Processor> processor_;
  std::shared_ptr<EpochSequencer> es_;

  std::atomic<bool> epoch_drained_{false};

  // various stats
  struct TestStats {
    std::atomic<size_t> appender_created{0};
    std::atomic<size_t> appender_destroyed{0};
    std::atomic<size_t> epoch_created{0};
    std::atomic<size_t> epoch_destroyed{0};

    std::atomic<size_t> appender_success{0};
    std::atomic<size_t> appender_failed_nobuf{0};
    std::atomic<size_t> appender_failed_disabled{0};
    std::atomic<size_t> appender_failed_toobig{0};
    std::atomic<size_t> appender_failed_nosequencer{0};

    std::atomic<size_t> epoch_drained{0};
  };

  TestStats stats;

  void setUp();
  std::shared_ptr<EpochSequencer> createEpochSequencer(epoch_t epoch = EPOCH);
  MockAppender* createAppender(bool not_retire = false);
  void checkStats();
  void maybeDeleteEpochSequencer();

  PayloadHolder genPayload(bool evbuffer);
  void checkTailRecord(lsn_t lsn,
                       folly::Optional<OffsetMap> offsets = folly::none);

  /////// for multi-threaded multi-epoch stress tests //////
  bool multi_epoch_ = false;
  std::atomic<uint32_t> next_epoch_{EPOCH.val_ + 1};
  std::atomic<bool> stop{false};
  int num_test_threads_ = 16;
  int requests_per_second = 600;
  struct EpochSequencerContainer {
    std::shared_ptr<EpochSequencer> current;
    std::shared_ptr<EpochSequencer> draining;
  };
  std::mutex epoch_mutex_;
  UpdateableSharedPtr<EpochSequencerContainer> sequencers_;

  std::shared_ptr<EpochSequencer> getSequencer() const {
    auto container = sequencers_.get();
    return container == nullptr ? nullptr : container->current;
  }

  void advanceEpoch();
};

class MockEpochSequencer : public EpochSequencer {
 public:
  MockEpochSequencer(EpochSequencerTest* test,
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
        test_(test) {
    ++test_->stats.epoch_created;
  }

  ~MockEpochSequencer() override {
    ++test_->stats.epoch_destroyed;
    ld_debug("epoch sequencer of epoch %u destroyed.", getEpoch().val_);
  }

  RunAppenderStatus runAppender(Appender* appender) override {
    RunAppenderStatus result = EpochSequencer::runAppender(appender);
    if (result == RunAppenderStatus::ERROR_DELETE) {
      switch (err) {
        case E::NOBUFS:
          ++test_->stats.appender_failed_nobuf;
          break;
        case E::DISABLED:
          ++test_->stats.appender_failed_disabled;
          break;
        case E::TOOBIG:
          ++test_->stats.appender_failed_toobig;
          break;
        case E::NOSEQUENCER:
          ++test_->stats.appender_failed_nosequencer;
          break;
        default:
          ld_check(false);
          break;
      }
    } else {
      EXPECT_EQ(RunAppenderStatus::SUCCESS_KEEP, result);
      atomic_fetch_max(test_->max_lsn_accepted_, appender->getLSN());
      ++test_->stats.appender_success;
    }

    return result;
  }

  void noteDrainingCompleted(Status status) override {
    if (status == E::OK) {
      bool prev_drained = test_->epoch_drained_.exchange(true);
      if (!test_->multi_epoch_) {
        ASSERT_FALSE(prev_drained);
      }
      ++test_->stats.epoch_drained;
    }
  }

  bool updateLastReleased(lsn_t /*reaped_lsn*/,
                          epoch_t* /*last_released_epoch_out*/) override {
    // don't care about last released lsn in this test, assuming everything
    // will be released
    return true;
  }

  Processor* getProcessor() const override {
    return test_->processor_.get();
  }

 private:
  EpochSequencerTest* const test_;
};

class MockAppender : public Appender {
 public:
  using MockSender = SenderTestProxy<MockAppender>;

  explicit MockAppender(EpochSequencerTest* test,
                        std::chrono::milliseconds retire_after,
                        bool evbuffer_payload = true)
      : Appender(Worker::onThisThread(),
                 Worker::onThisThread()->getTraceLogger(),
                 retire_after,
                 /* append_request_id= */ request_id_t(0),
                 STORE_flags_t(0),
                 test->LOG_ID,
                 test->genPayload(evbuffer_payload),
                 epoch_t(0),
                 /*size=*/size_t(0)),
        retire_after_(retire_after),
        timer_([this] { onTimerFired(); }),
        test_(test) {
    sender_ = std::make_unique<MockSender>(this);
    ++test_->stats.appender_created;
  }

  ~MockAppender() override {
    epoch_sequencer_.reset();
    ++test_->stats.appender_destroyed;
    ld_debug("Appender %s destroyed.", getStoreHeader().rid.toString().c_str());
  }

  int start(std::shared_ptr<EpochSequencer> epoch_sequencer,
            lsn_t lsn) override {
    int rv = Appender::start(std::move(epoch_sequencer), lsn);
    ld_debug("Appender (%p) %s started. retire_after: %lu ms",
             this,
             getStoreHeader().rid.toString().c_str(),
             retire_after_.count());
    return rv;
  }

  int sendMessageImpl(std::unique_ptr<Message>&& msg,
                      const Address& addr,
                      BWAvailableCallback*,
                      SocketCallback*) {
    ld_check(!addr.isClientAddress());
    if (msg->type_ != MessageType::STORE) {
      return 0;
    }

    STORE_Message* store_msg = dynamic_cast<STORE_Message*>(msg.get());
    EXPECT_TRUE(store_msg);
    store_header_ = std::make_unique<STORE_Header>(store_msg->getHeader());
    auto copyset = store_msg->getCopyset();
    shard_ = copyset[store_msg->getHeader().copyset_offset].destination;

    // Trigger the timer. We will reply to that message once it expires.
    if (retire_after_ != std::chrono::milliseconds::zero()) {
      timer_.activate(retire_after_);
    }

    // consume the message
    msg.reset();
    return 0;
  }

  void sendReply() {
    if (test_->suspend_retire_.load()) {
      // Busy wait for suspect_retire_ to become false.
      timer_.activate(std::chrono::milliseconds(1));
      return;
    }

    ld_spew("Appender (%p) %s send reply.",
            this,
            getStoreHeader().rid.toString().c_str());

    ASSERT_NE(nullptr, store_header_);

    // Simulate the socket acknowledging that the STORE message was sent.
    onCopySent(E::OK, shard_, *store_header_);

    // Simulate the recipient replying favorably.
    // This should cause the Appender to retire.
    STORED_Header hdr;
    hdr.rid = store_header_->rid;
    hdr.wave = store_header_->wave;
    hdr.status = E::OK;
    hdr.redirect = NodeID();
    hdr.flags = 0;
    hdr.shard = shard_.shard();
    onReply(hdr, shard_);
  }

  void abortInsteadOfStored() {
    abort_instead_ = true;
  }

  void onTimerFired() {
    if (abort_instead_) {
      abort();
    } else {
      sendReply();
    }
  }

  NodeID checkIfPreempted(epoch_t /*epoch*/) override {
    // TODO: do not consider preemption in this test for now
    return NodeID();
  }

  std::shared_ptr<CopySetManager> getCopySetManager() const override {
    return test_->copyset_manager_;
  }

  bool epochMetaDataAvailable(epoch_t /*epoch*/) const override {
    return true;
  }

  void schedulePeriodicReleases() override {}

  bool isDraining() const override {
    return epoch_sequencer_->getState() == EpochSequencer::State::DRAINING;
  }

  int registerOnSocketClosed(NodeID /*nid*/, SocketCallback& /*cb*/) override {
    return 0;
  }

  void cancelStoreTimer() override {
    Appender::cancelStoreTimer();
    timer_.cancel();
  }

  NodeLocationScope getCurrentBiggestReplicationScope() const override {
    return NodeLocationScope::NODE;
  }

  // needed by mocking sender
  bool canSendToImpl(const Address&, TrafficClass, BWAvailableCallback&) {
    return true;
  }

  bool checkNodeSet() const override {
    return true;
  }

 private:
  // retire after this duration. if zero, Appender will not automatically retire
  std::chrono::milliseconds retire_after_;
  Timer timer_;

  std::unique_ptr<STORE_Header> store_header_;
  // Recipient of first STORE message sent.
  ShardID shard_;

  // abort rather than stored after the timeout
  bool abort_instead_{false};
  EpochSequencerTest* const test_;
};

// a dummy copyset selector for this test that always return {N1} as the
// copyset. ony useful for the config "sequencer_test.conf"
class TestCopySetSelector : public CopySetSelector {
 public:
  CopySetSelector::Result select(copyset_size_t, /* extras, unused */
                                 StoreChainLink copyset_out[],
                                 copyset_size_t* copyset_size_out,
                                 bool*, /* chain_out, unused */
                                 State* /*selector_state*/,
                                 RNG&,
                                 bool /* retry, unused */) const override {
    copyset_out[0] = StoreChainLink{ShardID(1, 1), ClientID()};
    *copyset_size_out = 1;
    return CopySetSelector::Result::SUCCESS;
  }

  CopySetSelector::Result augment(ShardID /*inout_copyset*/ [],
                                  copyset_size_t /*existing_copyset_size*/,
                                  copyset_size_t*, /* out_full_size, unused */
                                  RNG&,
                                  bool /* retry, unused */) const override {
    ld_check(false);
    return CopySetSelector::Result::FAILED;
  }

  CopySetSelector::Result augment(StoreChainLink[] /* unused */,
                                  copyset_size_t /* unused */,
                                  copyset_size_t* /* unused */,
                                  bool /* unused */,
                                  bool* /* unused */,
                                  RNG& /* unused */,
                                  bool /* unused */) const override {
    throw std::runtime_error("unimplemented");
  }

  copyset_size_t getReplicationFactor() const override {
    return 1;
  }
};

EpochSequencerTest::EpochSequencerTest()
    : settings_(create_default_settings<Settings>()),
      stats_(StatsParams().setIsServer(true)) {}

void EpochSequencerTest::setUp() {
  dbg::currentLevel = log_level_;

  updateable_config_ = std::make_shared<UpdateableConfig>(
      Configuration::fromJsonFile(TEST_CONFIG_FILE("sequencer_test.conf")));

  std::shared_ptr<configuration::LocalLogsConfig> logs_config =
      std::make_shared<configuration::LocalLogsConfig>(
          *updateable_config_->get()->localLogsConfig());

  auto lid = logs_config->getLogGroupInDirectoryByIDRaw(LOG_ID);
  auto new_loggrp = lid->log_group->withLogAttributes(
      lid->log_group->attrs()
          .with_maxWritesInFlight(window_size_)
          .with_tailOptimized(tail_optimized_));
  logs_config->replaceLogGroup(lid->getFullyQualifiedName(), new_loggrp);
  auto new_config = std::make_shared<Configuration>(
      updateable_config_->get()->serverConfig(), std::move(logs_config));

  ld_check_eq(0,
              updateable_config_->updateableLogsConfig()->update(
                  new_config->logsConfig()));
  ld_check_eq(0,
              updateable_config_->updateableServerConfig()->update(
                  new_config->serverConfig()));

  // create copyset manager
  StorageSet shards{ShardID(0, 1), ShardID(1, 1)};
  auto nodeset_state = std::make_shared<NodeSetState>(
      shards, LOG_ID, NodeSetState::HealthCheck::DISABLED);

  std::unique_ptr<CopySetSelector> copyset_selector(new TestCopySetSelector());
  copyset_manager_.reset(new PassThroughCopySetManager(
      std::move(copyset_selector), nodeset_state));
  copyset_manager_->disableCopySetShuffling();

  settings_.num_workers = num_workers_;
  // turn on byte offsets
  settings_.byte_offsets = true;
  processor_ =
      make_test_processor(settings_, updateable_config_, &stats_, NodeID(1, 1));
  ASSERT_NE(nullptr, processor_.get());

  es_ = createEpochSequencer();
}

std::shared_ptr<EpochSequencer>
EpochSequencerTest::createEpochSequencer(epoch_t epoch) {
  EpochSequencerImmutableOptions opts;
  opts.window_size = window_size_;
  opts.esn_max = esn_max_;
  return std::make_shared<MockEpochSequencer>(this,
                                              LOG_ID,
                                              epoch,
                                              std::make_unique<EpochMetaData>(),
                                              opts,
                                              /*sequencer=*/nullptr);
}

constexpr logid_t EpochSequencerTest::LOG_ID;
constexpr epoch_t EpochSequencerTest::EPOCH;

PayloadHolder EpochSequencerTest::genPayload(bool evbuffer) {
  if (evbuffer) {
    struct evbuffer* evbuf = LD_EV(evbuffer_new)();
    ProtocolWriter writer(MessageType::APPEND, evbuf, 0);
    std::string raw(payload_size_, 'c');
    writer.write(raw.data(), raw.size());
    return PayloadHolder(evbuf);
  }

  void* payload_flat = malloc(payload_size_);
  memset(payload_flat, 'c', payload_size_);
  return PayloadHolder(payload_flat, payload_size_);
}

void EpochSequencerTest::checkStats() {
  ASSERT_EQ(stats.appender_created, stats.appender_destroyed);
  ASSERT_EQ(stats.epoch_created, stats.epoch_destroyed);
  const size_t total_appenders = stats.appender_success +
      stats.appender_failed_nobuf + stats.appender_failed_disabled +
      stats.appender_failed_toobig + stats.appender_failed_nosequencer;
  ASSERT_EQ(stats.appender_created, total_appenders);
}

MockAppender* EpochSequencerTest::createAppender(bool not_retire) {
  return new MockAppender(
      this,
      (not_retire ? std::chrono::milliseconds::zero()
                  : std::chrono::milliseconds(
                        folly::Random::rand64(max_appender_age_ms_) + 1)));
}

bool coin_toss() {
  return folly::Random::rand64(2) == 0;
}

void EpochSequencerTest::maybeDeleteEpochSequencer() {
  if (coin_toss()) {
    es_.reset();
  }
}

void EpochSequencerTest::advanceEpoch() {
  size_t step = folly::Random::rand64(4) + 1;
  {
    std::lock_guard<std::mutex> state_lock(epoch_mutex_);
    epoch_t next_epoch = epoch_t(next_epoch_.fetch_add(step));
    auto new_es = createEpochSequencer(next_epoch);
    auto cur_sequencers = sequencers_.get();
    bool drain = coin_toss();

    ld_info(
        "Advancing Epoch from %u to %u.",
        cur_sequencers->current ? cur_sequencers->current->getEpoch().val_ : 0,
        next_epoch.val_);
    sequencers_.update(
        std::make_shared<EpochSequencerContainer>(EpochSequencerContainer{
            /*current*/ new_es,
            /*draining*/ drain ? cur_sequencers->current : nullptr}));
    if (cur_sequencers->current != nullptr) {
      if (drain) {
        cur_sequencers->current->startDraining();
      } else {
        cur_sequencers->current->startDestruction();
      }
    }
    if (cur_sequencers->draining != nullptr) {
      cur_sequencers->draining->startDestruction();
    }
  }
}

void EpochSequencerTest::checkTailRecord(lsn_t lsn,
                                         folly::Optional<OffsetMap> offsets) {
  if (lsn_to_esn(lsn) == ESN_INVALID) {
    EXPECT_EQ(nullptr, es_->getTailRecord());
    return;
  }

  EXPECT_NE(nullptr, es_->getTailRecord());
  const auto& r = *es_->getTailRecord();
  EXPECT_EQ(lsn, r.header.lsn);
  EXPECT_GT(r.header.timestamp, 0);
  if (offsets.hasValue()) {
    EXPECT_EQ(offsets.value(), r.offsets_map_);
  }
  EXPECT_NE(0, r.header.flags & TailRecordHeader::OFFSET_WITHIN_EPOCH);

  if (tail_optimized_) {
    EXPECT_TRUE(r.hasPayload());
    auto s = r.getPayloadSlice();
    EXPECT_EQ(payload_size_, s.size);
    std::string raw(payload_size_, 'c');
    EXPECT_EQ(0, memcmp(s.data, raw.data(), s.size));
  } else {
    EXPECT_FALSE(r.hasPayload());
    EXPECT_FALSE(r.header.flags & TailRecordHeader::CHECKSUM);
    EXPECT_FALSE(r.header.flags & TailRecordHeader::CHECKSUM_64BIT);
    EXPECT_TRUE(r.header.flags & TailRecordHeader::CHECKSUM_PARITY);
  }
}

///////////////////// Single Thread Basic Tests ///////////////

TEST_F(EpochSequencerTest, BasicAppend) {
  setUp();
  auto test = [&]() {
    EXPECT_EQ(EpochSequencer::State::ACTIVE, es_->getState());
    MockAppender* appender = createAppender(/*not_retire=*/true);

    auto status = es_->runAppender(appender);
    EXPECT_EQ(RunAppenderStatus::SUCCESS_KEEP, status);

    lsn_t appender_lsn = appender->getLSN();
    EXPECT_EQ(compose_lsn(EPOCH, ESN_MIN), appender_lsn);
    EXPECT_EQ(EpochSequencer::State::ACTIVE, es_->getState());

    EXPECT_EQ(compose_lsn(EPOCH, ESN_INVALID), es_->getLastKnownGood());
    EXPECT_EQ(1, es_->getNumAppendsInFlight());
    EXPECT_EQ(nullptr, es_->getTailRecord());
    return 0;
  };

  run_on_worker(processor_.get(), /*worker_id=*/0, test);
  // both Appender and EpochSequencer should be properly destroyed when Worker
  // is destroyed
}

TEST_F(EpochSequencerTest, RetireMultipleAppends) {
  window_size_ = 128;
  setUp();
  lsn_t expect_first_lsn = compose_lsn(EPOCH, ESN_MIN);
  size_t num_appenders = 100;
  auto test = [&]() {
    for (int i = 0; i < num_appenders; ++i) {
      MockAppender* appender = createAppender();
      auto status = es_->runAppender(appender);
      EXPECT_EQ(RunAppenderStatus::SUCCESS_KEEP, status);

      lsn_t appender_lsn = appender->getLSN();
      EXPECT_EQ(expect_first_lsn + i, appender_lsn);
      EXPECT_EQ(EpochSequencer::State::ACTIVE, es_->getState());
    }
    return 0;
  };

  run_on_worker(processor_.get(), /*worker_id=*/0, test);

  const lsn_t last_lsn = expect_first_lsn + num_appenders - 1;
  ld_info("Wait for all appenders to retire, be reaped and destroyed");
  wait_until([&]() {
    return es_->getLastReaped() == last_lsn &&
        num_appenders == stats.appender_destroyed;
  });

  ASSERT_EQ(last_lsn, es_->getLastReaped());
  ASSERT_EQ(last_lsn, es_->getLastKnownGood());
  ASSERT_EQ(0, es_->getNumAppendsInFlight());
  checkTailRecord(
      last_lsn, OffsetMap({{BYTE_OFFSET, num_appenders * payload_size_}}));
  maybeDeleteEpochSequencer();
}

TEST_F(EpochSequencerTest, WindowFull) {
  window_size_ = 7;
  setUp();
  lsn_t expect_first_lsn = compose_lsn(EPOCH, ESN_MIN);
  auto test = [&]() {
    for (int i = 0; i < window_size_; ++i) {
      MockAppender* appender = createAppender(/*not_retire=*/true);
      auto status = es_->runAppender(appender);
      EXPECT_EQ(RunAppenderStatus::SUCCESS_KEEP, status);

      lsn_t appender_lsn = appender->getLSN();
      EXPECT_EQ(expect_first_lsn + i, appender_lsn);
      EXPECT_EQ(EpochSequencer::State::ACTIVE, es_->getState());
    }

    MockAppender* appender = createAppender();
    auto status = es_->runAppender(appender);
    EXPECT_EQ(RunAppenderStatus::ERROR_DELETE, status);
    EXPECT_EQ(E::NOBUFS, err);
    EXPECT_FALSE(appender->started());
    delete appender;
    return 0;
  };

  run_on_worker(processor_.get(), /*worker_id=*/0, test);
  EXPECT_EQ(compose_lsn(EPOCH, ESN_INVALID), es_->getLastKnownGood());
  EXPECT_EQ(window_size_, es_->getNumAppendsInFlight());
  EXPECT_EQ(nullptr, es_->getTailRecord());

  maybeDeleteEpochSequencer();
}

// starting Appender will get E::TOOBIG as maximum esn is reached for the epoch
TEST_F(EpochSequencerTest, ESN_MAX) {
  esn_max_ = esn_t(64);
  window_size_ = 256;
  setUp();
  auto test = [&]() {
    for (int i = 0; i < esn_max_.val_; ++i) {
      MockAppender* appender = createAppender();
      auto status = es_->runAppender(appender);
      EXPECT_EQ(RunAppenderStatus::SUCCESS_KEEP, status);
    }
    return 0;
  };
  run_on_worker(processor_.get(), /*worker_id=*/0, test);
  wait_until([&]() { return esn_max_.val_ == stats.appender_destroyed; });

  checkTailRecord(compose_lsn(EPOCH, esn_max_),
                  OffsetMap({{BYTE_OFFSET, esn_max_.val_ * payload_size_}}));

  auto test2 = [&]() {
    MockAppender* appender = createAppender();
    auto status = es_->runAppender(appender);
    EXPECT_EQ(RunAppenderStatus::ERROR_DELETE, status);
    EXPECT_EQ(E::TOOBIG, err);
    EXPECT_FALSE(appender->started());
    delete appender;
    return 0;
  };

  run_on_worker(processor_.get(), /*worker_id=*/0, test2);
  // tail should remain the same
  checkTailRecord(compose_lsn(EPOCH, esn_max_),
                  OffsetMap({{BYTE_OFFSET, esn_max_.val_ * payload_size_}}));
  maybeDeleteEpochSequencer();
}

// perform state transition in case that epoch sequencer never stored any
// records for the epoch
TEST_F(EpochSequencerTest, EmptySequencerStateTransition) {
  setUp();
  auto test = [&]() {
    EXPECT_EQ(EpochSequencer::State::ACTIVE, es_->getState());
    if (coin_toss()) {
      es_->startDraining();
    } else {
      es_->startDestruction();
    }
    EXPECT_EQ(nullptr, es_->getTailRecord());
    // sequencer should be immediately transition into QUIESCENT
    EXPECT_EQ(EpochSequencer::State::QUIESCENT, es_->getState());
    return 0;
  };
  run_on_worker(processor_.get(), /*worker_id=*/0, test);
  EXPECT_EQ(nullptr, es_->getTailRecord());
  maybeDeleteEpochSequencer();
}

// perform state transition in case that epoch sequencer stored some
// records but they are all reaped
TEST_F(EpochSequencerTest, EmptySequencerStateTransition2) {
  setUp();
  auto test = [&]() {
    for (int i = 0; i < 10; ++i) {
      MockAppender* appender = createAppender();
      auto status = es_->runAppender(appender);
      EXPECT_EQ(RunAppenderStatus::SUCCESS_KEEP, status);
    }
    return 0;
  };
  run_on_worker(processor_.get(), /*worker_id=*/0, test);
  // wait for all appenders to retire, be reaped and destroyed
  wait_until([&]() { return 10 == stats.appender_destroyed; });

  checkTailRecord(compose_lsn(EPOCH, esn_t(10)),
                  OffsetMap({{BYTE_OFFSET, 10 * payload_size_}}));

  auto test2 = [&]() {
    EXPECT_EQ(EpochSequencer::State::ACTIVE, es_->getState());
    if (coin_toss()) {
      es_->startDraining();
    } else {
      es_->startDestruction();
    }
    // sequencer should be immediately transition into QUIESCENT
    EXPECT_EQ(EpochSequencer::State::QUIESCENT, es_->getState());
    return 0;
  };
  run_on_worker(processor_.get(), /*worker_id=*/0, test2);
  checkTailRecord(compose_lsn(EPOCH, esn_t(10)),
                  OffsetMap({{BYTE_OFFSET, 10 * payload_size_}}));
  maybeDeleteEpochSequencer();
}

///////////////////// Basic Multi-Threaded Tests /////////////////////

TEST_F(EpochSequencerTest, NotAdvancingLNGonAbort) {
  num_workers_ = 16;
  window_size_ = 1024;
  const int append_per_worker = 20;
  setUp();
  std::atomic<lsn_t> first_aborted_lsn{LSN_MAX};

  auto test = [&]() {
    for (int i = 0; i < append_per_worker; ++i) {
      bool abort_instead = false;
      MockAppender* appender = createAppender();
      if (i == folly::Random::rand64(append_per_worker)) {
        abort_instead = true;
        appender->abortInsteadOfStored();
      }
      auto status = es_->runAppender(appender);
      EXPECT_EQ(RunAppenderStatus::SUCCESS_KEEP, status);
      if (abort_instead) {
        atomic_fetch_min(first_aborted_lsn, appender->getLSN());
      }
    }
    return 0;
  };
  run_on_worker_pool(processor_.get(), WorkerType::GENERAL, test);
  const int total_appenders = append_per_worker * num_workers_;
  const lsn_t last_lsn = compose_lsn(EPOCH, esn_t(total_appenders));

  // wait for all appenders to be destroyed
  wait_until([&]() {
    // we destroyed one extra Appender per Worker
    return total_appenders == stats.appender_destroyed;
  });
  ASSERT_EQ(0, es_->getNumAppendsInFlight());

  // all appenders must have been reaped
  ASSERT_EQ(last_lsn, es_->getLastReaped());
  // however, LNG should be exactly one less than first_aborted_lsn
  ASSERT_NE(last_lsn, es_->getLastKnownGood());
  ASSERT_EQ(first_aborted_lsn.load() - 1, es_->getLastKnownGood());
  checkTailRecord(es_->getLastKnownGood());

  maybeDeleteEpochSequencer();
}

TEST_F(EpochSequencerTest, Draining1) {
  num_workers_ = 16;
  window_size_ = 1024;
  suspend_retire_ = true;
  const int append_per_worker = 51;
  setUp();

  auto test = [&]() {
    for (int i = 0; i < append_per_worker; ++i) {
      MockAppender* appender = createAppender();
      auto status = es_->runAppender(appender);
      EXPECT_EQ(RunAppenderStatus::SUCCESS_KEEP, status);
    }
    return 0;
  };
  run_on_worker_pool(processor_.get(), WorkerType::GENERAL, test);

  const int total_appenders = append_per_worker * num_workers_;
  const lsn_t last_lsn = compose_lsn(EPOCH, esn_t(total_appenders));
  ASSERT_EQ(EpochSequencer::State::ACTIVE, es_->getState());
  ASSERT_EQ(compose_lsn(EPOCH, ESN_INVALID), es_->getLastReaped());
  ASSERT_EQ(compose_lsn(EPOCH, ESN_INVALID), es_->getLastKnownGood());
  EXPECT_EQ(nullptr, es_->getTailRecord());

  ASSERT_EQ(total_appenders, es_->getNumAppendsInFlight());
  ASSERT_EQ(last_lsn + 1, es_->getNextLSN());
  ASSERT_EQ(LSN_MAX, es_->getDrainingTarget());
  // now start draining the epoch sequencer
  es_->startDraining();
  ASSERT_EQ(EpochSequencer::State::DRAINING, es_->getState());

  // new appenders should be rejected
  auto test2 = [&]() {
    MockAppender* appender = createAppender();
    auto status = es_->runAppender(appender);
    EXPECT_EQ(RunAppenderStatus::ERROR_DELETE, status);
    EXPECT_EQ(E::NOSEQUENCER, err);
    delete appender;
    return 0;
  };
  run_on_worker_pool(processor_.get(), WorkerType::GENERAL, test2);
  EXPECT_EQ(nullptr, es_->getTailRecord());

  ASSERT_FALSE(epoch_drained_.load());
  ASSERT_EQ(last_lsn, es_->getDrainingTarget());
  // appender should start finishing replication after the next statement
  suspend_retire_ = false;
  wait_until([&]() {
    // we destroyed one extra Appender per Worker
    return epoch_drained_.load() &&
        total_appenders + num_workers_ == stats.appender_destroyed;
  });

  ASSERT_EQ(EpochSequencer::State::QUIESCENT, es_->getState());
  // all appenders must have been reaped
  ASSERT_EQ(last_lsn, es_->getLastReaped());
  ASSERT_EQ(last_lsn, es_->getLastKnownGood());
  ASSERT_EQ(0, es_->getNumAppendsInFlight());
  checkTailRecord(es_->getLastKnownGood());

  es_.reset();
  ASSERT_EQ(1, stats.epoch_destroyed);
  ASSERT_EQ(last_lsn, max_lsn_accepted_);
}

// test the condition that Appenders can be forcefully aborted when the epoch
// sequencer started its destruction
TEST_F(EpochSequencerTest, AbortAppenders) {
  num_workers_ = 16;
  window_size_ = 256;
  setUp();

  const int append_per_worker = 10;
  const int total_appenders = append_per_worker * num_workers_;
  const lsn_t last_lsn = compose_lsn(EPOCH, esn_t(total_appenders));

  auto test = [&]() {
    for (int i = 0; i < append_per_worker; ++i) {
      // first 3 appends will not retire
      MockAppender* appender = createAppender(
          /*not_retire=*/i < 3 ? true : false);
      auto status = es_->runAppender(appender);
      EXPECT_EQ(RunAppenderStatus::SUCCESS_KEEP, status);
    }
    return 0;
  };
  run_on_worker_pool(processor_.get(), WorkerType::GENERAL, test);
  // sleep for 2ms for some Appenders to retire
  /* sleep override */
  std::this_thread::sleep_for(std::chrono::milliseconds(2));

  ASSERT_EQ(EpochSequencer::State::ACTIVE, es_->getState());
  ASSERT_EQ(last_lsn + 1, es_->getNextLSN());
  ASSERT_EQ(LSN_MAX, es_->getDrainingTarget());
  checkTailRecord(es_->getLastKnownGood());

  // now start destroying the epoch sequencer
  run_on_worker(processor_.get(),
                /*worker_id=*/0,
                [this]() {
                  es_->startDestruction();
                  EXPECT_EQ(EpochSequencer::State::DYING, es_->getState());
                  return 0;
                });
  // new appenders should be rejected
  auto test2 = [&]() {
    MockAppender* appender = createAppender();
    auto status = es_->runAppender(appender);
    EXPECT_EQ(RunAppenderStatus::ERROR_DELETE, status);
    EXPECT_EQ(E::NOSEQUENCER, err);
    delete appender;
    return 0;
  };
  run_on_worker_pool(processor_.get(), WorkerType::GENERAL, test2);

  ASSERT_FALSE(epoch_drained_.load());
  ASSERT_EQ(last_lsn, es_->getDrainingTarget());
  wait_until([&]() {
    // we destroyed one extra Appender per Worker
    return total_appenders + num_workers_ == stats.appender_destroyed;
  });

  ASSERT_EQ(EpochSequencer::State::QUIESCENT, es_->getState());
  // all appenders must have been reaped
  ASSERT_EQ(last_lsn, es_->getLastReaped());

  // however, LNG shouldn't get advanced to last_lsn because some Appenders
  // are not fully replicated.
  ASSERT_NE(last_lsn, es_->getLastKnownGood());
  checkTailRecord(es_->getLastKnownGood());
  ASSERT_EQ(0, es_->getNumAppendsInFlight());

  es_.reset();
  ASSERT_EQ(1, stats.epoch_destroyed);
  ASSERT_EQ(last_lsn, max_lsn_accepted_);
}

// destroy the sequencer while it was previously in draining state
TEST_F(EpochSequencerTest, AbortWhileDraining) {
  num_workers_ = 8;
  window_size_ = 500;
  suspend_retire_ = true;
  const int append_per_worker = 10;
  setUp();

  auto test = [&]() {
    for (int i = 0; i < append_per_worker; ++i) {
      MockAppender* appender = createAppender();
      auto status = es_->runAppender(appender);
      EXPECT_EQ(RunAppenderStatus::SUCCESS_KEEP, status);
    }
    return 0;
  };
  run_on_worker_pool(processor_.get(), WorkerType::GENERAL, test);
  const int total_appenders = append_per_worker * num_workers_;
  const lsn_t last_lsn = compose_lsn(EPOCH, esn_t(total_appenders));
  // now start draining the epoch sequencer
  es_->startDraining();
  ASSERT_EQ(EpochSequencer::State::DRAINING, es_->getState());
  ASSERT_FALSE(epoch_drained_.load());
  ASSERT_EQ(last_lsn, es_->getDrainingTarget());
  ASSERT_EQ(last_lsn, max_lsn_accepted_);
  checkTailRecord(es_->getLastKnownGood());

  // now start destroying the epoch sequencer while it is draining
  run_on_worker(processor_.get(),
                /*worker_id=*/0,
                [this]() {
                  es_->startDestruction();
                  EXPECT_EQ(EpochSequencer::State::DYING, es_->getState());
                  return 0;
                });
  // draining target should remain the same
  ASSERT_EQ(last_lsn, es_->getDrainingTarget());
  wait_until([&]() {
    // we destroyed one extra Appender per Worker
    return total_appenders == stats.appender_destroyed;
  });

  ASSERT_EQ(EpochSequencer::State::QUIESCENT, es_->getState());
  // all appenders must have been reaped
  ASSERT_EQ(last_lsn, es_->getLastReaped());
  // however, the epoch is not considered drained and LNG should stay 0
  ASSERT_FALSE(epoch_drained_.load());
  ASSERT_EQ(compose_lsn(EPOCH, ESN_INVALID), es_->getLastKnownGood());
  EXPECT_EQ(nullptr, es_->getTailRecord());
  ASSERT_EQ(0, es_->getNumAppendsInFlight());
  maybeDeleteEpochSequencer();
}

///////////////////// Multi-Threaded Stress Tests /////////////////////

class TestAppenderRequest : public Request {
 public:
  explicit TestAppenderRequest(EpochSequencerTest* test)
      : Request(RequestType::TEST_APPENDER_REQUEST), test_(test) {}

  Request::Execution execute() override {
    MockAppender* appender = test_->createAppender();
    auto status = test_->getSequencer()->runAppender(appender);
    if (status == RunAppenderStatus::ERROR_DELETE) {
      delete appender;
    }
    return Execution::COMPLETE;
  }

 private:
  EpochSequencerTest* const test_;
};

class TestAdvanceEpochRequest : public Request {
 public:
  explicit TestAdvanceEpochRequest(EpochSequencerTest* test)
      : Request(RequestType::TEST_ADVANCE_EPOCH_REQUEST), test_(test) {}

  Request::Execution execute() override {
    test_->advanceEpoch();
    return Execution::COMPLETE;
  }

 private:
  EpochSequencerTest* const test_;
};

// test setup
// - processor running 16 workers
// - 16 application threads posting appends to sequencer
// - application threads occasionally advances epoch, set the
//   current epoch sequencer in draining state
//
// the goal is to verify that EpochSequencer and Appender objects are
// properly created and destroyed in a consistent manner.
TEST_F(EpochSequencerTest, MultiThreadedAppend) {
  num_workers_ = 16;
  window_size_ = folly::Random::rand64(2046) + 2;
  multi_epoch_ = true;
  setUp();
  sequencers_.update(std::make_shared<EpochSequencerContainer>(
      EpochSequencerContainer{es_, nullptr}));
  es_.reset();

  ld_info("Window size in this test run: %d, num_workers: %d.",
          window_size_,
          num_workers_);

  std::atomic<size_t> post_failed{0};
  std::vector<std::thread> threads;
  for (int i = 0; i < num_test_threads_; i++) {
    threads.emplace_back(std::thread([i, &post_failed, this] {
      while (!stop.load()) {
        std::unique_ptr<Request> rq =
            std::make_unique<TestAppenderRequest>(this);
        int rv = processor_->postRequest(rq);
        if (rv != 0) {
          ++post_failed;
        }

        if (i % 2 == 0 && folly::Random::rand64() % 211 == 1) {
          std::unique_ptr<Request> req =
              std::make_unique<TestAdvanceEpochRequest>(this);
          rv = processor_->postImportant(req);
          if (rv != 0) {
            ++post_failed;
          }
        }

        /* sleep override */
        std::this_thread::sleep_for(
            std::chrono::microseconds(1000000 / requests_per_second));
      }
    }));
  }

  /* sleep override */
  std::this_thread::sleep_for(std::chrono::seconds(10));

  stop.store(true);
  for (std::thread& t : threads) {
    t.join();
  }

  wait_until(
      [&]() { return stats.appender_created == stats.appender_destroyed; });

  Stats processor_stats = processor_->stats_->aggregate();

  // print some stats
  ld_info("\nAppender created: %lu, destroyed: %lu.\n"
          "Append accepted: %lu, nobuf: %lu, disabled: %lu, too big: %lu, "
          "no sequencer: %lu.\n"
          "Appender success: %lu, aborted: %lu.\n"
          "Epoch sequencer created: %lu, destroyed: %lu, drained: %lu.\n"
          "Post Request failed: %lu",
          stats.appender_created.load(),
          stats.appender_destroyed.load(),
          stats.appender_success.load(),
          stats.appender_failed_nobuf.load(),
          stats.appender_failed_disabled.load(),
          stats.appender_failed_toobig.load(),
          stats.appender_failed_nosequencer.load(),
          processor_stats.append_success.load(),
          processor_stats.appender_aborted_epoch.load(),
          stats.epoch_created.load(),
          stats.epoch_destroyed.load(),
          stats.epoch_drained.load(),
          post_failed.load());
}

} // anonymous namespace

// TODO: add a test about byte offsets in epoch
