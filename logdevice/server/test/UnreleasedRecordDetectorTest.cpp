/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/UnreleasedRecordDetector.h"

#include <chrono>
#include <csignal>
#include <functional>
#include <future>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "logdevice/common/AllSequencers.h"
#include "logdevice/common/AppendRequest.h"
#include "logdevice/common/EpochMetaDataUpdater.h"
#include "logdevice/common/FileEpochStore.h"
#include "logdevice/common/MetaDataLogWriter.h"
#include "logdevice/common/NodeSetSelector.h"
#include "logdevice/common/NodeSetSelectorFactory.h"
#include "logdevice/common/NoopTraceLogger.h"
#include "logdevice/common/PermissionChecker.h"
#include "logdevice/common/PrincipalParser.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/ReaderImpl.h"
#include "logdevice/common/ResourceBudget.h"
#include "logdevice/common/Sequencer.h"
#include "logdevice/common/SequencerLocator.h"
#include "logdevice/common/StaticSequencerLocator.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/NodesConfigParser.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/settings/SettingsUpdater.h"
#include "logdevice/common/settings/util.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/include/Err.h"
#include "logdevice/include/Record.h"
#include "logdevice/server/ConnectionListener.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/ServerSettings.h"
#include "logdevice/server/locallogstore/LocalLogStore.h"
#include "logdevice/server/locallogstore/test/TemporaryLogStore.h"
#include "logdevice/server/read_path/LogStorageStateMap.h"
#include "logdevice/server/shutdown.h"
#include "logdevice/server/storage_tasks/ShardedStorageThreadPool.h"
#include "logdevice/test/utils/MetaDataProvisioner.h"

namespace facebook { namespace logdevice {

using Params = ServerSettings::StoragePoolParams;

namespace {

static constexpr logid_t LOG_ID{1};
static constexpr int CLIENT_READ_BUFFER_SIZE = 512;
static constexpr int STORAGE_TASK_QUEUE_SIZE = 128;
static constexpr int NUM_WORKERS = 8;
static constexpr int NUM_INFLIGHT_STORAGE_TASKS = 16;

#define CONFIG_PATH                                                         \
  verifyFileExists(                                                         \
      "logdevice/server/test/configs/unreleased_record_detector_test.conf") \
      .c_str()
static constexpr const char* TEMP_DIR_PREFIX = "UnreleasedRecordDetectorTest";

class TemporaryRocksDBStoreExt final : public TemporaryRocksDBStore {
 public:
  void setHighestInsertedLSN(lsn_t highest_lsn) {
    highest_inserted_lsn_ = highest_lsn;
  }
  int getHighestInsertedLSN(logid_t /* unused */, lsn_t* highest_lsn) override {
    *highest_lsn = highest_inserted_lsn_;
    return 0;
  }

 private:
  lsn_t highest_inserted_lsn_{LSN_INVALID};
};

class TemporaryShardedStore final : public ShardedLocalLogStore {
 public:
  int numShards() const override {
    return 1;
  }
  LocalLogStore* getByIndex(int idx) override {
    ld_check(idx == 0);
    return &store_;
  }

 private:
  TemporaryRocksDBStoreExt store_;
};

class TestReader final : public ReaderImpl {
 public:
  explicit TestReader(Processor* processor)
      : ReaderImpl(1,
                   processor,
                   nullptr,
                   nullptr,
                   "",
                   CLIENT_READ_BUFFER_SIZE) {}
};

} // namespace

class UnreleasedRecordDetectorTest : public ::testing::Test {
 protected:
  void SetUp() override;
  void TearDown() override;
  void acceptConnections();
  void failSequencer();
  void setUnreleasedRecordDetectorInterval(std::string interval);
  bool sequencerIsActive();
  template <typename Rep, typename Period>
  bool waitUntilSequencerIsActive(std::chrono::duration<Rep, Period> duration,
                                  lsn_t expect_released);
  void setHighestInsertedLSN(lsn_t lsn);
  std::unique_ptr<folly::test::TemporaryDirectory> temp_dir_;
  std::unique_ptr<UpdateableSettings<Settings>> usettings_;
  std::unique_ptr<UpdateableSettings<ServerSettings>> userver_settings_;
  std::unique_ptr<UpdateableSettings<GossipSettings>> ugossip_settings_;
  std::unique_ptr<UpdateableSettings<AdminServerSettings>>
      uadmin_server_settings_;
  std::shared_ptr<TemporaryShardedStore> sharded_store_;
  std::unique_ptr<ShardedStorageThreadPool> sharded_storage_thread_pool_;
  std::shared_ptr<UpdateableConfig> config_;
  std::shared_ptr<ServerProcessor> processor_;
  ResourceBudget budget_{std::numeric_limits<uint64_t>::max()};
  std::unique_ptr<EventLoop> connection_listener_loop_;
  std::unique_ptr<ConnectionListener> connection_listener_;
  std::shared_ptr<UnreleasedRecordDetector> detector_;
};

void UnreleasedRecordDetectorTest::SetUp() {
  // ignore SIGPIPE (may be raised by libevent on termination)
  static thread_local bool sigpipe_is_ignored = false;
  if (!sigpipe_is_ignored) {
    sigset_t sigpipe_mask;
    sigemptyset(&sigpipe_mask);
    sigaddset(&sigpipe_mask, SIGPIPE);
    sigset_t prev_mask;
    ASSERT_EQ(0, pthread_sigmask(SIG_BLOCK, &sigpipe_mask, &prev_mask));
    sigpipe_is_ignored = true;
  }

  // create a temp directory for the test, removed at exit
  temp_dir_ = createTemporaryDir(TEMP_DIR_PREFIX);
  ASSERT_NE(nullptr, temp_dir_);
  const std::string temp_dir_path(temp_dir_->path().string());

  // create settings with unreleased record detector disabled
  Settings settings(create_default_settings<Settings>());
  settings.server = true;
  settings.enable_sticky_copysets = true;
  settings.num_workers = NUM_WORKERS;
  settings.max_inflight_storage_tasks = NUM_INFLIGHT_STORAGE_TASKS;
  settings.per_worker_storage_task_queue_size = STORAGE_TASK_QUEUE_SIZE;
  settings.unreleased_record_detector_interval = std::chrono::seconds::zero();
  usettings_ =
      std::make_unique<UpdateableSettings<Settings>>(std::move(settings));
  ServerSettings server_settings(create_default_settings<ServerSettings>());
  userver_settings_ = std::make_unique<UpdateableSettings<ServerSettings>>(
      std::move(server_settings));
  GossipSettings gossip_settings(create_default_settings<GossipSettings>());
  // We don't need gossip enabled
  gossip_settings.enabled = false;
  ugossip_settings_ = std::make_unique<UpdateableSettings<GossipSettings>>(
      std::move(gossip_settings));

  AdminServerSettings admin_settings(
      create_default_settings<AdminServerSettings>());
  uadmin_server_settings_ =
      std::make_unique<UpdateableSettings<AdminServerSettings>>(
          std::move(admin_settings));

  // create temporary store and associated storage thread pool
  Params params;
  params[(size_t)StorageTaskThreadType::SLOW].nthreads = 1;
  params[(size_t)StorageTaskThreadType::FAST_TIME_SENSITIVE].nthreads = 1;
  params[(size_t)StorageTaskThreadType::FAST_STALLABLE].nthreads = 1;
  params[(size_t)StorageTaskThreadType::DEFAULT].nthreads = 1;
  sharded_store_ = std::make_shared<TemporaryShardedStore>();
  sharded_storage_thread_pool_ = std::make_unique<ShardedStorageThreadPool>(
      sharded_store_.get(),
      params,
      *userver_settings_,
      *usettings_,
      NUM_WORKERS * NUM_INFLIGHT_STORAGE_TASKS,
      nullptr);
  ld_notify("ShardedStorageThreadPool created.");

  // create config from file
  config_ = std::make_shared<UpdateableConfig>(
      Configuration::fromJsonFile(CONFIG_PATH));
  ASSERT_NE(nullptr, config_);
  ASSERT_NE(nullptr, config_->getServerConfig());
  ld_notify("UpdateableConfig created from file %s.", CONFIG_PATH);

  // override node address with path to unix domain socket in temp directory
  std::string socketPath(temp_dir_path);
  socketPath.append("/socket");
  auto* const node =
      const_cast<configuration::Node*>(config_->getServerConfig()->getNode(0));
  ASSERT_TRUE(configuration::parser::parseHostString(
      socketPath, node->address, "host"));

  // do the same thing for NodesConfiguration
  auto nodes_configuration =
      config_->getNodesConfigurationFromServerConfigSource();
  ASSERT_NE(nullptr, nodes_configuration);
  const auto* serv_disc = nodes_configuration->getNodeServiceDiscovery(0);
  ASSERT_NE(nullptr, serv_disc);
  const_cast<configuration::nodes::NodeServiceDiscovery*>(serv_disc)->address =
      node->address;

  // create processor
  processor_ =
      ServerProcessor::create(/* audit log */ nullptr,
                              sharded_storage_thread_pool_.get(),
                              *userver_settings_,
                              *ugossip_settings_,
                              *uadmin_server_settings_,
                              config_,
                              std::make_shared<NoopTraceLogger>(config_),
                              *usettings_,
                              nullptr,
                              make_test_plugin_registry(),
                              "",
                              "",
                              "logdevice",
                              NodeID(0, 1));
  sharded_storage_thread_pool_->setProcessor(processor_.get());
  processor_->markShardAsNotMissingData(0);
  ld_notify("Processor created and initialized.");

  // create connection listener
  connection_listener_loop_ =
      std::make_unique<EventLoop>(ConnectionListener::listenerTypeNames()
                                      [ConnectionListener::ListenerType::DATA],
                                  ThreadID::Type::UTILITY);
  connection_listener_ = std::make_unique<ConnectionListener>(
      Listener::InterfaceDef(std::move(socketPath), false),
      folly::getKeepAliveToken(connection_listener_loop_.get()),
      std::make_shared<ConnectionListener::SharedState>(),
      ConnectionListener::ListenerType::DATA,
      budget_);
  connection_listener_->setProcessor(processor_.get());
  ld_notify("ConnectionListener created.");

  // initialize and provision the epoch store
  auto epoch_store = std::make_unique<FileEpochStore>(
      temp_dir_path, processor_.get(), config_->updateableNodesConfiguration());
  std::shared_ptr<NodeSetSelector> selector =
      NodeSetSelectorFactory::create(NodeSetSelectorType::SELECT_ALL);
  auto log_store_factory = [this](node_index_t nid) {
    // This cluster has only one node.
    ld_check(nid == 0);
    return sharded_store_;
  };
  auto provisioner =
      std::make_unique<IntegrationTestUtils::MetaDataProvisioner>(
          std::move(epoch_store), config_, log_store_factory);
  int rv = provisioner->provisionEpochMetaData(std::move(selector), true, true);
  ASSERT_EQ(0, rv);

  epoch_store = std::make_unique<FileEpochStore>(
      temp_dir_path, processor_.get(), config_->updateableNodesConfiguration());

  processor_->allSequencers().setEpochStore(std::move(epoch_store));
  ld_notify("FileEpochStore created and initialized in directory %s.",
            temp_dir_path.c_str());

  // activate and wait for sequencer
  rv = wait_until("sequencer becomes active", [this] {
    return processor_->allSequencers().activateSequencerIfNotActive(
               LOG_ID, "test") &&
        err == E::EXISTS;
  });
  ASSERT_EQ(0, rv);
  ld_notify("Sequencer activated.");

  // create unreleased record detector
  detector_ =
      std::make_shared<UnreleasedRecordDetector>(processor_.get(), *usettings_);
  detector_->start();
  ld_notify("Unreleased record detector started (but inactive).");
}

void UnreleasedRecordDetectorTest::TearDown() {
  // destroy connection listener und unreleased record detector
  connection_listener_.reset();
  connection_listener_loop_.reset();
  detector_.reset();
  ld_notify("Connection listener and unreleased record detector destroyed.");

  sharded_storage_thread_pool_->shutdown(/*persist_record_caches*/ false);
  processor_->getLogStorageStateMap().shutdownRecordCaches();

  // gracefully stop the processor and all its worker threads
  const int nworkers = processor_->getAllWorkersCount();
  int nsuccess = post_and_wait(
      processor_.get(), [](Worker* worker) { worker->stopAcceptingWork(); });
  ASSERT_EQ(nworkers, nsuccess);
  nsuccess = post_and_wait(processor_.get(), [](Worker* worker) {
    worker->finishWorkAndCloseSockets();
  });
  ASSERT_EQ(nworkers, nsuccess);
  processor_->waitForWorkers(nworkers);
  processor_->shutdown();
  ld_notify("Gracefully stopped processor and all its worker threads.");
}

void UnreleasedRecordDetectorTest::acceptConnections() {
  connection_listener_->startAcceptingConnections();
  ld_notify("ConnectionListener accepting connections and started.");
}

void UnreleasedRecordDetectorTest::failSequencer() {
  auto seq = processor_->allSequencers().findSequencer(LOG_ID);
  ASSERT_NE(nullptr, seq);
  // Note: this is a hack that forcefully set the sequencer's state to
  // UNAVAILABLE so that the next GET_SEQ_STATE message will guaranteed to
  // reactivate the sequencer
  seq->setState(Sequencer::State::UNAVAILABLE);
  ASSERT_FALSE(sequencerIsActive());
}

void UnreleasedRecordDetectorTest::setUnreleasedRecordDetectorInterval(
    std::string interval) {
  SettingsUpdater updater;
  updater.registerSettings(*usettings_);
  updater.registerSettings(*userver_settings_);
  updater.setFromAdminCmd(
      "unreleased-record-detector-interval", std::move(interval));
}

bool UnreleasedRecordDetectorTest::sequencerIsActive() {
  return processor_->allSequencers().findSequencer(LOG_ID)->getState() ==
      Sequencer::State::ACTIVE;
}

template <typename Rep, typename Period>
bool UnreleasedRecordDetectorTest::waitUntilSequencerIsActive(
    std::chrono::duration<Rep, Period> duration,
    lsn_t expect_released) {
  return wait_until("sequencer becomes active",
                    [this, expect_released] {
                      auto seq =
                          processor_->allSequencers().findSequencer(LOG_ID);
                      if (!seq) {
                        return false;
                      }
                      return seq->getState() == Sequencer::State::ACTIVE &&
                          seq->getLastReleased() >= expect_released;
                    },
                    std::chrono::steady_clock::now() + duration);
}

void UnreleasedRecordDetectorTest::setHighestInsertedLSN(lsn_t lsn) {
  static_cast<TemporaryRocksDBStoreExt*>(sharded_store_->getByIndex(0))
      ->setHighestInsertedLSN(lsn);
}

/**
 * This basic test just makes sure that UnreleasedRecordDetector behaves well
 * when stopped or destructed immediately after construction.
 */
TEST_F(UnreleasedRecordDetectorTest, StartStop) {
  const auto new_detector = [this] {
    return std::make_shared<UnreleasedRecordDetector>(
        processor_.get(), *usettings_);
  };

  // construct and immediately destruct
  detector_ = new_detector();
  detector_ = new_detector();

  // rapid start and stop
  detector_->start();
  detector_->stop();
  detector_->start();
  detector_->stop();

  // start and immediately destruct
  detector_->start();
  detector_ = new_detector();
  detector_->start();
  detector_.reset();
}

/**
 * This test fails the sequencer and artificially bumps up the highest inserted
 * LSN, triggering the UnreleasedRecordDetector to work its magic.
 */
TEST_F(UnreleasedRecordDetectorTest, TransientSequencerFailure) {
  // start accepting connections
  acceptConnections();

  // append record to log, expect success
  lsn_t lsn = LSN_INVALID;
  int rv = wait_until("can append to the log", [&]() -> bool {
    std::promise<std::pair<Status, lsn_t>> promise;
    auto future(promise.get_future());
    const auto append_callback = [&promise](Status st, const DataRecord& r) {
      promise.set_value(std::make_pair(st, r.attrs.lsn));
    };
    auto append_req(std::make_unique<AppendRequest>(nullptr,
                                                    LOG_ID,
                                                    AppendAttributes(),
                                                    std::string("one"),
                                                    std::chrono::seconds(30),
                                                    append_callback));
    append_req->bypassWriteTokenCheck();
    std::unique_ptr<Request> req(std::move(append_req));
    EXPECT_EQ(0, processor_->blockingRequest(req));
    EXPECT_EQ(
        std::future_status::ready, future.wait_for(std::chrono::seconds(60)));
    const auto result(future.get());
    if (result.first == Status::OK) {
      lsn = result.second;
      return true;
    } else {
      // Listener has not started yet.
      EXPECT_EQ(Status::CONNFAILED, result.first);
      return false;
    }
  });
  ASSERT_EQ(0, rv);

  ld_notify(
      "Successfully appended record with lsn %s.", lsn_to_string(lsn).c_str());

  // create a reader and start reading from the beginning
  TestReader reader(processor_.get());
  reader.forceNoSingleCopyDelivery();
  ASSERT_EQ(0, reader.setTimeout(std::chrono::seconds(30)));
  ASSERT_EQ(0, reader.startReading(LOG_ID, LSN_OLDEST, lsn));
  ld_notify("Reader created.");

  while (true) {
    std::vector<std::unique_ptr<DataRecord>> data_out;
    GapRecord gap_out;
    rv = reader.read(1, &data_out, &gap_out);
    if (rv == -1) {
      ASSERT_EQ(E::GAP, err);
      ASSERT_LT(gap_out.hi, lsn);
    } else if (rv >= 1) {
      ASSERT_EQ(1, rv); // we only wrote one record
      ASSERT_EQ(lsn, data_out[0].get()->attrs.lsn);
      break;
    }
  }
  // Reader should have been destroyed already
  ASSERT_EQ(-1, reader.stopReading(LOG_ID));

  for (int i = 0; i < 3; ++i) {
    // fail sequencer with transient error
    failSequencer();
    ld_notify("Sequencer deactivated.");

    // artificially bump up the highest inserted LSN in the local log, this
    // should not have any immediate effect
    setHighestInsertedLSN(lsn + i + 1);
    ld_notify("Articifially bumped up highest inserted LSN.");

    // unreleased record detector should eventually notice that the interval
    // changed and that the highest inserted LSN is greater than the last
    // released LSN, causing it to send a GET_SEQ_STATE message which
    // reactivates the sequencer
    ASSERT_FALSE(
        waitUntilSequencerIsActive(std::chrono::seconds(60), lsn + i + 1));
    ld_notify("Sequencer reactivated by unreleased record detector.");
  }
}

/**
 * This test runs the detector thread at a very high frequency, exercising the
 * detectors request throttling (no redundant requests while there are
 * outstanding requests) and generally increases coverage of the concurrency
 * control logic.
 */
TEST_F(UnreleasedRecordDetectorTest, HighFrequencyDetector) {
  // start accepting connections
  acceptConnections();

  // set a very short record detector interval, thereby enabling the unreleased
  // record detector and running it at a high frequency
  setUnreleasedRecordDetectorInterval("1ms");
  ld_notify("Activated unreleased record detector.");

  // fail sequencer with transient error
  failSequencer();
  ld_notify("Sequencer deactivated.");

  lsn_t lsn = compose_lsn(epoch_t(1), esn_t(1));

  // artificially bump up the highest inserted LSN in the local log
  setHighestInsertedLSN(lsn);
  ld_notify("Articifially bumped up highest inserted LSN.");

  // unreleased record detector should quickly reactivate the sequencer
  ASSERT_FALSE(waitUntilSequencerIsActive(std::chrono::seconds(60), lsn));
  ASSERT_TRUE(sequencerIsActive());
  ld_notify("Sequencer reactivated by unreleased record detector.");
}

}} // namespace facebook::logdevice
