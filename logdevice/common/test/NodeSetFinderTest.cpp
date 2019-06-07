/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/NodeSetFinder.h"

#include <functional>

#include <folly/Memory.h>
#include <gtest/gtest.h>

#include "logdevice/common/configuration/LocalLogsConfig.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/test/MockBackoffTimer.h"
#include "logdevice/common/test/MockTimer.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/include/types.h"

using namespace facebook::logdevice;
using S = NodeLocationScope;

#define N0 ShardID(0, 0)
#define N1 ShardID(1, 0)
#define N2 ShardID(2, 0)
#define N3 ShardID(3, 0)
#define N4 ShardID(4, 0)
#define N5 ShardID(5, 0)

namespace {

/* implicit */
class NodeSetFinderTest : public ::testing::Test {
 public:
  dbg::Level log_level_ = dbg::Level::DEBUG;
  Alarm alarm_{DEFAULT_TEST_TIMEOUT};

  static constexpr logid_t LOG_ID{23333};

  int nnodes = 6;
  std::chrono::milliseconds timeout_{9000};
  NodeSetFinder::Source source_ = NodeSetFinder::Source::BOTH;

  bool called_ = false;
  Status status_ = E::UNKNOWN;

  std::shared_ptr<Configuration> config_;

  int sequencer_attempts_ = 0;
  std::chrono::milliseconds seq_timeout_{0};
  int metadata_log_attempts_ = 0;

  // NodeSetFinder object used in the test
  std::unique_ptr<NodeSetFinder> finder_;

  void assertNotCalled() {
    ASSERT_FALSE(called_);
    ASSERT_NE(NodeSetFinder::State::FINISHED, finder_->getState());
  }

  void assertCalled(Status expected_status) {
    ASSERT_TRUE(called_);
    ASSERT_EQ(expected_status, status_);
    ASSERT_EQ(NodeSetFinder::State::FINISHED, finder_->getState());
  }

  explicit NodeSetFinderTest() {}
  ~NodeSetFinderTest() override {}

  void setUp();
};

class MockNodeSetFinder : public NodeSetFinder {
 public:
  MockNodeSetFinder(NodeSetFinderTest* test)
      : NodeSetFinder(test_->LOG_ID,
                      test->timeout_,
                      [test](Status status) {
                        test->called_ = true;
                        test->status_ = status;
                      },
                      test->source_),
        test_(test) {}

  std::unique_ptr<Timer>
  createJobTimer(std::function<void()> callback) override {
    auto timer = std::make_unique<MockTimer>();
    timer->setCallback(std::move(callback));
    return timer;
  }

  std::unique_ptr<BackoffTimer>
  createMetaDataLogRetryTimer(std::function<void()> callback) override {
    auto timer = std::make_unique<MockBackoffTimer>();
    timer->setCallback(std::move(callback));
    return timer;
  }

  void readFromMetaDataLog() override {
    ASSERT_EQ(NodeSetFinder::State::READ_METADATALOG, getState());
    ++test_->metadata_log_attempts_;
  }

  void stopReadingMetaDataLog() override {}

  void readFromSequencer(std::chrono::milliseconds timeout) override {
    ASSERT_EQ(NodeSetFinder::State::READ_FROM_SEQUENCER, getState());
    ++test_->sequencer_attempts_;
    test_->seq_timeout_ = timeout;
  }

 private:
  NodeSetFinderTest* const test_;
};

constexpr logid_t NodeSetFinderTest::LOG_ID;

void NodeSetFinderTest::setUp() {
  Configuration::NodesConfig nodes_config = createSimpleNodesConfig(nnodes);
  // metadata stored on all nodes with max replication factor 3
  Configuration::MetaDataLogsConfig meta_config =
      createMetaDataLogsConfig(nodes_config, nodes_config.getNodes().size(), 3);

  logsconfig::LogAttributes log_attrs;
  log_attrs.set_replicationFactor(3);
  auto logs_config = std::make_shared<configuration::LocalLogsConfig>();
  logs_config->insert(boost::icl::right_open_interval<logid_t::raw_type>(1, 2),
                      "mylog",
                      log_attrs);

  config_ = std::make_shared<Configuration>(
      ServerConfig::fromDataTest(
          __FILE__, std::move(nodes_config), std::move(meta_config)),
      std::move(logs_config));

  finder_ = std::make_unique<MockNodeSetFinder>(this);
}

MetaDataLogReader::Result
createMetaDataLogReaderResult(epoch_t req,
                              epoch_t until,
                              int replicationFactor,
                              StorageSet storage_set) {
  MetaDataLogReader::Result r;
  r.log_id = logid_t(1);
  r.epoch_req = req, r.epoch_until = until,
  r.source = until == EPOCH_MAX ? MetaDataLogReader::RecordSource::LAST
                                : MetaDataLogReader::RecordSource::NOT_LAST;

  r.metadata = std::make_unique<EpochMetaData>(
      std::move(storage_set),
      ReplicationProperty({{NodeLocationScope::NODE, replicationFactor}}),
      until,
      req);
  return r;
}

std::shared_ptr<const EpochMetaDataMap>
createSampleMetaDataMap(epoch_t effective_until) {
  EpochMetaDataMap::Map map;
  map[EPOCH_MIN] =
      EpochMetaData(StorageSet({N1, N2, N3, N4}),
                    ReplicationProperty({{NodeLocationScope::NODE, 3}}));
  return EpochMetaDataMap::create(
      std::make_shared<const EpochMetaDataMap::Map>(std::move(map)),
      effective_until);
}

TEST_F(NodeSetFinderTest, ReadFromSequencer) {
  source_ = NodeSetFinder::Source::SEQUENCER;
  setUp();

  assertNotCalled();
  finder_->start();
  assertNotCalled();

  ASSERT_EQ(1, sequencer_attempts_);
  ASSERT_EQ(0, metadata_log_attempts_);
  ASSERT_EQ(timeout_, seq_timeout_);
  finder_->onMetaDataFromSequencer(
      E::OK, NodeID(1, 1), createSampleMetaDataMap(epoch_t(9)));
  assertCalled(E::OK);
  auto result = finder_->getResult();
  ASSERT_EQ(*result, *createSampleMetaDataMap(epoch_t(9)));
}

TEST_F(NodeSetFinderTest, ReadFromSequencerTimedout) {
  source_ = NodeSetFinder::Source::SEQUENCER;
  setUp();

  assertNotCalled();
  finder_->start();
  assertNotCalled();

  ASSERT_EQ(1, sequencer_attempts_);
  ASSERT_EQ(0, metadata_log_attempts_);
  finder_->onMetaDataFromSequencer(E::TIMEDOUT, NodeID(1, 1), nullptr);
  assertCalled(E::TIMEDOUT);
  ASSERT_EQ(nullptr, finder_->getResult());
}

TEST_F(NodeSetFinderTest, ReadMetadataLog) {
  source_ = NodeSetFinder::Source::METADATA_LOG;
  setUp();

  assertNotCalled();
  finder_->start();
  assertNotCalled();
  ASSERT_EQ(0, sequencer_attempts_);
  ASSERT_EQ(1, metadata_log_attempts_);

  // job timer should started for the job with the nodeset finder timeout
  ASSERT_NE(nullptr, finder_->getMetaDataLogReadTimer());
  ASSERT_TRUE(finder_->getMetaDataLogReadTimer()->isActive());
  ASSERT_EQ(
      timeout_,
      ((MockTimer*)finder_->getMetaDataLogReadTimer())->getCurrentDelay());

  auto r1 =
      createMetaDataLogReaderResult(epoch_t(1), epoch_t(42), 2, {N1, N2, N3});
  auto r2 = createMetaDataLogReaderResult(epoch_t(43), EPOCH_MAX, 1, {N4, N5});
  finder_->onMetaDataLogRecord(E::OK, std::move(r1));
  finder_->onMetaDataLogRecord(E::OK, std::move(r2));

  // reading should be stopped
  ASSERT_EQ(nullptr, finder_->getMetaDataLogReadTimer());
  assertCalled(E::OK);

  auto& nodesets = finder_->getAllEpochsMetaData();
  ASSERT_EQ(nodesets.size(), 2);
  auto it = nodesets.begin();
  ASSERT_EQ(it->first, epoch_t(1));
  ASSERT_EQ(it->second.shards.size(), 3);
  ++it;
  ASSERT_EQ(it->first, epoch_t(43));
  ASSERT_EQ(it->second.shards.size(), 2);

  ReplicationProperty minRep = finder_->getNarrowestReplication();
  ASSERT_EQ(ReplicationProperty({{S::NODE, 1}}).toString(), minRep.toString());

  auto result = finder_->getResult();
  ASSERT_NE(nullptr, result);

  // extend the effective until of result to EPOCH_MAX
  result = result->withNewEffectiveUntil(EPOCH_MAX);

  minRep = *result->getNarrowestReplication(epoch_t(50), EPOCH_MAX);
  ASSERT_EQ(ReplicationProperty({{S::NODE, 1}}).toString(), minRep.toString());
  minRep = *result->getNarrowestReplication(EPOCH_MIN, epoch_t(50));
  ASSERT_EQ(ReplicationProperty({{S::NODE, 1}}).toString(), minRep.toString());
  minRep = *result->getNarrowestReplication(EPOCH_MIN, epoch_t(43));
  ASSERT_EQ(ReplicationProperty({{S::NODE, 1}}).toString(), minRep.toString());
  minRep = *result->getNarrowestReplication(EPOCH_MIN, epoch_t(42));
  ASSERT_EQ(ReplicationProperty({{S::NODE, 2}}).toString(), minRep.toString());
  minRep = *result->getNarrowestReplication(EPOCH_MIN, epoch_t(30));
  ASSERT_EQ(ReplicationProperty({{S::NODE, 2}}).toString(), minRep.toString());
  minRep = *result->getNarrowestReplication(EPOCH_MIN, EPOCH_MIN);
  ASSERT_EQ(ReplicationProperty({{S::NODE, 2}}).toString(), minRep.toString());

  auto storage_set = finder_->getUnionStorageSet(
      *config_->serverConfig()->getNodesConfigurationFromServerConfigSource());
  ASSERT_EQ(storage_set.size(), 5);

  storage_set = *result->getUnionStorageSet(
      *config_->serverConfig()->getNodesConfigurationFromServerConfigSource(),
      epoch_t(50),
      EPOCH_MAX);
  ASSERT_EQ(storage_set.size(), 2);

  storage_set = *result->getUnionStorageSet(
      *config_->serverConfig()->getNodesConfigurationFromServerConfigSource(),
      EPOCH_MAX,
      EPOCH_MAX);
  ASSERT_EQ(storage_set.size(), 2);

  storage_set = *result->getUnionStorageSet(
      *config_->serverConfig()->getNodesConfigurationFromServerConfigSource(),
      EPOCH_MIN,
      epoch_t(50));
  ASSERT_EQ(storage_set.size(), 5);

  storage_set = *result->getUnionStorageSet(
      *config_->serverConfig()->getNodesConfigurationFromServerConfigSource(),
      EPOCH_MIN,
      epoch_t(40));
  ASSERT_EQ(storage_set.size(), 3);

  storage_set = *result->getUnionStorageSet(
      *config_->serverConfig()->getNodesConfigurationFromServerConfigSource(),
      EPOCH_MIN,
      EPOCH_MIN);
  ASSERT_EQ(storage_set.size(), 3);
}

TEST_F(NodeSetFinderTest, ReadMetadataLogTimedout) {
  source_ = NodeSetFinder::Source::METADATA_LOG;
  setUp();

  assertNotCalled();
  finder_->start();
  assertNotCalled();
  ASSERT_EQ(0, sequencer_attempts_);
  ASSERT_EQ(1, metadata_log_attempts_);

  ASSERT_NE(nullptr, finder_->getMetaDataLogReadTimer());
  ASSERT_TRUE(finder_->getMetaDataLogReadTimer()->isActive());

  auto r1 =
      createMetaDataLogReaderResult(epoch_t(1), epoch_t(42), 2, {N1, N2, N3});
  finder_->onMetaDataLogRecord(E::OK, std::move(r1));

  // trigger the job timeout
  ((MockTimer*)finder_->getMetaDataLogReadTimer())->trigger();

  // reading should be stopped
  ASSERT_EQ(nullptr, finder_->getMetaDataLogReadTimer());
  assertCalled(E::TIMEDOUT);
}

TEST_F(NodeSetFinderTest, ReadMetadataLogRetryNOTFOUND) {
  source_ = NodeSetFinder::Source::METADATA_LOG;
  setUp();

  assertNotCalled();
  finder_->start();
  assertNotCalled();
  ASSERT_EQ(0, sequencer_attempts_);
  ASSERT_EQ(1, metadata_log_attempts_);

  // job timer should started for the job with the nodeset finder timeout
  ASSERT_NE(nullptr, finder_->getMetaDataLogReadTimer());
  ASSERT_TRUE(finder_->getMetaDataLogReadTimer()->isActive());

  finder_->onMetaDataLogRecord(E::NOTFOUND, MetaDataLogReader::Result());
  // retry timer should started
  auto* retry_timer = finder_->getMetaDataLogRetryTimer();
  ASSERT_NE(nullptr, retry_timer);
  ASSERT_TRUE(retry_timer->isActive());

  // trigger retry timer
  ((MockBackoffTimer*)retry_timer)->trigger();
  // should re-attempt metadata log reading
  ASSERT_EQ(0, sequencer_attempts_);
  ASSERT_EQ(2, metadata_log_attempts_);

  auto r1 =
      createMetaDataLogReaderResult(epoch_t(1), epoch_t(42), 2, {N1, N2, N3});
  auto r2 = createMetaDataLogReaderResult(epoch_t(43), EPOCH_MAX, 1, {N4, N5});
  finder_->onMetaDataLogRecord(E::OK, std::move(r1));
  finder_->onMetaDataLogRecord(E::OK, std::move(r2));

  // reading should be stopped with all timer destroyed
  ASSERT_EQ(nullptr, finder_->getMetaDataLogReadTimer());
  ASSERT_EQ(nullptr, finder_->getMetaDataLogRetryTimer());
  assertCalled(E::OK);

  auto& nodesets = finder_->getAllEpochsMetaData();
  ASSERT_EQ(nodesets.size(), 2);
}

TEST_F(NodeSetFinderTest, BadMetadataLogRecord) {
  source_ = NodeSetFinder::Source::METADATA_LOG;
  setUp();

  assertNotCalled();
  finder_->start();
  assertNotCalled();

  auto r1 = createMetaDataLogReaderResult(epoch_t(1), epoch_t(42), 1, {N1, N3});
  finder_->onMetaDataLogRecord(E::OK, std::move(r1));

  MetaDataLogReader::Result r2;
  r2.log_id = logid_t(1);
  r2.epoch_req = epoch_t(43), r2.epoch_until = epoch_t(44),
  r2.source = MetaDataLogReader::RecordSource::LAST;
  finder_->onMetaDataLogRecord(E::BADMSG, std::move(r2));

  // Since there was an error reading the metadata, complete the request.
  assertCalled(E::FAILED);
}

TEST_F(NodeSetFinderTest, ReadFromBothGotResultFromSequencer) {
  source_ = NodeSetFinder::Source::BOTH;
  // 12s timeout
  timeout_ = std::chrono::milliseconds(12000);
  setUp();

  finder_->start();
  assertNotCalled();

  ASSERT_EQ(1, sequencer_attempts_);
  ASSERT_EQ(0, metadata_log_attempts_);

  // 4s time out for the sequencer stage
  ASSERT_EQ(std::chrono::milliseconds(4000), seq_timeout_);
  finder_->onMetaDataFromSequencer(
      E::OK, NodeID(1, 1), createSampleMetaDataMap(epoch_t(9)));
  assertCalled(E::OK);
  auto result = finder_->getResult();
  ASSERT_EQ(*result, *createSampleMetaDataMap(epoch_t(9)));
}

TEST_F(NodeSetFinderTest, ReadFromBothSequencerFailed) {
  source_ = NodeSetFinder::Source::BOTH;
  timeout_ = std::chrono::milliseconds(12000);
  setUp();

  finder_->start();
  assertNotCalled();

  ASSERT_EQ(1, sequencer_attempts_);
  ASSERT_EQ(0, metadata_log_attempts_);

  ASSERT_EQ(std::chrono::milliseconds(4000), seq_timeout_);
  finder_->onMetaDataFromSequencer(E::NOSEQUENCER, NodeID(1, 1), nullptr);
  // should continue reading metadata log
  assertNotCalled();

  ASSERT_EQ(1, sequencer_attempts_);
  ASSERT_EQ(1, metadata_log_attempts_);

  ASSERT_NE(nullptr, finder_->getMetaDataLogReadTimer());
  ASSERT_TRUE(finder_->getMetaDataLogReadTimer()->isActive());
  // 8s timeout for the metadaata stage
  ASSERT_EQ(
      std::chrono::milliseconds(8000),
      ((MockTimer*)finder_->getMetaDataLogReadTimer())->getCurrentDelay());

  auto r1 =
      createMetaDataLogReaderResult(epoch_t(1), epoch_t(42), 2, {N1, N2, N3});
  auto r2 = createMetaDataLogReaderResult(epoch_t(43), EPOCH_MAX, 1, {N4, N5});
  finder_->onMetaDataLogRecord(E::OK, std::move(r1));
  finder_->onMetaDataLogRecord(E::OK, std::move(r2));

  ASSERT_EQ(nullptr, finder_->getMetaDataLogReadTimer());
  assertCalled(E::OK);
  auto& nodesets = finder_->getAllEpochsMetaData();
  ASSERT_EQ(nodesets.size(), 2);
}

} // namespace
