/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/FindKeyRequest.h"

#include <functional>

#include <folly/Memory.h>
#include <gtest/gtest.h>

#include "logdevice/common/configuration/LocalLogsConfig.h"
#include "logdevice/common/protocol/FINDKEY_Message.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/test/MockBackoffTimer.h"
#include "logdevice/common/test/MockNodeSetAccessor.h"
#include "logdevice/common/test/MockTimer.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/include/types.h"

using namespace facebook::logdevice;

constexpr std::chrono::milliseconds
/* implicit */
operator"" _ms(unsigned long long val) {
  return std::chrono::milliseconds(val);
}

#define N0 ShardID(0, 1)
#define N1 ShardID(1, 1)
#define N2 ShardID(2, 1)
#define N3 ShardID(3, 1)
#define N4 ShardID(4, 1)
#define N5 ShardID(5, 1)
#define N6 ShardID(6, 1)
#define N7 ShardID(7, 1)
#define N8 ShardID(8, 1)
#define N9 ShardID(9, 1)

namespace {

class FindTimeCallback {
 public:
  void operator()(Status status, lsn_t result) {
    called_ = true;
    result_ = result;
    status_ = status;
  }

  void assertNotCalled() {
    ASSERT_FALSE(called_);
  }

  void assertCalled(Status expected_status, lsn_t expected_result) {
    ASSERT_TRUE(called_);
    ASSERT_EQ(expected_result, result_);
    ASSERT_EQ(expected_status, status_);
  }

 private:
  bool called_ = false;
  lsn_t result_;
  Status status_;
};

class FindKeyCallback {
 public:
  void operator()(FindKeyResult result) {
    called_ = true;
    lo_ = result.lo;
    hi_ = result.hi;
    status_ = result.status;
  }

  void assertNotCalled() {
    ASSERT_FALSE(called_);
  }

  void assertCalled(FindKeyResult expected_result) {
    ASSERT_TRUE(called_);
    ASSERT_EQ(expected_result.lo, lo_);
    ASSERT_EQ(expected_result.hi, hi_);
    ASSERT_EQ(expected_result.status, status_);
  }

 private:
  bool called_ = false;
  lsn_t lo_;
  lsn_t hi_;
  Status status_;
};

MetaDataLogReader::Result createMetaDataLogReaderResult(epoch_t req,
                                                        epoch_t until,
                                                        StorageSet shards) {
  MetaDataLogReader::Result r;
  r.log_id = logid_t(1);
  r.epoch_req = req, r.epoch_until = until,
  r.source = until == EPOCH_MAX ? MetaDataLogReader::RecordSource::LAST
                                : MetaDataLogReader::RecordSource::NOT_LAST;

  r.metadata = std::make_unique<EpochMetaData>(
      std::move(shards), ReplicationProperty({{NodeLocationScope::NODE, 3}}));
  r.metadata->h.epoch = until;
  r.metadata->h.effective_since = req;
  return r;
}

class MockNodeSetFinder : public NodeSetFinder {
 public:
  MockNodeSetFinder(logid_t log_id,
                    std::chrono::milliseconds timeout,
                    std::function<void(Status)> callback)
      : // only read from the metadata log
        NodeSetFinder(log_id, timeout, callback, Source::METADATA_LOG) {}

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

  void readFromMetaDataLog() override {}

  void stopReadingMetaDataLog() override {}

  void readFromSequencer(std::chrono::milliseconds /*timeout*/) override {
    // do not read from sequencer in this test
    ld_check(false);
  }
};

class MockFindKeyRequest : public FindKeyRequest {
 public:
  typedef std::pair<FINDKEY_Header, ShardID> sent_t;

  MockFindKeyRequest(
      int nnodes,
      std::chrono::milliseconds timestamp,
      FindTimeCallback& callback,
      const Settings& settings = create_default_settings<Settings>())
      : FindKeyRequest(logid_t(1),
                       timestamp,
                       folly::none,
                       std::chrono::seconds(1),
                       std::ref(callback),
                       find_key_callback_t(),
                       FindKeyAccuracy::STRICT),
        settings_(settings) {
    Configuration::NodesConfig nodes_config = createSimpleNodesConfig(nnodes);
    // metadata stored on all nodes with max replication factor 3
    Configuration::MetaDataLogsConfig meta_config = createMetaDataLogsConfig(
        nodes_config, nodes_config.getNodes().size(), 3);

    logsconfig::LogAttributes log_attrs;
    log_attrs.set_replicationFactor(3);
    auto logs_config = std::make_shared<configuration::LocalLogsConfig>();
    logs_config->insert(
        boost::icl::right_open_interval<logid_t::raw_type>(1, 2),
        "mylog",
        log_attrs);
    config_ = ServerConfig::fromDataTest(
        __FILE__, std::move(nodes_config), std::move(meta_config));

    logs_config_ = std::move(logs_config);

    // Start immediately, every test needs this
    initNodeSetFinder();
  }

  MockFindKeyRequest(
      int nnodes,
      FindKeyCallback& callback,
      const Settings& settings = create_default_settings<Settings>())
      : FindKeyRequest(logid_t(1),
                       std::chrono::milliseconds(0),
                       folly::Optional<std::string>("12345678"),
                       std::chrono::seconds(1),
                       find_time_callback_t(),
                       std::ref(callback),
                       FindKeyAccuracy::STRICT),
        settings_(settings) {
    Configuration::NodesConfig nodes_config = createSimpleNodesConfig(nnodes);
    // metadata stored on all nodes with max replication factor 3
    Configuration::MetaDataLogsConfig meta_config = createMetaDataLogsConfig(
        nodes_config, nodes_config.getNodes().size(), 3);

    logsconfig::LogAttributes log_attrs;
    log_attrs.set_replicationFactor(3);
    auto logs_config = std::make_shared<configuration::LocalLogsConfig>();
    logs_config->insert(
        boost::icl::right_open_interval<logid_t::raw_type>(1, 2),
        "mylog",
        log_attrs);
    config_ = ServerConfig::fromDataTest(
        __FILE__, std::move(nodes_config), std::move(meta_config));

    logs_config_ = std::move(logs_config);

    // Start immediately, every test needs this
    initNodeSetFinder();
  }

  std::vector<sent_t>& getSent() {
    return sent_;
  }

  void onMetadata(Status st, MetaDataLogReader::Result result) {
    nodeset_finder_->onMetaDataLogRecord(st, std::move(result));
  }

  void onClientTimeout() override {
    FindKeyRequest::onClientTimeout();
  }

 protected: // mock stuff that communicates externally
  void deleteThis() override {}

  int sendOneMessage(const FINDKEY_Header& header, ShardID to) override {
    sent_.push_back(std::make_pair(header, to));
    return 0;
  }

  std::unique_ptr<StorageSetAccessor> makeStorageSetAccessor(
      std::shared_ptr<const configuration::nodes::NodesConfiguration>
          nodes_configuration,
      StorageSet shards,
      ReplicationProperty minRep,
      StorageSetAccessor::ShardAccessFunc shard_access,
      StorageSetAccessor::CompletionFunc completion) override {
    return std::make_unique<MockStorageSetAccessor>(
        logid_t(1),
        shards,
        nodes_configuration,
        minRep,
        shard_access,
        completion,
        StorageSetAccessor::Property::FMAJORITY,
        std::chrono::seconds(1));
  }

  std::unique_ptr<NodeSetFinder>
  makeNodeSetFinder(logid_t log_id,
                    std::chrono::milliseconds timeout,
                    std::function<void(Status status)> cb) const override {
    return std::make_unique<MockNodeSetFinder>(log_id, timeout, cb);
  }

  std::shared_ptr<const configuration::nodes::NodesConfiguration>
  getNodesConfiguration() const override {
    return config_->getNodesConfigurationFromServerConfigSource();
  }

  void onShardStatusChanged() override {}

 private:
  std::shared_ptr<ServerConfig> config_;
  std::shared_ptr<LogsConfig> logs_config_;
  std::vector<sent_t> sent_;
  Settings settings_;
};

lsn_t lsn(int epoch, int esn) {
  return compose_lsn(epoch_t(epoch), esn_t(esn));
}

} // namespace

#define ASSERT_SENT(rq, lo, hi, ...)                      \
  {                                                       \
    std::sort(rq.getSent().begin(),                       \
              rq.getSent().end(),                         \
              [](const MockFindKeyRequest::sent_t& lhs,   \
                 const MockFindKeyRequest::sent_t& rhs) { \
                return lhs.second < rhs.second;           \
              });                                         \
    std::vector<ShardID> expected{__VA_ARGS__};           \
    ASSERT_EQ(expected.size(), rq.getSent().size());      \
    for (int i = 0; i < expected.size(); ++i) {           \
      ASSERT_EQ(expected[i], rq.getSent()[i].second);     \
      ASSERT_EQ(lo, rq.getSent()[i].first.hint_lo);       \
      ASSERT_EQ(hi, rq.getSent()[i].first.hint_hi);       \
    }                                                     \
    rq.getSent().clear();                                 \
  }

TEST(FindKeyRequestTest, TimeSimple) {
  FindTimeCallback cb;
  MockFindKeyRequest req(10, 100_ms, cb);
  auto r1 = createMetaDataLogReaderResult(
      epoch_t(1), EPOCH_MAX, {N0, N1, N2, N3, N4, N5, N6, N7, N8, N9});
  req.onMetadata(E::OK, std::move(r1));
  req.onReply(N0, E::OK, lsn_t(1), lsn_t(4));
  req.onReply(N1, E::OK, lsn_t(2), lsn_t(5));
  cb.assertNotCalled();
  req.onReply(N2, E::OK, lsn_t(3), lsn_t(6));
  cb.assertCalled(E::OK, lsn_t(4));
}

TEST(FindKeyRequestTest, TimeInconclusive) {
  FindTimeCallback cb;
  MockFindKeyRequest req(5, 100_ms, cb);
  auto r1 = createMetaDataLogReaderResult(
      epoch_t(1), EPOCH_MAX, {N0, N1, N2, N3, N4});
  req.onMetadata(E::OK, std::move(r1));
  req.onReply(N0, E::OK, lsn_t(1), lsn_t(10));
  req.onReply(N1, E::OK, lsn_t(2), lsn_t(10));
  cb.assertNotCalled();
  req.onReply(N2, E::OK, lsn_t(3), lsn_t(10));
  // An f-majority of nodes replied.
  cb.assertCalled(E::OK, lsn_t(4));
}

TEST(FindKeyRequestTest, TimeFailures) {
  FindTimeCallback cb;
  MockFindKeyRequest req(5, 100_ms, cb);
  auto r1 = createMetaDataLogReaderResult(
      epoch_t(1), EPOCH_MAX, {N0, N1, N2, N3, N4});
  req.onMetadata(E::OK, std::move(r1));
  req.onReply(N0, E::OK, lsn_t(1), lsn_t(4));
  req.onReply(N1, E::OK, lsn_t(2), lsn_t(5));
  req.onReply(N2, E::FAILED, LSN_INVALID, LSN_INVALID);
  req.onReply(N3, E::FAILED, LSN_INVALID, LSN_INVALID);
  cb.assertNotCalled();
  req.onReply(N4, E::FAILED, LSN_INVALID, LSN_INVALID);
  cb.assertCalled(E::PARTIAL, lsn_t(3));
}

TEST(FindKeyRequestTest, TimeTotalFailure) {
  FindTimeCallback cb;
  MockFindKeyRequest req(3, 100_ms, cb);
  auto r1 = createMetaDataLogReaderResult(epoch_t(1), EPOCH_MAX, {N0, N1, N2});
  req.onMetadata(E::OK, std::move(r1));
  req.onReply(N0, E::FAILED, LSN_INVALID, LSN_INVALID);
  req.onReply(N1, E::FAILED, LSN_INVALID, LSN_INVALID);
  cb.assertNotCalled();
  req.onReply(N2, E::FAILED, LSN_INVALID, LSN_INVALID);
  cb.assertCalled(E::FAILED, LSN_INVALID);
}

TEST(FindKeyRequestTest, TimeClientTimeout) {
  FindTimeCallback cb;
  MockFindKeyRequest req(5, 100_ms, cb);
  auto r1 = createMetaDataLogReaderResult(
      epoch_t(1), EPOCH_MAX, {N0, N1, N2, N3, N4});
  req.onMetadata(E::OK, std::move(r1));
  req.onReply(N0, E::OK, lsn_t(1), lsn_t(4));
  req.onReply(N1, E::OK, lsn_t(2), lsn_t(5));
  cb.assertNotCalled();
  req.onClientTimeout();
  cb.assertCalled(E::PARTIAL, lsn_t(3));
}

TEST(FindKeyRequestTest, TimeClientTimeoutTotalFailure) {
  FindTimeCallback cb;
  MockFindKeyRequest req(3, 100_ms, cb);
  auto r1 = createMetaDataLogReaderResult(epoch_t(1), EPOCH_MAX, {N0, N1, N2});
  req.onMetadata(E::OK, std::move(r1));
  req.onReply(N0, E::FAILED, LSN_INVALID, LSN_INVALID);
  req.onReply(N1, E::FAILED, LSN_INVALID, LSN_INVALID);
  cb.assertNotCalled();
  req.onClientTimeout();
  cb.assertCalled(E::FAILED, LSN_INVALID);
}

// If any storage node replies with a small range, we can wrap up early
TEST(FindKeyRequestTest, TimeEarlyExit) {
  FindTimeCallback cb;
  MockFindKeyRequest req(10, 100_ms, cb);
  auto r1 = createMetaDataLogReaderResult(
      epoch_t(1), EPOCH_MAX, {N0, N1, N2, N3, N4, N5, N6, N7, N8, N9});
  req.onMetadata(E::OK, std::move(r1));
  req.onReply(N0, E::OK, lsn_t(1), lsn_t(100));
  cb.assertNotCalled();
  req.onReply(N1, E::OK, lsn_t(10), lsn_t(11));
  cb.assertCalled(E::OK, lsn_t(11));
}

// If a storage node is warming up, we should not consider that a system
// error, and we should retry
TEST(FindKeyRequestTest, TimeAgain) {
  FindTimeCallback cb;
  MockFindKeyRequest req(5, 100_ms, cb);
  auto r1 = createMetaDataLogReaderResult(
      epoch_t(1), EPOCH_MAX, {N0, N1, N2, N3, N4});
  req.onMetadata(E::OK, std::move(r1));
  req.onReply(N0, E::OK, lsn_t(1), lsn_t(4));
  req.onReply(N1, E::OK, lsn_t(2), lsn_t(5));
  req.onReply(N2, E::AGAIN, LSN_INVALID, LSN_INVALID);
  cb.assertNotCalled();
  req.onReply(N2, E::OK, lsn_t(3), lsn_t(6));
  cb.assertCalled(E::OK, lsn_t(4));
}

TEST(FindKeyRequestTest, TimeLaterThanAllRecords) {
  FindTimeCallback cb;
  MockFindKeyRequest req(5, 100_ms, cb);
  auto r1 = createMetaDataLogReaderResult(
      epoch_t(1), EPOCH_MAX, {N0, N1, N2, N3, N4});
  req.onMetadata(E::OK, std::move(r1));
  req.onReply(N0, E::OK, lsn_t(10), LSN_MAX);
  req.onReply(N1, E::OK, lsn_t(11), LSN_MAX);
  cb.assertNotCalled();
  req.onReply(N2, E::OK, lsn_t(12), LSN_MAX);
  cb.assertCalled(E::OK, lsn_t(13));
}

TEST(FindKeyRequestTest, TimeOnlyQueryUnionOfNodesets) {
  FindTimeCallback cb;
  MockFindKeyRequest req(100, 100_ms, cb);

  auto r1 =
      createMetaDataLogReaderResult(epoch_t(1), epoch_t(42), {N1, N2, N3});
  auto r2 = createMetaDataLogReaderResult(epoch_t(43), EPOCH_MAX, {N4, N5});
  req.onMetadata(E::OK, std::move(r1));
  req.onMetadata(E::OK, std::move(r2));

  req.onReply(N1, E::OK, lsn_t(13), LSN_MAX);
  req.onReply(N4, E::OK, lsn_t(12), LSN_MAX);
  cb.assertNotCalled();
  req.onReply(N5, E::OK, lsn_t(13), LSN_MAX);
  // We have an f-majority.
  cb.assertCalled(E::OK, lsn_t(14));
}

// Reading the metadata failed. FindKey should fail as well.
TEST(FindKeyRequestTest, TimeBadMetadataLogRecord) {
  FindTimeCallback cb;
  MockFindKeyRequest req(6, 100_ms, cb);

  auto r1 = createMetaDataLogReaderResult(epoch_t(1), epoch_t(42), {N1, N3});
  req.onMetadata(E::OK, std::move(r1));

  MetaDataLogReader::Result r2;
  r2.log_id = logid_t(1);
  r2.epoch_req = epoch_t(43), r2.epoch_until = epoch_t(44),
  r2.source = MetaDataLogReader::RecordSource::LAST;
  req.onMetadata(E::BADMSG, std::move(r2));
  cb.assertCalled(E::FAILED, LSN_INVALID);
}

// verify that callback called after sufficient replies
TEST(FindKeyRequestTest, OneNodeset) {
  Settings settings = create_default_settings<Settings>();

  FindTimeCallback cb;
  MockFindKeyRequest req(10, 100_ms, cb, settings);

  // There is one nodeset for epochs [42, EPOCH_MAX], it has 10 nodes.
  auto r1 =
      createMetaDataLogReaderResult(epoch_t(42),
                                    epoch_t(EPOCH_MAX),
                                    {N0, N1, N2, N3, N4, N5, N6, N7, N8, N9});
  req.onMetadata(E::OK, std::move(r1));

  const lsn_t hint_lo = lsn(44, 3);
  const lsn_t hint_hi = lsn(44, 10);

  // sent to all nodes.
  ASSERT_SENT(
      req, LSN_INVALID, LSN_MAX, {N0, N1, N2, N3, N4, N5, N6, N7, N8, N9});

  // two of them replies.
  req.onReply(N0, E::OK, hint_lo, hint_hi);
  req.onReply(N1, E::OK, lsn(44, 4), lsn(44, 9));
  cb.assertNotCalled();
  // Some more nodes reply until we find the result.
  req.onReply(N6, E::OK, lsn(44, 5), lsn(44, 7));
  req.onReply(N9, E::OK, lsn(44, 6), lsn(44, 7));
  cb.assertCalled(E::OK, lsn(44, 7));
}

// Same as OneNodeset but with multiple historical nodesets.
TEST(FindKeyRequestTest, MultipleNodesets) {
  Settings settings = create_default_settings<Settings>();

  FindTimeCallback cb;
  MockFindKeyRequest req(10, 100_ms, cb, settings);

  // There are two nodesets: [42, 46], [47, EPOCH_MAX]
  // Nodes 1, 2 are in both nodesets.
  auto r1 = createMetaDataLogReaderResult(
      epoch_t(42), epoch_t(46), {N0, N1, N2, N3, N4, N5, N6});
  req.onMetadata(E::OK, std::move(r1));
  auto r2 = createMetaDataLogReaderResult(
      epoch_t(47), epoch_t(EPOCH_MAX), {N1, N2, N7, N8, N9});
  req.onMetadata(E::OK, std::move(r2));

  // FINDKEY requests should be sent only to all nodes in nodeset
  ASSERT_SENT(
      req, LSN_INVALID, LSN_MAX, {N0, N1, N2, N3, N4, N5, N6, N7, N8, N9});

  const lsn_t hint_lo = lsn(49, 1332);
  const lsn_t hint_hi = lsn(49, 1337);
  // One node in nodeset of epochs [47, EPOCH_MAX] replies.
  req.onReply(N2, E::OK, hint_lo, hint_hi);

  cb.assertNotCalled();

  // Some nodes reply with the result.
  req.onReply(N8, E::OK, lsn(49, 1333), lsn(49, 1335));
  req.onReply(N9, E::OK, lsn(49, 1333), lsn(49, 1334));
  cb.assertCalled(E::OK, lsn(49, 1334));
}

TEST(FindKeyRequestTest, Test1) {
  Settings settings = create_default_settings<Settings>();

  FindTimeCallback cb;
  MockFindKeyRequest req(10, 100_ms, cb, settings);

  // There are two nodesets: [42, 46], [7, EPOCH_MAX]
  auto r1 = createMetaDataLogReaderResult(
      epoch_t(42), epoch_t(46), {N0, N1, N2, N3, N4});
  req.onMetadata(E::OK, std::move(r1));
  auto r2 = createMetaDataLogReaderResult(
      epoch_t(47), epoch_t(EPOCH_MAX), {N5, N6, N7, N8, N9});
  req.onMetadata(E::OK, std::move(r2));

  // requests should be sent only to all nodes
  ASSERT_SENT(
      req, LSN_INVALID, LSN_MAX, {N0, N1, N2, N3, N4, N5, N6, N7, N8, N9});

  // One node from the first nodeset replies.
  req.onReply(N0, E::OK, LSN_INVALID, LSN_MAX);

  // Some nodes reply with the result.
  req.onReply(N8, E::OK, lsn(49, 1333), lsn(49, 1335));
  req.onReply(N9, E::OK, lsn(49, 1333), lsn(49, 1334));
  cb.assertCalled(E::OK, lsn(49, 1334));
}

// first nodes respond with LSN_INVALID
TEST(FindKeyRequestTest, Test2) {
  Settings settings = create_default_settings<Settings>();

  FindTimeCallback cb;
  MockFindKeyRequest req(10, 100_ms, cb, settings);

  // There are two nodesets: [42, 46], [7, EPOCH_MAX]
  auto r1 = createMetaDataLogReaderResult(
      epoch_t(42), epoch_t(46), {N0, N1, N2, N3, N4});
  req.onMetadata(E::OK, std::move(r1));
  auto r2 = createMetaDataLogReaderResult(
      epoch_t(47), epoch_t(EPOCH_MAX), {N5, N6, N7, N8, N9});
  req.onMetadata(E::OK, std::move(r2));

  // FINDKEY requests should be sent to all nodes
  ASSERT_SENT(
      req, LSN_INVALID, LSN_MAX, {N0, N1, N2, N3, N4, N5, N6, N7, N8, N9});

  req.onReply(N0, E::OK, LSN_INVALID, LSN_MAX);
  req.onReply(N1, E::OK, LSN_INVALID, LSN_MAX);
  ASSERT_TRUE(req.getSent().empty());
  req.onReply(N6, E::OK, LSN_INVALID, LSN_MAX);

  cb.assertNotCalled();

  // Some nodes reply with the result.
  req.onReply(N8, E::OK, lsn(49, 1333), lsn(49, 1335));
  req.onReply(N9, E::OK, lsn(49, 1333), lsn(49, 1334));
  cb.assertCalled(E::OK, lsn(49, 1334));
}

TEST(FindKeyRequestTest, Test3) {
  Settings settings = create_default_settings<Settings>();

  FindTimeCallback cb;
  MockFindKeyRequest req(10, 100_ms, cb, settings);

  // There are two nodesets: [42, 46], [7, EPOCH_MAX]
  auto r1 = createMetaDataLogReaderResult(epoch_t(42), epoch_t(46), {N0, N1});
  req.onMetadata(E::OK, std::move(r1));
  auto r2 = createMetaDataLogReaderResult(epoch_t(47), epoch_t(47), {N2, N3});
  req.onMetadata(E::OK, std::move(r2));
  auto r3 =
      createMetaDataLogReaderResult(epoch_t(48), epoch_t(EPOCH_MAX), {N4, N5});
  req.onMetadata(E::OK, std::move(r3));

  ASSERT_SENT(req, LSN_INVALID, LSN_MAX, {N0, N1, N2, N3, N4, N5});

  req.onReply(N2, E::OK, lsn(47, 32), lsn(47, 34));
  req.onReply(N3, E::OK, lsn(47, 33), lsn(47, 45));
  cb.assertCalled(E::OK, lsn(47, 34));
}

TEST(FindKeyRequestTest, Test4) {
  Settings settings = create_default_settings<Settings>();

  FindTimeCallback cb;
  MockFindKeyRequest req(6, 100_ms, cb, settings);

  auto r1 = createMetaDataLogReaderResult(epoch_t(42), epoch_t(46), {N0, N1});
  req.onMetadata(E::OK, std::move(r1));
  auto r2 = createMetaDataLogReaderResult(epoch_t(47), epoch_t(47), {N2, N3});
  req.onMetadata(E::OK, std::move(r2));
  auto r3 =
      createMetaDataLogReaderResult(epoch_t(48), epoch_t(EPOCH_MAX), {N4, N5});
  req.onMetadata(E::OK, std::move(r3));

  // We should send a request to the whole cluster.
  ASSERT_SENT(req, LSN_INVALID, LSN_MAX, {N0, N1, N2, N3, N4, N5});

  req.onReply(N2, E::OK, lsn(49, 1333), lsn(49, 1335));
  req.onReply(N1, E::OK, lsn(49, 1333), lsn(49, 1334));
  cb.assertCalled(E::OK, lsn(49, 1334));
}

TEST(FindKeyRequestTest, ClientTimeout) {
  Settings settings = create_default_settings<Settings>();

  FindTimeCallback cb;
  MockFindKeyRequest req(6, 100_ms, cb, settings);

  req.onClientTimeout();
  cb.assertCalled(E::FAILED, LSN_INVALID);
}

TEST(FindKeyRequestTest, KeyStatusOkSameResult) {
  FindKeyCallback cb;
  MockFindKeyRequest req(5, cb);
  auto r1 = createMetaDataLogReaderResult(
      epoch_t(1), EPOCH_MAX, {N0, N1, N2, N3, N4});
  req.onMetadata(E::OK, std::move(r1));
  req.onReply(N0, E::OK, lsn_t(1), lsn_t(3));
  cb.assertNotCalled();
  req.onReply(N1, E::OK, lsn_t(1), lsn_t(3));
  cb.assertNotCalled();
  req.onReply(N2, E::OK, lsn_t(1), lsn_t(3));
  FindKeyResult result = {E::OK, lsn_t(1), lsn_t(3)};
  cb.assertCalled(result);
}

TEST(FindKeyRequestTest, KeyStatusOkReturnEarlySmallestRange) {
  FindKeyCallback cb;
  MockFindKeyRequest req(5, cb);
  auto r1 = createMetaDataLogReaderResult(
      epoch_t(1), EPOCH_MAX, {N0, N1, N2, N3, N4});
  req.onMetadata(E::OK, std::move(r1));
  req.onReply(N0, E::OK, lsn_t(1), lsn_t(3));
  cb.assertNotCalled();
  req.onReply(N1, E::OK, lsn_t(1), lsn_t(2));
  FindKeyResult result = {E::OK, lsn_t(1), lsn_t(2)};
  cb.assertCalled(result);
}

TEST(FindKeyRequestTest, KeyStatusOkDifferentResults) {
  FindKeyCallback cb;
  MockFindKeyRequest req(5, cb);
  auto r1 = createMetaDataLogReaderResult(
      epoch_t(1), EPOCH_MAX, {N0, N1, N2, N3, N4});
  req.onMetadata(E::OK, std::move(r1));
  req.onReply(N0, E::OK, lsn_t(1), lsn_t(5));
  cb.assertNotCalled();
  req.onReply(N1, E::OK, lsn_t(2), lsn_t(4));
  cb.assertNotCalled();
  req.onReply(N2, E::OK, lsn_t(2), lsn_t(5));
  FindKeyResult result = {E::OK, lsn_t(2), lsn_t(4)};
  cb.assertCalled(result);
}

TEST(FindKeyRequestTest, KeyStatusOkIgnoreBadRange) {
  FindKeyCallback cb;
  MockFindKeyRequest req(5, cb);
  auto r1 = createMetaDataLogReaderResult(
      epoch_t(1), EPOCH_MAX, {N0, N1, N2, N3, N4});
  req.onMetadata(E::OK, std::move(r1));
  req.onReply(N0, E::OK, lsn_t(1), lsn_t(5));
  cb.assertNotCalled();
  req.onReply(N1, E::OK, lsn_t(2), lsn_t(4));
  cb.assertNotCalled();
  req.onReply(N2, E::OK, lsn_t(3), lsn_t(2));
  cb.assertNotCalled();
  req.onReply(N3, E::OK, lsn_t(2), lsn_t(5));
  FindKeyResult result = {E::OK, lsn_t(2), lsn_t(4)};
  cb.assertCalled(result);
}

TEST(FindKeyRequestTest, KeyStatusOkOneFailed) {
  FindKeyCallback cb;
  MockFindKeyRequest req(5, cb);
  auto r1 = createMetaDataLogReaderResult(
      epoch_t(1), EPOCH_MAX, {N0, N1, N2, N3, N4});
  req.onMetadata(E::OK, std::move(r1));
  req.onReply(N0, E::OK, lsn_t(1), lsn_t(5));
  cb.assertNotCalled();
  req.onReply(N1, E::FAILED, LSN_INVALID, LSN_INVALID);
  cb.assertNotCalled();
  req.onReply(N2, E::OK, lsn_t(2), lsn_t(5));
  cb.assertNotCalled();
  req.onReply(N3, E::OK, lsn_t(1), lsn_t(4));
  FindKeyResult result = {E::OK, lsn_t(2), lsn_t(4)};
  cb.assertCalled(result);
}

TEST(FindKeyRequestTest, KeyStatusPartialMoreFailed) {
  FindKeyCallback cb;
  MockFindKeyRequest req(5, cb);
  auto r1 = createMetaDataLogReaderResult(
      epoch_t(1), EPOCH_MAX, {N0, N1, N2, N3, N4});
  req.onMetadata(E::OK, std::move(r1));
  req.onReply(N0, E::OK, lsn_t(1), lsn_t(5));
  cb.assertNotCalled();
  req.onReply(N1, E::FAILED, LSN_INVALID, LSN_INVALID);
  cb.assertNotCalled();
  req.onReply(N2, E::FAILED, LSN_INVALID, LSN_INVALID);
  cb.assertNotCalled();
  req.onReply(N3, E::FAILED, LSN_INVALID, LSN_INVALID);
  cb.assertNotCalled();
  req.onReply(N4, E::OK, lsn_t(1), lsn_t(4));
  FindKeyResult result = {E::PARTIAL, lsn_t(1), lsn_t(4)};
  cb.assertCalled(result);
}

TEST(FindKeyRequestTest, KeyStatusFailed) {
  FindKeyCallback cb;
  MockFindKeyRequest req(3, cb);
  auto r1 = createMetaDataLogReaderResult(epoch_t(1), EPOCH_MAX, {N0, N1, N2});
  req.onMetadata(E::OK, std::move(r1));
  req.onReply(N0, E::FAILED, LSN_INVALID, LSN_INVALID);
  cb.assertNotCalled();
  req.onReply(N1, E::FAILED, LSN_INVALID, LSN_INVALID);
  cb.assertNotCalled();
  req.onReply(N2, E::FAILED, LSN_INVALID, LSN_INVALID);
  FindKeyResult result = {E::FAILED, LSN_INVALID, LSN_INVALID};
  cb.assertCalled(result);
}

TEST(FindKeyRequestTest, KeyStatusPartialClientTimeout) {
  FindKeyCallback cb;
  MockFindKeyRequest req(5, cb);
  auto r1 = createMetaDataLogReaderResult(
      epoch_t(1), EPOCH_MAX, {N0, N1, N2, N3, N4});
  req.onMetadata(E::OK, std::move(r1));
  req.onReply(N0, E::FAILED, LSN_INVALID, LSN_INVALID);
  cb.assertNotCalled();
  req.onReply(N1, E::OK, lsn_t(2), lsn_t(4));
  cb.assertNotCalled();
  req.onClientTimeout();
  FindKeyResult result = {E::PARTIAL, lsn_t(2), lsn_t(4)};
  cb.assertCalled(result);
}

TEST(FindKeyRequestTest, KeyStatusFailedClientTimeout) {
  FindKeyCallback cb;
  MockFindKeyRequest req(5, cb);
  auto r1 = createMetaDataLogReaderResult(
      epoch_t(1), EPOCH_MAX, {N0, N1, N2, N3, N4});
  req.onMetadata(E::OK, std::move(r1));
  req.onReply(N0, E::FAILED, LSN_INVALID, LSN_INVALID);
  cb.assertNotCalled();
  req.onReply(N1, E::FAILED, LSN_INVALID, LSN_INVALID);
  cb.assertNotCalled();
  req.onClientTimeout();
  FindKeyResult result = {E::FAILED, LSN_INVALID, LSN_INVALID};
  cb.assertCalled(result);
}
