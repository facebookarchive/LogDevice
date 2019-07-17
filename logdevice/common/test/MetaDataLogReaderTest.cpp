/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/MetaDataLogReader.h"

#include <folly/Memory.h>
#include <folly/Optional.h>
#include <gtest/gtest.h>

#include "logdevice/common/DataRecordOwnsPayload.h"
#include "logdevice/common/EpochMetaData.h"
#include "logdevice/common/MetaDataLog.h"
#include "logdevice/common/configuration/LocalLogsConfig.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/common/test/NodeSetTestUtil.h"
#include "logdevice/include/Record.h"

namespace facebook { namespace logdevice {

#define N1 ShardID(1, 0)
#define N2 ShardID(2, 0)
#define N3 ShardID(3, 0)
#define N4 ShardID(4, 0)
#define N5 ShardID(5, 0)

using namespace NodeSetTestUtil;

constexpr logid_t NORMAL_LOGID(logid_t(9999));
constexpr logid_t META_LOGID(MetaDataLog::metaDataLogID(logid_t(9999)));

#define NOT_LAST MetaDataLogReader::RecordSource::NOT_LAST
#define LAST MetaDataLogReader::RecordSource::LAST
#define CACHED MetaDataLogReader::RecordSource::CACHED

class TestMetaDataLogReader : public MetaDataLogReader {
 protected:
  void startReading() override {
    ld_check(!started_);
    started_ = true;
  }

  void finalize() override {
    MetaDataLogReader::finalize();
    started_ = false;
    finalized_ = true;
  }

  void stopReading() override {
    ld_check(finalized_);
  }

  std::shared_ptr<Configuration> getClusterConfig() const override {
    return config_;
  }

  std::shared_ptr<const NodesConfiguration>
  getNodesConfiguration() const override {
    return config_->serverConfig()
        ->getNodesConfigurationFromServerConfigSource();
  }

 public:
  StatsHolder* getStats() override {
    return &stats_;
  }

  using ResultType = std::tuple<Status, MetaDataLogReader::Result>;

  TestMetaDataLogReader(logid_t log_id, epoch_t start, epoch_t end)
      : MetaDataLogReader(log_id,
                          start,
                          end,
                          std::bind(&TestMetaDataLogReader::onMetaDataCallback,
                                    this,
                                    std::placeholders::_1,
                                    std::placeholders::_2)),
        stats_(StatsParams().setIsServer(false)) {
    initConfig();
  }

  TestMetaDataLogReader(logid_t log_id,
                        epoch_t start,
                        MetaDataLogReader::Mode mode =
                            MetaDataLogReader::Mode::IGNORE_LAST_RELEASED)
      : MetaDataLogReader(log_id,
                          start,
                          std::bind(&TestMetaDataLogReader::onMetaDataCallback,
                                    this,
                                    std::placeholders::_1,
                                    std::placeholders::_2),
                          mode),
        stats_(StatsParams().setIsServer(false)) {
    initConfig();
  }

  void onMetaDataCallback(Status st, MetaDataLogReader::Result r) {
    ASSERT_TRUE(started_);
    ASSERT_FALSE(result_.hasValue());
    result_.assign(std::make_tuple(st, std::move(r)));
  }

  void initConfig() {
    Configuration::Nodes nodes;
    addNodes(&nodes, 3, 1, "rg0.dc0.cl0.ro0.rk0", 1);
    addNodes(&nodes, 3, 1, "rg1.dc0.cl0.ro0.rk0", 1);
    addNodes(&nodes, 3, 1, "rg1.dc0.cl0.ro0.rk1", 1);
    Configuration::NodesConfig nodes_config;
    nodes_config.setNodes(std::move(nodes));

    config_ = std::make_shared<Configuration>(
        ServerConfig::fromDataTest(__FILE__,
                                   std::move(nodes_config),
                                   Configuration::MetaDataLogsConfig()),
        std::make_shared<configuration::LocalLogsConfig>());
  }

  bool started_{false};
  bool finalized_{false};
  folly::Optional<ResultType> result_;
  StatsHolder stats_;
  std::shared_ptr<Configuration> config_;
};

// generate a DataRecord object contains the given epoch metadata in payload
static std::unique_ptr<DataRecordOwnsPayload>
genMetaRecord(const EpochMetaData& info, lsn_t lsn = LSN_INVALID) {
  ld_check(info.isValid());
  std::chrono::milliseconds ts(100);
  size_t size = info.sizeInPayload();
  void* buf = malloc(size);
  ld_check(buf);
  int rv = info.toPayload(buf, size);
  ld_check(rv == size);
  return std::make_unique<DataRecordOwnsPayload>(
      META_LOGID,
      Payload(buf, size),
      lsn == LSN_INVALID ? compose_lsn(info.h.epoch, esn_t(1)) : lsn,
      ts,
      0 // flags
  );
}

// generate epoch metadata stored in metadata log
static EpochMetaData genMetaData(epoch_t since,
                                 int replication,
                                 const StorageSet& shards) {
  return EpochMetaData(
      shards,
      ReplicationProperty({{NodeLocationScope::NODE, replication}}),
      since,
      since);
}

static lsn_t lsn(int epoch, int esn) {
  return compose_lsn(epoch_t(epoch), esn_t(esn));
}

// assert the metadata callback is called to deliver an epoch metadata
#define ASSERT_RESULT(_reader_, _req_, _until_, _source_, _metadata_) \
  do {                                                                \
    ASSERT_TRUE((_reader_).result_.hasValue());                       \
    ASSERT_EQ(E::OK, std::get<0>((_reader_).result_.value()));        \
    const MetaDataLogReader::Result& r =                              \
        std::get<1>((_reader_).result_.value());                      \
    ASSERT_EQ(NORMAL_LOGID, r.log_id);                                \
    ASSERT_EQ((_req_), r.epoch_req);                                  \
    ASSERT_EQ((_until_), r.epoch_until);                              \
    ASSERT_TRUE(((_source_) == r.source));                            \
    ASSERT_NE(nullptr, r.metadata);                                   \
    ASSERT_EQ(_metadata_, *r.metadata);                               \
    (reader).result_.clear();                                         \
  } while (0)

#define ASSERT_RESULT_ERROR(_reader_, _req_, _until_, _source_, _status_) \
  do {                                                                    \
    ASSERT_TRUE((_reader_).result_.hasValue());                           \
    ASSERT_EQ((_status_), std::get<0>((reader).result_.value()));         \
    const MetaDataLogReader::Result& r =                                  \
        std::get<1>((_reader_).result_.value());                          \
    ASSERT_EQ(NORMAL_LOGID, r.log_id);                                    \
    ASSERT_EQ((_req_), r.epoch_req);                                      \
    ASSERT_EQ((_until_), r.epoch_until);                                  \
    ASSERT_TRUE(((_source_) == r.source));                                \
    ASSERT_EQ(nullptr, r.metadata);                                       \
    (reader).result_.clear();                                             \
  } while (0)

// assert that MetaDataLogReader is finalized after delivering this metadata
#define ASSERT_FINAL_RESULT(reader, requested, until, source, metadata)  \
  do {                                                                   \
    ASSERT_TRUE((reader).finalized_);                                    \
    ASSERT_FALSE((reader).started_);                                     \
    ASSERT_RESULT((reader), (requested), (until), (source), (metadata)); \
  } while (0)

// no records in the metadata log
TEST(MetaDataLogReaderTest, EmptyMetaData) {
  TestMetaDataLogReader reader(NORMAL_LOGID, epoch_t(1));
  reader.start();
  reader.onGapRecord(
      GapRecord(META_LOGID, GapType::BRIDGE, lsn(0, 1), LSN_MAX - 1));
  ASSERT_RESULT_ERROR(reader, epoch_t(1), epoch_t(1), LAST, E::NOTFOUND);
  ASSERT_TRUE(reader.finalized_);
  ASSERT_EQ(reader.getStats()->aggregate().metadata_log_read_failed_other, 1);
}

// common sequence of metadata records used for tests
static std::map<int, EpochMetaData> epoch_map{
    {10, genMetaData(epoch_t(10), 1, StorageSet{N1, N2, N3})},
    {22, genMetaData(epoch_t(22), 1, StorageSet{N2, N3, N4})},
    {30, genMetaData(epoch_t(30), 1, StorageSet{N2, N3, N4, N5})},
};

// user requests an epoch which is less than the smallest epoch metadata
// available in the metadata log, should return the first epoch metadata
// in such case
TEST(MetaDataLogReaderTest, FirstMetaData) {
  // one record: {10}, ask for metadata for epoch 1
  TestMetaDataLogReader reader(NORMAL_LOGID, epoch_t(1));
  reader.start();
  reader.onDataRecord(genMetaRecord(epoch_map[10]));
  reader.onGapRecord(
      GapRecord(META_LOGID, GapType::BRIDGE, lsn(10, 2), LSN_MAX));
  // requested: 1, got: 10, until: 10
  ASSERT_FINAL_RESULT(reader, epoch_t(1), epoch_t(10), LAST, epoch_map[10]);
}

// given the message sequence _msg_seq_, request metadata for epoch _request_,
// expect to get metadata with epoch _got_ and effective until epoch _until_
#define ASSERT_READ(msg_seq, request, got, until, source)             \
  do {                                                                \
    TestMetaDataLogReader reader(NORMAL_LOGID, epoch_t((request)));   \
    reader.start();                                                   \
    msg_seq(reader);                                                  \
    ASSERT_FINAL_RESULT(                                              \
        reader, (request), (until), (source), epoch_map[(got).val_]); \
  } while (0)

#define ASSERT_READ_ERROR(msg_seq, request, until, source, status)       \
  do {                                                                   \
    TestMetaDataLogReader reader(NORMAL_LOGID, epoch_t((request)));      \
    reader.start();                                                      \
    msg_seq(reader);                                                     \
    ASSERT_RESULT_ERROR(reader, (request), (until), (source), (status)); \
    ASSERT_TRUE(reader.finalized_);                                      \
  } while (0)

#define MESSAGE_SEQ_WITH_NORMAL_RECORD(reader)                               \
  do {                                                                       \
    const GapType gt = GapType::BRIDGE;                                      \
    (reader).onGapRecord(GapRecord(META_LOGID, gt, lsn(0, 1), lsn(10, 0)));  \
    (reader).onDataRecord(genMetaRecord(epoch_map[10]));                     \
    (reader).onGapRecord(GapRecord(META_LOGID, gt, lsn(10, 2), lsn(22, 0))); \
    (reader).onDataRecord(genMetaRecord(epoch_map[22]));                     \
    (reader).onGapRecord(GapRecord(META_LOGID, gt, lsn(22, 2), lsn(30, 0))); \
    (reader).onDataRecord(genMetaRecord(epoch_map[30]));                     \
    (reader).onGapRecord(GapRecord(META_LOGID, gt, lsn(30, 2), LSN_MAX));    \
  } while (0)

#define ASSERT_READ_NORMAL(request, got, until, source) \
  ASSERT_READ(                                          \
      MESSAGE_SEQ_WITH_NORMAL_RECORD, (request), (got), (until), (source))

TEST(MetaDataLogReaderTest, NormalRecordsRead) {
  // three records: {10, 22, 30}
  // requested: EPOCH_INVALID, got: 10, until: 21
  ASSERT_READ_NORMAL(EPOCH_INVALID, epoch_t(10), epoch_t(21), NOT_LAST);
  // requested: 1, got: 10, until: 21
  ASSERT_READ_NORMAL(epoch_t(1), epoch_t(10), epoch_t(21), NOT_LAST);
  // requested: 10, got: 10, until: 21
  ASSERT_READ_NORMAL(epoch_t(10), epoch_t(10), epoch_t(21), NOT_LAST);
  // requested: 15, got: 10, until: 21
  ASSERT_READ_NORMAL(epoch_t(15), epoch_t(10), epoch_t(21), NOT_LAST);
  // requested: 22, got: 22, until: 29
  ASSERT_READ_NORMAL(epoch_t(22), epoch_t(22), epoch_t(29), NOT_LAST);
  // requested: 29, got: 22, until: 29
  ASSERT_READ_NORMAL(epoch_t(29), epoch_t(22), epoch_t(29), NOT_LAST);
  // requested: 30, got: 30, until: 30
  ASSERT_READ_NORMAL(epoch_t(30), epoch_t(30), epoch_t(30), LAST);
  // requested: 100, got: 30, until: 100
  ASSERT_READ_NORMAL(epoch_t(100), epoch_t(30), epoch_t(100), LAST);
}

// create MetaDataLogReader for reading epoch metadata for a range of epoch
TEST(MetaDataLogReaderTest, ReadEpochRange) {
  // three records: {10, 22, 30}
  // Read epoch metadata for epochs in range [1, 35], expect to deliver three
  // epoch metadata intervals: [1, 21], [22, 29], [30, 35]
  TestMetaDataLogReader reader(NORMAL_LOGID, epoch_t(1), epoch_t(35));
  reader.start();
  const GapType gt = GapType::BRIDGE;
  reader.onGapRecord(GapRecord(META_LOGID, gt, lsn(0, 1), lsn(10, 0)));
  reader.onDataRecord(genMetaRecord(epoch_map[10]));
  reader.onGapRecord(GapRecord(META_LOGID, gt, lsn(10, 2), lsn(22, 0)));
  reader.onDataRecord(genMetaRecord(epoch_map[22]));
  // requested: 1, got: 10, until: 21
  ASSERT_RESULT(reader, epoch_t(1), epoch_t(21), NOT_LAST, epoch_map[10]);
  reader.onGapRecord(GapRecord(META_LOGID, gt, lsn(22, 2), lsn(30, 0)));
  reader.onDataRecord(genMetaRecord(epoch_map[30]));
  // requested: 22, got: 22, until: 29
  ASSERT_RESULT(reader, epoch_t(22), epoch_t(29), NOT_LAST, epoch_map[22]);
  reader.onGapRecord(GapRecord(META_LOGID, gt, lsn(30, 2), LSN_MAX));
  // requested: 30, got: 30, until: 35
  ASSERT_FINAL_RESULT(reader, epoch_t(30), epoch_t(35), LAST, epoch_map[30]);
}

// a message sequence where each epoch metadata appears twice
#define MESSAGE_SEQ_WITH_DUPLICATED_RECORD(reader)                           \
  do {                                                                       \
    const GapType gt = GapType::BRIDGE;                                      \
    (reader).onGapRecord(GapRecord(META_LOGID, gt, lsn(0, 1), lsn(10, 0)));  \
    (reader).onDataRecord(genMetaRecord(epoch_map[10], lsn(10, 1)));         \
    (reader).onDataRecord(genMetaRecord(epoch_map[10], lsn(10, 2)));         \
    (reader).onGapRecord(GapRecord(META_LOGID, gt, lsn(10, 3), lsn(22, 0))); \
    (reader).onDataRecord(genMetaRecord(epoch_map[22], lsn(22, 1)));         \
    (reader).onGapRecord(GapRecord(META_LOGID, gt, lsn(22, 2), lsn(25, 0))); \
    (reader).onDataRecord(genMetaRecord(epoch_map[22], lsn(25, 1)));         \
    (reader).onGapRecord(GapRecord(META_LOGID, gt, lsn(25, 2), lsn(28, 0))); \
    (reader).onDataRecord(genMetaRecord(epoch_map[30], lsn(28, 1)));         \
    (reader).onGapRecord(GapRecord(META_LOGID, gt, lsn(28, 2), lsn(36, 0))); \
    (reader).onDataRecord(genMetaRecord(epoch_map[30], lsn(36, 1)));         \
    (reader).onGapRecord(GapRecord(META_LOGID, gt, lsn(36, 2), LSN_MAX));    \
  } while (0)

#define ASSERT_READ_DUPLICATE(request, got, until, source) \
  ASSERT_READ(                                             \
      MESSAGE_SEQ_WITH_DUPLICATED_RECORD, (request), (got), (until), (source))

// despite there are records with duplicated epoch metadata, the result should
// be the same as there is no duplicates
TEST(MetaDataLogReaderTest, ReadDuplicateMetaData) {
  // three records: {10, 22, 30}
  ASSERT_READ_DUPLICATE(EPOCH_INVALID, epoch_t(10), epoch_t(21), NOT_LAST);
  ASSERT_READ_DUPLICATE(epoch_t(1), epoch_t(10), epoch_t(21), NOT_LAST);
  ASSERT_READ_DUPLICATE(epoch_t(10), epoch_t(10), epoch_t(21), NOT_LAST);
  ASSERT_READ_DUPLICATE(epoch_t(15), epoch_t(10), epoch_t(21), NOT_LAST);
  ASSERT_READ_DUPLICATE(epoch_t(22), epoch_t(22), epoch_t(29), NOT_LAST);
  ASSERT_READ_DUPLICATE(epoch_t(25), epoch_t(22), epoch_t(29), NOT_LAST);
  ASSERT_READ_DUPLICATE(epoch_t(29), epoch_t(22), epoch_t(29), NOT_LAST);
  ASSERT_READ_DUPLICATE(epoch_t(30), epoch_t(30), epoch_t(30), LAST);
  ASSERT_READ_DUPLICATE(epoch_t(100), epoch_t(30), epoch_t(100), LAST);
}

static std::unique_ptr<DataRecordOwnsPayload>
genBadMetaRecord(lsn_t lsn = LSN_INVALID) {
  auto ptr = genMetaRecord(epoch_map[22], lsn);
  memset((void*)ptr->payload.data(), 0, ptr->payload.size());
  return ptr;
}

#define MESSAGE_SEQ_WITH_BAD_RECORD(reader)                                  \
  do {                                                                       \
    const GapType gt = GapType::BRIDGE;                                      \
    (reader).onDataRecord(genMetaRecord(epoch_map[10]));                     \
    (reader).onGapRecord(GapRecord(META_LOGID, gt, lsn(10, 2), lsn(22, 0))); \
    (reader).onDataRecord(genBadMetaRecord(lsn(22, 1)));                     \
    (reader).onGapRecord(GapRecord(META_LOGID, gt, lsn(22, 2), lsn(30, 0))); \
    (reader).onDataRecord(genMetaRecord(epoch_map[30]));                     \
    (reader).onGapRecord(GapRecord(META_LOGID, gt, lsn(30, 2), LSN_MAX));    \
  } while (0)

#define ASSERT_READ_BAD_RECORD(request, got, until, source) \
  ASSERT_READ(MESSAGE_SEQ_WITH_BAD_RECORD, (request), (got), (until), (source))

TEST(MetaDataLogReaderTest, MalformedRecord) {
  // five records: {10, X, 30}, but the middle one is malformed
  ASSERT_READ_BAD_RECORD(epoch_t(1), epoch_t(10), epoch_t(10), NOT_LAST);
  ASSERT_READ_BAD_RECORD(epoch_t(10), epoch_t(10), epoch_t(10), NOT_LAST);
  ASSERT_READ_BAD_RECORD(epoch_t(30), epoch_t(30), epoch_t(30), LAST);
  ASSERT_READ_BAD_RECORD(epoch_t(90), epoch_t(30), epoch_t(90), LAST);
  // request metadata for epoch [11, 29] should fail with E::BADMSG
  ASSERT_READ_ERROR(MESSAGE_SEQ_WITH_BAD_RECORD,
                    epoch_t(18),
                    epoch_t(29),
                    NOT_LAST,
                    E::BADMSG);
}

#define MESSAGE_SEQ_DATALOSS(reader)                                         \
  do {                                                                       \
    const GapType dl = GapType::DATALOSS;                                    \
    const GapType bg = GapType::BRIDGE;                                      \
    (reader).onGapRecord(GapRecord(META_LOGID, dl, lsn(0, 1), lsn(10, 0)));  \
    (reader).onDataRecord(genMetaRecord(epoch_map[10]));                     \
    (reader).onGapRecord(GapRecord(META_LOGID, dl, lsn(10, 2), lsn(30, 0))); \
    (reader).onDataRecord(genMetaRecord(epoch_map[30]));                     \
    (reader).onGapRecord(GapRecord(META_LOGID, dl, lsn(30, 2), lsn(30, 4))); \
    (reader).onGapRecord(GapRecord(META_LOGID, bg, lsn(30, 5), LSN_MAX));    \
  } while (0)

#define ASSERT_READ_DATALOSS(request, got, until, source) \
  ASSERT_READ(MESSAGE_SEQ_DATALOSS, (request), (got), (until), (source))

TEST(MetaDataLogReaderTest, DataLoss) {
  // message sequence {X, 10, X, 30, X}, X stands for DATALOSS gaps
  ASSERT_READ_ERROR(
      MESSAGE_SEQ_DATALOSS, epoch_t(0), epoch_t(9), NOT_LAST, E::DATALOSS);
  ASSERT_READ_ERROR(
      MESSAGE_SEQ_DATALOSS, epoch_t(9), epoch_t(9), NOT_LAST, E::DATALOSS);
  ASSERT_READ_DATALOSS(epoch_t(10), epoch_t(10), epoch_t(10), NOT_LAST);
  ASSERT_READ_ERROR(
      MESSAGE_SEQ_DATALOSS, epoch_t(12), epoch_t(29), NOT_LAST, E::DATALOSS);
  ASSERT_READ_ERROR(
      MESSAGE_SEQ_DATALOSS, epoch_t(29), epoch_t(29), NOT_LAST, E::DATALOSS);
  ASSERT_READ_DATALOSS(epoch_t(30), epoch_t(30), epoch_t(30), NOT_LAST);
  ASSERT_READ_ERROR(
      MESSAGE_SEQ_DATALOSS, epoch_t(31), epoch_t(31), LAST, E::DATALOSS);
}

#define MESSAGE_SEQ_FOR_WAIT_APPEAR(reader)                                  \
  do {                                                                       \
    const GapType gt = GapType::BRIDGE;                                      \
    (reader).onDataRecord(genMetaRecord(epoch_map[10]));                     \
    (reader).onGapRecord(GapRecord(META_LOGID, gt, lsn(10, 2), lsn(22, 0))); \
    (reader).onDataRecord(genMetaRecord(epoch_map[22]));                     \
    (reader).onGapRecord(GapRecord(META_LOGID, gt, lsn(22, 2), lsn(30, 0))); \
    (reader).onDataRecord(genMetaRecord(epoch_map[30]));                     \
  } while (0)

#define ASSERT_READ_APPEAR(request, got, until)                       \
  do {                                                                \
    TestMetaDataLogReader reader(                                     \
        NORMAL_LOGID,                                                 \
        epoch_t((request)),                                           \
        MetaDataLogReader::Mode::WAIT_UNTIL_RELEASED);                \
    reader.start();                                                   \
    MESSAGE_SEQ_FOR_WAIT_APPEAR(reader);                              \
    ASSERT_FINAL_RESULT(                                              \
        reader, (request), (until), NOT_LAST, epoch_map[(got).val_]); \
  } while (0)

TEST(MetaDataLogReaderTest, WaitUntilEpochAppear) {
  // three records: {10, 22, 30}
  // requested: EPOCH_INVALID, got: 10, until: 21
  ASSERT_READ_APPEAR(EPOCH_INVALID, epoch_t(10), epoch_t(10));
  ASSERT_READ_APPEAR(epoch_t(1), epoch_t(10), epoch_t(10));
  ASSERT_READ_APPEAR(epoch_t(10), epoch_t(10), epoch_t(10));
  ASSERT_READ_APPEAR(epoch_t(18), epoch_t(10), epoch_t(21));
  ASSERT_READ_APPEAR(epoch_t(22), epoch_t(22), epoch_t(22));
  ASSERT_READ_APPEAR(epoch_t(25), epoch_t(22), epoch_t(29));
  ASSERT_READ_APPEAR(epoch_t(30), epoch_t(30), epoch_t(30));
  {
    TestMetaDataLogReader reader(NORMAL_LOGID,
                                 epoch_t(32),
                                 MetaDataLogReader::Mode::WAIT_UNTIL_RELEASED);
    reader.start();
    MESSAGE_SEQ_FOR_WAIT_APPEAR(reader);
    ASSERT_TRUE(reader.started_);
    ASSERT_FALSE(reader.finalized_);
  }
}

}} // namespace facebook::logdevice
