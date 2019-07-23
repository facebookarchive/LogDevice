/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/locallogstore/PartitionedRocksDBStore.h"

#include <algorithm>
#include <fstream>
#include <future>
#include <iterator>
#include <queue>
#include <set>

#include <folly/Random.h>
#include <folly/Varint.h>
#include <gtest/gtest.h>
#include <rocksdb/sst_file_manager.h>

#include "logdevice/common/LocalLogStoreRecordFormat.h"
#include "logdevice/common/NodeID.h"
#include "logdevice/common/ThreadID.h"
#include "logdevice/common/configuration/InternalLogs.h"
#include "logdevice/common/configuration/LocalLogsConfig.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/settings/SettingsUpdater.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/common/util.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/locallogstore/PartitionMetadata.h"
#include "logdevice/server/locallogstore/RocksDBCompactionFilter.h"
#include "logdevice/server/locallogstore/RocksDBEnv.h"
#include "logdevice/server/locallogstore/RocksDBKeyFormat.h"
#include "logdevice/server/locallogstore/RocksDBListener.h"
#include "logdevice/server/locallogstore/RocksDBMemTableRep.h"
#include "logdevice/server/locallogstore/RocksDBWriterMergeOperator.h"
#include "logdevice/server/locallogstore/WriteOps.h"
#include "logdevice/server/locallogstore/test/TemporaryLogStore.h"
#include "logdevice/server/read_path/LogStorageStateMap.h"
#include "logdevice/server/storage_tasks/StorageThreadPool.h"
#include "rocksdb/db.h"

using namespace facebook::logdevice;
using RocksDBKeyFormat::CopySetIndexKey;
using RocksDBKeyFormat::DataKey;
using RocksDBKeyFormat::LogMetaKey;
using RocksDBKeyFormat::PartitionDirectoryKey;
using DirectoryEntry = PartitionedRocksDBStore::DirectoryEntry;
using Params = ServerSettings::StoragePoolParams;
using WriteBufStats = RocksDBLogStoreBase::WriteBufStats;
using WriteThrottleState = LocalLogStore::WriteThrottleState;

// In milliseconds.
const uint64_t SECOND = 1000ul;
const uint64_t MINUTE = SECOND * 60;
const uint64_t HOUR = MINUTE * 60;
const uint64_t DAY = HOUR * 24;
const uint64_t WEEK = DAY * 7;
// Just some reasonable time point. Intentionally round for readability of test
// failures. 2001-09-08 18:46:40
const uint64_t BASE_TIME = 1000000000000;
// Some time point in distant future.
const uint64_t FUTURE = 2000000000000;
const shard_index_t THIS_SHARD = 0;

// Partition ID of the initial partition. In comments throughout this file
// "partition n" refers to partition with ID = ID0 + n
partition_id_t ID0 = PartitionedRocksDBStore::INITIAL_PARTITION_ID;

const logid_t EVENT_LOG_ID(123123123);

TailRecord tail_record;
OffsetMap epoch_size_map;
OffsetMap epoch_end_offsets;

// Payloads used in these tests are a function of record's log ID, LSN and
// timestamp. This makes them easy to verify.
static std::string formTestPayload(logid_t log, lsn_t lsn, uint64_t timestamp) {
  return std::to_string(log.val_) + ":" + std::to_string(lsn) + ":" +
      std::to_string(timestamp);
}

class TestPartitionedRocksDBStore : public PartitionedRocksDBStore {
 public:
  struct State {
    std::condition_variable cv_;
    std::queue<std::promise<void>> promises_;
    folly::Optional<std::promise<void>> current_promise_;
  };

  explicit TestPartitionedRocksDBStore(const std::string& path,
                                       RocksDBLogStoreConfig rocksdb_config,
                                       const Configuration* config,
                                       StatsHolder* stats,
                                       SystemTimestamp* time)
      : PartitionedRocksDBStore(0,
                                1,
                                path,
                                std::move(rocksdb_config),
                                config,
                                stats,
                                /* io_tracing */ nullptr,
                                DeferInit::YES),
        time_(time) {
    PartitionedRocksDBStore::init(config);
  }

  ~TestPartitionedRocksDBStore() override {
    shutdown_event_.signal();
    for (State& s : states_) {
      {
        // This weird thing acts as sort of a barrier to make sure condition
        // variable wakes up. Without it there is a race condition: if
        // cv_.wait() has just seen that shutdown_event_.signaled() is false
        // but hasn't gone back to sleep, the notify_all() won't wake it up.
        // This lock makes sure that notify_all() is called when either cv_
        // is sleeping or it has already seen that shutdown_event_.signaled().
        std::lock_guard<std::mutex> lock(test_mutex_);
      }
      s.cv_.notify_all();
    }
    PartitionedRocksDBStore::joinBackgroundThreads();
  }

  int registerOnFlushCallback(FlushCallback& /*cb*/) override {
    // Never call flush callback. It wants too much of the environment to exist,
    // like workers and full config.
    return 0;
  }

  SystemTimestamp currentTime() override {
    ld_check(time_);
    return *time_;
  }

  // Use the same time value for both system and steady timestamps.
  // Since time is controlled by the test framework, jumps can't happen.
  SteadyTimestamp currentSteadyTime() override {
    ld_check(time_);
    return SteadyTimestamp(time_->time_since_epoch());
  }

  // By default prevents background thread from running. Only resumes it for
  // one iteration each time backgroundThreadIteration() is called.
  void backgroundThreadSleep(BackgroundThreadType type) override {
    auto& state = states_[(int)type];
    std::unique_lock<std::mutex> lock(test_mutex_);

    if (state.current_promise_.hasValue()) {
      state.current_promise_.value().set_value();
      state.current_promise_.clear();
    }

    state.cv_.wait(lock, [&] {
      return !state.promises_.empty() || shutdown_event_.signaled();
    });
    if (shutdown_event_.signaled()) {
      return;
    }

    ld_check(!state.promises_.empty());

    state.current_promise_.emplace(std::move(state.promises_.front()));
    state.promises_.pop();
  }

  // Allow background thread to make one iteration. The returned future will be
  // signaled when the iteration completes.
  std::future<void> backgroundThreadIteration(BackgroundThreadType type)
      __attribute__((__warn_unused_result__)) {
    auto& state = states_[(int)type];
    std::future<void> res;
    {
      std::lock_guard<std::mutex> lock(test_mutex_);
      std::promise<void> p;
      res = p.get_future();
      state.promises_.emplace(std::move(p));
    }
    state.cv_.notify_one();
    return res;
  }

  void performCompactionInternal(PartitionToCompact to_compact) override {
    std::future<void> future_to_wait;
    {
      std::lock_guard<std::mutex> lock(test_mutex_);
      future_to_wait = std::move(compaction_stall_future_);
    }
    if (future_to_wait.valid()) {
      future_to_wait.wait();
    }

    PartitionedRocksDBStore::performCompactionInternal(std::move(to_compact));
  }

  // Makes the next call to performCompactionInternal() stall until the returned
  // std::promise is signaled with set_value().
  std::promise<void> stallCompaction() {
    std::promise<void> p;
    {
      std::lock_guard<std::mutex> lock(test_mutex_);
      // Check that tests don't call stallCompaction() twice in a row without
      // a compaction in between.
      EXPECT_FALSE(compaction_stall_future_.valid());
      compaction_stall_future_ = p.get_future();
    }
    return p;
  }

  // Fake throttle state for unit tests.
  WriteThrottleState subclassSuggestedThrottleState() override {
    return std::max(suggested_throttle_state_,
                    PartitionedRocksDBStore::subclassSuggestedThrottleState());
  }

  WriteBufStats getLastFlushEvalStats() const {
    return last_flush_eval_stats_;
  }

  bool flushMetadataMemtables(bool wait = true) {
    ld_check(!getSettings()->read_only);
    ld_check(!immutable_.load());
    auto options = rocksdb::FlushOptions();
    options.wait = wait;
    rocksdb::Status status = db_->Flush(options, getMetadataCFHandle());
    enterFailSafeIfFailed(status, "Flush()");
    return status.ok();
  }

  std::mutex test_mutex_;
  State states_[(int)BackgroundThreadType::COUNT];
  std::future<void> compaction_stall_future_;

  SystemTimestamp* time_;
  WriteThrottleState suggested_throttle_state_ = WriteThrottleState::NONE;
};

using filter_history_t = std::vector<std::pair<std::string, std::string>>;

class TestCompactionFilter : public RocksDBCompactionFilter {
 public:
  TestCompactionFilter(
      StorageThreadPool* pool,
      UpdateableSettings<RocksDBSettings> settings,
      CompactionContext* context,
      LocalLogStoreUtils::TrimPointUpdateMap* out_trim_points,
      LocalLogStoreUtils::PerEpochLogMetadataTrimPointUpdateMap*
          out_metadata_trim_points,
      SystemTimestamp* time,
      std::chrono::microseconds sleep_per_record,
      std::atomic<size_t>* records_seen,
      std::atomic<bool>* stall,
      filter_history_t* history)
      : RocksDBCompactionFilter(pool, std::move(settings), context),
        out_trim_points_(out_trim_points),
        out_metadata_trim_points_(out_metadata_trim_points),
        time_(time),
        sleep_per_record_(sleep_per_record),
        total_records_seen_(records_seen),
        stall_(stall),
        history_(history) {
    start_time_ = SteadyTimestamp::now();
  }

  ~TestCompactionFilter() override {
    // This is the last chance to call TestCompactionFilter::flush() before
    // TestCompactionFilter is destroyed. Similar calls in
    // ~RocksDBCompactionFilter() will be non-virtual since TestCompactionFilter
    // will already have been destroyed.
    flushSkippedLogs();
    flushUpdatedTrimPoints();
  }

  void fakeFilter() const {
    ++records_seen_;
    ++*total_records_seen_;
    while (stall_->load()) {
      /* sleep override */
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    if (sleep_per_record_.count() > 0) {
      /* sleep override */
      sleep_until_safe(start_time_ + sleep_per_record_ * records_seen_);
    }
  }

 protected:
  Decision filterImpl(const rocksdb::Slice& key,
                      const rocksdb::Slice& value,
                      std::string* skip_until) override {
    if (history_ && context_) {
      history_->push_back(std::make_pair(key.ToString(), value.ToString()));
    }
    fakeFilter();
    return RocksDBCompactionFilter::filterImpl(key, value, skip_until);
  }

  std::chrono::milliseconds currentTime() override {
    ld_check(time_);
    return time_->toMilliseconds();
  }

  void flushSkippedLogs() override {
    // Store should have loaded all trim points before starting compaction.
    EXPECT_EQ(0, logs_skipped_.size());
    logs_skipped_.clear();
  }

  void flushUpdatedTrimPoints() override {
    ld_check(out_trim_points_);
    for (auto pt : next_trim_points_) {
      (*out_trim_points_)[pt.first] =
          std::max((*out_trim_points_)[pt.first], pt.second);
    }
    next_trim_points_.clear();

    ld_check(out_metadata_trim_points_);
    for (auto pt : next_metadata_trim_points_) {
      (*out_metadata_trim_points_)[pt.first] =
          std::max((*out_metadata_trim_points_)[pt.first], pt.second);
    }
    next_metadata_trim_points_.clear();
  }

 private:
  LocalLogStoreUtils::TrimPointUpdateMap* out_trim_points_;
  LocalLogStoreUtils::PerEpochLogMetadataTrimPointUpdateMap*
      out_metadata_trim_points_;
  SystemTimestamp* time_;
  std::chrono::microseconds sleep_per_record_;
  SteadyTimestamp start_time_;
  std::atomic<size_t>* total_records_seen_;
  std::atomic<bool>* stall_;
  mutable size_t records_seen_ = 0;
  mutable filter_history_t* history_;
};

class TestCompactionFilterFactory : public RocksDBCompactionFilterFactory {
 public:
  explicit TestCompactionFilterFactory(
      SystemTimestamp* time,
      UpdateableSettings<RocksDBSettings> settings)
      : RocksDBCompactionFilterFactory(std::move(settings)), time_(time) {}

  bool hasPendingTrimPointUpdates() const {
    return !trim_points_to_update_.empty() ||
        !metadata_trim_points_to_update_.empty();
  }

  // Applies trim point updates scheduled during compaction.
  // Outside tests this happens in a storage thread.
  void flushTrimPoints() {
    if (!hasPendingTrimPointUpdates()) {
      return;
    }

    ld_debug("Updating %lu trim points", trim_points_to_update_.size());

    auto pool = storage_thread_pool_.load();
    ld_check(pool != nullptr);

    int rv = LocalLogStoreUtils::updateTrimPoints(trim_points_to_update_,
                                                  &pool->getProcessor(),
                                                  pool->getLocalLogStore(),
                                                  /* sync */ false,
                                                  pool->stats());

    EXPECT_EQ(0, rv);

    trim_points_to_update_.clear();

    LocalLogStoreUtils::updatePerEpochLogMetadataTrimPoints(
        THIS_SHARD, metadata_trim_points_to_update_, &pool->getProcessor());

    metadata_trim_points_to_update_.clear();
  }

  void setSleepPerRecord(std::chrono::microseconds s) {
    sleep_per_record_ = s;
  }

  size_t getRecordsSeen() {
    return records_seen_.load();
  }

  void stall(bool s) {
    stall_.store(s);
  }

  // If not nullptr, this will be filled with key-values from calls to
  // filterImpl() for manual compactions (non-nullptr CompactionContext).
  filter_history_t* out_history_ = nullptr;

 protected:
  std::unique_ptr<rocksdb::CompactionFilter>
  createCompactionFilterImpl(StorageThreadPool* pool,
                             CompactionContext* context) override {
    return std::make_unique<TestCompactionFilter>(
        pool,
        settings_,
        context,
        &trim_points_to_update_,
        &metadata_trim_points_to_update_,
        time_,
        sleep_per_record_,
        &records_seen_,
        &stall_,
        out_history_);
  }

 protected:
  LocalLogStoreUtils::TrimPointUpdateMap trim_points_to_update_;
  LocalLogStoreUtils::PerEpochLogMetadataTrimPointUpdateMap
      metadata_trim_points_to_update_;
  SystemTimestamp* time_;
  std::chrono::microseconds sleep_per_record_{0};
  std::atomic<size_t> records_seen_{0};
  std::atomic<bool> stall_{false};
};

class TestReadFilter : public LocalLogStore::ReadFilter {
 public:
  struct Call {
    enum class Type {
      TIME_RANGE,
      RECORD_RANGE,
      CALL,
    };
    static constexpr int FULL_TIME_RANGE = -1;
    static constexpr int EMPTY_TIME_RANGE = -2;

    Type type;
    int copyset = -1;
    // min and max timestamps from shouldProcess{Time,Record}Range().
    // If [RecordTimestamp::min(), RecordTimestamp::max()],
    // min_ts = max_ts = FULL_TIME_RANGE.
    // If [RecordTimestamp::max(), RecordTimestamp::min()],
    // min_ts = max_ts = EMPTY_TIME_RANGE.
    int min_ts = -3, max_ts = -3; // only for TIME_RANGE
    bool exact_ts = false;        // only for CALL
    // Zero out the padding bytes to make the byte representation of the struct
    // deterministic, making gtest error output more readable.
    char padding[3] = {};
    // Only for RECORD_RANGE.
    logid_t log = LOGID_INVALID;
    lsn_t min_lsn = LSN_INVALID;
    lsn_t max_lsn = LSN_INVALID;

    static Call timeRange(int min, int max) {
      Call c;
      c.type = Type::TIME_RANGE;
      c.min_ts = min;
      c.max_ts = max;
      return c;
    }
    static Call recordRange(logid_t log, lsn_t min_lsn, lsn_t max_lsn) {
      Call c;
      c.type = Type::RECORD_RANGE;
      c.log = log;
      c.min_lsn = min_lsn;
      c.max_lsn = max_lsn;
      return c;
    }
    static Call call(int copyset, bool exact_ts = false) {
      Call c;
      c.type = Type::CALL;
      c.copyset = copyset;
      c.exact_ts = exact_ts;
      return c;
    }
    static Call fullRange() {
      return timeRange(FULL_TIME_RANGE, FULL_TIME_RANGE);
    }
    static Call emptyRange() {
      return timeRange(EMPTY_TIME_RANGE, EMPTY_TIME_RANGE);
    }
    bool operator==(const Call& c) const {
      return std::tie(type, copyset, min_ts, max_ts) ==
          std::tie(c.type, c.copyset, c.min_ts, c.max_ts);
    }
  };

  static int convertTs(RecordTimestamp ts) {
    return (int)(ts.toMilliseconds().count() - BASE_TIME);
  }

  bool operator()(logid_t,
                  lsn_t,
                  const ShardID* copyset,
                  const copyset_size_t,
                  const csi_flags_t,
                  RecordTimestamp min_ts,
                  RecordTimestamp max_ts) override {
    if (min_ts == max_ts) {
      // Filter was called with an exact timestamp. The timestamp should be
      // inside the range provided previously.
      EXPECT_GE(min_ts, prev_min_ts);
      EXPECT_LE(max_ts, prev_max_ts);
    } else {
      // Filter was called with a timestamp range. It should be the same range
      // that shouldProcessTimeRange() got.
      EXPECT_EQ(prev_min_ts, min_ts);
      EXPECT_EQ(prev_max_ts, max_ts);
    }

    // No need to put min_ts and max_ts into history because we've just
    // verified them.
    history.push_back(Call::call(copyset[0].node(), min_ts == max_ts));

    return records.count(copyset[0].node());
  }

  bool shouldProcessTimeRange(RecordTimestamp min_ts,
                              RecordTimestamp max_ts) override {
    prev_min_ts = min_ts;
    prev_max_ts = max_ts;
    int min, max;
    if (min_ts == RecordTimestamp::min() && max_ts == RecordTimestamp::max()) {
      min = max = Call::FULL_TIME_RANGE;
    } else if (min_ts == RecordTimestamp::max() &&
               max_ts == RecordTimestamp::min()) {
      min = max = Call::EMPTY_TIME_RANGE;
    } else {
      min = convertTs(min_ts);
      max = convertTs(max_ts);
    }
    history.push_back(Call::timeRange(min, max));
    return time_ranges.count(min);
  }

  bool shouldProcessRecordRange(logid_t log,
                                lsn_t min_lsn,
                                lsn_t max_lsn,
                                RecordTimestamp min_ts,
                                RecordTimestamp max_ts) override {
    // Time range should be the same range that shouldProcessTimeRange() got.
    EXPECT_EQ(prev_min_ts, min_ts);
    EXPECT_EQ(prev_max_ts, max_ts);

    history.push_back(Call::recordRange(log, min_lsn, max_lsn));
    return record_ranges.count(std::make_pair(log, min_lsn));
  }

  // operator() returns true for records whose copyset[0].node() is in this set.
  std::set<int> records;
  // shouldProcessTimeRange() returns true if `min - BASE_TIME` is in this set.
  std::set<int> time_ranges;
  // shouldProcessRecordRange() returns true if (log, min_lsn) is in this set.
  std::set<std::pair<logid_t, lsn_t>> record_ranges;

  std::vector<Call> history;

  RecordTimestamp prev_min_ts = RecordTimestamp::min();
  RecordTimestamp prev_max_ts = RecordTimestamp::max();
};

class TestRocksDBMemTableRep : public RocksDBMemTableRep {
 public:
  TestRocksDBMemTableRep(RocksDBMemTableRepFactory& factory,
                         std::unique_ptr<rocksdb::MemTableRep> wrapped,
                         rocksdb::Allocator* allocator,
                         uint32_t cf_id)
      : RocksDBMemTableRep(factory, std::move(wrapped), allocator, cf_id) {}

  rocksdb::KeyHandle Allocate(const size_t len, char** buf) override {
    if (allocate_forwarder) {
      rocksdb::KeyHandle handle =
          allocate_forwarder(column_family_id_, len, buf);
      if (handle) {
        return handle;
      }
    }
    return RocksDBMemTableRep::Allocate(len, buf);
  }

  static std::function<
      rocksdb::KeyHandle(uint32_t cf_id, const size_t len, char** buf)>
      allocate_forwarder;
};

std::function<rocksdb::KeyHandle(uint32_t cf_id, const size_t len, char** buf)>
    TestRocksDBMemTableRep::allocate_forwarder = nullptr;

class TestRocksDBMemTableRepFactory : public RocksDBMemTableRepFactory {
 public:
  explicit TestRocksDBMemTableRepFactory(
      std::unique_ptr<MemTableRepFactory> factory)
      : RocksDBMemTableRepFactory(nullptr, std::move(factory)) {}

  rocksdb::MemTableRep*
  CreateMemTableRep(const rocksdb::MemTableRep::KeyComparator& cmp,
                    rocksdb::Allocator* mta,
                    const rocksdb::SliceTransform* st,
                    rocksdb::Logger* logger,
                    uint32_t cf_id) override {
    std::unique_ptr<rocksdb::MemTableRep> wrapped_mtr(
        mtr_factory_->CreateMemTableRep(cmp, mta, st, logger, cf_id));
    return new TestRocksDBMemTableRep(
        *this, std::move(wrapped_mtr), mta, cf_id);
  }
};

std::ostream& operator<<(std::ostream& o, const TestReadFilter::Call& c) {
  using Type = TestReadFilter::Call::Type;
  switch (c.type) {
    case Type::TIME_RANGE:
      return o << "timeRange(" << c.min_ts << "," << c.max_ts << ")";
    case Type::RECORD_RANGE:
      return o << "recordRange(L" << c.log.val() << "," << c.min_lsn << ","
               << c.max_lsn << ")";
    case Type::CALL:
      return o << "call(" << c.copyset << "," << c.exact_ts << ")";
  }
  ld_check(false);
  return o;
}

class PartitionedRocksDBStoreTest : public ::testing::Test {
 public:
  PartitionedRocksDBStoreTest()
      : settings_(create_default_settings<Settings>()),
        server_settings_(create_default_settings<ServerSettings>()),
        stats_(StatsParams().setIsServer(true)) {}
  ~PartitionedRocksDBStoreTest() override {}

 protected:
  struct TestRecord {
    enum class Type {
      PUT,
      DELETE,
    };

    enum class StoreType { APPEND, REBUILD, RECOVERY };

    Type type = Type::PUT;
    StoreType store_type = StoreType::APPEND;
    logid_t logid;
    lsn_t lsn;
    uint64_t timestamp;
    Durability durability = Durability::ASYNC_WRITE;
    bool index = false;
    bool is_amend = false;
    std::map<KeyType, std::string> optional_keys;
    std::string additional_payload;

    TestRecord() {}
    TestRecord(logid_t logid,
               lsn_t lsn,
               uint64_t timestamp = 0,
               std::string additional_payload = "")
        : logid(logid),
          lsn(lsn),
          timestamp(timestamp),
          additional_payload(additional_payload) {}
    // For backward compatibility, the existing constructor is kept but
    // adjusted to use map. For forward compatibility, TestRecord constructor
    // should support std::map<KeyType, std::string> optional_keys.
    TestRecord(logid_t logid,
               lsn_t lsn,
               Type type,
               std::map<KeyType, std::string> optional_keys =
                   std::map<KeyType, std::string>())
        : type(type), logid(logid), lsn(lsn), optional_keys(optional_keys) {}
    TestRecord(logid_t logid,
               lsn_t lsn,
               bool index,
               uint64_t timestamp,
               std::map<KeyType, std::string> optional_keys =
                   std::map<KeyType, std::string>())
        : logid(logid),
          lsn(lsn),
          timestamp(timestamp),
          index(index),
          optional_keys(optional_keys) {}
    TestRecord(logid_t logid,
               lsn_t lsn,
               Durability d,
               StoreType st,
               uint64_t timestamp = 0,
               std::map<KeyType, std::string> optional_keys =
                   std::map<KeyType, std::string>())
        : store_type(st),
          logid(logid),
          lsn(lsn),
          timestamp(timestamp),
          durability(d),
          optional_keys(optional_keys) {}
    TestRecord(logid_t logid,
               lsn_t lsn,
               Type type,
               folly::Optional<std::string> key)
        : type(type), logid(logid), lsn(lsn) {
      if (key.hasValue()) {
        optional_keys.insert(std::make_pair(KeyType::FINDKEY, key.value()));
      }
    }
    TestRecord(logid_t logid,
               lsn_t lsn,
               bool index,
               uint64_t timestamp,
               folly::Optional<std::string> key)
        : logid(logid), lsn(lsn), timestamp(timestamp), index(index) {
      if (key.hasValue()) {
        optional_keys.insert(std::make_pair(KeyType::FINDKEY, key.value()));
      }
    }
    TestRecord(logid_t logid,
               lsn_t lsn,
               Durability d,
               StoreType st,
               uint64_t timestamp,
               folly::Optional<std::string> key)
        : store_type(st),
          logid(logid),
          lsn(lsn),
          timestamp(timestamp),
          durability(d) {
      if (key.hasValue()) {
        optional_keys.insert(std::make_pair(KeyType::FINDKEY, key.value()));
      }
    }
    TestRecord& amend() {
      is_amend = true;
      return *this;
    }
    bool operator<(const TestRecord& rhs) const {
      return std::tie(logid, lsn) < std::tie(rhs.logid, rhs.lsn);
    }
  };

  struct LogData {
    lsn_t first_lsn = LSN_INVALID;
    lsn_t max_lsn = LSN_INVALID;
    uint64_t timestamp = 0;
    std::vector<lsn_t> records;
  };

  using PartitionData = std::map<logid_t, LogData>;
  using DBData = std::vector<PartitionData>;
  using PartitionOrphans = std::map<logid_t, std::vector<lsn_t>>;
  using OrphanedData = std::map<partition_id_t, PartitionOrphans>;
  using LsnToPartitionMap = std::map<lsn_t, partition_id_t>;
  using PartitionSet = std::set<partition_id_t>;

  void SetUp() override {
    dbg::currentLevel = getLogLevelFromEnv().value_or(dbg::Level::INFO);
    dbg::assertOnData = true;

    // Initialize a processor with a config.
    // 1-day retention for logs 1-99,
    // 2-day retention for logs 100-199,
    // 3-day retention for logs 200-299,
    // 7-day retention for logs 300-399,
    // Infinite retention for logs 400-409.
    auto logs_config = std::make_shared<configuration::LocalLogsConfig>();
    logsconfig::LogAttributes log_attrs;

    log_attrs.set_backlogDuration(std::chrono::seconds(3600 * 24 * 1));
    log_attrs.set_replicationFactor(3);
    logs_config->insert(
        boost::icl::right_open_interval<logid_t::raw_type>(1, 100),
        "lg1",
        log_attrs);
    log_attrs.set_backlogDuration(std::chrono::seconds(3600 * 24 * 2));
    logs_config->insert(
        boost::icl::right_open_interval<logid_t::raw_type>(100, 200),
        "lg2",
        log_attrs);
    log_attrs.set_backlogDuration(std::chrono::seconds(3600 * 24 * 3));
    logs_config->insert(
        boost::icl::right_open_interval<logid_t::raw_type>(200, 300),
        "lg3",
        log_attrs);

    log_attrs.set_backlogDuration(std::chrono::seconds(3600 * 24 * 7));
    logs_config->insert(
        boost::icl::right_open_interval<logid_t::raw_type>(300, 400),
        "logs_can_disappear",
        log_attrs);

    log_attrs.set_backlogDuration(folly::none);
    logs_config->insert(
        boost::icl::right_open_interval<logid_t::raw_type>(400, 410),
        "infinite_retention",
        log_attrs);

    // Event log.
    log_attrs.set_backlogDuration(folly::none);
    log_attrs.set_replicationFactor(3);
    configuration::InternalLogs internal_logs;
    ld_check(internal_logs.insert("event_log_deltas", log_attrs));

    logs_config->setInternalLogsConfig(internal_logs);
    auto updateable_config = UpdateableConfig::createEmpty();
    logs_config->markAsFullyLoaded();
    updateable_config->updateableLogsConfig()->update(std::move(logs_config));
    settings_.server = true;
    auto updateable_settings = UpdateableSettings<Settings>(settings_);
    GossipSettings gossip_settings = create_default_settings<GossipSettings>();
    gossip_settings.enabled = false; // we don't need failure detector
    UpdateableSettings<GossipSettings> ugossip_settings(
        std::move(gossip_settings));
    processor_ = ServerProcessor::createNoInit(
        /* audit log */ nullptr,
        /* sharded storage thread pool */ nullptr,
        UpdateableSettings<ServerSettings>(
            create_default_settings<ServerSettings>()),
        std::move(ugossip_settings),
        UpdateableSettings<AdminServerSettings>(
            create_default_settings<AdminServerSettings>()),
        updateable_config,
        updateable_settings,
        true,
        300,
        &stats_);

    temp_dir_ = createTemporaryDir("TemporaryLogStore");
    ld_check(temp_dir_);
    path_ = temp_dir_->path().string();
    openStore();
    ld_debug("initialized temporary log store in %s", path_.c_str());

    std::string buf;
    std::vector<StoreChainLink> chain = formCopySet();
    STORE_Header hdr = formStoreHeader(0, chain.size());
    Slice header = formRecordHeader(
        buf, hdr, chain.data(), std::map<KeyType, std::string>());
    header_size_ = header.size;
  }

  void TearDown() override {
    closeStore();
  }

  virtual ServerConfig::SettingsConfig getDefaultSettingsOverrides() {
    ServerConfig::SettingsConfig s;
    s["rocksdb-partition-duration"] = "0s";
    s["rocksdb-partition-compactions-enabled"] = "true";
    s["rocksdb-allow-fallocate"] = "false";
    s["rocksdb-low-ioprio"] = "any";
    s["rocksdb-new-partition-timestamp-margin"] = "0s";
    return s;
  }

  void openStore(
      ServerConfig::SettingsConfig overrides = ServerConfig::SettingsConfig(),
      std::function<void(RocksDBLogStoreConfig&)> customize_config_fn =
          nullptr) {
    if (store_) {
      return;
    }

    // Env is required for certain configuration of RocksDB stores for
    // filesystem operations, and may be precreated by tests.
    if (!env_) {
      rocksdb_settings_ = UpdateableSettings<RocksDBSettings>(
          RocksDBSettings::defaultTestSettings());
      settings_updater_ = std::make_unique<SettingsUpdater>();
      settings_updater_->registerSettings(rocksdb_settings_);

      settings_overrides_.clear();
      settings_overrides_ = getDefaultSettingsOverrides();
      for (auto s : overrides) {
        settings_overrides_[s.first] = s.second;
      }
      settings_updater_->setFromCLI(settings_overrides_);
    }
    auto log_store_config =
        RocksDBLogStoreConfig(rocksdb_settings_,
                              UpdateableSettings<RebuildingSettings>(),
                              env_.get(),
                              nullptr,
                              &stats_);
    log_store_config.createMergeOperator(THIS_SHARD);
    ASSERT_FALSE(log_store_config.options_.allow_fallocate);
    log_store_config.options_.statistics = rocksdb::CreateDBStatistics();

    if (customize_config_fn) {
      customize_config_fn(log_store_config);
    }

    ++num_opens;

    filter_factory_ = std::make_shared<TestCompactionFilterFactory>(
        &time_, log_store_config.rocksdb_settings_);
    log_store_config.options_.compaction_filter_factory = filter_factory_;

    mtr_factory_ = std::make_shared<TestRocksDBMemTableRepFactory>(
        std::make_unique<rocksdb::SkipListFactory>(
            rocksdb_settings_->skip_list_lookahead));

    log_store_config.options_.memtable_factory =
        log_store_config.metadata_options_.memtable_factory = mtr_factory_;

    store_ = std::make_unique<TestPartitionedRocksDBStore>(
        path_, std::move(log_store_config), config_.get(), &stats_, &time_);

    Params params;
    params[(size_t)StorageTaskThreadType::SLOW].nthreads = 1;

    storage_thread_pool_ = std::make_unique<StorageThreadPool>(
        /* shard_idx */ 0,
        /* num_shards */ 1,
        params,
        UpdateableSettings<ServerSettings>(server_settings_),
        UpdateableSettings<Settings>(settings_),
        store_.get(),
        1,
        &stats_);
    storage_thread_pool_->setProcessor(processor_.get());

    filter_factory_->setStorageThreadPool(storage_thread_pool_.get());
    store_->setProcessor(processor_.get());
  }

  void updateSetting(const std::string& name, const std::string& val) {
    settings_overrides_[name] = val;
    settings_updater_->setFromConfig(settings_overrides_);
  }

  void setTime(uint64_t t) {
    time_ = SystemTimestamp(std::chrono::milliseconds(t));
  }

  void closeStore() {
    // First shutdown the storage_thread_pool (which
    // references local_log_store_ during shutdown.
    if (storage_thread_pool_) {
      storage_thread_pool_->shutDown();
      storage_thread_pool_->join();
    }

    mtr_factory_.reset();
    // Then destroy store. It has to be destroyed before
    // storage_thread_pool_, as it may still reference it.
    store_.reset();
    filter_factory_.reset();

    storage_thread_pool_.reset();
    settings_updater_.reset();
    env_.reset();
  }

  void openStoreWithReadFindTimeIndex() {
    closeStore();
    ServerConfig::SettingsConfig s;
    s["rocksdb-read-find-time-index"] = "true";
    openStore(s);
  }

  void openStoreWithoutLDManagedFlushes() {
    closeStore();
    ServerConfig::SettingsConfig s;
    s["rocksdb-ld-managed-flushes"] = "false";
    openStore(s);
  }

  std::vector<rocksdb::ColumnFamilyDescriptor>
  createCFDescriptors(const std::vector<std::string>& cf_names,
                      const rocksdb::Options& base_options) {
    std::vector<rocksdb::ColumnFamilyDescriptor> cf_descriptors;
    for (const auto& name : cf_names) {
      rocksdb::ColumnFamilyOptions cf_options(base_options);
      if (name == PartitionedRocksDBStore::METADATA_CF_NAME) {
        cf_options.merge_operator.reset(
            new PartitionedRocksDBStore::MetadataMergeOperator);
      } else {
        cf_options.merge_operator.reset(
            new RocksDBWriterMergeOperator(THIS_SHARD));
      }
      cf_descriptors.emplace_back(name, cf_options);
    }
    return cf_descriptors;
  }

  void openDirectly(
      std::unique_ptr<rocksdb::DB>& out_db,
      std::map<std::string, std::unique_ptr<rocksdb::ColumnFamilyHandle>>&
          out_cfs) {
    RocksDBSettings settings = RocksDBSettings::defaultTestSettings();
    rocksdb::Options options = settings.passThroughRocksDBOptions();
    std::vector<std::string> column_families;
    rocksdb::Status status =
        rocksdb::DB::ListColumnFamilies(options, path_, &column_families);
    ASSERT_TRUE(status.ok());

    // Sort column families so that partition IDs are increasing.
    std::sort(column_families.begin(),
              column_families.end(),
              [](const std::string& a, const std::string& b) -> bool {
                // Compare lengths first, then strings.
                // For numbers it's equivalent to numeric comparison.
                return std::forward_as_tuple(a.size(), a) <
                    std::forward_as_tuple(b.size(), b);
              });

    std::vector<rocksdb::ColumnFamilyDescriptor> cf_descriptors =
        createCFDescriptors(column_families, options);
    std::vector<rocksdb::ColumnFamilyHandle*> cf_handles_raw;
    rocksdb::DB* db;

    status =
        rocksdb::DB::Open(options, path_, cf_descriptors, &cf_handles_raw, &db);

    ASSERT_TRUE(status.ok());
    out_db.reset(db);

    for (auto handle : cf_handles_raw) {
      out_cfs[handle->GetName()].reset(handle);
    }
  }

  lsn_t getTrimPoint(logid_t log_id) {
    auto& state_map = processor_->getLogStorageStateMap();
    auto state = state_map.find(log_id, THIS_SHARD);
    if (!state) {
      // No need to recover trim point from TrimMetadata because Processor
      // lifetime is a superset of DB lifelime.
      return LSN_INVALID;
    }
    return state->getTrimPoint().value_or(LSN_INVALID);
  }

  epoch_t getPerEpochLogMetadataTrimPoint(logid_t log_id) {
    auto& state_map = processor_->getLogStorageStateMap();
    auto state = state_map.find(log_id, THIS_SHARD);
    if (!state) {
      return EPOCH_INVALID;
    }
    auto mtp = state->getPerEpochLogMetadataTrimPoint();
    return mtp.hasValue() ? mtp.value() : EPOCH_INVALID;
  }

  int updateTrimPoint(logid_t log_id, lsn_t trim_point) {
    auto& state_map = processor_->getLogStorageStateMap();
    auto state = state_map.insertOrGet(log_id, THIS_SHARD);
    if (!state) {
      return -1;
    }
    return state->updateTrimPoint(trim_point);
  }

  bool shardIdInCopySet() const {
    return settings_.write_shard_id_in_copyset;
  }

  STORE_Header formStoreHeader(uint64_t timestamp, size_t copyset_size) {
    STORE_Header hdr;
    hdr.rid = RecordID(LSN_INVALID, LOGID_INVALID);
    hdr.timestamp = timestamp;
    hdr.last_known_good = esn_t(0);
    hdr.wave = 1;
    hdr.flags = STORE_Header::CHECKSUM_PARITY;
    hdr.copyset_size = copyset_size;
    return hdr;
  }

  Slice formRecordHeader(std::string& buf,
                         const STORE_Header& hdr,
                         StoreChainLink* copyset,
                         std::map<KeyType, std::string> optional_keys) {
    return LocalLogStoreRecordFormat::formRecordHeader(
        hdr, copyset, &buf, shardIdInCopySet(), optional_keys);
  }

  std::vector<StoreChainLink> formCopySet() {
    std::vector<StoreChainLink> chain;
    for (node_index_t index = 1; index <= 4; ++index) {
      chain.push_back(StoreChainLink{ShardID(index, 0), ClientID::INVALID});
    }
    return chain;
  }

  // Payload is some function of (logid,lsn,timestamp), so that it can be
  // easily checked.
  void put(std::vector<TestRecord> ops,
           std::vector<StoreChainLink> csi_copyset = {},
           bool assert_write_fail = false,
           DataKeyFormat key_format = DataKeyFormat::DEFAULT) {
    std::vector<PutWriteOp> put_write_ops(ops.size());
    std::vector<DeleteWriteOp> delete_write_ops(ops.size());
    std::vector<const WriteOp*> op_ptrs(ops.size());

    std::vector<std::string> headers(ops.size());
    std::vector<std::string> payloads(ops.size());
    std::vector<std::string> copyset_index_entries(ops.size());
    if (csi_copyset.empty()) {
      csi_copyset = formCopySet();
    }

    if (key_format != DataKeyFormat::DEFAULT) {
      have_old_data_key_format_ = true;
    }

    for (size_t i = 0; i < ops.size(); ++i) {
      if (ops[i].type == TestRecord::Type::PUT) {
        if (timestamps_are_lsns_) {
          if (ops[i].timestamp == 0) {
            ops[i].timestamp = FUTURE + ops[i].lsn * 2;
          } else {
            timestamps_are_lsns_ = false;
          }
        }
        if (!ops[i].additional_payload.empty()) {
          no_additional_payloads_ = false;
        }
        if (!ops[i].is_amend) {
          payloads[i] =
              formTestPayload(ops[i].logid, ops[i].lsn, ops[i].timestamp) +
              ops[i].additional_payload;
        }

        STORE_Header hdr =
            formStoreHeader(ops[i].timestamp, csi_copyset.size());
        switch (ops[i].store_type) {
          case TestRecord::StoreType::APPEND:
            break;
          case TestRecord::StoreType::REBUILD:
            hdr.flags |= STORE_Header::REBUILDING;
            break;
          case TestRecord::StoreType::RECOVERY:
            hdr.flags |= STORE_Header::WRITTEN_BY_RECOVERY;
            break;
        }
        if (ops[i].is_amend) {
          hdr.flags |= STORE_Header::AMEND;
        }
        Slice header = formRecordHeader(
            headers[i], hdr, csi_copyset.data(), ops[i].optional_keys);
        Slice payload(payloads[i].data(), payloads[i].size());
        Slice csi_entry = LocalLogStoreRecordFormat::formCopySetIndexEntry(
            hdr,
            STORE_Extra(),
            csi_copyset.data(),
            LSN_INVALID,
            shardIdInCopySet(),
            &copyset_index_entries[i]);

        put_write_ops[i] =
            PutWriteOp(ops[i].logid,
                       ops[i].lsn,
                       // Slice(&ops[i].timestamp, sizeof(uint64_t)),
                       header,
                       payload,
                       /*coordinator*/ node_index_t(1),
                       LSN_INVALID,
                       csi_entry,
                       {},
                       ops[i].durability,
                       ops[i].store_type == TestRecord::StoreType::REBUILD);
        put_write_ops[i].TEST_data_key_format = key_format;

        if (ops[i].index) {
          uint64_t timestamp_big_endian = htobe64(ops[i].timestamp);
          std::string timestamp_key(
              reinterpret_cast<const char*>(&timestamp_big_endian),
              sizeof(uint64_t));
          put_write_ops[i].index_key_list.emplace_back(
              FIND_TIME_INDEX, std::move(timestamp_key));
        }
        if (ops[i].optional_keys.find(KeyType::FINDKEY) !=
            ops[i].optional_keys.end()) {
          put_write_ops[i].index_key_list.emplace_back(
              FIND_KEY_INDEX, ops[i].optional_keys.at(KeyType::FINDKEY));
        }

        op_ptrs[i] = &put_write_ops[i];
      } else {
        delete_write_ops[i] = DeleteWriteOp(ops[i].logid, ops[i].lsn);
        op_ptrs[i] = &delete_write_ops[i];
      }
    }

    ASSERT_EQ(assert_write_fail ? -1 : 0,
              store_->writeMulti(op_ptrs, LocalLogStore::WriteOptions()))
        << error_name(err);
  }

  // Extracts the wave number from the row value.
  uint32_t extractWave(const rocksdb::Iterator& it) {
    Slice value_slice(it.value().data(), it.value().size());
    uint32_t wave;
    int rv = LocalLogStoreRecordFormat::parse(value_slice,
                                              nullptr,
                                              nullptr,
                                              nullptr,
                                              &wave,
                                              nullptr,
                                              nullptr,
                                              0,
                                              nullptr,
                                              nullptr,
                                              nullptr,
                                              THIS_SHARD);
    ld_check(rv == 0);
    return wave;
  }

  void verifyRecord(logid_t log, lsn_t lsn, Slice record) {
    std::chrono::milliseconds timestamp;
    Payload payload;
    LocalLogStoreRecordFormat::flags_t flags;
    int rv = LocalLogStoreRecordFormat::parse(record,
                                              &timestamp,
                                              nullptr,
                                              &flags,
                                              nullptr,
                                              nullptr,
                                              nullptr,
                                              0,
                                              nullptr,
                                              nullptr,
                                              &payload,
                                              THIS_SHARD);
    ASSERT_EQ(0, rv);

    if (timestamps_are_lsns_) {
      EXPECT_EQ((uint64_t)timestamp.count(), FUTURE + lsn * 2);
    }

    bool is_amend = flags & LocalLogStoreRecordFormat::FLAG_AMEND;
    if (is_amend) {
      EXPECT_EQ(0, payload.size());
    } else {
      std::string expected_payload =
          formTestPayload(log, lsn, (uint64_t)timestamp.count());
      if (no_additional_payloads_) {
        EXPECT_EQ(expected_payload, payload.toString());
      } else {
        EXPECT_EQ(expected_payload,
                  payload.toString().substr(0, expected_payload.size()));
      }
    }
  }

  // Writes a partition directory entry via the raw database.
  void putDirectoryEntry(PartitionDirectoryKey& key,
                         PartitionDirectoryValue& value) {
    closeStore();

    std::unique_ptr<rocksdb::DB> db;
    std::map<std::string, std::unique_ptr<rocksdb::ColumnFamilyHandle>> cfs;
    openDirectly(db, cfs);

    auto& metadata_cf = cfs["metadata"];
    ASSERT_NE(metadata_cf, nullptr);

    rocksdb::WriteBatch batch;
    Slice value_slice = value.serialize();
    batch.Put(metadata_cf.get(),
              rocksdb::Slice(reinterpret_cast<const char*>(&key), sizeof(key)),
              rocksdb::Slice(reinterpret_cast<const char*>(value_slice.data),
                             value_slice.size));

    db->Write(rocksdb::WriteOptions(), &batch);
  }

  // Reads directly from RocksDB and checks that
  //  - lsn ranges don't intersect among partitions,
  //  - min_lsn metadata is correct for all partitions,
  //  - payloads are correct.
  //  - verifies the set of orphaned lsns (lsns for which the maxLSN update
  //    in the partition directory was lost due to a crash) matches the
  //    passed in value (0 if expected_orphans is nullptr).
  DBData readAndCheck(OrphanedData* expected_orphans = nullptr) {
    closeStore();

    std::vector<std::string> cfs;
    EXPECT_TRUE(
        rocksdb::DB::ListColumnFamilies(rocksdb::Options(), path_, &cfs).ok());
    EXPECT_GE(cfs.size(), 3);

    // Compare lengths first, so that numbers are sorted in increasing order.
    std::sort(cfs.begin(),
              cfs.end(),
              [](const std::string& a, const std::string& b) -> bool {
                if (a.size() != b.size()) {
                  return a.size() < b.size();
                }
                return a < b;
              });

    size_t uniques = std::unique(cfs.begin(), cfs.end()) - cfs.begin();
    EXPECT_EQ(cfs.size(), uniques);

    EXPECT_EQ("unpartitioned", cfs[cfs.size() - 1]);
    EXPECT_EQ("metadata", cfs[cfs.size() - 2]);
    EXPECT_EQ("default", cfs[cfs.size() - 3]);

    // Check that partitions are numbererd consecutively.
    int first_partition = stoi(cfs[0]);
    int partition_count = (int)cfs.size() - 3;
    for (int i = 0; i < partition_count; ++i) {
      EXPECT_EQ(std::to_string(first_partition + i), cfs[i]);
    }

    // Open DB.
    std::unique_ptr<rocksdb::DB> db;
    std::vector<std::unique_ptr<rocksdb::ColumnFamilyHandle>> cf_handles;
    {
      // build a list of CF descriptors
      std::vector<rocksdb::ColumnFamilyDescriptor> cf_descriptors =
          createCFDescriptors(cfs, rocksdb::Options());
      std::vector<rocksdb::ColumnFamilyHandle*> raw_cf_handles;
      rocksdb::DB* raw_db;

      rocksdb::Status status = rocksdb::DB::OpenForReadOnly(
          rocksdb::Options(), path_, cf_descriptors, &raw_cf_handles, &raw_db);

      EXPECT_TRUE(status.ok());
      if (!status.ok()) {
        return DBData();
      }

      db.reset(raw_db);
      for (auto h : raw_cf_handles) {
        cf_handles.emplace_back(h);
      }
    }
    auto metadata_cf = cf_handles[cfs.size() - 2].get();

    // Read all data.

    DataKey first_key = DataKey(logid_t(0), 0);
    rocksdb::Slice first_key_slice = first_key.sliceForForwardSeek();

    DBData db_data(partition_count);
    OrphanedData orphans;
    std::set<logid_t> all_logids;
    std::unordered_map<logid_t, lsn_t, logid_t::Hash> max_lsns;

    for (size_t i = 0; i < partition_count; ++i) {
      std::unique_ptr<rocksdb::Iterator> it(db->NewIterator(
          RocksDBLogStoreBase::getDefaultReadOptions(), cf_handles[i].get()));
      for (it->Seek(first_key_slice); it->Valid(); it->Next()) {
        if (!have_old_data_key_format_) {
          EXPECT_EQ(DataKey::sizeForWriting(), it->key().size());
        }
        EXPECT_TRUE(DataKey::valid(it->key().data(), it->key().size()));
        EXPECT_EQ(1, extractWave(*it));

        logid_t logid = DataKey::getLogID(it->key().data());
        lsn_t lsn = DataKey::getLSN(it->key().data());

        verifyRecord(logid, lsn, Slice(it->value().data(), it->value().size()));

        db_data[i][logid].records.push_back(lsn);
        all_logids.insert(logid);
        max_lsns[logid] = std::max(max_lsns[logid], lsn);
      }
      EXPECT_TRUE(it->status().ok());
    }

    // Read partition directory.
    std::unique_ptr<rocksdb::Iterator> it(db->NewIterator(
        RocksDBLogStoreBase::getDefaultReadOptions(), metadata_cf));
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      if (it->key().size() == 0 ||
          it->key().data()[0] != PartitionDirectoryKey::HEADER) {
        continue;
      }
      if (!PartitionDirectoryKey::valid(it->key().data(), it->key().size())) {
        ADD_FAILURE() << hexdump_buf(it->key().data(), it->key().size());
        continue;
      }
      if (!PartitionDirectoryValue::valid(
              it->value().data(), it->value().size())) {
        ADD_FAILURE();
        return {};
      }

      logid_t logid = PartitionDirectoryKey::getLogID(it->key().data());
      lsn_t lsn = PartitionDirectoryKey::getLSN(it->key().data());
      partition_id_t partition =
          PartitionDirectoryKey::getPartition(it->key().data());
      lsn_t max_lsn = PartitionDirectoryValue::getMaxLSN(
          it->value().data(), it->value().size());
      ld_spew("Directory entry: log %lu, min lsn %s, p %lu, max lsn %s",
              logid.val_,
              lsn_to_string(lsn).c_str(),
              partition,
              lsn_to_string(max_lsn).c_str());

      if (partition < first_partition ||
          partition >= first_partition + partition_count) {
        ADD_FAILURE() << first_partition << ' ' << partition << ' '
                      << first_partition + partition_count - 1;
        return DBData();
      }

      partition -= first_partition;
      EXPECT_EQ(LSN_INVALID, db_data[partition][logid].first_lsn);
      db_data[partition][logid].first_lsn = lsn;
      db_data[partition][logid].max_lsn = max_lsn;

      // NOTE: due to trimming, directory can have logids that no partition has.
      all_logids.insert(logid);
    }
    EXPECT_TRUE(it->status().ok());

    // Check that lsn ranges don't intersect and that first_lsns are correct.
    for (logid_t logid : all_logids) {
      lsn_t trim = getTrimPoint(logid);
      lsn_t max_seen = LSN_INVALID;
      for (int i = 0; i < partition_count; ++i) {
        if (!db_data[i].count(logid)) {
          continue;
        }

        auto& recs = db_data[i][logid].records;
        lsn_t first_lsn = db_data[i][logid].first_lsn;
        lsn_t max_lsn = db_data[i][logid].max_lsn;
        EXPECT_GT(first_lsn, max_seen);
        EXPECT_GE(max_lsn, first_lsn);
        bool have_data = !recs.empty();

        auto have_orphans = [&]() {
          return expected_orphans != nullptr &&
              expected_orphans->count(i) != 0 &&
              (*expected_orphans)[i].count(logid) != 0;
        };

        if (no_deletes_ && max_lsn > trim) {
          // The only known way to end up with partition that has directory
          // entry but no data for the log is to delete all records one by one.
          EXPECT_TRUE(have_data);
        }

        EXPECT_GT(first_lsn, max_seen)
            << logid.val_ << ' ' << first_partition + i << ' ' << first_lsn
            << ' ' << max_seen;

        if (single_threaded_ && no_deletes_) {
          // There's data iff there's a directory entry.
          EXPECT_EQ(have_data, max_lsn != LSN_INVALID)
              << logid.val_ << ' ' << i << ' ' << partition_count << ' '
              << first_lsn << ' ' << max_lsn << ' ' << recs.size();
        }

        if (have_data) {
          if (no_deletes_ && aligned_trims_ &&
              (first_lsn > trim || single_threaded_)) {
            EXPECT_EQ(first_lsn, recs[0])
                << logid.val_ << ' ' << first_partition + i;
          } else {
            EXPECT_LE(first_lsn, recs[0]);
          }

          if (!have_orphans()) {
            if (no_deletes_ &&
                (std::max(recs.back(), max_lsn) > trim || single_threaded_)) {
              EXPECT_EQ(max_lsn, recs.back());
            } else {
              EXPECT_GE(max_lsn, recs.back());
            }
          }
        }

        if (have_orphans()) {
          // Record and discard records that we expect the store's iterator
          // to omit due to inconsistent max lsn metadata.
          auto it = recs.begin();
          while (it != recs.end() && *it <= max_lsn) {
            ++it;
          }
          if (it != recs.end()) {
            auto part = orphans.insert({i, PartitionOrphans()});
            auto log = part.first->second.insert(
                {logid, std::vector<lsn_t>(it, recs.end())});
            ld_check(log.second);
            recs.erase(it, recs.end());
          }
        }
        max_seen = max_lsn;
      }
    }

    if (expected_orphans != nullptr) {
      EXPECT_EQ(*expected_orphans, orphans);
    } else {
      EXPECT_TRUE(orphans.empty());
    }

    return db_data;
  }

  void populateUnderReplicatedStore(logid_t logid,
                                    LsnToPartitionMap& lsns,
                                    PartitionSet& under_replicated_partitions,
                                    partition_id_t cur_partition,
                                    partition_id_t max_partition) {
    for (auto lsn_kv : lsns) {
      while (cur_partition != lsn_kv.second) {
        if (under_replicated_partitions.count(cur_partition)) {
          store_->setUnderReplicated(store_->getLatestPartition());
        }
        store_->createPartition();
        ++cur_partition;
      }
      put({TestRecord(logid, lsn_kv.first)});
    }

    while (cur_partition <= max_partition) {
      if (under_replicated_partitions.count(cur_partition)) {
        store_->setUnderReplicated(store_->getLatestPartition());
      }
      store_->createPartition();
      ++cur_partition;
    }
  }

  bool firstLsnInPartition(LsnToPartitionMap& map, lsn_t lsn) {
    auto kv_it = map.find(lsn);
    ld_check(kv_it != map.end());
    partition_id_t id = kv_it->second;
    return (kv_it == map.begin() || (--kv_it)->second != id);
  };

  static size_t numRecords(const DBData& data) {
    size_t res = 0;
    for (const auto& partition : data) {
      for (const auto& log : partition) {
        res += log.second.records.size();
      }
    }
    return res;
  }

  std::unique_ptr<folly::test::TemporaryDirectory> temp_dir_;
  std::string path_;

  Settings settings_;
  ServerSettings server_settings_;

  std::shared_ptr<Configuration> config_;

  // A dummy processor. Only LogStorageStates and Configuration are used.
  std::unique_ptr<ServerProcessor> processor_;
  // A dummy storage thread pool. Doesn't actually run any tasks.
  std::unique_ptr<StorageThreadPool> storage_thread_pool_;

  std::shared_ptr<TestCompactionFilterFactory> filter_factory_;
  StatsHolder stats_;
  std::unique_ptr<TestPartitionedRocksDBStore> store_;
  // to remain in scope for store_ destructor when sst_file_manager used
  std::unique_ptr<RocksDBEnv> env_;

  // dummy factory that allows to peek into allocation deallocation etc.
  std::shared_ptr<TestRocksDBMemTableRepFactory> mtr_factory_;

  // Fake current time.
  SystemTimestamp time_{std::chrono::milliseconds(BASE_TIME)};

  // If true, put() uses lsn*2 as timestamp, and readAndCheck() uses this to
  // do additional checks. Automatically set to false when a record with
  // non-default timestamp is written.
  bool timestamps_are_lsns_ = true;

  // There may be amends without corresponding full records.
  bool have_dangling_amends_ = false;

  // There may be DataKey-s in old format.
  bool have_old_data_key_format_ = false;

  // If true, payloads consist of just log ID, LSN and timestamp.
  bool no_additional_payloads_ = true;

  // If true, readAndCheck() does additional checks assuming that all
  // writes, createPartition()s and compactions were from a single thread.
  bool single_threaded_ = true;

  // If true, readAndCheck() assumes that records for each log were written in
  // order of increasing lsn.
  bool increasing_lsns_ = true;

  // If true, we never do DELETE ops.
  bool no_deletes_ = true;

  // If true, trim points are only moved to partition boundaries.
  // I.e. either all or none of the records for each partition+log are trimmed.
  bool aligned_trims_ = true;

  // How many times we (re-)opened the store.
  size_t num_opens = 0;

  // Header size in local log store
  size_t header_size_;

  std::unique_ptr<SettingsUpdater> settings_updater_;
  ServerConfig::SettingsConfig settings_overrides_;
  UpdateableSettings<RocksDBSettings> rocksdb_settings_;

 private:
  // Timer to kill the test if it takes too long
  Alarm alarm_{DEFAULT_TEST_TIMEOUT};
};

static rocksdb::Slice convertSlice(Slice s) {
  return rocksdb::Slice(reinterpret_cast<const char*>(s.data), s.size);
}

static lsn_t lsn(int epoch, int esn) {
  return compose_lsn(epoch_t(epoch), esn_t(esn));
}

// Write one record.
TEST_F(PartitionedRocksDBStoreTest, OneRecord) {
  put({TestRecord(logid_t(1), 1)});
  EXPECT_EQ(1, numRecords(readAndCheck()));
}

// Write one record and reopen store.
TEST_F(PartitionedRocksDBStoreTest, ReopenStore) {
  put({TestRecord(logid_t(1), 1)});
  EXPECT_EQ(1, numRecords(readAndCheck()));
  openStore();
}

// Open an empty store and reopen it.
TEST_F(PartitionedRocksDBStoreTest, ReopenEmptyStore) {
  closeStore();
  EXPECT_EQ(0, numRecords(readAndCheck()));
  openStore();
}

// Make records go to two partitions.
TEST_F(PartitionedRocksDBStoreTest, TwoPartitions) {
  const logid_t logid(1);
  put({TestRecord(logid, 10)});
  store_->createPartition();
  put({TestRecord(logid, 20), TestRecord(logid, 30)});
  auto data = readAndCheck();
  EXPECT_EQ(2, data.size());
  EXPECT_EQ(std::vector<lsn_t>({10}), data[0][logid].records);
  EXPECT_EQ(std::vector<lsn_t>({20, 30}), data[1][logid].records);
}

TEST_F(PartitionedRocksDBStoreTest, SimpleOutOfOrderWrites) {
  increasing_lsns_ = false;
  const logid_t logid(1);
  put({TestRecord(logid, 20)});
  store_->createPartition();
  put({TestRecord(logid, 10)});
  auto data = readAndCheck();
  EXPECT_EQ(2, data.size());
  EXPECT_EQ(std::vector<lsn_t>({10, 20}), data[0][logid].records);
  EXPECT_EQ(std::vector<lsn_t>({}), data[1][logid].records);
}

// Do random stuff from single thread, check consistency. Increasing LSNs.
TEST_F(PartitionedRocksDBStoreTest, SingleThreadedIncreasingWriteStressTest) {
  std::mt19937 rnd; // Deterministic generator with constant seed.
  uint64_t lsn[] = {0, 111, 222, 131};
  std::set<TestRecord> all_records;
  for (int i = 0; i < 100; ++i) {
    // Reopen store occasionally;
    if (rnd() % 10 == 0) {
      closeStore();
      ASSERT_EQ(all_records.size(), numRecords(readAndCheck()));
      openStore();
    }
    // Write records.
    int batch_size = rnd() % 10 + 1;
    std::vector<TestRecord> records;
    for (int j = 0; j < batch_size; ++j) {
      logid_t logid(rnd() % 3 + 1);
      lsn[logid.val_] += rnd() % 4 + 1;
      records.emplace_back(logid, lsn[logid.val_]);
    }
    all_records.insert(records.begin(), records.end());
    // Start new partition occasionally.
    if (rnd() % 3 == 0) {
      store_->createPartition();
    }
    put(records);
  }

  auto data = readAndCheck();
  ASSERT_EQ(all_records.size(), numRecords(data));
  // Expected number of partition is 100/3.
  ASSERT_GE(data.size(), 20);
}

// Do random stuff from single thread, check consistency.
TEST_F(PartitionedRocksDBStoreTest, SingleThreadedWriteStressTest) {
  increasing_lsns_ = false;
  std::mt19937 rnd; // Deterministic generator with constant seed.
  uint64_t lsn_window_start = 111; // Sliding window.
  std::set<TestRecord> all_records;
  for (int i = 0; i < 100; ++i) {
    // Reopen store occasionally;
    if (rnd() % 10 == 0) {
      closeStore();
      ASSERT_EQ(all_records.size(), numRecords(readAndCheck()));
      openStore();
    }
    // Write records.
    int batch_size = rnd() % 10 + 1;
    std::vector<TestRecord> records;
    for (int j = 0; j < batch_size; ++j) {
      logid_t logid(rnd() % 7 + 1);
      lsn_t lsn(lsn_window_start + rnd() % 200); // Ignore collisions.
      records.emplace_back(logid, lsn);
    }
    all_records.insert(records.begin(), records.end());
    // Start new partition occasionally.
    if (rnd() % 3 == 0) {
      store_->createPartition();
    }
    put(records);
    // Move sliding window.
    lsn_window_start += rnd() % 20;
  }

  auto data = readAndCheck();
  ASSERT_EQ(all_records.size(), numRecords(data));
  // Expected number of partition is 100/3.
  ASSERT_GE(data.size(), 20);
}

// Do random stuff from multiple threads, check consistency.
TEST_F(PartitionedRocksDBStoreTest, MultiThreadedWriteStressTest) {
  single_threaded_ = increasing_lsns_ = false;
  std::atomic<uint64_t> lsn_window_start(1111); // Sliding window.

  // Each thread does some batches of writes, starting new partition sometimes.
  auto thread_func = [&lsn_window_start,
                      this](int thread_id) -> std::set<TestRecord> {
    std::mt19937 rnd(thread_id);
    std::set<TestRecord> record_set;
    for (int i = rnd() % 20 + 1; i > 0; --i) {
      // Write records.
      int batch_size = rnd() % 10 + 1;
      std::vector<TestRecord> records;
      for (int j = 0; j < batch_size; ++j) {
        logid_t logid(rnd() % 7 + 1);
        lsn_t lsn(lsn_window_start.load() + rnd() % 200); // Ignore collisions.
        records.emplace_back(logid, lsn);
      }
      record_set.insert(records.begin(), records.end());
      // Start new partition occasionally.
      if (rnd() % 5 == 0) {
        store_->createPartition();
      }
      put(records);
      // Move sliding window.
      lsn_window_start += rnd() % 20;
    }
    return record_set;
  };

  std::mt19937 rnd;
  // Spawn and join threads several times, reopening store each time.
  std::set<TestRecord> all_records;
  int next_thread_id = 0;
  DBData data;
  for (int i = 0; i < 5; ++i) {
    openStore();
    std::vector<std::future<std::set<TestRecord>>> futures;
    for (int j = rnd() % 10 + 1; j > 0; --j) {
      futures.push_back(
          std::async(std::launch::async, thread_func, next_thread_id++));
    }
    for (auto& future : futures) {
      auto records = future.get();
      all_records.insert(records.begin(), records.end());
    }
    closeStore();
    data = readAndCheck();
    ASSERT_EQ(all_records.size(), numRecords(data));
  }

  // Without concurrency, expected number of partition would be around 50,
  // but it's less than that becasuse of a race.
  ASSERT_GE(data.size(), 20);
}

TEST_F(PartitionedRocksDBStoreTest, IteratorSanity) {
  logid_t logid(3);

  put({TestRecord(logid, 10)});

  auto it = store_->read(logid, LocalLogStore::ReadOptions("IteratorSanity"));
  it->seek(10);
  EXPECT_EQ(IteratorState::AT_RECORD, it->state());
  EXPECT_EQ(10, it->getLSN());
  it->next();
  EXPECT_EQ(IteratorState::AT_END, it->state());
}

TEST_F(PartitionedRocksDBStoreTest, IteratorEmpty) {
  logid_t logid(3);

  auto it = store_->read(logid, LocalLogStore::ReadOptions("IteratorEmpty"));
  it->seek(10);
  EXPECT_EQ(IteratorState::AT_END, it->state());

  put({TestRecord(logid_t(250), 10)});

  it = store_->read(logid, LocalLogStore::ReadOptions("IteratorEmpty"));
  it->seek(10);
  EXPECT_EQ(IteratorState::AT_END, it->state());
}

TEST_F(PartitionedRocksDBStoreTest, Iterator) {
  logid_t logid(3);

  put({TestRecord(logid, 10)});
  store_->createPartition();
  put({TestRecord(logid, 20), TestRecord(logid, 30)});

  auto it = store_->read(logid, LocalLogStore::ReadOptions("Iterator"));
  it->seek(10);

  EXPECT_EQ(IteratorState::AT_RECORD, it->state());
  EXPECT_EQ(10, it->getLSN());
  it->next();
  EXPECT_EQ(IteratorState::AT_RECORD, it->state());
  EXPECT_EQ(20, it->getLSN());
  it->next();
  EXPECT_EQ(IteratorState::AT_RECORD, it->state());
  EXPECT_EQ(30, it->getLSN());
}

TEST_F(PartitionedRocksDBStoreTest, ReadOnly) {
  logid_t logid(3);

  put({TestRecord(logid, 10)});
  store_->createPartition();
  put({TestRecord(logid, 20), TestRecord(logid, 30)});

  closeStore();
  ServerConfig::SettingsConfig s;
  s["rocksdb-read-only"] = "true";
  openStore(s);

  auto it = store_->read(logid, LocalLogStore::ReadOptions("ReadOnly"));
  it->seek(10);

  EXPECT_EQ(IteratorState::AT_RECORD, it->state());
  EXPECT_EQ(10, it->getLSN());
  it->next();
  EXPECT_EQ(IteratorState::AT_RECORD, it->state());
  EXPECT_EQ(20, it->getLSN());
  it->next();
  EXPECT_EQ(IteratorState::AT_RECORD, it->state());
  EXPECT_EQ(30, it->getLSN());
}

TEST_F(PartitionedRocksDBStoreTest, ReadOnlyWritesCrash) {
  logid_t logid(3);

  put({TestRecord(logid, 10)});
  store_->createPartition();
  put({TestRecord(logid, 20), TestRecord(logid, 30)});

  closeStore();
  ServerConfig::SettingsConfig s;
  s["rocksdb-read-only"] = "true";
  openStore(s);

  // ld_check should get hit
  ASSERT_DEATH(put({TestRecord(logid, 40)}), "");
}

TEST_F(PartitionedRocksDBStoreTest, TailingIterator) {
  logid_t logid(2);

  LocalLogStore::ReadOptions options("TailingIterator");
  options.tailing = true;
  auto it = store_->read(logid, options);
  put({TestRecord(logid, 10)});
  store_->createPartition();
  it->seek(10);
  put({TestRecord(logid, 20), TestRecord(logid, 30)});
  ASSERT_EQ(IteratorState::AT_RECORD, it->state());
  EXPECT_EQ(10, it->getLSN());
  it->next();
  // next() usually doesn't see records written after the last seek().
  // It's not really guaranteed, but in this particular scenario, with current
  // implementation it's the case.
  EXPECT_EQ(IteratorState::AT_END, it->state());

  it->seek(15);
  ASSERT_EQ(IteratorState::AT_RECORD, it->state());
  EXPECT_EQ(20, it->getLSN());
  it->next();
  ASSERT_EQ(IteratorState::AT_RECORD, it->state());
  EXPECT_EQ(30, it->getLSN());
  it.reset();

  auto data = readAndCheck();
  EXPECT_EQ(2, data.size());
  EXPECT_EQ(std::vector<lsn_t>({10}), data[0][logid].records);
  EXPECT_EQ(std::vector<lsn_t>({20, 30}), data[1][logid].records);
}

// Randomly do stuff with a bunch of iterators and write records.
TEST_F(PartitionedRocksDBStoreTest, IteratorStressTest) {
  struct ReferenceIterator {
    std::map<logid_t, std::set<lsn_t>>* lsns;
    logid_t logid;
    lsn_t lsn = LSN_INVALID;

    ReferenceIterator(std::map<logid_t, std::set<lsn_t>>* lsns, logid_t logid)
        : lsns(lsns), logid(logid) {}

    IteratorState state() {
      return lsn == LSN_INVALID ? IteratorState::AT_END
                                : IteratorState::AT_RECORD;
    }

    lsn_t getLSN() {
      return lsn;
    }

    void seek(lsn_t new_lsn) {
      auto it = (*lsns)[logid].lower_bound(new_lsn);
      if (it == (*lsns)[logid].end()) {
        lsn = LSN_INVALID;
      } else {
        lsn = *it;
      }
    }

    void seekForPrev(lsn_t new_lsn) {
      auto it = (*lsns)[logid].upper_bound(new_lsn);
      if (it == (*lsns)[logid].begin()) {
        lsn = LSN_INVALID;
      } else {
        --it;
        lsn = *it;
      }
    }

    void next() {
      ASSERT_NE(LSN_INVALID, lsn);
      seek(lsn + 1);
    }

    void prev() {
      ASSERT_NE(LSN_INVALID, lsn);
      auto it = (*lsns)[logid].lower_bound(lsn);
      if (it == (*lsns)[logid].begin()) {
        lsn = LSN_INVALID;
      } else {
        --it;
        lsn = *it;
      }
    }
  };

  struct IteratorPair {
    PartitionedRocksDBStore* store;
    bool tailing;
    // Only check that real_it iterates over some subset of what ref_it iterates
    // over. Otherwise check that they iterate over the same set.
    bool subset_check;

    std::unique_ptr<LocalLogStore::ReadIterator> real_it;
    ReferenceIterator ref_it;

    IteratorPair(PartitionedRocksDBStore* store,
                 bool tailing,
                 bool subset_check,
                 std::map<logid_t, std::set<lsn_t>>* lsns,
                 logid_t logid)
        : store(store),
          tailing(tailing),
          subset_check(subset_check),
          ref_it(lsns, logid) {
      LocalLogStore::ReadOptions options("IteratorStressTest");
      options.tailing = tailing;
      real_it = store->read(logid, options);
    }

    void check() {
      ASSERT_EQ(ref_it.state(), real_it->state());
      if (ref_it.state() == IteratorState::AT_RECORD) {
        EXPECT_EQ(ref_it.getLSN(), real_it->getLSN());
      }
    }

    IteratorState state() {
      return real_it->state();
    }

    void seek(lsn_t new_lsn) {
      check();
      real_it->seek(new_lsn);

      if (subset_check) {
        if (real_it->state() == IteratorState::AT_END) {
          ref_it.seek(LSN_MAX);
        } else {
          ASSERT_GE(real_it->getLSN(), new_lsn);
          ref_it.seek(real_it->getLSN());
        }
      } else {
        ref_it.seek(new_lsn);
      }

      check();
    }

    void seekForPrev(lsn_t new_lsn) {
      check();
      real_it->seekForPrev(new_lsn);
      if (subset_check) {
        if (real_it->state() == IteratorState::AT_END) {
          ref_it.seek(LSN_MAX);
        } else {
          ASSERT_LE(real_it->getLSN(), new_lsn);
          ref_it.seekForPrev(real_it->getLSN());
        }
      } else {
        ref_it.seekForPrev(new_lsn);
      }
      check();
    }

    void next() {
      check();
      auto old_lsn = real_it->getLSN();
      real_it->next();
      if (subset_check) {
        if (real_it->state() == IteratorState::AT_END) {
          ref_it.seek(LSN_MAX);
        } else {
          ASSERT_GT(real_it->getLSN(), old_lsn);
          ref_it.seek(real_it->getLSN());
        }
      } else {
        ref_it.next();
      }
      check();
    }

    void prev() {
      check();
      auto old_lsn = real_it->getLSN();
      real_it->prev();
      if (subset_check) {
        if (real_it->state() == IteratorState::AT_END) {
          ref_it.seek(LSN_MAX);
        } else {
          ASSERT_LT(real_it->getLSN(), old_lsn);
          ref_it.seek(real_it->getLSN());
        }
      } else {
        ref_it.prev();
      }
      check();
    }

    void iterateToEnd() {
      while (state() == IteratorState::AT_RECORD) {
        next();
      }
    }

    void iterateToBeginning() {
      while (state() == IteratorState::AT_RECORD) {
        prev();
      }
    }
  };

  increasing_lsns_ = false;
  std::mt19937 rnd;
  std::vector<IteratorPair> its;
  std::map<logid_t, std::set<lsn_t>> lsns;
  // Are we writing to the store?
  bool store_mutable = true;

  const int log_count = 3;
  const int it_count = 20;

  // Table of what we do on which iterations:
  //
  //                               i=0..999      i=1000..1999
  //  writing data                   yes             no
  //  iterator checks               subset          full
  //
  for (int i = 0; i < 2000; ++i) {
    if (i == 1000) {
      store_mutable = false;
      its.clear();
    }

    // Reopen store sometimes.
    if (rnd() % 300 == 0) {
      its.clear();
      closeStore();
      openStore();
    }

    // Write new records sometimes.
    if (store_mutable && rnd() % 3 == 0) {
      // Add (almost) empty partitions sometimes.
      if (rnd() % 10 == 0) {
        // Sometimes two in a row.
        for (int j = rnd() % 2; j >= 0; --j) {
          store_->createPartition();
          put({TestRecord(logid_t(250), rnd() % 10000)});
        }
      }

      int batch_size = rnd() % 3 + 1;
      std::vector<TestRecord> records;
      for (int j = 0; j < batch_size; ++j) {
        logid_t logid(rnd() % log_count + 1);
        lsn_t lsn(i + rnd() % 200);
        records.emplace_back(logid, lsn);
        lsns[logid].insert(lsn);
      }
      if (rnd() % 5 == 0) {
        store_->createPartition();
      }
      put(records);
    }

    // Create new iterators sometimes.
    if (its.size() < it_count || rnd() % 10 == 0) {
      if (its.size() == it_count) {
        std::swap(its.back(), its[rnd() % its.size()]);
        its.pop_back();
      }
      bool tailing = rnd() % 2 == 0;
      its.push_back(IteratorPair(store_.get(),
                                 tailing,
                                 store_mutable, // subset check
                                 &lsns,
                                 logid_t(rnd() % log_count + 1)));
    }

    // Move some iterator.
    IteratorPair& it = its[rnd() % its.size()];
    if (it.state() == IteratorState::AT_END || rnd() % 10 == 0) {
      lsn_t new_lsn = lsn_t(std::max(0, i - 400 + (int)(rnd() % 600)));
      if (it.tailing || rnd() % 3 < 2) {
        it.seek(new_lsn);
      } else {
        it.seekForPrev(new_lsn);
      }
    } else if (rnd() % 4 == 0) {
      if (it.tailing || rnd() % 2 == 0) {
        it.iterateToEnd();
      } else {
        it.iterateToBeginning();
      }
    } else {
      if (it.tailing || rnd() % 2 == 0) {
        it.next();
      } else {
        it.prev();
      }
    }

    // Check all iterators.
    for (IteratorPair& iter : its) {
      iter.check();
    }
  }
}

// Test dropping partitions.
TEST_F(PartitionedRocksDBStoreTest, MinimalDropTest) {
  const logid_t logid(1);
  put({TestRecord(logid, 10)});
  store_->createPartition();
  put({TestRecord(logid, 20), TestRecord(logid, 30)});

  store_->dropPartitionsUpTo(ID0 + 1);
  auto data = readAndCheck();
  EXPECT_EQ(1, data.size());
  EXPECT_EQ(std::vector<lsn_t>({20, 30}), data[0][logid].records);
}

// For some time do random writes and iteration from multiple threads and
// sometimes drop partitions. Then close store and check directory consistency.
// No meaningful data checks.
TEST_F(PartitionedRocksDBStoreTest, DropMultithreadedSmokeTest) {
  single_threaded_ = increasing_lsns_ = false;
  const int log_count = 5;
  std::atomic<bool> shutdown{};
  std::atomic<lsn_t> max_lsn{0};

  // Do random writes.
  auto write_func = [&](int seed) {
    ThreadID::set(
        ThreadID::Type::UTILITY, ("test:write" + std::to_string(seed)).c_str());
    std::mt19937 rnd(seed);
    lsn_t base_lsn = max_lsn.load();
    base_lsn -= std::min(10ul, base_lsn);
    for (int i = 0; !shutdown.load(); ++i) {
      // Create partitions sometimes.
      if (rnd() % 10 == 0) {
        store_->createPartition();
      }

      lsn_t lsn = base_lsn + i + rnd() % 10;
      put({TestRecord(logid_t(rnd() % log_count + 1), lsn)});
      atomic_fetch_max(max_lsn, lsn);
    }
  };

  // Do random iteration.
  auto iterator_func = [&](int seed) {
    ThreadID::set(
        ThreadID::Type::UTILITY, ("test:iter" + std::to_string(seed)).c_str());
    std::mt19937 rnd(seed);
    std::unique_ptr<LocalLogStore::ReadIterator> it;
    LocalLogStore::ReadOptions options("DropMultithreadedSmokeTest");
    for (int i = 0; !shutdown.load(); ++i) {
      // Create a new iterator sometimes.
      if (!it || rnd() % 2 == 0) {
        options.tailing = rnd() % 2 == 0;
        it = store_->read(logid_t(rnd() % log_count + 1), options);
      }

      // Seek to random lsn or to end.
      lsn_t lsn = rnd() % (max_lsn.load() + 10);
      if (options.tailing || rnd() % 2 == 0) {
        it->seek(lsn);
        if (it->state() == IteratorState::AT_RECORD) {
          EXPECT_GE(it->getLSN(), lsn);
        }
      } else {
        it->seekForPrev(lsn);
        if (it->state() == IteratorState::AT_RECORD) {
          EXPECT_LE(it->getLSN(), lsn);
        }
      }

      // Move back a little if we can.
      if (!options.tailing) {
        for (int j = rnd() % 10;
             it->state() == IteratorState::AT_RECORD && j > 0;
             --j) {
          lsn = it->getLSN();
          EXPECT_NE(LSN_INVALID, lsn);
          it->prev();
          if (it->state() == IteratorState::AT_RECORD) {
            EXPECT_LE(it->getLSN(), lsn);
          }
        }
      }

      // Move forward.
      // Sometimes to the end, most of the times limited to 100 steps.
      int limit = rnd() % 10 == 0 ? 1000000000 : 100;
      for (int j = 0; it->state() == IteratorState::AT_RECORD && j < limit;
           ++j) {
        lsn = it->getLSN();
        EXPECT_NE(LSN_INVALID, lsn);
        it->next();
        if (it->state() == IteratorState::AT_RECORD) {
          EXPECT_GE(it->getLSN(), lsn);
        }
      }
      EXPECT_EQ(IteratorState::AT_END, it->state());
    }
  };

  // Drop partitions sometimes.
  auto drop_func = [&](int seed) {
    ThreadID::set(
        ThreadID::Type::UTILITY, ("test:drop" + std::to_string(seed)).c_str());
    std::mt19937 rnd(seed);
    int thresh = -1;
    partition_id_t up_to = PARTITION_INVALID;
    while (!shutdown.load()) {
      // Drop when the number of partitions is >= thresh.
      // Thresh is randomly chosen after each drop.
      if (thresh == -1) {
        thresh = rnd() % 20 + 1;
      }

      auto partitions = store_->getPartitionList();
      EXPECT_GE(partitions->size(), 1);
      if (partitions->size() >= thresh) {
        // Keep random number of partitions up to thresh.
        up_to = partitions->nextID() - 1 - rnd() % thresh;
        EXPECT_EQ(0, store_->dropPartitionsUpTo(up_to));
        thresh = -1;
      }
    }

    // To see how many partitions were dropped.
    ld_info("last up_to: %lu", up_to);
  };

  // Reopen store sometimes.
  for (int epoch = 0; epoch < 3; ++epoch) {
    shutdown.store(false);
    std::vector<std::thread> threads;
    for (int i = 0; i < 3; ++i) {
      threads.emplace_back(write_func, epoch * 1000 + threads.size());
    }
    for (int i = 0; i < 3; ++i) {
      threads.emplace_back(iterator_func, epoch * 1000 + threads.size());
    }
    threads.emplace_back(drop_func, epoch * 1000 + threads.size());

    /* sleep override */
    std::this_thread::sleep_for(std::chrono::milliseconds(150));

    shutdown.store(true);
    for (auto& thread : threads) {
      thread.join();
    }

    readAndCheck();
    openStore();
  }

  Stats stats = stats_.aggregate();
  ld_info("logsdb_partition_pick_retries: %lu",
          stats.logsdb_partition_pick_retries.load());
}

TEST_F(PartitionedRocksDBStoreTest, Timestamps) {
  increasing_lsns_ = false;
  logid_t logid(1);

  RocksDBSettings default_settings = RocksDBSettings::defaultTestSettings();
  uint64_t g = default_settings.partition_timestamp_granularity_.count();

  const auto t0 = FUTURE;

  put({TestRecord(logid, 10, t0 + 10 * g)}); // partition 0
  put({TestRecord(logid, 20, t0 + 20 * g)}); // partition 0
  put({TestRecord(logid, 15, t0 + 15 * g)}); // partition 0

  for (int i = 0; i < 2; ++i) {
    {
      auto partitions = store_->getPartitionList();
      ASSERT_EQ(1, partitions->size());
      ASSERT_EQ(t0 + 9 * g,
                partitions->get(ID0)->min_timestamp.toMilliseconds().count());
      ASSERT_EQ(t0 + 21 * g,
                partitions->get(ID0)->max_timestamp.toMilliseconds().count());
    }

    if (i == 1) {
      break;
    }

    closeStore();
    openStore();
  }

  put({TestRecord(logid, 30, t0 + 20 * g + g / 2)}); // partition 0
  store_->createPartition();
  put({TestRecord(logid, 40, t0 + 90 * g)}); // partition 1
  put({TestRecord(logid, 25, t0 + 8 * g)});  // partition 0
  put({TestRecord(logid, 35, t0 + 91 * g)}); // partition 1
  put({TestRecord(logid, 50, t0 + 89 * g)}); // partition 1
  store_->createPartition();
  put({TestRecord(logid, 60, t0 + 200 * g)}); // partition 2
  put({TestRecord(logid, 61, t0 + 100 * g)}); // partition 2

  for (int i = 0; i < 2; ++i) {
    {
      auto partitions = store_->getPartitionList();
      ASSERT_EQ(3, partitions->size());
      ASSERT_EQ(t0 + 7 * g,
                partitions->get(ID0)->min_timestamp.toMilliseconds().count());
      ASSERT_EQ(t0 + 21 * g,
                partitions->get(ID0)->max_timestamp.toMilliseconds().count());
      ASSERT_EQ(
          t0 + 89 * g,
          partitions->get(ID0 + 1)->min_timestamp.toMilliseconds().count());
      ASSERT_EQ(
          t0 + 91 * g,
          partitions->get(ID0 + 1)->max_timestamp.toMilliseconds().count());
      ASSERT_EQ(
          t0 + 99 * g,
          partitions->get(ID0 + 2)->min_timestamp.toMilliseconds().count());
      ASSERT_EQ(
          t0 + 201 * g,
          partitions->get(ID0 + 2)->max_timestamp.toMilliseconds().count());
    }

    if (i == 1) {
      break;
    }

    closeStore();
    openStore();
  }

  store_->dropPartitionsUpTo(ID0 + 2);

  for (int i = 0; i < 2; ++i) {
    {
      auto partitions = store_->getPartitionList();
      ASSERT_EQ(1, partitions->size());
      ASSERT_EQ(
          t0 + 99 * g,
          partitions->get(ID0 + 2)->min_timestamp.toMilliseconds().count());
      ASSERT_EQ(
          t0 + 201 * g,
          partitions->get(ID0 + 2)->max_timestamp.toMilliseconds().count());
    }

    if (i == 1) {
      break;
    }

    closeStore();
    openStore();
  }

  ASSERT_EQ(1, readAndCheck().size());
}

// Check that time-based auto-trimming trims.
TEST_F(PartitionedRocksDBStoreTest, AutoTrim) {
  logid_t logid(1);

  put({TestRecord(logid, 10, BASE_TIME - WEEK)});
  store_->createPartition();
  put({TestRecord(logid, 20, BASE_TIME - WEEK)});
  put({TestRecord(logid, 30, BASE_TIME - WEEK)});
  store_->createPartition();

  // Lo-pri background thread should drop both non-empty partitions.
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + DAY));
  EXPECT_EQ(3, store_->getPartitionList()->size());
  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::LO_PRI)
      .wait();

  auto data = readAndCheck();
  ASSERT_EQ(1, data.size());
  ASSERT_EQ(0, data[0].size());
}

// Check that time-based auto-trimming trims properly when partitions are dirty.
TEST_F(PartitionedRocksDBStoreTest, AutoTrimDirty) {
  logid_t logid(1);

  // Non-empty, dirty partition.
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME));
  put({TestRecord(logid, 10, BASE_TIME)});

  // Empty, dirty partition.
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + 15 * MINUTE));
  store_->createPartition();

  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + 30 * MINUTE));
  store_->createPartition();

  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + HOUR));
  store_->createPartition();
  put({TestRecord(logid, 20, BASE_TIME + HOUR)});
  put({TestRecord(logid, 30, BASE_TIME + HOUR)});

  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + 2 * HOUR));
  store_->createPartition();
  put({TestRecord(logid, 40, BASE_TIME + 2 * HOUR)});
  put({TestRecord(logid, 50, BASE_TIME + 2 * HOUR)});
  store_->createPartition();

  // Mark the first two parittions dirty.
  store_->modifyUnderReplicatedTimeRange(
      TimeIntervalOp::ADD,
      DataClass::APPEND,
      RecordTimeInterval(
          RecordTimestamp(std::chrono::milliseconds(BASE_TIME)),
          RecordTimestamp(std::chrono::milliseconds(BASE_TIME + 16 * MINUTE))));
  auto data = readAndCheck();
  ASSERT_EQ(6, data.size());
  ASSERT_EQ(1, data[0].size());
  ASSERT_EQ(0, data[1].size());
  ASSERT_EQ(0, data[2].size());
  ASSERT_EQ(1, data[3].size());
  ASSERT_EQ(1, data[4].size());
  ASSERT_EQ(0, data[5].size());

  openStore();
  ASSERT_TRUE(store_->isUnderReplicated());

  // The first two partitions are dirty, increasing the effective retention
  // for those partitions to the max retention in the config (7 days).
  // The Lo-pri background thread should compact the first partition but not
  // drop any partitions.
  time_ =
      SystemTimestamp(std::chrono::milliseconds(BASE_TIME + DAY + 16 * MINUTE));
  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::LO_PRI)
      .wait();
  ASSERT_TRUE(store_->isUnderReplicated());
  data = readAndCheck();
  ASSERT_EQ(6, data.size());
  ASSERT_EQ(0, data[0].size());
  ASSERT_EQ(0, data[1].size());
  ASSERT_EQ(0, data[2].size());
  ASSERT_EQ(1, data[3].size());
  ASSERT_EQ(1, data[4].size());
  ASSERT_EQ(0, data[5].size());

  openStore();
  ASSERT_TRUE(store_->isUnderReplicated());

  // Lo-pri background thread should compact the rest of the partitions,
  // but still not drop any.
  time_ =
      SystemTimestamp(std::chrono::milliseconds(BASE_TIME + DAY + 3 * HOUR));
  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::LO_PRI)
      .wait();
  ASSERT_TRUE(store_->isUnderReplicated());
  data = readAndCheck();
  ASSERT_EQ(6, data.size());
  ASSERT_EQ(0, data[0].size());
  ASSERT_EQ(0, data[1].size());
  ASSERT_EQ(0, data[2].size());
  ASSERT_EQ(0, data[3].size());
  ASSERT_EQ(0, data[4].size());
  ASSERT_EQ(0, data[5].size());

  openStore();
  ASSERT_TRUE(store_->isUnderReplicated());

  // As soon as the dirty partitions are over 7 days old, drops are allowed.
  // This should clean the store and leave us with one empty partition.
  time_ =
      SystemTimestamp(std::chrono::milliseconds(BASE_TIME + 7 * DAY + HOUR));
  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::LO_PRI)
      .wait();
  ASSERT_FALSE(store_->isUnderReplicated());
  data = readAndCheck();
  ASSERT_EQ(1, data.size());
  ASSERT_EQ(0, data[0].size());
}

TEST_F(PartitionedRocksDBStoreTest, TrimPerEpochLogMetadata) {
  logid_t logid(1);

  // write some data record
  put({TestRecord(logid, lsn(3, 1), BASE_TIME)});
  put({TestRecord(logid, lsn(4, 3), BASE_TIME)});
  put({TestRecord(logid, lsn(5, 7), BASE_TIME)});
  put({TestRecord(logid, lsn(8, 2), BASE_TIME)});
  // initially load the data trim point to 0
  {
    int rv = updateTrimPoint(logid, LSN_INVALID);
    ASSERT_TRUE(rv == 0 || err == E::UPTODATE);
  }

  // write some metadata
  epoch_size_map.setCounter(BYTE_OFFSET, 12);
  tail_record.offsets_map_.setCounter(BYTE_OFFSET, 10);
  tail_record.header.log_id = logid_t(1);
  epoch_end_offsets.setCounter(BYTE_OFFSET, 10);
  EpochRecoveryMetadata metadata(epoch_t(9),
                                 esn_t(1),
                                 esn_t(3),
                                 1,
                                 tail_record,
                                 epoch_size_map,
                                 epoch_end_offsets);
  auto write_metadata = [&](const std::vector<int>& epochs) {
    for (auto e : epochs) {
      int rv = store_->updatePerEpochLogMetadata(
          logid,
          epoch_t(e),
          metadata,
          LocalLogStore::SealPreemption::ENABLE,
          LocalLogStore::WriteOptions());
      ASSERT_TRUE(rv == 0 || err == E::UPTODATE);
    }
  };
  write_metadata({2, 3, 4, 6});
  store_->createPartition();
  store_->performMetadataCompaction();
  ASSERT_EQ(EPOCH_INVALID, getPerEpochLogMetadataTrimPoint(logid));

  auto verify_metadata = [&](const std::set<int>& epochs) {
    for (int e = 0; e < 10; ++e) {
      EpochRecoveryMetadata metadata_read;
      int rv =
          store_->readPerEpochLogMetadata(logid, epoch_t(e), &metadata_read);
      if (epochs.count(e) > 0) {
        ASSERT_EQ(0, rv);
        ASSERT_EQ(metadata, metadata_read);
      } else {
        ASSERT_EQ(-1, rv);
        ASSERT_EQ(E::NOTFOUND, err);
      }
    }
  };

  auto check_trim_stats = [&](int64_t expected_trimmed) {
    auto stats = stats_.aggregate();
    ASSERT_EQ(expected_trimmed, stats.per_epoch_log_metadata_trimmed_removed);
  };

  verify_metadata({2, 3, 4, 6});
  // move the trim point of data logs to epoch 3
  ASSERT_EQ(0, updateTrimPoint(logid, lsn(3, 4)));
  store_->performMetadataCompaction();
  filter_factory_->flushTrimPoints();
  // metadata trim point should be in epoch 1
  ASSERT_EQ(epoch_t(1), getPerEpochLogMetadataTrimPoint(logid));
  store_->performMetadataCompaction();
  filter_factory_->flushTrimPoints();
  // but still nothing gets trimmed
  verify_metadata({2, 3, 4, 6});
  check_trim_stats(0);

  // move the trim point of data logs to epoch 4
  ASSERT_EQ(0, updateTrimPoint(logid, lsn(4, 9)));
  store_->performMetadataCompaction();
  filter_factory_->flushTrimPoints();
  // metadata trim point should be in epoch 2
  ASSERT_EQ(epoch_t(2), getPerEpochLogMetadataTrimPoint(logid));
  // but metadata for epoch 2 should not get trimmed for now
  verify_metadata({2, 3, 4, 6});
  check_trim_stats(0);

  // kick off another round of compaction, epoch 2 metadata should be trimmed
  store_->performMetadataCompaction();
  filter_factory_->flushTrimPoints();
  ASSERT_EQ(epoch_t(2), getPerEpochLogMetadataTrimPoint(logid));
  verify_metadata({3, 4, 6});
  check_trim_stats(1);

  // move the data trim point to epoch 8, all metadata except for epoch 6
  // should be trimmed after two compaction rounds
  ASSERT_EQ(0, updateTrimPoint(logid, lsn(8, 0)));
  store_->performMetadataCompaction();
  filter_factory_->flushTrimPoints();
  ASSERT_EQ(epoch_t(5), getPerEpochLogMetadataTrimPoint(logid));
  verify_metadata({3, 4, 6});
  store_->performMetadataCompaction();
  filter_factory_->flushTrimPoints();
  ASSERT_EQ(epoch_t(5), getPerEpochLogMetadataTrimPoint(logid));
  verify_metadata({6});
  check_trim_stats(3);
}

// Check that time-based auto-trimming doesn't trim if the partition has recent
// records.
TEST_F(PartitionedRocksDBStoreTest, NoAutoTrim) {
  logid_t logid(1);

  put({TestRecord(logid, 10, BASE_TIME)});
  store_->createPartition();
  put({TestRecord(logid, 20, BASE_TIME)});
  put({TestRecord(logid, 30, BASE_TIME)});

  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::HI_PRI)
      .wait();
  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::LO_PRI)
      .wait();

  auto data = readAndCheck();
  ASSERT_EQ(2, data.size());
}

TEST_F(PartitionedRocksDBStoreTest, ApproximateTimestamp) { // no v1 support
  const logid_t logid(1);
  const logid_t logid_second(2);
  const logid_t logid_third(3);
  put({TestRecord(logid, 10, BASE_TIME)});
  put({TestRecord(logid, 15, BASE_TIME + 5)});
  put({TestRecord(logid_second, 11, BASE_TIME)});
  put({TestRecord(logid_second, 12, BASE_TIME + 5)});
  put({TestRecord(logid_second, 13, BASE_TIME + 10)});
  store_->createPartition();
  put({TestRecord(logid, 20, BASE_TIME + 10),
       TestRecord(logid, 30, BASE_TIME + 20)});

  auto partitions = store_->getPartitionList();
  std::vector<RecordTimestamp> partitions_min_timestamp;
  std::vector<RecordTimestamp> partitions_max_timestamp;
  for (int i = 0; i < partitions->size(); ++i) {
    partitions_min_timestamp.push_back(partitions->get(i + ID0)->min_timestamp);
    partitions_max_timestamp.push_back(partitions->get(i + ID0)->max_timestamp);
  }

  std::chrono::milliseconds timestamp_out;
  int rv;

  // Look for PartitionedRocksDBStore::getApproximateTimestamp doc block
  // for list of use cases this test covering.

  // 1) between two partitions
  rv = store_->getApproximateTimestamp(logid, 17, true, &timestamp_out);
  EXPECT_EQ(0, rv);
  EXPECT_EQ(partitions_min_timestamp[1], RecordTimestamp(timestamp_out));

  // 2) behind last partition
  rv = store_->getApproximateTimestamp(logid, 5, true, &timestamp_out);
  EXPECT_EQ(0, rv);
  EXPECT_EQ(partitions_min_timestamp[0], RecordTimestamp(timestamp_out));
  rv = store_->getApproximateTimestamp(logid_second, 5, true, &timestamp_out);
  EXPECT_EQ(0, rv);
  EXPECT_EQ(partitions_min_timestamp[0], RecordTimestamp(timestamp_out));

  // 3) inside partition
  rv = store_->getApproximateTimestamp(logid, 11, true, &timestamp_out);
  EXPECT_EQ(0, rv);
  EXPECT_EQ(partitions_max_timestamp[0], RecordTimestamp(timestamp_out));
  rv = store_->getApproximateTimestamp(logid, 25, true, &timestamp_out);
  EXPECT_EQ(0, rv);
  EXPECT_EQ(partitions_max_timestamp[1], RecordTimestamp(timestamp_out));
  rv = store_->getApproximateTimestamp(logid_second, 12, true, &timestamp_out);
  EXPECT_EQ(0, rv);
  EXPECT_EQ(partitions_max_timestamp[0], RecordTimestamp(timestamp_out));

  // 4) inside last partition
  rv = store_->getApproximateTimestamp(logid_second, 12, true, &timestamp_out);
  EXPECT_EQ(0, rv);
  EXPECT_EQ(partitions_max_timestamp[0], RecordTimestamp(timestamp_out));
  rv = store_->getApproximateTimestamp(logid, 25, true, &timestamp_out);
  EXPECT_EQ(0, rv);
  EXPECT_EQ(partitions_max_timestamp[1], RecordTimestamp(timestamp_out));

  // 5) older than all partitions
  rv = store_->getApproximateTimestamp(logid, 100, true, &timestamp_out);
  EXPECT_EQ(-1, rv);
  EXPECT_EQ(err, E::NOTFOUND);
  rv = store_->getApproximateTimestamp(logid_second, 100, true, &timestamp_out);
  EXPECT_EQ(-1, rv);
  EXPECT_EQ(err, E::NOTFOUND);

  // 6) no partitions for log
  rv = store_->getApproximateTimestamp(logid_third, 40, true, &timestamp_out);
  EXPECT_EQ(-1, rv);
  EXPECT_EQ(err, E::NOTFOUND);
}

TEST_F(PartitionedRocksDBStoreTest, Compactions) { // No v1 support.
  /*
    Time quantum is 1/3 of a day. Each record is in the middle of some quantum.
    Current time takes values on borders between quanta.
    'xx' means record exists, '__' means it doesn't, | is partition border:

     time ->    0  1  2  3  4  5  6  7  8  9  10 11 12 13 14 15 16
     logs       __ __ __ __ __ __ __ __ __ __ __ __ __ __ __ __ __
       |    0  |xx __ __|__ __ __|__ __ __|__ xx __|__ xx __|xx xx
       V    1  |__ xx __|__ __ __|__ __ __|__ __ __|__ __ __|__ __
            2  |__ xx __|__ __ __|__ xx xx|__ __ __|__ __ __|__ __
            3  |__ __ __|__ __ __|__ __ xx|__ __ __|__ __ __|__ __
            4  |__ xx xx|__ __ __|__ __ __|__ __ __|__ xx __|__ __
            5  |__ xx __|__ __ __|__ xx __|__ __ __|__ __ __|__ __

    compaction times:             ^^ p0    ^^ p0    ^^       ^^ p2 & p3
    drop times:                                     p0 & p1
   */

  const std::vector<logid_t> logs = {logid_t(10),
                                     logid_t(20), // 1 day
                                     logid_t(188),
                                     logid_t(111), // 2 days
                                     logid_t(250),
                                     logid_t(251)}; // 3 days
  const uint64_t Q = DAY / 3;                       // length of time quantum
  const int T = 16;                                 // # time quanta
  // log -> {time quantum of a record} of last record
  const std::vector<std::vector<int>> recs = {
      {0, 10, 13, 15, 16}, {1}, {1, 7, 8}, {8}, {1, 2, 13}, {1, 7}};
  FastUpdateableSharedPtr<StatsParams> params(std::make_shared<StatsParams>());
  Stats stats(&params);

  auto expect_trim_pts = [&](std::initializer_list<lsn_t> pts) {
    int i = 0;
    for (lsn_t pt : pts) {
      EXPECT_EQ(getTrimPoint(logs[i]), pt);
      ++i;
    }
    EXPECT_EQ(6, i);
  };

  // Create partitions in advance.
  for (int i = 1; i < 6; ++i) {
    setTime(BASE_TIME + Q * 3 * i);
    store_->createPartition();
  }

  // Put records.
  for (size_t i = 0; i < logs.size(); ++i) {
    for (size_t j = 0; j < recs[i].size(); ++j) {
      put({TestRecord(
          logs[i], j * 10 + 5, BASE_TIME + Q * recs[i][j] + Q / 2)});
    }
  }

  // Check that records are distributed across partitions as expected.
  auto data = readAndCheck();
  EXPECT_EQ(6, data.size());
  EXPECT_EQ(5, data[0].size());
  EXPECT_EQ(0, data[1].size());
  EXPECT_EQ(3, data[2].size());
  EXPECT_EQ(1, data[3].size());
  EXPECT_EQ(2, data[4].size());
  EXPECT_EQ(1, data[5].size());
  openStore();

  // 4/3 days. Nothing is trimmed.
  setTime(BASE_TIME + Q * 4);
  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::LO_PRI)
      .wait();
  filter_factory_->flushTrimPoints();
  stats = stats_.aggregate();
  EXPECT_EQ(0, stats.partitions_dropped);
  EXPECT_EQ(0, stats.partitions_compacted);
  {
    SCOPED_TRACE("");
    expect_trim_pts({0, 0, 0, 0, 0, 0});
  }

  // Force compaction of partition 0 and check that it moved trim points to
  // precise locations.
  store_->performCompaction(ID0);
  filter_factory_->flushTrimPoints();
  stats = stats_.aggregate();
  EXPECT_EQ(0, stats.partitions_dropped);
  EXPECT_EQ(1, stats.partitions_compacted);
  {
    SCOPED_TRACE("");
    expect_trim_pts({5, 0, 0, 0, 0, 0});
  }

  // 2+eps days. Partition 0 is compacted
  setTime(BASE_TIME + Q * 6 + Q / 2);
  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::LO_PRI)
      .wait();
  filter_factory_->flushTrimPoints();
  stats = stats_.aggregate();
  EXPECT_EQ(0, stats.partitions_dropped);
  EXPECT_EQ(2, stats.partitions_compacted);
  {
    SCOPED_TRACE("");
    expect_trim_pts({5, 5, 0, 0, 0, 0});
  }

  // Check that data for logs[0] and logs[1] is removed from first partition.
  data = readAndCheck();
  EXPECT_EQ(6, data.size());
  EXPECT_EQ(0, data[0][logs[0]].records.size());
  EXPECT_EQ(0, data[0][logs[1]].records.size());
  openStore();

  // 3-eps days. Nothing happens.
  setTime(BASE_TIME + Q * 9 - Q / 2);
  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::LO_PRI)
      .wait();
  filter_factory_->flushTrimPoints();
  stats = stats_.aggregate();
  EXPECT_EQ(0, stats.partitions_dropped);
  EXPECT_EQ(2, stats.partitions_compacted);
  {
    SCOPED_TRACE("");
    expect_trim_pts({5, 5, 0, 0, 0, 0});
  }

  // 3+eps days. Partition 0 is compacted.
  setTime(BASE_TIME + Q * 9 + Q / 2);
  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::LO_PRI)
      .wait();
  filter_factory_->flushTrimPoints();
  stats = stats_.aggregate();
  EXPECT_EQ(0, stats.partitions_dropped);
  EXPECT_EQ(3, stats.partitions_compacted);
  {
    SCOPED_TRACE("");
    expect_trim_pts({5, 5, 5, 0, 0, 0});
  }

  // 4-eps days. Nothing happens.
  setTime(BASE_TIME + Q * 12 - Q / 2);
  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::LO_PRI)
      .wait();
  filter_factory_->flushTrimPoints();
  stats = stats_.aggregate();
  EXPECT_EQ(0, stats.partitions_dropped);
  EXPECT_EQ(3, stats.partitions_compacted);
  {
    SCOPED_TRACE("");
    expect_trim_pts({5, 5, 5, 0, 0, 0});
  }

  // 4+eps days. Partitions 0 and 1 are dropped.
  setTime(BASE_TIME + Q * 12 + Q / 2);
  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::LO_PRI)
      .wait();
  filter_factory_->flushTrimPoints();
  stats = stats_.aggregate();
  EXPECT_EQ(2, stats.partitions_dropped);
  EXPECT_EQ(3, stats.partitions_compacted);
  {
    SCOPED_TRACE("");
    expect_trim_pts({5, 5, 5, 0, 15, 5});
  }

  // 5-eps days. Nothing happens.
  setTime(BASE_TIME + Q * 15 - Q / 2);
  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::LO_PRI)
      .wait();
  filter_factory_->flushTrimPoints();
  stats = stats_.aggregate();
  EXPECT_EQ(2, stats.partitions_dropped);
  EXPECT_EQ(3, stats.partitions_compacted);
  {
    SCOPED_TRACE("");
    expect_trim_pts({5, 5, 5, 0, 15, 5});
  }

  // 5+eps days. Partitions 2 and 3 are compacted.
  setTime(BASE_TIME + Q * 15 + Q / 2);
  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::LO_PRI)
      .wait();
  filter_factory_->flushTrimPoints();
  stats = stats_.aggregate();
  EXPECT_EQ(2, stats.partitions_dropped);
  EXPECT_EQ(5, stats.partitions_compacted);
  {
    SCOPED_TRACE("");
    expect_trim_pts({15, 5, 25, 5, 15, 5});
  }

  // Check data.
  data = readAndCheck();
  ASSERT_EQ(4, data.size());
  EXPECT_EQ(0, data[0][logs[2]].records.size());
  EXPECT_EQ(0, data[0][logs[3]].records.size());
  EXPECT_EQ(1, data[0][logs[5]].records.size());
  EXPECT_EQ(0, data[1][logs[0]].records.size());
  EXPECT_EQ(1, data[2][logs[0]].records.size());
  EXPECT_EQ(1, data[2][logs[4]].records.size());
  EXPECT_EQ(2, data[3][logs[0]].records.size());
  openStore();

  // Drop partition 2 manually and check that trim points are updated.
  store_->dropPartitionsUpTo(ID0 + 3);
  stats = stats_.aggregate();
  EXPECT_EQ(3, stats.partitions_dropped);
  EXPECT_EQ(5, stats.partitions_compacted);
  {
    SCOPED_TRACE("");
    expect_trim_pts({15, 5, 25, 5, 15, 15});
  }

  // Manually update trim points and check that partition 3 gets dropped.
  int rv = LocalLogStoreUtils::updateTrimPoints(
      {{logs[0], 25}}, processor_.get(), *store_, false, &stats_);
  EXPECT_EQ(0, rv);
  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::LO_PRI)
      .wait();
  filter_factory_->flushTrimPoints();
  stats = stats_.aggregate();
  EXPECT_EQ(4, stats.partitions_dropped);
  EXPECT_EQ(5, stats.partitions_compacted);
  {
    SCOPED_TRACE("");
    expect_trim_pts({25, 5, 25, 5, 15, 15});
  }

  // Manually update trim points and check that partition 4 gets dropped.
  rv = LocalLogStoreUtils::updateTrimPoints(
      {{logs[4], 25}}, processor_.get(), *store_, false, &stats_);
  EXPECT_EQ(0, rv);
  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::LO_PRI)
      .wait();
  filter_factory_->flushTrimPoints();
  stats = stats_.aggregate();
  EXPECT_EQ(5, stats.partitions_dropped);
  EXPECT_EQ(5, stats.partitions_compacted);
  {
    SCOPED_TRACE("");
    expect_trim_pts({25, 5, 25, 5, 25, 15});
  }

  // 100 days. Latest partition 5 is left alone.
  setTime(BASE_TIME + Q * 300);
  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::LO_PRI)
      .wait();
  filter_factory_->flushTrimPoints();
  stats = stats_.aggregate();
  EXPECT_EQ(5, stats.partitions_dropped);
  EXPECT_EQ(5, stats.partitions_compacted);
  {
    SCOPED_TRACE("");
    expect_trim_pts({25, 5, 25, 5, 25, 15});
  }

  data = readAndCheck();
  EXPECT_EQ(1, data.size());
  EXPECT_EQ(1, data[0].size());
  EXPECT_EQ(2, data[0][logs[0]].records.size());
}

TEST_F(PartitionedRocksDBStoreTest, Deletes) {
  no_deletes_ = false;
  logid_t logid(42);
  put({TestRecord(logid, 5)});
  put({TestRecord(logid, 10)});
  store_->createPartition();
  put({TestRecord(logid, 10, TestRecord::Type::DELETE), TestRecord(logid, 20)});
  put({TestRecord(logid, 30), TestRecord(logid, 30, TestRecord::Type::DELETE)});

  auto data = readAndCheck();
  EXPECT_EQ(2, data.size());
  EXPECT_EQ(std::vector<lsn_t>({5}), data[0][logid].records);
  EXPECT_EQ(std::vector<lsn_t>({20}), data[1][logid].records);
}

// Test for RocksDBSettings::partition_compaction_schedule.
TEST_F(PartitionedRocksDBStoreTest, CompactionSchedule) {
  closeStore();
  // Unlike timestamps, it's in seconds.
  auto sched = std::chrono::seconds(DAY / 1000 * 2);
  ServerConfig::SettingsConfig s;
  s["rocksdb-partition-compaction-schedule"] =
      std::to_string(sched.count()) + "s";
  openStore(s);

  put({TestRecord(logid_t(50), 10, BASE_TIME + HOUR),
       TestRecord(logid_t(150), 10, BASE_TIME + HOUR),
       TestRecord(logid_t(250), 10, BASE_TIME + HOUR)});
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + HOUR));
  store_->createPartition();

  // Log 150 has untrimmed records, shouldn't compact.
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + DAY * 2));
  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::LO_PRI)
      .wait();
  auto stats = stats_.aggregate();
  EXPECT_EQ(0, stats.partitions_compacted);

  // Logs 50 and 150 are trimmed from partition 0. Compaction.
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + DAY * 3));
  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::LO_PRI)
      .wait();
  stats = stats_.aggregate();
  EXPECT_EQ(1, stats.partitions_compacted);
}

// Just to make sure previous test doesn't work by coincidence, do the same
// without compaction schedule and check that results are different.
TEST_F(PartitionedRocksDBStoreTest, CompactionScheduleBaseline) {
  put({TestRecord(logid_t(50), 10, BASE_TIME + HOUR),
       TestRecord(logid_t(150), 10, BASE_TIME + HOUR),
       TestRecord(logid_t(250), 10, BASE_TIME + HOUR)});
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + HOUR));
  store_->createPartition();

  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + DAY * 2));
  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::LO_PRI)
      .wait();
  auto stats = stats_.aggregate();
  EXPECT_EQ(1, stats.partitions_compacted);

  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + DAY * 3));
  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::LO_PRI)
      .wait();
  stats = stats_.aggregate();
  EXPECT_EQ(2, stats.partitions_compacted);
}

// Test for RocksDBSettings proactive_compaction_enabled option.
TEST_F(PartitionedRocksDBStoreTest, ProactiveCompaction) {
  closeStore();
  ServerConfig::SettingsConfig s;
  s["rocksdb-proactive-compaction-enabled"] = "true";
  openStore(s);

  put({TestRecord(logid_t(1), 1, BASE_TIME)});
  store_->createPartition();
  put({TestRecord(logid_t(2), 2, BASE_TIME + HOUR)});

  // We do not compact two latest partitions, so no compaction have to
  // be made at this point.
  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::LO_PRI)
      .wait();
  auto stats = stats_.aggregate();
  EXPECT_EQ(0, stats.partitions_dropped);
  EXPECT_EQ(0, stats.partitions_compacted);

  store_->createPartition();
  put({TestRecord(logid_t(1), 3, BASE_TIME + HOUR * 2)});

  // After we added third partition, the oldest partition has to be compacted.
  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::LO_PRI)
      .wait();
  stats = stats_.aggregate();
  EXPECT_EQ(0, stats.partitions_dropped);
  EXPECT_EQ(1, stats.partitions_compacted);
  EXPECT_EQ(1, stats.partition_proactive_compactions);

  // Once we already perform compaction over first partition we should not do
  // it again.
  put({TestRecord(logid_t(2), 3, BASE_TIME + HOUR * 3)});
  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::LO_PRI)
      .wait();
  stats = stats_.aggregate();
  EXPECT_EQ(0, stats.partitions_dropped);
  EXPECT_EQ(1, stats.partitions_compacted);
}

TEST_F(PartitionedRocksDBStoreTest, MetaDataLogsAreUnpartitioned) {
  const logid_t data_log(10);
  const logid_t meta_log = MetaDataLog::metaDataLogID(data_log);
  // Write an old data record and a metadata log record.
  put({TestRecord(data_log, 10, BASE_TIME + HOUR),
       TestRecord(meta_log, 20, BASE_TIME + HOUR)});
  store_->createPartition();
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + DAY * 2));
  // Check that partition 0 is dropped.
  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::LO_PRI)
      .wait();
  EXPECT_EQ(ID0 + 1, store_->getPartitionList()->firstID());
  // Check that there are no records in data log and one record in metadata log.
  auto it = store_->read(
      data_log, LocalLogStore::ReadOptions("MetaDataLogsAreUnpartitioned"));
  it->seek(lsn_t(0));
  EXPECT_EQ(IteratorState::AT_END, it->state());
  it = store_->read(
      meta_log, LocalLogStore::ReadOptions("MetaDataLogsAreUnpartitioned"));
  it->seek(lsn_t(0));
  EXPECT_EQ(IteratorState::AT_RECORD, it->state());
  EXPECT_EQ(20, it->getLSN());
}

TEST_F(PartitionedRocksDBStoreTest, DropVsTrimRace) {
  std::atomic<bool> shutdown{false};

  std::thread dropping_thread([&] {
    int i = 0;
    while (!shutdown.load()) {
      ++i;
      store_->createPartition();
      put({TestRecord(logid_t(i % 300 + 1), i, BASE_TIME + HOUR + i)});
      store_->dropPartitionsUpTo(store_->getPartitionList()->nextID() - 1);
    }

    ld_info("dropped %d times", i);
  });

  std::thread trimming_thread([&] {
    int i = 0;
    while (!shutdown.load()) {
      ++i;
      store_
          ->backgroundThreadIteration(
              PartitionedRocksDBStore::BackgroundThreadType::LO_PRI)
          .get();
    }

    ld_info("trimmed %d times", i);
  });

  /* sleep override */
  std::this_thread::sleep_for(std::chrono::milliseconds(500));
  shutdown.store(true);
  dropping_thread.join();
  trimming_thread.join();
}

// Drop a CF in rocksdb directly, check that PartitionedRocksDBStore() drops
// preceding partitions and removes obsolete directory entries.
TEST_F(PartitionedRocksDBStoreTest, InterruptedDrop) {
  put({TestRecord(logid_t(1), 10)});
  put({TestRecord(logid_t(2), 5)});
  store_->createPartition();
  put({TestRecord(logid_t(1), 15)});
  put({TestRecord(logid_t(2), 100)});
  store_->createPartition();
  put({TestRecord(logid_t(1), 20)});
  put({TestRecord(logid_t(2), 300)});
  closeStore();

  {
    std::unique_ptr<rocksdb::DB> db;
    std::map<std::string, std::unique_ptr<rocksdb::ColumnFamilyHandle>> cfs;
    openDirectly(db, cfs);

    EXPECT_EQ(cfs.size(), 6); // Default, metadata, unpartitioned, 0, 1, 2.
    EXPECT_TRUE(cfs.count(toString(ID0)));
    EXPECT_TRUE(cfs.count(toString(ID0 + 1)));
    EXPECT_TRUE(cfs.count(toString(ID0 + 2)));
    ASSERT_TRUE(cfs.count("metadata"));

    auto status = db->DropColumnFamily(cfs[toString(ID0 + 1)].get());
    ASSERT_TRUE(status.ok());
  }
  openStore();
  auto data = readAndCheck();
  EXPECT_EQ(1, data.size());
  EXPECT_EQ(2, data[0].size());
  EXPECT_EQ(std::vector<lsn_t>({20}), data[0][logid_t(1)].records);
  EXPECT_EQ(std::vector<lsn_t>({300}), data[0][logid_t(2)].records);
}

// Test the findtime api.
#define FINDTIME(logid, timestamp, lo, hi, expected_lo, expected_hi) \
  {                                                                  \
    lsn_t lo_ = lo;                                                  \
    lsn_t hi_ = hi;                                                  \
    int rv = store_->findTime(                                       \
        logid, std::chrono::milliseconds(timestamp), &lo_, &hi_);    \
    ASSERT_EQ(0, rv);                                                \
    EXPECT_EQ(expected_lo, lo_);                                     \
    EXPECT_EQ(expected_hi, hi_);                                     \
  }

// Test the findKey API.
#define FINDKEY(logid, key, expected_lo, expected_hi, approximate) \
  {                                                                \
    lsn_t lo_;                                                     \
    lsn_t hi_;                                                     \
    int rv = store_->findKey(logid, key, &lo_, &hi_, approximate); \
    ASSERT_EQ(0, rv);                                              \
    EXPECT_EQ(expected_lo, lo_);                                   \
    EXPECT_EQ(expected_hi, hi_);                                   \
  }

void testFindTimeFuzzy(TestPartitionedRocksDBStore& store,
                       logid_t logid,
                       uint64_t timestamp,
                       lsn_t /*last_released_lsn*/,
                       lsn_t expected_lo) {
  // Findtime() with approximate option will not use low and high
  // boundaries hint optimization
  lsn_t lo_ = LSN_INVALID;
  lsn_t hi_ = LSN_MAX;
  int rv = store.findTime(
      logid, std::chrono::milliseconds(timestamp), &lo_, &hi_, true);
  ASSERT_EQ(0, rv);
  EXPECT_EQ(expected_lo, lo_);
  EXPECT_GT(hi_, lo_);
}

TEST_F(PartitionedRocksDBStoreTest, FindTimeSimple) {
  logid_t logid(3);

  put({TestRecord(logid, 10, BASE_TIME)});
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME));
  store_->createPartition();
  put({TestRecord(logid, 20, BASE_TIME + 1)});
  put({TestRecord(logid, 30, BASE_TIME + 20)});
  put({TestRecord(logid, 40, BASE_TIME + 30)});
  put({TestRecord(logid, 50, BASE_TIME + 40)});
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + 40));
  store_->createPartition();
  put({TestRecord(logid, 60, BASE_TIME + 50)});

  FINDTIME(logid, BASE_TIME - 20, LSN_INVALID, LSN_MAX, LSN_INVALID, 10);
  FINDTIME(logid, BASE_TIME + 20, LSN_INVALID, LSN_MAX, 20, 30);
  FINDTIME(logid, BASE_TIME + 25, LSN_INVALID, LSN_MAX, 30, 40);
  FINDTIME(logid, BASE_TIME + 30, LSN_INVALID, LSN_MAX, 30, 40);
  FINDTIME(logid, BASE_TIME + 90, LSN_INVALID, LSN_MAX, 60, LSN_MAX);
}

TEST_F(PartitionedRocksDBStoreTest, FindTimeTimedout) {
  logid_t logid(3);

  put({TestRecord(logid, 10, BASE_TIME)});
  store_->createPartition();
  put({TestRecord(logid, 20, BASE_TIME + 1)});
  store_->createPartition();
  put({TestRecord(logid, 60, BASE_TIME + 50)});

  lsn_t lo_ = LSN_INVALID;
  lsn_t hi_ = LSN_MAX;
  int rv = store_->findTime(
      logid,
      std::chrono::milliseconds(BASE_TIME + 2),
      &lo_,
      &hi_,
      false, // approximate
      true,  // allow blocking
      std::chrono::steady_clock::now() - std::chrono::seconds(1));
  ASSERT_EQ(-1, rv);
  ASSERT_EQ(E::TIMEDOUT, err);
  ASSERT_EQ(LSN_INVALID, lo_);
  ASSERT_EQ(LSN_MAX, hi_);
}

TEST_F(PartitionedRocksDBStoreTest, FindTimeApproximate) {
  logid_t logid(3);

  put({TestRecord(logid, 10, BASE_TIME)});
  store_->createPartition();
  put({TestRecord(logid, 20, BASE_TIME + 1)});
  put({TestRecord(logid, 30, BASE_TIME + 20)});
  put({TestRecord(logid, 40, BASE_TIME + 30)});
  put({TestRecord(logid, 50, BASE_TIME + 40)});
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + 40));
  store_->createPartition();
  lsn_t last_released_lsn = 60;
  put({TestRecord(logid, last_released_lsn, BASE_TIME + 50)});

  testFindTimeFuzzy(*store_, logid, BASE_TIME - 20, last_released_lsn, 0);
  testFindTimeFuzzy(*store_, logid, BASE_TIME + 25, last_released_lsn, 19);
  testFindTimeFuzzy(*store_, logid, BASE_TIME + 90, last_released_lsn, 59);
  // Drop first partition which have timestamp 0 to check findTime() behaviour
  // when requested timestamp is less than timestamps of all partitions
  store_->dropPartitionsUpTo(ID0 + 1);
  testFindTimeFuzzy(
      *store_, logid, LSN_INVALID, last_released_lsn, LSN_INVALID);
}

TEST_F(PartitionedRocksDBStoreTest, FindTimeSimpleWithLoLimit) {
  logid_t logid(3);

  put({TestRecord(logid, 10, BASE_TIME)});
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + 1));
  store_->createPartition();
  put({TestRecord(logid, 20, BASE_TIME + 1)});
  put({TestRecord(logid, 30, BASE_TIME + 20)});
  put({TestRecord(logid, 40, BASE_TIME + 30)});
  // Lo limit only allows records starting from here to be considered.
  put({TestRecord(logid, 50, BASE_TIME + 40)});
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + 41));
  store_->createPartition();
  put({TestRecord(logid, 60, BASE_TIME + 50)});

  FINDTIME(logid, BASE_TIME - 20, 41, LSN_MAX, 41, 42);
  FINDTIME(logid, BASE_TIME + 20, 41, LSN_MAX, 41, 50);
  FINDTIME(logid, BASE_TIME + 25, 41, LSN_MAX, 41, 50);
  FINDTIME(logid, BASE_TIME + 30, 41, LSN_MAX, 41, 50);
  FINDTIME(logid, BASE_TIME + 90, 41, LSN_MAX, 60, LSN_MAX);
}

TEST_F(PartitionedRocksDBStoreTest, FindTimeSimpleWithHiLimit) {
  logid_t logid(3);

  put({TestRecord(logid, 10, BASE_TIME)});
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME));
  store_->createPartition();
  put({TestRecord(logid, 20, BASE_TIME + 1)});
  put({TestRecord(logid, 30, BASE_TIME + 20)});
  // Hi limit only allows record up to this point to be considered.
  put({TestRecord(logid, 40, BASE_TIME + 30)});
  put({TestRecord(logid, 50, BASE_TIME + 40)});
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + 45));
  store_->createPartition();
  put({TestRecord(logid, 60, BASE_TIME + 50)});

  FINDTIME(logid, BASE_TIME - 20, LSN_INVALID, 35, LSN_INVALID, 10);
  FINDTIME(logid, BASE_TIME + 20, LSN_INVALID, 35, 20, 30);
  FINDTIME(logid, BASE_TIME + 25, LSN_INVALID, 35, 30, 60);
  FINDTIME(logid, BASE_TIME + 30, LSN_INVALID, 35, 30, 60);
  FINDTIME(logid, BASE_TIME + 90, LSN_INVALID, 35, 50, LSN_MAX);
}

TEST_F(PartitionedRocksDBStoreTest, FindTimeNoDataForLogInMultiplePartitions) {
  logid_t logid(3);
  logid_t logid_searched(42);

  // partition 0
  put({TestRecord(logid, 10, BASE_TIME)});
  put({TestRecord(logid_searched, 1335, BASE_TIME + 1)});
  put({TestRecord(logid_searched, 1336, BASE_TIME + 2)});
  put({TestRecord(logid, 11, BASE_TIME + 3)});
  put({TestRecord(logid, 20, BASE_TIME + 4)});
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + 10));
  store_->createPartition();
  // partition 1
  put({TestRecord(logid, 30, BASE_TIME + 20)});
  put({TestRecord(logid, 35, BASE_TIME + 25)});
  put({TestRecord(logid, 40, BASE_TIME + 30)});
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + 31));
  store_->createPartition();
  // partition 2
  put({TestRecord(logid, 45, BASE_TIME + 32)});
  put({TestRecord(logid, 50, BASE_TIME + 40)});
  put({TestRecord(logid, 60, BASE_TIME + 50)});
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + 50));
  store_->createPartition();
  // partition 3
  put({TestRecord(logid, 61, BASE_TIME + 51)});
  put({TestRecord(logid, 62, BASE_TIME + 52)});
  put({TestRecord(logid, 63, BASE_TIME + 53)});
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + 54));
  store_->createPartition();
  // partition 4
  put({TestRecord(logid, 64, BASE_TIME + 54)});
  put({TestRecord(logid_searched, 1337, BASE_TIME + 3)}); // goes to partition 0
  put({TestRecord(logid_searched, 1339, BASE_TIME + 55)});
  put({TestRecord(logid, 65, BASE_TIME + 55)});

  // Partition 4 should be selected for the binary search. The binary search
  // will find hi=1339 but will not find lo, which is 1337 in partition 0.
  FINDTIME(logid_searched, BASE_TIME + 33, LSN_INVALID, LSN_MAX, 1337, 1339);
}

TEST_F(PartitionedRocksDBStoreTest, FindTimeBehindTrimPoint) {
  logid_t logid(3);

  // partition 0
  put({TestRecord(logid, 10, BASE_TIME)});
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME));
  store_->createPartition();
  // partition 1
  put({TestRecord(logid, 20, BASE_TIME + 1)});
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + 1));
  store_->createPartition();
  // partition 2
  // Lo limit only allows record up to this point to be considered.
  put({TestRecord(logid, 30, BASE_TIME + 2)});
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + 2));
  store_->createPartition();
  // partition 3
  put({TestRecord(logid, 40, BASE_TIME + 3)});

  // Pretend lsns up to (and including) 30 are trimmed.
  FINDTIME(logid, BASE_TIME, 30, LSN_MAX, 30, 31);
}

TEST_F(PartitionedRocksDBStoreTest, FindTimeWithIndexSimple) {
  logid_t logid(3);
  openStoreWithReadFindTimeIndex();

  // partition 0
  put({TestRecord(logid, 10, /*index=*/true, BASE_TIME)});
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME));
  store_->createPartition();
  // partition 1
  put({TestRecord(logid, 20, /*index=*/true, BASE_TIME + 1)});
  put({TestRecord(logid, 30, /*index=*/true, BASE_TIME + 2)});
  put({TestRecord(logid, 40, /*index=*/true, BASE_TIME + 3)});
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + 4));
  store_->createPartition();
  // partition 2
  put({TestRecord(logid, 50, /*index=*/true, BASE_TIME + 5)});

  FINDTIME(logid, BASE_TIME + 3, LSN_INVALID, LSN_MAX, 30, 40);
}

TEST_F(PartitionedRocksDBStoreTest, FindTimeWithIndexLoBehind) {
  logid_t logid(3);
  openStoreWithReadFindTimeIndex();

  // partition 0
  put({TestRecord(logid, 10, /*index=*/true, BASE_TIME)});
  put({TestRecord(logid, 20, /*index=*/true, BASE_TIME + 1)});
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + 2));
  store_->createPartition();
  // partition 1
  put({TestRecord(logid, 30, /*index=*/true, BASE_TIME + 3)});
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + 4));
  store_->createPartition();
  // partition 2
  put({TestRecord(logid, 40, /*index=*/true, BASE_TIME + 5)});

  FINDTIME(logid, BASE_TIME + 3, LSN_INVALID, LSN_MAX, 20, 30);
}

TEST_F(PartitionedRocksDBStoreTest, FindTimeWithIndexHiAhead) {
  logid_t logid(3);
  openStoreWithReadFindTimeIndex();

  // partition 0
  put({TestRecord(logid, 10, /*index=*/true, BASE_TIME)});
  put({TestRecord(logid, 20, /*index=*/true, BASE_TIME + 1)});
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + 2));
  store_->createPartition();
  // partition 1
  put({TestRecord(logid, 30, /*index=*/true, BASE_TIME + 3)});
  put({TestRecord(logid, 40, /*index=*/false, BASE_TIME + 4)});
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + 5));
  store_->createPartition();
  // partition 2
  put({TestRecord(logid, 50, /*index=*/true, BASE_TIME + 6)});

  FINDTIME(logid, BASE_TIME + 4, LSN_INVALID, LSN_MAX, 30, 50);
}

TEST_F(PartitionedRocksDBStoreTest, FindTimeWithIndexLoBehindAndHiAhead) {
  logid_t logid(3);
  openStoreWithReadFindTimeIndex();

  // partition 0
  put({TestRecord(logid, 10, /*index=*/true, BASE_TIME)});
  put({TestRecord(logid, 20, /*index=*/true, BASE_TIME + 1)});
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + 2));
  store_->createPartition();
  // partition 1
  put({TestRecord(logid, 30, /*index=*/false, BASE_TIME + 3)});
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + 4));
  store_->createPartition();
  // partition 2
  put({TestRecord(logid, 40, /*index=*/true, BASE_TIME + 5)});

  FINDTIME(logid, BASE_TIME + 3, LSN_INVALID, LSN_MAX, 20, 40);
}

TEST_F(PartitionedRocksDBStoreTest, FindTimeWithIndexSearchInMorePartitions) {
  logid_t logid(3);
  openStoreWithReadFindTimeIndex();

  // partition 0
  put({TestRecord(logid, 10, /*index=*/true, BASE_TIME)});
  put({TestRecord(logid, 20, /*index=*/false, BASE_TIME + 1)});
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + 2));
  store_->createPartition();
  // partition 1
  put({TestRecord(logid, 30, /*index=*/false, BASE_TIME + 3)});
  put({TestRecord(logid, 40, /*index=*/false, BASE_TIME + 4)});
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + 5));
  store_->createPartition();
  // partition 2
  put({TestRecord(logid, 50, /*index=*/false, BASE_TIME + 6)});
  put({TestRecord(logid, 60, /*index=*/false, BASE_TIME + 7)});
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + 8));
  store_->createPartition();
  // partition 3
  put({TestRecord(logid, 70, /*index=*/false, BASE_TIME + 9)});
  put({TestRecord(logid, 80, /*index=*/false, BASE_TIME + 10)});
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + 11));
  store_->createPartition();
  // partition 4
  put({TestRecord(logid, 90, /*index=*/true, BASE_TIME + 12)});
  put({TestRecord(logid, 100, /*index=*/true, BASE_TIME + 13)});

  // Finds upper bound in partition 4, takes max_lsn of partition 3 as lo.
  FINDTIME(logid, BASE_TIME + 12, LSN_INVALID, LSN_MAX, 80, 90);
  // Finds lower bound in partition 0, takes min_lsn of partition 1 as hi.
  FINDTIME(logid, BASE_TIME + 1, LSN_INVALID, LSN_MAX, 10, 30);
}

TEST_F(PartitionedRocksDBStoreTest, FindTimeWithIndex) {
  logid_t logid(3);
  openStoreWithReadFindTimeIndex();

  // partition 0
  put({TestRecord(logid, 10, /*index=*/true, BASE_TIME)});
  put({TestRecord(logid, 20, /*index=*/true, BASE_TIME + 1)});
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + 2));
  store_->createPartition();
  // partition 1
  put({TestRecord(logid, 30, /*index=*/true, BASE_TIME + 3)});
  put({TestRecord(logid, 40, /*index=*/true, BASE_TIME + 4)});
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + 5));
  store_->createPartition();
  // partition 2
  put({TestRecord(logid, 50, /*index=*/true, BASE_TIME + 6)});

  FINDTIME(logid, BASE_TIME + 3, 31, 42, 31, 32);
}

TEST_F(PartitionedRocksDBStoreTest, FindTimeUnpartitionedInternalLog) {
  // Perform findtime on an internal log, which should be in the unpartitioned
  // column family.
  logid_t logid(configuration::InternalLogs::EVENT_LOG_DELTAS);

  put({TestRecord(logid, 10, BASE_TIME)});
  put({TestRecord(logid, 20, BASE_TIME + 1)});
  put({TestRecord(logid, 30, BASE_TIME + 20)});
  put({TestRecord(logid, 40, BASE_TIME + 30)});
  put({TestRecord(logid, 50, BASE_TIME + 40)});
  put({TestRecord(logid, 60, BASE_TIME + 50)});

  FINDTIME(logid, BASE_TIME - 20, LSN_INVALID, LSN_MAX, LSN_INVALID, 10);
  FINDTIME(logid, BASE_TIME + 20, LSN_INVALID, LSN_MAX, 20, 30);
  FINDTIME(logid, BASE_TIME + 25, LSN_INVALID, LSN_MAX, 30, 40);
  FINDTIME(logid, BASE_TIME + 30, LSN_INVALID, LSN_MAX, 30, 40);
  FINDTIME(logid, BASE_TIME + 90, LSN_INVALID, LSN_MAX, 60, LSN_MAX);
}

TEST_F(PartitionedRocksDBStoreTest, FindTimeUnpartitionedMetaDataLog) {
  // Perform findtime on a metadata log, which should be in the unpartitioned
  // column family.
  logid_t logid(configuration::InternalLogs::EVENT_LOG_DELTAS);
  const logid_t meta_log = MetaDataLog::metaDataLogID(logid);

  put({TestRecord(meta_log, 10, BASE_TIME)});
  put({TestRecord(meta_log, 20, BASE_TIME + 1)});
  put({TestRecord(meta_log, 30, BASE_TIME + 20)});
  put({TestRecord(meta_log, 40, BASE_TIME + 30)});
  put({TestRecord(meta_log, 50, BASE_TIME + 40)});
  put({TestRecord(meta_log, 60, BASE_TIME + 50)});

  FINDTIME(meta_log, BASE_TIME - 20, LSN_INVALID, LSN_MAX, LSN_INVALID, 10);
  FINDTIME(meta_log, BASE_TIME + 20, LSN_INVALID, LSN_MAX, 20, 30);
  FINDTIME(meta_log, BASE_TIME + 25, LSN_INVALID, LSN_MAX, 30, 40);
  FINDTIME(meta_log, BASE_TIME + 30, LSN_INVALID, LSN_MAX, 30, 40);
  FINDTIME(meta_log, BASE_TIME + 90, LSN_INVALID, LSN_MAX, 60, LSN_MAX);
}

TEST_F(PartitionedRocksDBStoreTest, FindTimeUnpartitionedInternalLogWithIndex) {
  // Perform findtime on an internal log, which should be in the unpartitioned
  // column family. Use the find time index.
  logid_t logid(configuration::InternalLogs::EVENT_LOG_DELTAS);
  openStoreWithReadFindTimeIndex();

  put({TestRecord(logid, 10, /*index=*/true, BASE_TIME)});
  put({TestRecord(logid, 20, /*index=*/true, BASE_TIME + 1)});
  put({TestRecord(logid, 30, /*index=*/true, BASE_TIME + 20)});
  put({TestRecord(logid, 40, /*index=*/true, BASE_TIME + 30)});
  put({TestRecord(logid, 50, /*index=*/true, BASE_TIME + 40)});
  put({TestRecord(logid, 60, /*index=*/true, BASE_TIME + 50)});

  FINDTIME(logid, BASE_TIME - 20, LSN_INVALID, LSN_MAX, LSN_INVALID, 10);
  FINDTIME(logid, BASE_TIME + 20, LSN_INVALID, LSN_MAX, 20, 30);
  FINDTIME(logid, BASE_TIME + 25, LSN_INVALID, LSN_MAX, 30, 40);
  FINDTIME(logid, BASE_TIME + 30, LSN_INVALID, LSN_MAX, 30, 40);
  FINDTIME(logid, BASE_TIME + 90, LSN_INVALID, LSN_MAX, 60, LSN_MAX);
}

TEST_F(PartitionedRocksDBStoreTest, FindTimeUnpartitionedMetaDataLogWithIndex) {
  // Perform findtime on a metadata log, which should be in the unpartitioned
  // column family. Use the find time index.
  logid_t logid(configuration::InternalLogs::EVENT_LOG_DELTAS);
  const logid_t meta_log = MetaDataLog::metaDataLogID(logid);
  openStoreWithReadFindTimeIndex();

  put({TestRecord(meta_log, 10, /*index=*/true, BASE_TIME)});
  put({TestRecord(meta_log, 20, /*index=*/true, BASE_TIME + 1)});
  put({TestRecord(meta_log, 30, /*index=*/true, BASE_TIME + 20)});
  put({TestRecord(meta_log, 40, /*index=*/true, BASE_TIME + 30)});
  put({TestRecord(meta_log, 50, /*index=*/true, BASE_TIME + 40)});
  put({TestRecord(meta_log, 60, /*index=*/true, BASE_TIME + 50)});

  FINDTIME(meta_log, BASE_TIME - 20, LSN_INVALID, LSN_MAX, LSN_INVALID, 10);
  FINDTIME(meta_log, BASE_TIME + 20, LSN_INVALID, LSN_MAX, 20, 30);
  FINDTIME(meta_log, BASE_TIME + 25, LSN_INVALID, LSN_MAX, 30, 40);
  FINDTIME(meta_log, BASE_TIME + 30, LSN_INVALID, LSN_MAX, 30, 40);
  FINDTIME(meta_log, BASE_TIME + 90, LSN_INVALID, LSN_MAX, 60, LSN_MAX);
}

TEST_F(PartitionedRocksDBStoreTest, FindKeyApproximateSimple) {
  logid_t logid(3);
  openStore();

  // partition 0
  put({TestRecord(
      logid, 10, false, BASE_TIME, folly::Optional<std::string>("10000000"))});
  put({TestRecord(logid,
                  20,
                  false,
                  BASE_TIME + 1,
                  folly::Optional<std::string>("10000002"))});
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + 2));
  store_->createPartition();
  // partition 1
  put({TestRecord(logid,
                  30,
                  false,
                  BASE_TIME + 3,
                  folly::Optional<std::string>("10000004"))});
  put({TestRecord(logid,
                  40,
                  false,
                  BASE_TIME + 4,
                  folly::Optional<std::string>("10000006"))});
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + 5));
  store_->createPartition();
  // partition 2 - empty
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + 6));
  store_->createPartition();
  // partition 3 - has only keyless records
  put({TestRecord(logid, 50, false, BASE_TIME + 7)});
  put({TestRecord(logid, 60, false, BASE_TIME + 8)});
  put({TestRecord(logid, 70, false, BASE_TIME + 9)});
  put({TestRecord(logid, 80, false, BASE_TIME + 10)});
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + 11));
  store_->createPartition();
  // partition 4
  put({TestRecord(logid,
                  90,
                  false,
                  BASE_TIME + 12,
                  folly::Optional<std::string>("10000008"))});
  put({TestRecord(logid,
                  100,
                  false,
                  BASE_TIME + 13,
                  folly::Optional<std::string>("10000010"))});
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + 14));
  store_->createPartition();
  // partition 5 - empty
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + 15));
  store_->createPartition();
  // partition 6 - empty
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + 16));
  store_->createPartition();
  // partition 7 - has some keyless records
  put({TestRecord(logid,
                  110,
                  false,
                  BASE_TIME + 17,
                  folly::Optional<std::string>("10000012"))});
  put({TestRecord(logid, 120, false, BASE_TIME + 18)});
  put({TestRecord(logid, 130, false, BASE_TIME + 19)});
  put({TestRecord(logid,
                  140,
                  false,
                  BASE_TIME + 20,
                  folly::Optional<std::string>("10000014"))});
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + 21));
  store_->createPartition();
  // partition 8
  put({TestRecord(logid,
                  150,
                  false,
                  BASE_TIME + 22,
                  folly::Optional<std::string>("10000016"))});
  put({TestRecord(logid,
                  160,
                  false,
                  BASE_TIME + 23,
                  folly::Optional<std::string>("10000018"))});
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + 24));
  store_->createPartition();
  // partition 9 - has only keyless records
  put({TestRecord(logid, 170, false, BASE_TIME + 25)});
  put({TestRecord(logid, 180, false, BASE_TIME + 26)});
  put({TestRecord(logid, 190, false, BASE_TIME + 27)});
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + 28));
  store_->createPartition();

  FINDKEY(logid, std::string("10000000"), LSN_INVALID, 10, true);
  FINDKEY(logid, std::string("10000001"), 10, 30, true);
  FINDKEY(logid, std::string("10000002"), 10, 30, true);
  FINDKEY(logid, std::string("10000003"), 10, 30, true);
  FINDKEY(logid, std::string("10000004"), 10, 30, true);
  FINDKEY(logid, std::string("10000005"), 30, 90, true);
  FINDKEY(logid, std::string("10000006"), 30, 90, true);
  FINDKEY(logid, std::string("10000007"), 30, 90, true);
  FINDKEY(logid, std::string("10000008"), 30, 90, true);
  FINDKEY(logid, std::string("10000009"), 90, 110, true);
  FINDKEY(logid, std::string("10000010"), 90, 110, true);
  FINDKEY(logid, std::string("10000011"), 90, 110, true);
  FINDKEY(logid, std::string("10000012"), 90, 110, true);
  FINDKEY(logid, std::string("10000013"), 110, 150, true);
  FINDKEY(logid, std::string("10000014"), 110, 150, true);
  FINDKEY(logid, std::string("10000015"), 110, 150, true);
  FINDKEY(logid, std::string("10000016"), 110, 150, true);
  FINDKEY(logid, std::string("10000017"), 150, LSN_MAX, true);
  FINDKEY(logid, std::string("10000018"), 150, LSN_MAX, true);
}

TEST_F(PartitionedRocksDBStoreTest, FindKeyApproximateDifferentKeyLengths) {
  logid_t logid(3);
  openStore();

  // partition 0
  put({TestRecord(
      logid, 10, false, BASE_TIME, folly::Optional<std::string>("10000000"))});
  put({TestRecord(
      logid, 20, false, BASE_TIME + 1, folly::Optional<std::string>("10002"))});
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + 2));
  store_->createPartition();
  // partition 1
  put({TestRecord(logid,
                  30,
                  false,
                  BASE_TIME + 3,
                  folly::Optional<std::string>("100024"))});
  put({TestRecord(logid,
                  40,
                  false,
                  BASE_TIME + 4,
                  folly::Optional<std::string>("1000246"))});
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + 5));
  store_->createPartition();

  FINDKEY(logid, std::string("10000000"), LSN_INVALID, 10, true);
  FINDKEY(logid, std::string("10001"), 10, 30, true);
  FINDKEY(logid, std::string("10002"), 10, 30, true);
  FINDKEY(logid, std::string("100024"), 10, 30, true);
  FINDKEY(logid, std::string("1000243"), 30, LSN_MAX, true);
  FINDKEY(logid, std::string("1000246"), 30, LSN_MAX, true);
}

TEST_F(PartitionedRocksDBStoreTest, FindKeyApproximateSameKeyFor2LSNs) {
  logid_t logid(3);
  openStore();

  // partition 0
  put({TestRecord(
      logid, 10, false, BASE_TIME, folly::Optional<std::string>("10000000"))});
  put({TestRecord(logid,
                  20,
                  false,
                  BASE_TIME + 1,
                  folly::Optional<std::string>("10000001"))});
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + 2));
  store_->createPartition();
  // partition 1
  put({TestRecord(logid,
                  30,
                  false,
                  BASE_TIME + 3,
                  folly::Optional<std::string>("10000002"))});
  put({TestRecord(logid,
                  40,
                  false,
                  BASE_TIME + 4,
                  folly::Optional<std::string>("10000002"))});
  put({TestRecord(logid,
                  50,
                  false,
                  BASE_TIME + 5,
                  folly::Optional<std::string>("10000003"))});
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + 6));
  store_->createPartition();

  FINDKEY(logid, std::string("10000002"), 10, 30, true);
}

TEST_F(PartitionedRocksDBStoreTest, FindKeyStrictSimple) {
  logid_t logid(3);
  openStore();

  // partition 0
  put({TestRecord(
      logid, 10, false, BASE_TIME, folly::Optional<std::string>("10000000"))});
  put({TestRecord(logid,
                  20,
                  false,
                  BASE_TIME + 1,
                  folly::Optional<std::string>("10000002"))});
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + 2));
  store_->createPartition();
  // partition 1
  put({TestRecord(logid,
                  30,
                  false,
                  BASE_TIME + 3,
                  folly::Optional<std::string>("10000004"))});
  put({TestRecord(logid,
                  40,
                  false,
                  BASE_TIME + 4,
                  folly::Optional<std::string>("10000006"))});
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + 5));
  store_->createPartition();
  // partition 2 - empty
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + 6));
  store_->createPartition();
  // partition 3 - has only keyless records
  put({TestRecord(logid, 50, false, BASE_TIME + 7)});
  put({TestRecord(logid, 60, false, BASE_TIME + 8)});
  put({TestRecord(logid, 70, false, BASE_TIME + 9)});
  put({TestRecord(logid, 80, false, BASE_TIME + 10)});
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + 11));
  store_->createPartition();
  // partition 4
  put({TestRecord(logid,
                  90,
                  false,
                  BASE_TIME + 12,
                  folly::Optional<std::string>("10000008"))});
  put({TestRecord(logid,
                  100,
                  false,
                  BASE_TIME + 13,
                  folly::Optional<std::string>("10000010"))});
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + 14));
  store_->createPartition();
  // partition 5 - empty
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + 15));
  store_->createPartition();
  // partition 6 - empty
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + 16));
  store_->createPartition();
  // partition 7 - has some keyless records
  put({TestRecord(logid,
                  110,
                  false,
                  BASE_TIME + 17,
                  folly::Optional<std::string>("10000012"))});
  put({TestRecord(logid, 120, false, BASE_TIME + 18)});
  put({TestRecord(logid, 130, false, BASE_TIME + 19)});
  put({TestRecord(logid,
                  140,
                  false,
                  BASE_TIME + 20,
                  folly::Optional<std::string>("10000014"))});
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + 21));
  store_->createPartition();
  // partition 8
  put({TestRecord(logid,
                  150,
                  false,
                  BASE_TIME + 22,
                  folly::Optional<std::string>("10000016"))});
  put({TestRecord(logid,
                  160,
                  false,
                  BASE_TIME + 23,
                  folly::Optional<std::string>("10000018"))});
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + 24));
  store_->createPartition();
  // partition 9 - has only keyless records
  put({TestRecord(logid, 170, false, BASE_TIME + 25)});
  put({TestRecord(logid, 180, false, BASE_TIME + 26)});
  put({TestRecord(logid, 190, false, BASE_TIME + 27)});
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + 28));
  store_->createPartition();

  FINDKEY(logid, std::string("10000000"), LSN_INVALID, 10, false);
  FINDKEY(logid, std::string("10000001"), 10, 20, false);
  FINDKEY(logid, std::string("10000002"), 10, 20, false);
  FINDKEY(logid, std::string("10000003"), 20, 30, false);
  FINDKEY(logid, std::string("10000004"), 20, 30, false);
  FINDKEY(logid, std::string("10000005"), 30, 40, false);
  FINDKEY(logid, std::string("10000006"), 30, 40, false);
  FINDKEY(logid, std::string("10000007"), 40, 90, false);
  FINDKEY(logid, std::string("10000008"), 40, 90, false);
  FINDKEY(logid, std::string("10000009"), 90, 100, false);
  FINDKEY(logid, std::string("10000010"), 90, 100, false);
  FINDKEY(logid, std::string("10000011"), 100, 110, false);
  FINDKEY(logid, std::string("10000012"), 100, 110, false);
  FINDKEY(logid, std::string("10000013"), 110, 140, false);
  FINDKEY(logid, std::string("10000014"), 110, 140, false);
  FINDKEY(logid, std::string("10000015"), 140, 150, false);
  FINDKEY(logid, std::string("10000016"), 140, 150, false);
  FINDKEY(logid, std::string("10000017"), 150, 160, false);
  FINDKEY(logid, std::string("10000018"), 150, 160, false);
  FINDKEY(logid, std::string("10000019"), 160, LSN_MAX, false);
}

TEST_F(PartitionedRocksDBStoreTest, FindKeyStrictSameKeyAtPartitionBoundaries) {
  logid_t logid(3);
  openStore();

  // partition 0
  put({TestRecord(
      logid, 10, false, BASE_TIME, folly::Optional<std::string>("10000000"))});
  put({TestRecord(logid,
                  20,
                  false,
                  BASE_TIME + 1,
                  folly::Optional<std::string>("10000002"))});
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + 2));
  store_->createPartition();
  // partition 1
  put({TestRecord(logid,
                  30,
                  false,
                  BASE_TIME + 3,
                  folly::Optional<std::string>("10000002"))});
  put({TestRecord(logid,
                  40,
                  false,
                  BASE_TIME + 4,
                  folly::Optional<std::string>("10000002"))});
  put({TestRecord(logid,
                  50,
                  false,
                  BASE_TIME + 5,
                  folly::Optional<std::string>("10000004"))});
  put({TestRecord(logid,
                  60,
                  false,
                  BASE_TIME + 6,
                  folly::Optional<std::string>("10000006"))});
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + 7));
  store_->createPartition();

  FINDKEY(logid, std::string("10000000"), LSN_INVALID, 10, false);
  FINDKEY(logid, std::string("10000001"), 10, 20, false);
  FINDKEY(logid, std::string("10000002"), 10, 20, false);
  FINDKEY(logid, std::string("10000003"), 40, 50, false);
  FINDKEY(logid, std::string("10000004"), 40, 50, false);
  FINDKEY(logid, std::string("10000005"), 50, 60, false);
  FINDKEY(logid, std::string("10000006"), 50, 60, false);
  FINDKEY(logid, std::string("10000007"), 60, LSN_MAX, false);
}

TEST_F(PartitionedRocksDBStoreTest, FindKeyStrictSameMinKey) {
  logid_t logid(3);
  openStore();

  // partition 0
  put({TestRecord(
      logid, 10, false, BASE_TIME, folly::Optional<std::string>("10000000"))});
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + 2));
  store_->createPartition();
  // partition 1 - contains 2 records with minimum key
  put({TestRecord(logid,
                  30,
                  false,
                  BASE_TIME + 3,
                  folly::Optional<std::string>("10000002"))});
  put({TestRecord(logid,
                  40,
                  false,
                  BASE_TIME + 4,
                  folly::Optional<std::string>("10000002"))});
  put({TestRecord(logid,
                  50,
                  false,
                  BASE_TIME + 5,
                  folly::Optional<std::string>("10000004"))});
  put({TestRecord(logid,
                  60,
                  false,
                  BASE_TIME + 6,
                  folly::Optional<std::string>("10000006"))});
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + 7));
  store_->createPartition();

  FINDKEY(logid, std::string("10000000"), LSN_INVALID, 10, false);
  FINDKEY(logid, std::string("10000001"), 10, 30, false);
  FINDKEY(logid, std::string("10000002"), 10, 30, false);
  FINDKEY(logid, std::string("10000003"), 40, 50, false);
  FINDKEY(logid, std::string("10000004"), 40, 50, false);
}

TEST_F(PartitionedRocksDBStoreTest, FindKeyStrictSameMaxKey) {
  logid_t logid(3);
  openStore();

  // partition 0 - contains 2 records with maximum key
  put({TestRecord(
      logid, 10, false, BASE_TIME, folly::Optional<std::string>("10000000"))});
  put({TestRecord(logid,
                  30,
                  false,
                  BASE_TIME + 1,
                  folly::Optional<std::string>("10000002"))});
  put({TestRecord(logid,
                  40,
                  false,
                  BASE_TIME + 2,
                  folly::Optional<std::string>("10000002"))});
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + 3));
  store_->createPartition();
  // partition 1
  put({TestRecord(logid,
                  50,
                  false,
                  BASE_TIME + 4,
                  folly::Optional<std::string>("10000004"))});
  put({TestRecord(logid,
                  60,
                  false,
                  BASE_TIME + 5,
                  folly::Optional<std::string>("10000006"))});
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + 6));
  store_->createPartition();

  FINDKEY(logid, std::string("10000000"), LSN_INVALID, 10, false);
  FINDKEY(logid, std::string("10000001"), 10, 30, false);
  FINDKEY(logid, std::string("10000002"), 10, 30, false);
  FINDKEY(logid, std::string("10000003"), 40, 50, false);
  FINDKEY(logid, std::string("10000004"), 40, 50, false);
  FINDKEY(logid, std::string("10000005"), 50, 60, false);
}

TEST_F(PartitionedRocksDBStoreTest, FindKeyApproximatePartitionDrop) {
  logid_t logid(3);
  openStore();

  // partition 0
  put({TestRecord(
      logid, 10, false, BASE_TIME, folly::Optional<std::string>("10000000"))});
  put({TestRecord(logid,
                  20,
                  false,
                  BASE_TIME + 1,
                  folly::Optional<std::string>("10000002"))});
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + 2));
  store_->createPartition();
  // partition 1
  put({TestRecord(logid,
                  30,
                  false,
                  BASE_TIME + 3,
                  folly::Optional<std::string>("10000004"))});
  put({TestRecord(logid,
                  40,
                  false,
                  BASE_TIME + 4,
                  folly::Optional<std::string>("10000006"))});
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + 5));
  store_->createPartition();
  // partition 2 - empty
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + 6));
  store_->createPartition();

  FINDKEY(logid, std::string("10000000"), LSN_INVALID, 10, true);
  FINDKEY(logid, std::string("10000001"), 10, 30, true);
  FINDKEY(logid, std::string("10000002"), 10, 30, true);
  FINDKEY(logid, std::string("10000003"), 10, 30, true);
  FINDKEY(logid, std::string("10000004"), 10, 30, true);

  store_->dropPartitionsUpTo(ID0 + 1);

  FINDKEY(logid, std::string("10000000"), LSN_INVALID, 30, true);
  FINDKEY(logid, std::string("10000001"), LSN_INVALID, 30, true);
  FINDKEY(logid, std::string("10000002"), LSN_INVALID, 30, true);
  FINDKEY(logid, std::string("10000003"), LSN_INVALID, 30, true);
  FINDKEY(logid, std::string("10000004"), LSN_INVALID, 30, true);

  store_->dropPartitionsUpTo(ID0 + 2);

  FINDKEY(logid, std::string("10000000"), LSN_INVALID, LSN_MAX, true);
  FINDKEY(logid, std::string("10000001"), LSN_INVALID, LSN_MAX, true);
  FINDKEY(logid, std::string("10000002"), LSN_INVALID, LSN_MAX, true);
  FINDKEY(logid, std::string("10000003"), LSN_INVALID, LSN_MAX, true);
  FINDKEY(logid, std::string("10000004"), LSN_INVALID, LSN_MAX, true);
}

TEST_F(PartitionedRocksDBStoreTest, FindKeyStrictDeleteRecords) {
  logid_t logid(3);
  openStore();

  // partition 0
  put({TestRecord(logid,
                  20,
                  false,
                  BASE_TIME + 1,
                  folly::Optional<std::string>("10000002"))});
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + 2));
  store_->createPartition();
  // partition 1
  put({TestRecord(logid,
                  30,
                  false,
                  BASE_TIME + 3,
                  folly::Optional<std::string>("10000004"))});
  put({TestRecord(logid,
                  40,
                  false,
                  BASE_TIME + 4,
                  folly::Optional<std::string>("10000006"))});
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + 5));
  store_->createPartition();

  FINDKEY(logid, std::string("10000005"), 30, 40, false);
  FINDKEY(logid, std::string("10000006"), 30, 40, false);

  // Both this record and its findKey index entry will be deleted.
  put({TestRecord(logid,
                  40,
                  TestRecord::Type::DELETE,
                  folly::Optional<std::string>("10000006"))});

  FINDKEY(logid, std::string("10000005"), 30, LSN_MAX, false);
  FINDKEY(logid, std::string("10000006"), 30, LSN_MAX, false);
}

TEST_F(PartitionedRocksDBStoreTest, DecreasingDirectory) {
  increasing_lsns_ = false;
  logid_t logid(1);

  put({TestRecord(logid, 1000)});
  store_->createPartition();
  store_->dropPartitionsUpTo(ID0 + 1);
  put({TestRecord(logid, 10)});

  readAndCheck();
  openStore();

  store_->createPartition();
  put({TestRecord(logid, 20)});

  store_->dropPartitionsUpTo(ID0 + 2);

  readAndCheck();
  openStore();
}

// Used to reproduce a race condition in getWritePartition().
TEST_F(PartitionedRocksDBStoreTest, DirectoryRace) {
  single_threaded_ = increasing_lsns_ = false;

  // The race only needs two put()s and one createPartition() ran concurrently.
  // It takes many attempts though, so we'll do them as fast as we can.

  const int log_cnt = 5;
  const int threads_per_log = 2;
  std::atomic<bool> shutdown{false};
  std::atomic<lsn_t> lsn[log_cnt] = {};
  std::vector<std::thread> threads(log_cnt * threads_per_log);

  for (int i = 0; i < threads.size(); ++i) {
    threads[i] = std::thread([&, i] {
      logid_t log = logid_t(i / threads_per_log + 1);
      while (!shutdown.load()) {
        lsn_t cur_lsn = std::max(1ul, ++lsn[log.val_ - 1] ^ 1);
        put({TestRecord(log, cur_lsn)});
        if (folly::Random::rand32() % 3 == 0) {
          store_->createPartition();
        }
      }
    });
  }

  /* sleep override */
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  shutdown.store(true);
  for (std::thread& t : threads) {
    t.join();
  }

  readAndCheck();
}

TEST_F(PartitionedRocksDBStoreTest, DeletesForEmptyLog) {
  no_deletes_ = false;
  put({TestRecord(logid_t(1), 1000, TestRecord::Type::DELETE)});

  auto data = readAndCheck();
  EXPECT_EQ(1, data.size());
  EXPECT_EQ(0, data[0].size());

  openStore();

  put({TestRecord(logid_t(2), 1000),
       TestRecord(logid_t(1), 1000, TestRecord::Type::DELETE),
       TestRecord(logid_t(2), 2000),
       TestRecord(logid_t(2), 2000, TestRecord::Type::DELETE)});

  data = readAndCheck();
  EXPECT_EQ(1, data.size());
  EXPECT_EQ(1, data[0].size());
  EXPECT_EQ(std::vector<lsn_t>({1000}), data[0][logid_t(2)].records);
}

TEST_F(PartitionedRocksDBStoreTest, IOPrio) {
  class CompactionFilter : public rocksdb::CompactionFilter {
   public:
    void setPriority() const {
      std::pair<int, int> p(-1, -1);
      int rv = get_io_priority_of_this_thread(&p);
      EXPECT_EQ(0, rv);
      {
        std::lock_guard<std::mutex> lock(mutex_);
        prios_.insert(p);
      }
    }

    bool Filter(int /*level*/,
                const rocksdb::Slice& /*key*/,
                const rocksdb::Slice& /*existing_value*/,
                std::string* /*new_value*/,
                bool* /*value_changed*/) const override {
      setPriority();
      return false;
    }

    bool FilterMergeOperand(int,
                            const rocksdb::Slice&,
                            const rocksdb::Slice&) const override {
      setPriority();
      return false;
    }

    const char* Name() const override {
      return "PartitionedRocksDBStoreTest::IOPrio::CompactionFilter";
    }

    std::set<std::pair<int, int>> getPrios() {
      std::lock_guard<std::mutex> lock(mutex_);
      return prios_;
    }

    mutable std::mutex mutex_;
    mutable std::set<std::pair<int, int>> prios_;
  };

  closeStore();
  ServerConfig::SettingsConfig s;
  s["rocksdb-num-bg-threads-lo"] = "2";
  s["rocksdb-low-ioprio"] = "2,2";

  std::unique_ptr<RocksDBEnv> env;
  CompactionFilter filter;

  auto customize_config = [&](RocksDBLogStoreConfig& cfg) {
    env = std::make_unique<RocksDBEnv>(cfg.rocksdb_settings_,
                                       /* stats */ nullptr,
                                       std::vector<IOTracing*>());
    cfg.options_.env = env.get();
    cfg.options_.env->SetBackgroundThreads(
        cfg.rocksdb_settings_->num_bg_threads_lo, rocksdb::Env::LOW);
    cfg.options_.compaction_filter = &filter;
  };

  std::pair<int, int> my_prio(-1, -1);
  int rv = get_io_priority_of_this_thread(&my_prio);
  EXPECT_EQ(0, rv);

  ld_info("ioprio of this thread: %d,%d", my_prio.first, my_prio.second);

  openStore(s, customize_config);
  SCOPE_EXIT {
    closeStore();
  };

  put({TestRecord(logid_t(1), 10, 10)});
  store_->createPartition();
  store_->performCompaction(ID0);

  std::pair<int, int> my_new_prio(-1, -1);
  rv = get_io_priority_of_this_thread(&my_new_prio);
  EXPECT_EQ(0, rv);
  // Check that we didn't screw up ioprio of user thread.
  EXPECT_EQ(my_prio, my_new_prio);

  // Here we assume that compaction filter is called on the same thread
  // that does the IO that we're trying to make low-pri.
  auto prios = filter.getPrios();
  ASSERT_EQ(1, prios.size());
  EXPECT_EQ(std::make_pair(2, 2), *prios.begin());
}

// Check that compaction is canceled on shutdown.
TEST_F(PartitionedRocksDBStoreTest, CancelingCompactions) {
  // Make compactions slow.
  filter_factory_->setSleepPerRecord(std::chrono::milliseconds(3));

  // Put enough records for compaction to take 30 seconds.
  for (int i = 0; i < 10000; ++i) {
    put({TestRecord(logid_t(250), 10 + i, BASE_TIME)});
  }
  put({TestRecord(logid_t(1), 10, BASE_TIME)});
  store_->createPartition();
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + DAY + HOUR));

  // All 1-day records in partition 0 are too old. This will trigger compaction.
  auto future = store_->backgroundThreadIteration(
      PartitionedRocksDBStore::BackgroundThreadType::LO_PRI);

  // Wait for compaction to start.
  while (filter_factory_->getRecordsSeen() == 0) {
    /* sleep override */
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  // Close store. If it waits for compaction without waiting, test will
  // time out here.
  closeStore();

  future.wait();

  // Check that compaction didn't finish.

  auto stats = stats_.aggregate();
  EXPECT_EQ(0, stats.partitions_dropped);
  EXPECT_EQ(0, stats.partitions_compacted);

  auto data = readAndCheck();
  EXPECT_EQ(2, data.size());
  EXPECT_EQ(2, data[0].size());
  EXPECT_EQ(std::vector<lsn_t>({10}), data[0][logid_t(1)].records);
}

// Same but don't wait for compaction to start before shutting down.
// This way compaction start races with shutdown. As of rocksdb 3.11.2 this
// test almost always times out because of a deadlock.
TEST_F(PartitionedRocksDBStoreTest, CancelingCompactionsDeadlock) {
  filter_factory_->setSleepPerRecord(std::chrono::milliseconds(3));

  for (int i = 0; i < 10000; ++i) {
    put({TestRecord(logid_t(250), 10 + i, BASE_TIME)});
  }
  put({TestRecord(logid_t(1), 10, BASE_TIME - DAY - HOUR)});
  store_->createPartition();

  auto future = store_->backgroundThreadIteration(
      PartitionedRocksDBStore::BackgroundThreadType::LO_PRI);

  closeStore();

  future.wait();

  auto stats = stats_.aggregate();
  EXPECT_EQ(0, stats.partitions_dropped);
  EXPECT_EQ(0, stats.partitions_compacted);

  auto data = readAndCheck();
  EXPECT_EQ(2, data.size());
  EXPECT_EQ(2, data[0].size());
  EXPECT_EQ(std::vector<lsn_t>({10}), data[0][logid_t(1)].records);
}

TEST_F(PartitionedRocksDBStoreTest, DecreaseLatestPartitionOfLog) {
  logid_t long_retention_log(400);
  logid_t short_retention_log(100);

  closeStore();
  ServerConfig::SettingsConfig s;
  s["rocksdb-partition-compaction-schedule"] = "disabled";
  s["rocksdb-partition-duration"] = "15min";
  openStore(s);
  ASSERT_TRUE(store_->getPartitionList()->get(ID0));
  auto time_raw = BASE_TIME + 2 * MINUTE;
  partition_id_t latest_partition = ID0;

  // Forwards 15 minutes and lets background threads run to create a partition.
  auto add_partition = [&]() {
    time_raw += 15 * MINUTE;
    time_ = SystemTimestamp(std::chrono::milliseconds(time_raw));
    store_
        ->backgroundThreadIteration(
            PartitionedRocksDBStore::BackgroundThreadType::HI_PRI)
        .wait();
    store_
        ->backgroundThreadIteration(
            PartitionedRocksDBStore::BackgroundThreadType::LO_PRI)
        .wait();
    ASSERT_TRUE(store_->getPartitionList()->get(++latest_partition));
    ASSERT_FALSE(store_->getPartitionList()->get(latest_partition + 1));
  };

  // 1) Write record to long-retention log to prevent partition drops.
  //    This will force any trims to use compactions to get rid of data.
  time_ = SystemTimestamp(std::chrono::milliseconds(time_raw));
  put({TestRecord(long_retention_log, lsn_t(1), time_raw)});
  ASSERT_TRUE(store_->getPartitionList()->get(ID0));
  ASSERT_FALSE(store_->getPartitionList()->get(ID0 + 1));

  // 2) Write a record for the short-retention log to each of two later
  //    partitions.
  add_partition();
  put({TestRecord(short_retention_log, lsn_t(7), time_raw)});
  add_partition();
  put({TestRecord(short_retention_log, lsn_t(23), time_raw)});
  partition_id_t partition_to_compact = latest_partition;
  // No partition should be dropped
  ASSERT_TRUE(store_->getPartitionList()->get(ID0));
  ASSERT_EQ(ID0, store_->getPartitionList()->firstID());

  // 3) Compact partition with latest record for short-retention log.
  ASSERT_EQ(getTrimPoint(short_retention_log), LSN_INVALID);
  store_->scheduleManualCompaction(partition_to_compact, /* hi_pri= */ true);
  ASSERT_EQ(getTrimPoint(short_retention_log), LSN_INVALID);

  // 4) Forward trim point for short-retention log past both records, and let
  //    retention run. Since the first of the two partitions used by this log
  //    hasn't been compacted, it won't be trimmed (see cleanUpDirectory).
  //    The later partition will be trimmed, causing the latest partition
  //    used by this log to decrease. Before fix, we didn't decrease this in
  //    LogState, causing a discrepancy between it and the latest record in
  //    the directory (both on disk and in memory).
  int rv =
      LocalLogStoreUtils::updateTrimPoints({{short_retention_log, lsn_t(78)}},
                                           processor_.get(),
                                           *store_,
                                           false,
                                           &stats_);
  ASSERT_EQ(getTrimPoint(short_retention_log), lsn_t(78));
  add_partition();

  // No partition should be dropped
  ASSERT_TRUE(store_->getPartitionList()->get(ID0));
  ASSERT_EQ(ID0, store_->getPartitionList()->firstID());

  // 5) Write a new record to short-retention log -- before fix, the
  //    discrepancy mentioned in 4) caused an assertion failure in
  //    PartitionedRocksDBStore::getWritePartition.
  put({TestRecord(short_retention_log, lsn_t(777), time_raw)});
}

TEST_F(PartitionedRocksDBStoreTest, NoRepeatedCompactions) {
  put({TestRecord(logid_t(250), lsn_t(2), BASE_TIME)});
  put({TestRecord(logid_t(150), lsn_t(2), BASE_TIME)});
  put({TestRecord(logid_t(50), lsn_t(2), BASE_TIME)});
  store_->createPartition();
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + DAY + HOUR));

  // Partition should get compacted to get rid of the record for log 50.
  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::LO_PRI)
      .wait();
  filter_factory_->flushTrimPoints();
  EXPECT_EQ(1, stats_.aggregate().partitions_compacted);

  put({TestRecord(logid_t(50), lsn_t(1), BASE_TIME)});

  // Store should be tempted to compact the same partition again but should
  // hold back because it already did.
  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::LO_PRI)
      .wait();
  EXPECT_EQ(1, stats_.aggregate().partitions_compacted);

  closeStore();
  openStore();
  EXPECT_EQ(1, stats_.aggregate().partitions_compacted);

  // This information should be persisted.
  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::LO_PRI)
      .wait();
  EXPECT_EQ(1, stats_.aggregate().partitions_compacted);

  time_ =
      SystemTimestamp(std::chrono::milliseconds(BASE_TIME + DAY * 2 + HOUR));
  // Partition should be compacted to get rid of the record for log 150.
  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::LO_PRI)
      .wait();
  filter_factory_->flushTrimPoints();
  EXPECT_EQ(2, stats_.aggregate().partitions_compacted);

  EXPECT_EQ(0, stats_.aggregate().partitions_dropped);
}

TEST_F(PartitionedRocksDBStoreTest, ManualCompactions) {
  put({TestRecord(logid_t(50), lsn_t(2), BASE_TIME)});
  store_->createPartition();

  // Nothing should get compacted
  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::LO_PRI)
      .wait();
  EXPECT_EQ(0, stats_.aggregate().partitions_compacted);

  store_->scheduleManualCompaction(ID0, true /* hi_pri */);
  auto list = store_->getManualCompactionList();
  std::list<std::pair<partition_id_t, bool>> exp_list = {
      {ID0, true},
  };

  // This should override the existing entry
  store_->scheduleManualCompaction(ID0, false /* hi_pri */);
  list = store_->getManualCompactionList();
  exp_list = {{ID0, false}};
  EXPECT_EQ(exp_list, list);

  // the partition should be compacted now
  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::LO_PRI)
      .wait();
  EXPECT_EQ(1, stats_.aggregate().partitions_compacted);
  list = store_->getManualCompactionList();
  exp_list.clear();
  EXPECT_EQ(exp_list, list);

  store_->scheduleManualCompaction(ID0, false /* hi_pri */);
  store_->scheduleManualCompaction(ID0 + 1, false /* hi_pri */);
  list = store_->getManualCompactionList();
  exp_list = {
      {ID0, false},
      {ID0 + 1, false},
  };
  EXPECT_EQ(exp_list, list);

  store_->cancelManualCompaction(ID0 + 1);
  list = store_->getManualCompactionList();
  exp_list = {
      {ID0, false},
  };
  EXPECT_EQ(exp_list, list);

  // This should cancel nothing, as the list doesn't have pending compactions
  // for partition 1 anymore
  store_->cancelManualCompaction(ID0 + 1);
  list = store_->getManualCompactionList();
  EXPECT_EQ(exp_list, list);

  // This should cancel all pending manual compactions
  store_->cancelManualCompaction(PARTITION_INVALID);
  list = store_->getManualCompactionList();
  exp_list.clear();
  EXPECT_EQ(exp_list, list);
}

TEST_F(PartitionedRocksDBStoreTest, PartialCompactionEvaluator) {
  // tests various sets of file sizes and verifies that the output of the
  // evaluator is as expected.
  class PartitionGenerator
      : public PartitionedRocksDBStore::PartialCompactionEvaluator::Deps {
   public:
    explicit PartitionGenerator(std::vector<std::vector<size_t>> sizes) {
      size_t partNum = sizes.size();
      for (size_t i = 0; i < sizes.size(); ++i) {
        // setting CF pointers to numeric values to reference it later
        auto cf_holder = std::make_shared<RocksDBColumnFamily>(
            reinterpret_cast<rocksdb::ColumnFamilyHandle*>(i));

        // Space out partitions by 6 hours
        RecordTimestamp creation_time =
            RecordTimestamp::now() - std::chrono::hours(6 * partNum);
        partNum--;
        partitions_.emplace_back(new PartitionedRocksDBStore::Partition(
            i, cf_holder, creation_time));
        rocksdb::ColumnFamilyMetaData cf_metadata;
        std::vector<rocksdb::SstFileMetaData> l0_files;
        for (size_t j = 0; j < sizes[i].size(); ++j) {
          rocksdb::SstFileMetaData sst_file;
          sst_file.size = sizes[i][j];
          sst_file.name = folly::to<std::string>(j); // filename is file number
          l0_files.push_back(std::move(sst_file));
        }
        cf_metadata.levels.emplace_back(0,
                                        0, /* total size of level - not used */
                                        std::move(l0_files));
        metadata_.push_back(std::move(cf_metadata));
      }
    }

    ~PartitionGenerator() override {
      // We set some weird pointer values in cf_. We have to drop them before
      // destroying partitions
      for (auto& p : partitions_) {
        const_cast<std::unique_ptr<rocksdb::ColumnFamilyHandle>&>(p->cf_->cf_)
            .release();
      }
    }

    std::vector<PartitionedRocksDBStore::PartitionPtr> getPartitions() {
      return partitions_;
    }

    void
    getColumnFamilyMetaData(rocksdb::ColumnFamilyHandle* column_family,
                            rocksdb::ColumnFamilyMetaData* cf_meta) override {
      ld_check(cf_meta);
      uint64_t offset = reinterpret_cast<uint64_t>(column_family);
      ASSERT_LT(offset, metadata_.size());
      ASSERT_GT(metadata_[offset].levels.size(), 0);
      cf_meta->levels.clear();
      cf_meta->levels.emplace_back(metadata_[offset].levels[0]);
    }

   private:
    std::vector<rocksdb::ColumnFamilyMetaData> metadata_;
    std::vector<std::shared_ptr<PartitionedRocksDBStore::Partition>>
        partitions_;
  };

  // pairs of partition numbers and vectors of file numbers
  using t_result = std::vector<std::pair<size_t, std::vector<size_t>>>;
  auto run_test = [&](std::vector<std::vector<size_t>> sizes,
                      size_t max_results) -> t_result {
    auto part_gen = std::make_unique<PartitionGenerator>(std::move(sizes));
    auto partitions = part_gen->getPartitions();
    size_t num_partitions = partitions.size();
    std::chrono::hours old_age_threshold = std::chrono::hours{24};
    PartitionedRocksDBStore::PartialCompactionEvaluator evaluator(
        std::move(partitions),
        old_age_threshold, /* age threhold for considered as old partition */
        5,                 /* min files old */
        3,                 /* min files recent */
        10,                /* max files */
        100,               /* max_avg_file_size */
        200,               /* max_file_size */
        0.5,               /* max_largest_file_share */
        std::move(part_gen));
    std::vector<PartitionedRocksDBStore::PartitionToCompact> to_compact;
    evaluator.evaluateAll(&to_compact, max_results);
    t_result results;
    for (auto& p : to_compact) {
      ld_check(PartitionedRocksDBStore::PartitionToCompact::Reason::PARTIAL ==
               p.reason);
      size_t partition_id = reinterpret_cast<size_t>(p.partition->cf_->get());
      ld_check(partition_id < num_partitions);
      std::vector<size_t> files;
      for (auto& filename : p.partial_compaction_filenames) {
        // getting file number from filename
        files.push_back(folly::to<size_t>(filename));
      }
      results.push_back(std::make_pair(partition_id, files));
    }
    return results;
  };

  t_result results = run_test(
      {
          {10, 10, 10}, // 21 hours old
          {5, 5, 5},    // 14 hours old
          {20, 5, 5},   // 7 hours old
      },
      3);
  // the last 2 partitions are not evaluated
  t_result expected_results = {{0, {0, 1, 2}}};
  ASSERT_EQ(expected_results, results);

  results = run_test(
      {
          {10, 10, 10, 10, 10}, // 28 hours old
          {5, 5, 5},
          {20, 5, 5},
          {30, 5, 5},
      },
      3);
  // the first 2 partitions are evaluated, both should be compacted
  expected_results = {{0, {0, 1, 2, 3, 4}}, {1, {0, 1, 2}}};
  ASSERT_EQ(expected_results, results);

  results = run_test(
      {
          {10, 10, 10}, // 28 hours old
          {5, 5, 5},
          {20, 5, 5},
          {30, 5, 5},
      },
      3);
  // the first 2 partitions are evaluated, first is not compacted because
  // it does not meet the min file threshold for old partitions.
  expected_results = {{1, {0, 1, 2}}};
  ASSERT_EQ(expected_results, results);

  results = run_test(
      {
          {10, 10, 10, 10, 10},
          {5, 5, 5},
          {20, 5, 5},
          {30, 5, 5},
      },
      1);
  // First two evaluated, but 1 result max
  expected_results = {{0, {0, 1, 2, 3, 4}}};
  ASSERT_EQ(expected_results, results);

  results = run_test({{10, 10, 10, 10, 10},
                      {
                          5,
                          5,
                          5,
                      },
                      {20, 5, 5},
                      {30, 5, 5},
                      {},
                      {}},
                     3);
  // partitions 2 and 3 should not be compacted, because largest file is larger
  // than 50% of total file size. 1 is not compacted because it does not meet
  // the min files threshold for old partitions.
  expected_results = {{0, {0, 1, 2, 3, 4}}};
  ASSERT_EQ(expected_results, results);

  results = run_test({{300, 200, 200, 200, 200}, {200, 100, 100}, {}, {}}, 3);
  // nothing should be compacted, because the files are too large
  expected_results = {};
  ASSERT_EQ(expected_results, results);

  results = run_test(
      {{50, 50, 50, 50, 50, 50, 50},                  // all should be compacted
       {50, 50, 50, 150, 60, 50, 50},                 // all should be compacted
       {50, 50, 50, 10, 10, 250, 60, 50, 50, 10, 10}, // should be compacted in
                                                      // 2 groups of 5
       {50, 50, 160, 50, 50},                         // all should be compacted
       {50, 50, 200, 50, 50},               // none should be compacted
       {150, 50, 50, 50, 160, 50, 50, 150}, // files 1-6 should be compacted
       {50, 50, 200, 50, 50},               // all should be compacted
       {150, 50, 50, 50, 160, 50, 150},     // files 1-3 should be compacted
       {},
       {}},
      3);
  // top 3
  expected_results = {
      {0, {0, 1, 2, 3, 4, 5, 6}},
      {1, {0, 1, 2, 3, 4, 5, 6}},
      {5, {1, 2, 3, 4, 5, 6}},
  };
  results = run_test(
      {{50, 50, 50, 50, 50, 50, 50},                  // all should be compacted
       {50, 50, 50, 150, 65, 50, 50},                 // all should be compacted
       {50, 50, 50, 50, 50, 250, 65, 50, 50, 50, 50}, // should be compacted in
                                                      // 2 groups of 5
       {50, 50, 160, 50, 50},                         // all should be compacted
       {50, 50, 250, 50, 50},               // none should be compacted
       {150, 50, 50, 50, 160, 50, 50, 150}, // files 1-6 should be compacted
       {50, 50, 200, 50, 50},               // all should be compacted
       {150, 50, 50, 50, 160, 50, 150},     // files 1-3 should be compacted
       {},
       {}},
      10);
  // all
  expected_results = {
      {0, {0, 1, 2, 3, 4, 5, 6}},
      {2, {0, 1, 2, 3, 4}},
      {2, {6, 7, 8, 9, 10}},
      {1, {0, 1, 2, 3, 4, 5, 6}},
      {5, {1, 2, 3, 4, 5, 6}},
      {7, {1, 2, 3}},
      {3, {0, 1, 2, 3, 4}},
      {6, {0, 1, 2, 3, 4}},
  };
  ASSERT_EQ(expected_results, results);

  // Last 2 partitions should be compacted because number of files is > 70% of
  // max_files.
  results = run_test({{},
                      {},
                      {10, 10, 10, 10, 10, 10, 10, 10},
                      {10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10}},
                     10);
  expected_results = {
      {3, {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}},
      {2, {0, 1, 2, 3, 4, 5, 6, 7}},
  };
  ASSERT_EQ(expected_results, results);
}

TEST_F(PartitionedRocksDBStoreTest, CompactionVsWriteRace) {
  put({TestRecord(logid_t(150), lsn_t(1), BASE_TIME)});
  put({TestRecord(logid_t(50), lsn_t(2), BASE_TIME)});
  setTime(BASE_TIME + HOUR);
  store_->createPartition();
  setTime(BASE_TIME + DAY + HOUR * 2);

  // Start compaction in a separate thread.
  filter_factory_->stall(true);
  ld_check(filter_factory_->getRecordsSeen() == 0);
  std::thread th([&] {
    // Partition should get compacted to get rid of the record for log 50.
    store_
        ->backgroundThreadIteration(
            PartitionedRocksDBStore::BackgroundThreadType::LO_PRI)
        .wait();
    filter_factory_->flushTrimPoints();
    EXPECT_EQ(1, stats_.aggregate().partitions_compacted);
  });

  // Wait for it to get stuck in compaction filter.
  while (filter_factory_->getRecordsSeen() == 0) {
    /* sleep override */
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  // Put a more recent record to the partition being compacted.
  put({TestRecord(logid_t(50), lsn_t(3), BASE_TIME + HOUR / 2)});

  // Resume compaction and wait for it.
  filter_factory_->stall(false);
  th.join();

  // Check that the recent record is readable.
  auto it = store_->read(
      logid_t(50), LocalLogStore::ReadOptions("CompactionVsWriteRace"));
  it->seek(0);
  EXPECT_EQ(IteratorState::AT_RECORD, it->state());
  EXPECT_EQ(3, it->getLSN());
  it->next();
  EXPECT_EQ(IteratorState::AT_END, it->state());
}

TEST_F(PartitionedRocksDBStoreTest, TightDirectory) {
  logid_t logid(3);

  put({TestRecord(logid, 10, BASE_TIME)});
  store_->createPartition();
  put({TestRecord(logid, 20, BASE_TIME + 1)});

  auto data = readAndCheck();
  ASSERT_EQ(2, data.size());
  ASSERT_EQ(1, data[0].size());
  ASSERT_EQ(10, data[0][logid].first_lsn);
  ASSERT_EQ(1, data[1].size());
  ASSERT_EQ(20, data[1][logid].first_lsn);
}

// Check that event log records are not written to partitions.
TEST_F(PartitionedRocksDBStoreTest, UnpartitionedEventLog) {
  put({TestRecord(
      configuration::InternalLogs::EVENT_LOG_DELTAS, 10, BASE_TIME)});
  auto data = readAndCheck();
  ASSERT_EQ(1, data.size());
  ASSERT_EQ(0, data[0].size());
}

TEST_F(PartitionedRocksDBStoreTest, FixedKeysMap) {
  FixedKeysMapKeys<std::string> k;
  k.add("foo");
  k.add("bar");
  k.add("foobar");
  k.add("bar");
  k.finalize();
  FixedKeysMap<std::string, int> v1(&k);

  EXPECT_EQ(1, k.getIdx("foo"));
  EXPECT_EQ(3, k.size());
  EXPECT_EQ("foobar", k[2]);
  EXPECT_EQ(std::vector<std::string>({"bar", "foo", "foobar"}),
            std::vector<std::string>(k.begin(), k.end()));

  EXPECT_EQ(0, v1["foo"]);
  v1["foo"] = 42;
  EXPECT_EQ(42, v1["foo"]);
  EXPECT_EQ(0, v1["bar"] + v1["foobar"]);

  {
    int i = 0;
    for (auto it : v1) {
      EXPECT_EQ(k[i], it.first);
      EXPECT_EQ(i == 1 ? 42 : 0, *it.second);
      if (i == 2) {
        *it.second = 1337;
      }
      ++i;
    }
    EXPECT_EQ(3, i);
  }
  EXPECT_EQ(1337, v1["foobar"]);

  FixedKeysMap<std::string, bool> v2(v1.keys());
  EXPECT_FALSE(v2["foobar"]);
  v2["foobar"] = true;
  EXPECT_TRUE(v2["foobar"]);
  {
    int i = 0;
    for (auto it : v2) {
      EXPECT_EQ(k[i], it.first);
      EXPECT_EQ(i == 2, *it.second);
      ++i;
    }
    EXPECT_EQ(3, i);
  }
}

// Test for getHighestInsertedLSN().
TEST_F(PartitionedRocksDBStoreTest, HighestInsertedLSN) {
  auto get = [&](logid_t log) {
    lsn_t lsn;
    EXPECT_EQ(0, store_->getHighestInsertedLSN(log, &lsn));
    return lsn;
  };

  EXPECT_EQ(LSN_INVALID, get(logid_t(10)));
  put({TestRecord(logid_t(10), 42)});
  EXPECT_EQ(LSN_INVALID, get(logid_t(20)));
  EXPECT_EQ(42, get(logid_t(10)));
  put({TestRecord(logid_t(30), 1337)});
  EXPECT_EQ(1337, get(logid_t(30)));
  EXPECT_EQ(42, get(logid_t(10)));
  store_->createPartition();
  put({TestRecord(logid_t(30), 1000), TestRecord(logid_t(10), 123)});
  EXPECT_EQ(1337, get(logid_t(30)));
  EXPECT_EQ(123, get(logid_t(10)));

  readAndCheck();
  openStore();

  EXPECT_EQ(1337, get(logid_t(30)));
  EXPECT_EQ(123, get(logid_t(10)));
  put({TestRecord(logid_t(30), 2000), TestRecord(logid_t(10), 12300)});
  EXPECT_EQ(2000, get(logid_t(30)));
  EXPECT_EQ(12300, get(logid_t(10)));

  EXPECT_EQ(6, numRecords(readAndCheck()));
}

// Drop all partitions that had records for a log, then insert records with
// smaller LSNs.
TEST_F(PartitionedRocksDBStoreTest, ClearingLog) {
  logid_t log(42);
  put({TestRecord(log, 100), TestRecord(log, 200)});
  store_->createPartition();
  store_->dropPartitionsUpTo(ID0 + 1);
  put({TestRecord(log, 10), TestRecord(log, 20)});
  auto data = readAndCheck();
  ASSERT_EQ(1, data.size());
  EXPECT_EQ(10, data[0][log].first_lsn);
  EXPECT_EQ(20, data[0][log].max_lsn);
}

TEST_F(PartitionedRocksDBStoreTest, WritePathCases) {
  increasing_lsns_ = false;

  const logid_t log(42);
  FastUpdateableSharedPtr<StatsParams> params(std::make_shared<StatsParams>());
  Stats stats(&params);

  // Write a record to partition 1.
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + HOUR));
  store_->createPartition();                                   // partition 1
  put({TestRecord(log, 300, BASE_TIME + HOUR + 30 * MINUTE)}); // partition 1
  stats = stats_.aggregate();
  EXPECT_EQ(1, stats.logsdb_writes_dir_key_add);
  EXPECT_EQ(0, stats.logsdb_writes_dir_key_decrease);

  // A record with a smaller LSN that wants to be in partition 1.
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + 2 * HOUR));
  store_->createPartition();                                   // partition 2
  put({TestRecord(log, 200, BASE_TIME + HOUR + 20 * MINUTE)}); // partition 1
  stats = stats_.aggregate();
  EXPECT_EQ(1, stats.logsdb_writes_dir_key_decrease);

  auto data = readAndCheck();
  ASSERT_EQ(3, data.size());
  EXPECT_EQ(200, data[1][log].first_lsn);
  EXPECT_EQ(2, data[1][log].records.size());
  openStore();

  // Same after a reopen.
  put({TestRecord(log, 100, BASE_TIME + HOUR + 10 * MINUTE)}); // partition 1
  stats = stats_.aggregate();
  EXPECT_EQ(2, stats.logsdb_writes_dir_key_decrease);
  EXPECT_EQ(0, stats.logsdb_directory_premature_flushes);

  // Records with smaller LSNs that want to be in partition 0.
  // Two dependent writes in a batch.
  put({TestRecord(log, 30, BASE_TIME + 30 * MINUTE),   // partition 0
       TestRecord(log, 40, BASE_TIME + 40 * MINUTE)}); // partition 0
  stats = stats_.aggregate();
  EXPECT_EQ(2, stats.logsdb_writes_dir_key_add);
  EXPECT_EQ(1, stats.logsdb_directory_premature_flushes);

  // A record in between that wants to be in partition 1.
  put({TestRecord(log, 65, BASE_TIME + HOUR + 5 * MINUTE)}); // partition 1
  stats = stats_.aggregate();
  EXPECT_EQ(3, stats.logsdb_writes_dir_key_decrease);

  // A record in between that wants to be in partition 0.
  put({TestRecord(log, 50, BASE_TIME + 50 * MINUTE)}); // partition 0
  stats = stats_.aggregate();
  EXPECT_EQ(0, stats.logsdb_target_partition_clamped);

  // A record in between that wants to be in partition 1 but but has to go to 0.
  put({TestRecord(log, 45, BASE_TIME + HOUR + 45 * MINUTE)}); // partition 0
  stats = stats_.aggregate();
  EXPECT_EQ(1, stats.logsdb_partition_pick_retries);
  EXPECT_EQ(2, stats.logsdb_target_partition_clamped);

  // A record that wants to be in partition 2 but has to go to 1.
  put({TestRecord(
      log, 150, BASE_TIME + 2 * HOUR + 30 * MINUTE)}); // partition 1
  stats = stats_.aggregate();
  EXPECT_EQ(2, stats.logsdb_partition_pick_retries);
  EXPECT_EQ(4, stats.logsdb_target_partition_clamped);

  // A record that wants to be in partition 0 but has to go to 1.
  put({TestRecord(log, 250, BASE_TIME + 30 * MINUTE)}); // partition 1
  stats = stats_.aggregate();
  EXPECT_EQ(2, stats.logsdb_partition_pick_retries);
  EXPECT_EQ(5, stats.logsdb_target_partition_clamped);

  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + 3 * HOUR));
  store_->createPartition(); // partition 3
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + 4 * HOUR));
  store_->createPartition(); // partition 4
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + 5 * HOUR));
  store_->createPartition(); // partition 5

  // A record that wants to be in partition 5.
  put({TestRecord(
      log, 530, BASE_TIME + 5 * HOUR + 30 * MINUTE)}); // partition 5
  stats = stats_.aggregate();
  EXPECT_EQ(2, stats.logsdb_partition_pick_retries);
  EXPECT_EQ(3, stats.logsdb_writes_dir_key_add);
  EXPECT_EQ(0, stats.logsdb_skipped_writes);

  // A delete for a record between partitions 1 and 5.
  put({TestRecord(log, 500, TestRecord::Type::DELETE)});
  stats = stats_.aggregate();
  EXPECT_EQ(2, stats.logsdb_partition_pick_retries);
  EXPECT_EQ(1, stats.logsdb_skipped_writes);

  // A delete for a nonexistent record in partition 1.
  put({TestRecord(log, 265, TestRecord::Type::DELETE)});
  stats = stats_.aggregate();
  // Can't tell from directory that record doesn't exist.
  EXPECT_EQ(3, stats.logsdb_partition_pick_retries);
  EXPECT_EQ(1, stats.logsdb_skipped_writes);

  // Delete an existing record in partition 1. No need to change no_deletes_
  // because the record is in the middle of partition.
  put({TestRecord(log, 200, TestRecord::Type::DELETE)});
  stats = stats_.aggregate();
  EXPECT_EQ(4, stats.logsdb_partition_pick_retries);

  // A record that wants to be in partition 3.
  put({TestRecord(
      log, 330, BASE_TIME + 3 * HOUR + 30 * MINUTE)}); // partition 3
  stats = stats_.aggregate();
  EXPECT_EQ(4, stats.logsdb_partition_pick_retries);
  EXPECT_EQ(4, stats.logsdb_writes_dir_key_add);

  EXPECT_EQ(4, stats.logsdb_writes_dir_key_add);
  EXPECT_EQ(3, stats.logsdb_writes_dir_key_decrease);
  EXPECT_EQ(1, stats.logsdb_skipped_writes);

  {
    auto p = store_->getPartitionList();
    EXPECT_EQ(BASE_TIME + 0 * HOUR,
              p->get(ID0)->starting_timestamp.toMilliseconds().count());
    EXPECT_EQ(BASE_TIME + 1 * HOUR,
              p->get(ID0 + 1)->starting_timestamp.toMilliseconds().count());
    EXPECT_EQ(BASE_TIME + 2 * HOUR,
              p->get(ID0 + 2)->starting_timestamp.toMilliseconds().count());
    EXPECT_EQ(BASE_TIME + 3 * HOUR,
              p->get(ID0 + 3)->starting_timestamp.toMilliseconds().count());
    EXPECT_EQ(BASE_TIME + 4 * HOUR,
              p->get(ID0 + 4)->starting_timestamp.toMilliseconds().count());
    EXPECT_EQ(BASE_TIME + 5 * HOUR,
              p->get(ID0 + 5)->starting_timestamp.toMilliseconds().count());
  }

  data = readAndCheck();

  ASSERT_EQ(6, data.size());

  EXPECT_EQ(1, data[0].size());
  EXPECT_EQ(1, data[1].size());
  EXPECT_EQ(0, data[2].size());
  EXPECT_EQ(1, data[3].size());
  EXPECT_EQ(0, data[4].size());
  EXPECT_EQ(1, data[5].size());

  EXPECT_EQ(30, data[0][log].first_lsn);
  EXPECT_EQ(50, data[0][log].max_lsn);
  EXPECT_EQ(std::vector<lsn_t>({30, 40, 45, 50}), data[0][log].records);

  EXPECT_EQ(65, data[1][log].first_lsn);
  EXPECT_EQ(300, data[1][log].max_lsn);
  EXPECT_EQ(std::vector<lsn_t>({65, 100, 150, 250, 300}), data[1][log].records);

  EXPECT_EQ(330, data[3][log].first_lsn);
  EXPECT_EQ(330, data[3][log].max_lsn);
  EXPECT_EQ(std::vector<lsn_t>({330}), data[3][log].records);

  EXPECT_EQ(530, data[5][log].first_lsn);
  EXPECT_EQ(530, data[5][log].max_lsn);
  EXPECT_EQ(std::vector<lsn_t>({530}), data[5][log].records);
}

TEST_F(PartitionedRocksDBStoreTest, IteratorInvalidation) {
  no_deletes_ = false;

  // Create partitions 1..9. Partition p covers timestamps
  // [BASE_TIME + p*HOUR, BASE_TIME + p*HOUR].
  for (int i = 1; i < 10; ++i) {
    time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + i * HOUR));
    store_->createPartition();
  }

  const logid_t log(24);
  FastUpdateableSharedPtr<StatsParams> params(std::make_shared<StatsParams>());
  Stats stats(&params);

  LocalLogStore::ReadOptions options("IteratorInvalidation");
  options.tailing = true;
  auto it = store_->read(log, options);

  auto expect_lsn = [&](lsn_t lsn) {
    if (lsn == LSN_INVALID) {
      EXPECT_EQ(IteratorState::AT_END, it->state());
    } else {
      ASSERT_EQ(IteratorState::AT_RECORD, it->state());
      EXPECT_EQ(lsn, it->getLSN());
    }
  };

#define EXPECT_LSN(lsn)               \
  {                                   \
    SCOPED_TRACE("called from here"); \
    expect_lsn(lsn);                  \
  }

  it->seek(100);
  EXPECT_LSN(LSN_INVALID);

  put({TestRecord(
      log, 230, BASE_TIME + 1 * HOUR + 30 * MINUTE)}); // partition 1
  it->seek(50);
  EXPECT_LSN(230);
  stats = stats_.aggregate();
  EXPECT_EQ(1, stats.logsdb_iterator_dir_seek_needed);

  put({TestRecord(
      log, 330, BASE_TIME + 2 * HOUR + 30 * MINUTE)}); // partition 2
  put({TestRecord(
      log, 530, BASE_TIME + 4 * HOUR + 30 * MINUTE)}); // partition 4
  put({TestRecord(
      log, 630, BASE_TIME + 5 * HOUR + 30 * MINUTE)}); // partition 5
  it->next();
  EXPECT_LSN(LSN_INVALID); // not guaranteed but true for current implementation
  it->seek(325);
  EXPECT_LSN(330);
  stats = stats_.aggregate();
  EXPECT_EQ(2, stats.logsdb_iterator_dir_seek_needed);

  it->next();
  EXPECT_LSN(530);
  stats = stats_.aggregate();
  EXPECT_EQ(2, stats.logsdb_iterator_dir_seek_needed);

  // New directory entry was added but latest partition didn't change.
  put({TestRecord(
      log, 430, BASE_TIME + 3 * HOUR + 30 * MINUTE)}); // partition 3

  // Seek to 330 and this should make 430 visible now
  it->seek(300);
  EXPECT_LSN(330);
  stats = stats_.aggregate();
  EXPECT_EQ(3, stats.logsdb_iterator_dir_seek_needed);

  it->next();
  EXPECT_LSN(430);
  stats = stats_.aggregate();

  // All records in partition were deleted one by one. Iterator should skip to
  // next partition.
  put({TestRecord(log, 530, TestRecord::Type::DELETE)}); // partition 4
  it->next();
  EXPECT_LSN(630);
  stats = stats_.aggregate();
  EXPECT_EQ(3, stats.logsdb_iterator_dir_seek_needed);

  // The below part of the scenario used to be closely tailored to the iterator
  // implementation, then implementaiton changed, and now this kind of doesn't
  // have a point, but maybe still better than nothing.

  // To prepare for the next step, seek to an old partition, then to latest
  // partition, so that metadata iterator is not positioned correctly.
  it->seek(1);
  EXPECT_LSN(230);
  stats = stats_.aggregate();
  EXPECT_EQ(4, stats.logsdb_iterator_dir_seek_needed);
  it->seek(630);
  EXPECT_LSN(630);
  stats = stats_.aggregate();

  // Decrease key in directory and make iterator seek to this key.
  put({TestRecord(
      log, 620, BASE_TIME + 5 * HOUR + 20 * MINUTE)}); // partition 5
  put({TestRecord(
      log, 830, BASE_TIME + 7 * HOUR + 30 * MINUTE)}); // partition 7
  it->seek(631);
  EXPECT_LSN(830);
  stats = stats_.aggregate();
  EXPECT_EQ(5, stats.logsdb_iterator_dir_seek_needed);
  EXPECT_EQ(0, stats.logsdb_iterator_dir_reseek_needed);

#undef EXPECT_LSN
}

TEST_F(PartitionedRocksDBStoreTest, IteratorPrevAwayFromLatest) {
  logid_t log(42);
  put({TestRecord(log, 10)});
  store_->createPartition();
  put({TestRecord(log, 20)});
  auto it = store_->read(
      log, LocalLogStore::ReadOptions("IteratorPrevAwayFromLatest"));
  it->seek(20);
  ASSERT_EQ(IteratorState::AT_RECORD, it->state());
  EXPECT_EQ(20, it->getLSN());
  Stats stats = stats_.aggregate();
  // Hit latest partition fast path.
  EXPECT_EQ(0, stats.logsdb_iterator_dir_seek_needed);
  it->prev();
  ASSERT_EQ(IteratorState::AT_RECORD, it->state());
  EXPECT_EQ(10, it->getLSN());
  stats = stats_.aggregate();
  // Went from latest to non-latest partition, need to seek meta iterator.
  EXPECT_EQ(0, stats.logsdb_iterator_dir_seek_needed);
  EXPECT_EQ(1, stats.logsdb_iterator_dir_reseek_needed);

  it->prev();
  EXPECT_EQ(IteratorState::AT_END, it->state());
}

TEST_F(PartitionedRocksDBStoreTest, ObsoleteDataEstimate) {
  auto customize_fn = [&](RocksDBLogStoreConfig& cfg) {
    cfg.options_.table_properties_collector_factories.push_back(
        std::make_shared<RocksDBTablePropertiesCollectorFactory>(
            processor_->config_, nullptr /* stats */));
  };

  // Reopen with our settings.
  readAndCheck();
  openStore(ServerConfig::SettingsConfig(), customize_fn);

  for (int i = 0; i < 1000; ++i) {
    put({TestRecord(logid_t(1), i + 10, BASE_TIME + HOUR)});
  }
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + HOUR * 2));
  store_->createPartition();

  // Looks like rocksdb doesn't use user properties collector for sst files
  // written by WAL recovery, so we have to flush explicitly.
  EXPECT_TRUE(store_->flushMemtable(store_->getPartitionList()->get(ID0)->cf_));

  time_ =
      SystemTimestamp(std::chrono::milliseconds(BASE_TIME + DAY + HOUR / 2));
  EXPECT_EQ(0, store_->getApproximateObsoleteBytes(ID0));
  time_ =
      SystemTimestamp(std::chrono::milliseconds(BASE_TIME + DAY + HOUR * 3));
  EXPECT_GT(store_->getApproximateObsoleteBytes(ID0), 100);
}

TEST_F(PartitionedRocksDBStoreTest, MonotonicIteratorStressTest) {
  // Move iterators while writing new records. The concurrent writes used to
  // sometimes cause e.g. seek(`lsn`) to seek to an LSN smaller than `lsn`.
  const logid_t logid(3);
  const lsn_t max_lsn = (lsn_t)1e12;
  std::atomic<lsn_t> cur_lsn{max_lsn};
  lsn_t first_lsn = 42;
  // Write first_lsn to partition 0.
  put({TestRecord(logid, first_lsn)});
  // Create partition 1 that all other writes will go to.
  time_ += std::chrono::milliseconds(HOUR);
  store_->createPartition();
  uint64_t ts = time_.toMilliseconds().count() + HOUR;
  put({TestRecord(logid, cur_lsn--, ts)});

  std::atomic<size_t> reads_done{0};
  std::atomic<size_t> writes_done{0};
  auto should_stop = [&] {
    // At the time of writing 50k reads and 10k writes take 0.5 seconds.
    return reads_done.load() > 50000 && writes_done.load() > 10000;
  };

  // Thread that does seek() and next() on iterators.
  std::thread reading_thread([&] {
    std::unique_ptr<LocalLogStore::ReadIterator> it;
    while (!should_stop()) {
      // Create a new iterator sometimes.
      if (it == nullptr || folly::Random::rand32() % 10 == 0) {
        LocalLogStore::ReadOptions options(
            "PartitionedRocksDBStoreTest.MonotonicIteratorStressTest");
        options.tailing = folly::Random::rand32() % 2;
        it = store_->read(logid, options);
      }
      // Seek to cur_lsn +- 5 to try to hit the race with writes.
      lsn_t lsn =
          std::min(max_lsn, cur_lsn.load() - 5 + folly::Random::rand32() % 11);
      it->seek(lsn);
      EXPECT_EQ(IteratorState::AT_RECORD, it->state());
      ASSERT_GE(it->getLSN(), lsn);
      ++reads_done;
    }
  });

  // Thread that writes records with decreasing LSNs to partition 1.
  std::thread writing_thread([&] {
    while (!should_stop()) {
      put({TestRecord(logid, cur_lsn--, ts)});
      ++writes_done;
    }
  });

  reading_thread.join();
  writing_thread.join();

  auto partitions = readAndCheck();
  ASSERT_EQ(2, partitions.size());
  ASSERT_EQ(std::vector<lsn_t>({first_lsn}), partitions[0][logid].records);
  ASSERT_EQ(max_lsn - cur_lsn.load(), partitions[1][logid].records.size());
}

TEST_F(PartitionedRocksDBStoreTest, TestGracePeriod) {
  // This test writes data to 3 logs(each with 7day retention)
  // a) log 300 will be removed from config, and the test verifies
  //    that all of its data is removed after grace period
  // b) log 301 will be removed from config but later gets added back,
  //    the test verifies that the data is not lost.
  // c) log 302 will not be removed, and the test verifies that its
  //    data startes getting trimmed only after 7days have elapsed
  //
  // Also verifies that NoTrimUntilMetadata is persisted,
  // i.e. grace period doesn't start all over again, thereby keeping
  // the data no longer than it should.
  ServerConfig::SettingsConfig s;
  // override default grace period
  s["rocksdb-unconfigured-log-trimming-grace-period"] = "2h";
  closeStore();
  openStore(s);

  auto logs_config = processor_->config_->getLocalLogsConfig();
  auto log_group = processor_->config_->getLocalLogsConfig()->getLogGroup(
      "/logs_can_disappear");
  ASSERT_NE(nullptr, log_group);

  // P0 has records for log 300 only
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME));
  put({TestRecord(logid_t(300), 1, time_.toMilliseconds().count())});
  EXPECT_TRUE(store_->flushMemtable(store_->getPartitionList()->get(ID0)->cf_));

  // P1 has records for logs 300,301,302
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + 15 * MINUTE));
  store_->createPartition(); // 2nd(P1) partition
  put({TestRecord(logid_t(300), 2, time_.toMilliseconds().count())});
  put({TestRecord(logid_t(301), 1, time_.toMilliseconds().count())});
  put({TestRecord(logid_t(302), 1, time_.toMilliseconds().count())});
  EXPECT_TRUE(
      store_->flushMemtable(store_->getPartitionList()->get(ID0 + 1)->cf_));

  // P2 has records for log 300,301,302
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + 30 * MINUTE));
  store_->createPartition(); // 3rd(P2) partition
  put({TestRecord(logid_t(300), 3, time_.toMilliseconds().count())});
  put({TestRecord(logid_t(301), 2, time_.toMilliseconds().count())});
  put({TestRecord(logid_t(302), 2, time_.toMilliseconds().count())});
  EXPECT_TRUE(
      store_->flushMemtable(store_->getPartitionList()->get(ID0 + 2)->cf_));

  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + 45 * MINUTE));
  store_->createPartition(); // latest partition, 4th(P3)

  ASSERT_TRUE(store_->getPartitionList()->get(ID0));
  ASSERT_TRUE(store_->getPartitionList()->get(ID0 + 1));
  ASSERT_TRUE(store_->getPartitionList()->get(ID0 + 2));
  ASSERT_TRUE(store_->getPartitionList()->get(ID0 + 3));
  EXPECT_EQ(ID0, store_->getPartitionList()->firstID());

  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::LO_PRI)
      .wait();

  ld_info("Deleting log 300-301, new log range [302,399]");
  ASSERT_TRUE(processor_->config_->getLocalLogsConfig()->replaceLogGroup(
      "/logs_can_disappear",
      log_group->withRange(logid_range_t({logid_t(302), logid_t(399)}))));

  // this will start the grace period for both 300 and 301
  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::LO_PRI)
      .wait();

  auto& state_map = processor_->getLogStorageStateMap();
  LogStorageState* lss = state_map.find(logid_t(301), THIS_SHARD);
  ld_check(lss != nullptr);
  ld_check(lss->getLogRemovalTime().count() * 1000 == BASE_TIME + 45 * MINUTE);

  // some more time passed, but grace period hasn't expired yet
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + 1 * HOUR));
  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::LO_PRI)
      .wait();
  // verify that log 300's data didn't get trimmed if grace period
  // didn't expire
  EXPECT_EQ(0, stats_.aggregate().partitions_compacted);
  EXPECT_EQ(0, stats_.aggregate().partitions_dropped);

  ld_info("Simulate restart by clearing LogStorageState for the logs, "
          "and closing and reopening the store...");
  closeStore();
  state_map.clear();
  openStore(s);

  ld_info("Reintroducing log 301, new log range [301,399]");
  ASSERT_TRUE(processor_->config_->getLocalLogsConfig()->replaceLogGroup(
      "/logs_can_disappear",
      log_group->withRange(logid_range_t({logid_t(301), logid_t(399)}))));

  // verify that grace period got reset after readding log 301 into config
  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::LO_PRI)
      .wait();
  lss = state_map.find(logid_t(301), THIS_SHARD);
  ld_check(lss != nullptr);
  ld_check(lss->getLogRemovalTime() == std::chrono::seconds::max());

  // Data still not trimmed after restart
  EXPECT_EQ(0, getTrimPoint(logid_t(300)));
  EXPECT_EQ(0, stats_.aggregate().partitions_dropped);
  EXPECT_EQ(0, stats_.aggregate().partitions_compacted);

  // P0 won't get dropped unless cutoff timestamp > start of P1.
  ld_info("Advancing time to 2hrs46min, expect data to get trimmed now");
  time_ = SystemTimestamp(std::chrono::milliseconds(
      BASE_TIME + 2 * HOUR + 46 * MINUTE)); // +46min since grace period of 2h
                                            // was started at BASE_TIME+45min
  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::LO_PRI)
      .wait();

  filter_factory_->flushTrimPoints();
  EXPECT_EQ(3, getTrimPoint(logid_t(300)));
  EXPECT_EQ(0, getTrimPoint(logid_t(301)));
  EXPECT_EQ(1, stats_.aggregate().partitions_dropped);   // P0
  EXPECT_EQ(2, stats_.aggregate().partitions_compacted); // P1, P2

  ld_info("Advance time to 7days+30minutes");
  // now log 302's time retention expired, so we'll move its trim point,
  // i.e record in P1 should be deleted.
  // Extra 30mins to account for start of Partition2, this allows
  // cutoff timestamp to be >= P2 start time(which is BASE_TIME+30mins)
  time_ = SystemTimestamp(
      std::chrono::milliseconds(BASE_TIME + 7 * DAY + 30 * MINUTE));
  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::LO_PRI)
      .wait();
  filter_factory_->flushTrimPoints();
  EXPECT_EQ(1, getTrimPoint(logid_t(301)));
  EXPECT_EQ(1, getTrimPoint(logid_t(302)));

  EXPECT_EQ(2, stats_.aggregate().partitions_dropped);
  EXPECT_EQ(2, stats_.aggregate().partitions_compacted);

  // Fix log range for other tests
  ASSERT_TRUE(processor_->config_->getLocalLogsConfig()->replaceLogGroup(
      "/logs_can_disappear",
      log_group->withRange(logid_range_t({logid_t(300), logid_t(399)}))));

  EXPECT_EQ(3, getTrimPoint(logid_t(300)));
}

TEST_F(PartitionedRocksDBStoreTest, ApplySpaceBasedRetentionPerDisk) {
  ServerConfig::SettingsConfig s;
  s["rocksdb-compression-type"] = "none";
  // Reopen with our settings.
  closeStore();
  openStore(s);

  lsn_t lsn = 1;
  auto write_1mb = [&] {
    // 10 100KB records.
    for (int i = 0; i < 10; ++i) {
      put({TestRecord(logid_t(1),
                      lsn++,
                      time_.toMilliseconds().count(),
                      std::string((size_t)1e5, 'x'))});
    }
  };

  // Partition 0.
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME));
  write_1mb();
  EXPECT_TRUE(store_->flushMemtable(store_->getPartitionList()->get(ID0)->cf_));

  // Partition 1.
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + HOUR / 2));
  store_->createPartition();
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + HOUR));
  EXPECT_TRUE(
      store_->flushMemtable(store_->getPartitionList()->get(ID0 + 1)->cf_));

  ASSERT_TRUE(store_->getPartitionList()->get(ID0));
  ASSERT_TRUE(store_->getPartitionList()->get(ID0 + 1));
  EXPECT_EQ(ID0, store_->getPartitionList()->firstID());

  // Nothing should happen.
  store_->setSpaceBasedTrimLimit(PARTITION_INVALID);
  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::LO_PRI)
      .wait();
  ASSERT_TRUE(store_->getPartitionList()->get(ID0));
  EXPECT_EQ(ID0, store_->getPartitionList()->firstID());

  ASSERT_TRUE(store_->getPartitionList()->get(ID0));
  ASSERT_TRUE(store_->getPartitionList()->get(ID0 + 1));

  // Nothing should happen.
  store_->setSpaceBasedTrimLimit(ID0);
  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::LO_PRI)
      .wait();
  EXPECT_EQ(ID0, store_->getPartitionList()->firstID());

  ASSERT_TRUE(store_->getPartitionList()->get(ID0));
  ASSERT_TRUE(store_->getPartitionList()->get(ID0 + 1));

  // Should drop partition 0.
  store_->setSpaceBasedTrimLimit(ID0 + 1);
  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::LO_PRI)
      .wait();
  EXPECT_EQ(ID0 + 1, store_->getPartitionList()->firstID());

  ASSERT_FALSE(store_->getPartitionList()->get(ID0));
  ASSERT_TRUE(store_->getPartitionList()->get(ID0 + 1));

  readAndCheck();
}

TEST_F(PartitionedRocksDBStoreTest, DeleteRatelimit) {
  ServerConfig::SettingsConfig s_slow;
  s_slow["rocksdb-compression-type"] = "none";
  s_slow["rocksdb-sst-delete-bytes-per-sec"] = "100000"; // should take 20s
  s_slow["rocksdb-partition-duration"] = "0";
  s_slow["rocksdb-partition-file-limit"] = "0";
  ServerConfig::SettingsConfig s_fast = s_slow;
  s_fast["rocksdb-sst-delete-bytes-per-sec"] = "10000000"; // should take 100ms
  lsn_t lsn = 1;
  auto rec_time = BASE_TIME;
  partition_id_t baseID = ID0;
  partition_id_t currentID = baseID;

  namespace fs = boost::filesystem;
  auto num_trash_files = [&] {
    int count = 0;
    size_t tot_size = 0;
    size_t size = 0;

    for (auto it = fs::directory_iterator(path_);
         it != fs::directory_iterator();
         it++) {
      try {
        if (it->path().extension() == ".trash") {
          tot_size += fs::file_size(it->path());
          count++;
        }
      } catch (const fs::filesystem_error& e) {
      }
    }

    ld_info("trash file count: %d, total size: %ld", count, tot_size);
    return count;
  };

  auto wait_until_trash_is_gone = [&] {
    auto start = std::chrono::steady_clock::now();
    wait_until("Wait until trash files are gone",
               [&] { return num_trash_files() == 0; });
    auto end = std::chrono::steady_clock::now();
    return to_msec(end - start);
  };

  auto write_to_one_partition = [&](size_t record_size, int num_records) {
    time_ = SystemTimestamp(std::chrono::milliseconds(rec_time));
    for (int j = 0; j < num_records; ++j, rec_time += HOUR) {
      put({TestRecord(logid_t(1),
                      lsn++,
                      time_.toMilliseconds().count(),
                      std::string(record_size, 'x'))});
    }
    ASSERT_TRUE(
        store_->flushMemtable(store_->getPartitionList()->get(currentID)->cf_));
    ASSERT_TRUE(store_->getPartitionList()->get(currentID));
    store_->createPartition();
    currentID++;
    store_->flushAllMemtables();
    store_
        ->backgroundThreadIteration(
            PartitionedRocksDBStore::BackgroundThreadType::HI_PRI)
        .wait();
  };

  auto write_100K_to_10_partitions = [&]() {
    for (int i = 0; i < 10; i++) {
      write_to_one_partition((size_t)1e4, 10); // 10 x 10KB to each partition
    }
  };

  auto measure_deletion_time =
      [&](ServerConfig::SettingsConfig s, bool fast, int64_t* ret_val) {
        std::chrono::milliseconds deletions_duration(0);

        settings_overrides_.clear();
        settings_overrides_ = getDefaultSettingsOverrides();
        for (auto setting_override : s) {
          settings_overrides_[setting_override.first] = setting_override.second;
        }
        // Update rocksdb settings
        settings_updater_->setFromCLI(settings_overrides_);

        // Send settings to store. In case of usual operation, shardedlogstore
        // instance subscribes for setting update notification and forwards the
        // notification to shards.
        store_->onSettingsUpdated(store_->getSettings());

        // Don't expect to see much trash
        ASSERT_LT(store_->getTotalTrashSize(), 1e4);

        write_100K_to_10_partitions();
        ASSERT_EQ(baseID, store_->getPartitionList()->firstID());

        // 1 MB of WAL files should be trashed, but some may have already been
        // deleted, especially when using 10 MB/s rate limit.
        if (!fast) {
          ASSERT_GT(store_->getTotalTrashSize(), 600000);
        }
        ASSERT_LT(store_->getTotalTrashSize(), 1200000);

        deletions_duration += wait_until_trash_is_gone();

        ASSERT_LT(store_->getTotalTrashSize(), 1e4);

        partition_id_t dropToID = currentID;

        // Create a tiny partition so we can drop everything else
        write_to_one_partition((size_t)1, 1);

        // Drop up to latest (empty) partition
        store_->setSpaceBasedTrimLimit(dropToID);
        store_
            ->backgroundThreadIteration(
                PartitionedRocksDBStore::BackgroundThreadType::LO_PRI)
            .wait();
        ASSERT_EQ(dropToID, store_->getPartitionList()->firstID());

        ASSERT_GT(store_->getTotalTrashSize(), fast ? 0 : 800000);
        ASSERT_LT(store_->getTotalTrashSize(), 1200000);

        // All partitions but the last should be gone as far as LogsDB is
        // concerned
        for (int j = 0; j < 10; j++) {
          ASSERT_FALSE(store_->getPartitionList()->get(baseID + j));
        }
        ASSERT_TRUE(store_->getPartitionList()->get(dropToID));

        deletions_duration += wait_until_trash_is_gone();

        // Expect minimal trash
        ASSERT_LT(store_->getTotalTrashSize(), 1e4);

        // Drop extra file
        store_->setSpaceBasedTrimLimit(currentID);
        store_
            ->backgroundThreadIteration(
                PartitionedRocksDBStore::BackgroundThreadType::LO_PRI)
            .wait();
        ASSERT_EQ(currentID, store_->getPartitionList()->firstID());
        wait_until("Wait until extra file is gone",
                   [&] { return num_trash_files() == 0; });

        // Expect minimal trash
        ASSERT_LT(store_->getTotalTrashSize(), 1e4);

        ASSERT_FALSE(store_->getPartitionList()->get(dropToID));
        ASSERT_TRUE(store_->getPartitionList()->get(currentID));

        // Reset base to current only partition
        baseID = currentID;

        *ret_val = deletions_duration.count();
      };

  // Open store with high delete rate settings.
  closeStore();
  rocksdb_settings_ = UpdateableSettings<RocksDBSettings>(
      RocksDBSettings::defaultTestSettings());
  settings_updater_ = std::make_unique<SettingsUpdater>();
  settings_updater_->registerSettings(rocksdb_settings_);
  // Callback function that'll add an SstFileManager to the conf,
  // which will enforce the ratelimit
  auto customize_conf_fn = [this](RocksDBLogStoreConfig& cfg) {
    cfg.addSstFileManagerForShard();
  };
  openStore(s_fast, customize_conf_fn);
  // SstFileManager, which ratelimits deletes, needs RocksDBEnv for OS
  // functionality like the filesystem
  if (!env_) {
    env_ = std::make_unique<RocksDBEnv>(
        rocksdb_settings_, /* stats */ nullptr, std::vector<IOTracing*>());
  }

  // Measure with fast settings.
  int64_t elapsed_ms = -1;
  measure_deletion_time(s_fast, true, &elapsed_ms);
  ASSERT_NE(elapsed_ms, -1);
  // We wrote 1 MB to WAL and to SST files. Since we can drop 10M per sec,
  // trash files should be gone in ~200ms.
  EXPECT_GT(elapsed_ms, 0);
  EXPECT_LT(elapsed_ms, 500);
  ld_info("Fast settings: %ldms (expected 200ms)", elapsed_ms);

  // Measure with slow settings.
  elapsed_ms = -1;
  measure_deletion_time(s_slow, false, &elapsed_ms);
  ASSERT_NE(elapsed_ms, -1);

  // Since we can drop 500KB per sec, trash files should be gone in ~4s
  EXPECT_GT(elapsed_ms, 17000);
  EXPECT_LT(elapsed_ms, 25000);
  ld_info("Slow settings: %ldms (expected 20000ms)", elapsed_ms);

  readAndCheck();
}

// This test is kind of obsolete and doesn't test anything in particular.
TEST_F(PartitionedRocksDBStoreTest, MaxRetentionLogsEmpty) {
  ServerConfig::SettingsConfig s;
  closeStore();
  openStore(s);

  // Create partitions starting every 12 hours for 4 days.
  // Put a 1day and a 2day record in the middle of each partition.
  for (int i = 1; i <= 2 * 4; ++i) {
    time_ =
        SystemTimestamp(std::chrono::milliseconds(BASE_TIME + i * 12 * HOUR));
    store_->createPartition();
    put({TestRecord(logid_t(50), i * 2, BASE_TIME + (i * 12 + 6) * HOUR)});
    put({TestRecord(logid_t(150), i * 3, BASE_TIME + (i * 12 + 6) * HOUR)});
  }

  // After 4 days 9 hours we should drop 4 partitions and compact 2 partitions.
  time_ = SystemTimestamp(
      std::chrono::milliseconds(BASE_TIME + (4 * 2 * 12 + 9) * HOUR));
  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::LO_PRI)
      .wait();
  ASSERT_EQ(ID0 + 4, store_->getPartitionList()->firstID());
  ASSERT_EQ(ID0 + 9, store_->getPartitionList()->nextID());
  EXPECT_EQ(2, stats_.aggregate().partitions_compacted);

  auto data = readAndCheck();
  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ(i < 2 ? 0 : 1, data[i][logid_t(50)].records.size());
    EXPECT_EQ(1, data[i][logid_t(150)].records.size());
  }
}

// All logs with longest backlog duration are empty, and compaction schedule
// is the longest backlog duration. Shouldn't compact anything ever.
// Like previous test, this one is somewhat pointless now but I've kept it
// just in case.
TEST_F(PartitionedRocksDBStoreTest, MaxRetentionLogsEmptyWithSchedule) {
  closeStore();

  ServerConfig::SettingsConfig s;
  s["rocksdb-partition-compaction-schedule"] = "3d";
  openStore(s);

  // Same setup as previous test.
  // Create partitions starting every 12 hours for 4 days.
  // Put a 1day and a 2day record in the middle of each partition.
  for (int i = 1; i <= 2 * 4; ++i) {
    time_ =
        SystemTimestamp(std::chrono::milliseconds(BASE_TIME + i * 12 * HOUR));
    store_->createPartition();
    put({TestRecord(logid_t(50), i * 2, BASE_TIME + (i * 12 + 6) * HOUR)});
    put({TestRecord(logid_t(150), i * 3, BASE_TIME + (i * 12 + 6) * HOUR)});
  }

  // After 4 days 9 hours we should drop 4 partitions and compact nothing.
  time_ = SystemTimestamp(
      std::chrono::milliseconds(BASE_TIME + (4 * 2 * 12 + 9) * HOUR));
  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::LO_PRI)
      .wait();
  ASSERT_EQ(ID0 + 4, store_->getPartitionList()->firstID());
  ASSERT_EQ(ID0 + 9, store_->getPartitionList()->nextID());
  EXPECT_EQ(4, stats_.aggregate().partitions_dropped);
  EXPECT_EQ(0, stats_.aggregate().partitions_compacted);
  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::LO_PRI)
      .wait();
  EXPECT_EQ(4, stats_.aggregate().partitions_dropped);
  EXPECT_EQ(0, stats_.aggregate().partitions_compacted);

  // Add 2d to schedule. Nothing happens.
  updateSetting("rocksdb-partition-compaction-schedule", "2d,3d");

  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::LO_PRI)
      .wait();
  EXPECT_EQ(4, stats_.aggregate().partitions_dropped);
  EXPECT_EQ(0, stats_.aggregate().partitions_compacted);

  // Add 1d to schedule. 2 partitions should be compacted.
  updateSetting("rocksdb-partition-compaction-schedule", "1d,2d,3d");

  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::LO_PRI)
      .wait();
  EXPECT_EQ(4, stats_.aggregate().partitions_dropped);
  EXPECT_EQ(2, stats_.aggregate().partitions_compacted);
  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::LO_PRI)
      .wait();
  EXPECT_EQ(4, stats_.aggregate().partitions_dropped);
  EXPECT_EQ(2, stats_.aggregate().partitions_compacted);

  auto data = readAndCheck();
  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ(i < 2 ? 0 : 1, data[i][logid_t(50)].records.size());
    EXPECT_EQ(1, data[i][logid_t(150)].records.size());
  }
}

TEST_F(PartitionedRocksDBStoreTest, SnapshotsPersistence) {
  auto snapshots_type = LocalLogStore::LogSnapshotBlobType::RECORD_CACHE;
  std::map<logid_t, std::string> snapshots_content{
      {logid_t(1), std::string(5000000, '1')},    // 5mb
      {logid_t(777), std::string(10000000, '2')}, // 10mb
      {logid_t(1337), std::string(1, '3')}        // 1b
  };
  std::vector<std::pair<logid_t, Slice>> snapshots;
  for (auto& kv : snapshots_content) {
    snapshots.emplace_back(
        kv.first, Slice(kv.second.c_str(), kv.second.size()));
  }

  // Assert initially empty
  std::map<logid_t, std::string> blob_map;
  LocalLogStore::LogSnapshotBlobCallback callback = [&blob_map](logid_t logid,
                                                                Slice blob) {
    blob_map[logid] =
        std::string(reinterpret_cast<const char*>(blob.data), blob.size);
    return 0;
  };
  int rv = store_->readAllLogSnapshotBlobs(snapshots_type, callback);
  ASSERT_TRUE(blob_map.empty());

  // Add snapshots, verify correct snapshot blobs can be read
  rv = store_->writeLogSnapshotBlobs(snapshots_type, snapshots);
  ASSERT_EQ(0, rv);
  rv = store_->readAllLogSnapshotBlobs(snapshots_type, callback);
  ASSERT_EQ(0, rv);
  ASSERT_EQ(blob_map, snapshots_content);

  // Verify deletion works
  rv = store_->deleteAllLogSnapshotBlobs();
  ASSERT_EQ(0, rv);
  blob_map.clear();
  rv = store_->readAllLogSnapshotBlobs(snapshots_type, callback);
  ASSERT_EQ(0, rv);
  ASSERT_TRUE(blob_map.empty());

  // Should not be a problem to call delete when there are no snapshots
  rv = store_->deleteAllLogSnapshotBlobs();
  ASSERT_EQ(0, rv);
  rv = store_->readAllLogSnapshotBlobs(snapshots_type, callback);
  ASSERT_EQ(0, rv);
  ASSERT_TRUE(blob_map.empty());

  // Write one snapshot twice, should be fine
  std::string final_example = "final example";
  auto final_example_list = std::vector<std::pair<logid_t, Slice>>{
      {logid_t(999), Slice(final_example.c_str(), final_example.size())}};

  rv = store_->writeLogSnapshotBlobs(snapshots_type, final_example_list);
  ASSERT_EQ(0, rv);
  rv = store_->writeLogSnapshotBlobs(snapshots_type, final_example_list);
  ASSERT_EQ(0, rv);

  // Verify that final_example snapshot was added
  blob_map.clear();
  rv = store_->readAllLogSnapshotBlobs(snapshots_type, callback);
  ASSERT_EQ(0, rv);
  std::map<logid_t, std::string> final_example_content{
      {logid_t(999), final_example}};
  ASSERT_EQ(blob_map, final_example_content);

  // Also write the three initially used, verify all four are now stored
  rv = store_->writeLogSnapshotBlobs(snapshots_type, snapshots);
  ASSERT_EQ(0, rv);

  blob_map.clear();
  snapshots_content[logid_t(999)] = final_example_content[logid_t(999)];
  rv = store_->readAllLogSnapshotBlobs(snapshots_type, callback);
  ASSERT_EQ(0, rv);
  ASSERT_EQ(blob_map, snapshots_content);
}

TEST_F(PartitionedRocksDBStoreTest, CompactionRangeSkip) {
  filter_history_t filter_calls;
  filter_factory_->out_history_ = &filter_calls;

  put({TestRecord(logid_t(10), 1, BASE_TIME)});
  put({TestRecord(logid_t(210), 100, BASE_TIME)});
  put({TestRecord(logid_t(210), 250, BASE_TIME)});
  put({TestRecord(logid_t(220), 200, BASE_TIME)});
  put({TestRecord(logid_t(220), 350, BASE_TIME)});
  put({TestRecord(logid_t(230), 300, BASE_TIME)});
  put({TestRecord(logid_t(230), 450, BASE_TIME)});
  put({TestRecord(logid_t(240), 400, BASE_TIME)});
  put({TestRecord(logid_t(240), 550, BASE_TIME)});
  store_->createPartition();

  // Manual compaction, nothing trimmed. A filterImpl() for every record and
  // CSI entry.
  store_->performCompaction(ID0);
  EXPECT_EQ(1, stats_.aggregate().partitions_compacted);
  EXPECT_EQ(18, filter_calls.size());
  filter_calls.clear();

  // Manually trim away logs 220 and 240, partially trim log 230, and advance
  // time to time-based trim log 10.
  aligned_trims_ = false;
  int rv = LocalLogStoreUtils::updateTrimPoints(
      {{logid_t(220), 350}, {logid_t(230), 400}, {logid_t(240), 1000}},
      processor_.get(),
      *store_,
      false,
      &stats_);
  EXPECT_EQ(0, rv);
  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + DAY * 2));

  // Retention-based compaction should call filterImpl() almost only for
  // records/CSI that it'll keep.
  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::LO_PRI)
      .wait();
  EXPECT_EQ(2, stats_.aggregate().partitions_compacted);
  EXPECT_EQ(12, filter_calls.size());
  using keys_t = std::vector<std::tuple<int, logid_t::raw_type, lsn_t>>;
  keys_t keys;
  for (auto kv : filter_calls) {
    if (kv.first[0] == DataKey::HEADER) {
      ASSERT_TRUE(DataKey::valid(kv.first.data(), kv.first.size()));
      keys.emplace_back(0,
                        DataKey::getLogID(kv.first.data()).val_,
                        DataKey::getLSN(kv.first.data()));
    } else {
      ASSERT_TRUE(CopySetIndexKey::valid(kv.first.data(), kv.first.size()));
      keys.emplace_back(1,
                        CopySetIndexKey::getLogID(kv.first.data()).val_,
                        CopySetIndexKey::getLSN(kv.first.data()));
    }
  }
// CSI, then records.
#define MT std::make_tuple
  EXPECT_EQ(keys_t({MT(1, 10, 1),
                    MT(1, 210, 100),
                    MT(1, 210, 250),
                    MT(1, 220, 200),
                    MT(1, 230, 450),
                    MT(1, 240, 400),
                    MT(0, 10, 1),
                    MT(0, 210, 100),
                    MT(0, 210, 250),
                    MT(0, 220, 200),
                    MT(0, 230, 450),
                    MT(0, 240, 400)}),
            keys);
#undef MT

  auto data = readAndCheck();
  ASSERT_EQ(2, data.size());
  EXPECT_EQ(0, data[1].size());
  EXPECT_EQ(std::vector<lsn_t>(), data[0][logid_t(10)].records);
  EXPECT_EQ(std::vector<lsn_t>(), data[0][logid_t(220)].records);
  EXPECT_EQ(std::vector<lsn_t>(), data[0][logid_t(240)].records);
  EXPECT_EQ(std::vector<lsn_t>({100, 250}), data[0][logid_t(210)].records);
  EXPECT_EQ(std::vector<lsn_t>({450}), data[0][logid_t(230)].records);
}

TEST_F(PartitionedRocksDBStoreTest, NewPartitionTriggers) {
  closeStore();
  openStore({{"rocksdb-partition-duration", "10h"},
             {"rocksdb-partition-file-limit", "3"},
             {"rocksdb-partition-size-limit", "3M"},
             {"rocksdb-compression-type", "none"}});
  EXPECT_EQ(1, store_->getPartitionList()->size());

  // Time trigger.

  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + HOUR * 9));
  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::HI_PRI)
      .wait();
  EXPECT_EQ(1, store_->getPartitionList()->size());

  time_ = SystemTimestamp(std::chrono::milliseconds(BASE_TIME + HOUR * 11));
  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::HI_PRI)
      .wait();
  EXPECT_EQ(2, store_->getPartitionList()->size());

  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::HI_PRI)
      .wait();
  EXPECT_EQ(2, store_->getPartitionList()->size());

  // File count trigger.

  put({TestRecord(logid_t(42), 1337, BASE_TIME + HOUR * 15)});
  store_->flushAllMemtables();
  put({TestRecord(logid_t(42), 1338, BASE_TIME + HOUR * 15)});
  store_->flushAllMemtables();
  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::HI_PRI)
      .wait();
  EXPECT_EQ(2, store_->getPartitionList()->size());

  put({TestRecord(logid_t(42), 1339, BASE_TIME + HOUR * 15)});
  store_->flushAllMemtables();
  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::HI_PRI)
      .wait();
  EXPECT_EQ(3, store_->getPartitionList()->size());

  store_->flushAllMemtables();
  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::HI_PRI)
      .wait();
  EXPECT_EQ(3, store_->getPartitionList()->size());

  // Size trigger.

  put({TestRecord(
      logid_t(42), 1340, BASE_TIME + HOUR * 15, std::string(1 << 20, 'X'))});
  put({TestRecord(
      logid_t(42), 1341, BASE_TIME + HOUR * 15, std::string(1 << 20, 'X'))});
  store_->flushAllMemtables();
  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::HI_PRI)
      .wait();
  EXPECT_EQ(3, store_->getPartitionList()->size());

  put({TestRecord(
      logid_t(42), 1342, BASE_TIME + HOUR * 15, std::string(1 << 20, 'X'))});
  store_->flushAllMemtables();
  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::HI_PRI)
      .wait();
  EXPECT_EQ(4, store_->getPartitionList()->size());

  store_->flushAllMemtables();
  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::HI_PRI)
      .wait();
  EXPECT_EQ(4, store_->getPartitionList()->size());

  readAndCheck();
}

TEST_F(PartitionedRocksDBStoreTest, IsEmpty) {
  // A newly created store must be considered empty.
  EXPECT_EQ(1, store_->isEmpty());
  store_->createPartition();
  // Creating an empty partition doesn't change that.
  EXPECT_EQ(1, store_->isEmpty());
  // Writing a record makes it non-empty.
  put({TestRecord(logid_t(9), 10, BASE_TIME)});
  EXPECT_EQ(0, store_->isEmpty());
}

class MutablePerEpochMetadataTest : public PartitionedRocksDBStoreTest {};

/**
 * Single writes that are immediately read back.
 */
TEST_F(MutablePerEpochMetadataTest, SingeWrites) {
  // Write log 1, epoch 2, lng 3, size 100
  OffsetMap epoch_size_map;
  epoch_size_map.setCounter(BYTE_OFFSET, 100);
  MutablePerEpochLogMetadata metadata(1 /* flags */, esn_t{3}, epoch_size_map);
  MergeMutablePerEpochLogMetadataWriteOp write_op(
      logid_t{1}, epoch_t{2}, &metadata);
  std::vector<const WriteOp*> op_ptrs(1, &write_op);
  ASSERT_EQ(0, store_->writeMulti(op_ptrs, LocalLogStore::WriteOptions()));
  MutablePerEpochLogMetadata metadata_out;
  ASSERT_EQ(
      0,
      store_->readPerEpochLogMetadata(logid_t{1}, epoch_t{2}, &metadata_out));
  ASSERT_EQ(esn_t{3}, metadata_out.data_.last_known_good);
  ASSERT_EQ(
      uint64_t{100}, metadata_out.epoch_size_map_.getCounter(BYTE_OFFSET));

  // Write lng 5, size 10; should merge into lng 5, size 100
  metadata.data_.last_known_good = esn_t{5};
  metadata.epoch_size_map_.setCounter(BYTE_OFFSET, 10);
  ASSERT_EQ(0, store_->writeMulti(op_ptrs, LocalLogStore::WriteOptions()));
  ASSERT_EQ(
      0,
      store_->readPerEpochLogMetadata(logid_t{1}, epoch_t{2}, &metadata_out));
  ASSERT_EQ(esn_t{5}, metadata_out.data_.last_known_good);
  ASSERT_EQ(
      uint64_t{100}, metadata_out.epoch_size_map_.getCounter(BYTE_OFFSET));

  // Write lng 1, size 200; should merge into lng 5, size 200
  metadata.data_.last_known_good = esn_t{1};
  metadata.epoch_size_map_.setCounter(BYTE_OFFSET, 200);
  ASSERT_EQ(0, store_->writeMulti(op_ptrs, LocalLogStore::WriteOptions()));
  ASSERT_EQ(
      0,
      store_->readPerEpochLogMetadata(logid_t{1}, epoch_t{2}, &metadata_out));
  ASSERT_EQ(esn_t{5}, metadata_out.data_.last_known_good);
  ASSERT_EQ(
      uint64_t{200}, metadata_out.epoch_size_map_.getCounter(BYTE_OFFSET));
}

/**
 * A batch of writes to be merged.
 */
TEST_F(MutablePerEpochMetadataTest, MultiWrite) {
  // Create vector of 100 write operations.
  static constexpr size_t NWRITES = 100;
  std::vector<MutablePerEpochLogMetadata> metadata_vec(NWRITES);
  std::vector<MergeMutablePerEpochLogMetadataWriteOp> write_op_vec;
  for (uint i = 0; i < metadata_vec.size(); ++i) {
    MutablePerEpochLogMetadata& metadata = metadata_vec[i];
    metadata.reset();
    metadata.data_.flags =
        MutablePerEpochLogMetadata::Data::SUPPORT_OFFSET_MAP; /* serialize
                                                                 OffsetMap */
    metadata.data_.last_known_good = esn_t(i + 1);
    metadata.epoch_size_map_.setCounter(BYTE_OFFSET, (i + 1) * 100);
    write_op_vec.emplace_back(logid_t{1}, epoch_t{2}, &metadata);
  }
  std::vector<const WriteOp*> op_ptrs;
  op_ptrs.reserve(write_op_vec.size());
  std::transform(write_op_vec.begin(),
                 write_op_vec.end(),
                 std::back_inserter(op_ptrs),
                 [](auto& write_op) { return &write_op; });

  // Randomly shuffle the write operation vector to exercise the merge logic.
  std::shuffle(
      write_op_vec.begin(), write_op_vec.end(), folly::ThreadLocalPRNG());

  // Perform all write operations in one go.
  ASSERT_EQ(0, store_->writeMulti(op_ptrs, LocalLogStore::WriteOptions()));

  // Read back merged value.
  MutablePerEpochLogMetadata metadata_out;
  ASSERT_EQ(
      0,
      store_->readPerEpochLogMetadata(logid_t{1}, epoch_t{2}, &metadata_out));
  ASSERT_EQ(esn_t{NWRITES}, metadata_out.data_.last_known_good);
  ASSERT_EQ(uint64_t{NWRITES * 100},
            metadata_out.epoch_size_map_.getCounter(BYTE_OFFSET));
}

/**
 * Test Iterator behavior when crossing dirty partitions.
 *
 * First seeking to any partition with under-replicated partitions:
 *   - Iterator indicates underreplication.
 * Next/NextFiltered:
 *   - Stays under-replicated if under-replicated regardless
 *     of partition under-replication status.
 *   - Sets under-replicated if it touches or crosses an
 *     under-replicated partition.
 * Subsequent seek:
 *   - Resets to fully-replicated if the range from start to end
 *     position of seek does not intersect an underreplicated
 *     partition.
 *
 * Test Partition Layout:
 *
 *   Partition            | 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 |
 *   Underreplicated      | X | X |   |   |   | X |   |   |   | X |
 *   Has Records          |   | X |   | X | X | X |   | X | X |   |
 *   Has Multiple Records |   | X |   | X |   |   |   | X | X |   |
 */
TEST_F(PartitionedRocksDBStoreTest, IterateOnDirtyPartitions) {
  // Create a store that matches the state of the diagram above.
  // Partition 0
  const logid_t logid(1);
  const logid_t no_records_logid(2);

  LsnToPartitionMap lsn_to_partition{// Partition 1
                                     {10, ID0 + 1},
                                     {11, ID0 + 1},
                                     {13, ID0 + 1},

                                     // Partition 3
                                     {15, ID0 + 3},
                                     {16, ID0 + 3},

                                     // Partition 4
                                     {20, ID0 + 4},

                                     // Partition 5
                                     {25, ID0 + 5},

                                     // Partition 7
                                     {30, ID0 + 7},
                                     {32, ID0 + 7},
                                     {33, ID0 + 7},

                                     // Partition 8
                                     {40, ID0 + 8},
                                     {41, ID0 + 8},
                                     {42, ID0 + 8}};
  PartitionSet under_replicated_partitions{ID0, ID0 + 1, ID0 + 5, ID0 + 9};

  populateUnderReplicatedStore(
      logid, lsn_to_partition, under_replicated_partitions, ID0, ID0 + 9);

  LocalLogStore::ReadOptions read_options("IterateOnDirtyPartitions");
  for (auto lsn_kv : lsn_to_partition) {
    lsn_t lsn = lsn_kv.first;
    SCOPED_TRACE(lsn);
    auto it = store_->read(logid, read_options);
    it->seek(lsn);
    EXPECT_EQ(IteratorState::AT_RECORD, it->state());
    // Initial seek to any under-replicated partition should be flagged
    // as under-replicated.
    EXPECT_EQ(under_replicated_partitions.count(lsn_kv.second) != 0,
              it->accessedUnderReplicatedRegion());

    // Check stickiness
    if (it->accessedUnderReplicatedRegion()) {
      while (it->state() == IteratorState::AT_RECORD) {
        ASSERT_TRUE(it->accessedUnderReplicatedRegion());
        it->next();
      }
    }
  }

  // If we seek into a partition with an lsn that is less than
  // the starting lsn for the partition, the iterator should be
  // flagged as accessing an under-replicated region since there
  // are some under-replicated partitions. This may overestimate
  // under-replication, but avoids a linear search of the partition
  // space to see if a record may have been missed.
  for (auto lsn_kv : lsn_to_partition) {
    lsn_t lsn = lsn_kv.first;
    partition_id_t cur_partition = lsn_kv.second;
    auto kv_it = lsn_to_partition.find(lsn);
    ld_check(kv_it != lsn_to_partition.end());
    partition_id_t prev_partition = ID0 + 1;

    if (kv_it != lsn_to_partition.begin()) {
      --kv_it;
      prev_partition = kv_it->second;
      if (prev_partition == cur_partition || kv_it->first == lsn - 1) {
        continue;
      }
    }

    bool saw_underreplicated = false;
    for (partition_id_t i = prev_partition; i <= cur_partition; ++i) {
      saw_underreplicated |= under_replicated_partitions.count(i);
    }

    auto it = store_->read(logid, read_options);
    it->seek(lsn - 1);
    EXPECT_EQ(saw_underreplicated, it->accessedUnderReplicatedRegion());
  }

  // If an initial seek doesn't mark the iterator as under-replicated,
  // a next operation that accesses or crosses an under-replicated partition
  // should set the flag.
  for (auto lsn_kv : lsn_to_partition) {
    auto it = store_->read(logid, read_options);
    it->seek(lsn_kv.first);
    if (it->accessedUnderReplicatedRegion()) {
      continue;
    }

    bool saw_underreplicated = false;
    partition_id_t prev_partition = lsn_kv.second;
    while (it->state() == IteratorState::AT_RECORD) {
      lsn_t lsn = it->getLSN();
      partition_id_t cur_partition = lsn_to_partition[lsn];
      for (; prev_partition <= cur_partition; ++prev_partition) {
        saw_underreplicated |=
            under_replicated_partitions.count(prev_partition);
      }
      ASSERT_EQ(saw_underreplicated, it->accessedUnderReplicatedRegion());
      it->next();
    }
  }

  // If we iterate to the end of the log, and next'ing to the last
  // record didn't flag under-replication, hitting the end should
  // if there are under-replicated partitions after the partition
  // that held the last record.
  {
    auto it = store_->read(logid, read_options);
    auto lsn_kv = lsn_to_partition.rbegin();
    lsn_t last_lsn = lsn_kv->first;
    lsn_t start_lsn = lsn_kv->first;
    lsn_t start_partition = lsn_kv->second;
    while (++lsn_kv != lsn_to_partition.rend()) {
      if (lsn_kv->second != start_partition) {
        break;
      }
      start_lsn = lsn_kv->first;
    }
    // Must have under-replicated partitions after our stop point.
    ASSERT_LT(start_partition, *under_replicated_partitions.rbegin());
    EXPECT_EQ(under_replicated_partitions.count(start_partition), 0);
    it->seek(start_lsn);
    while (it->state() == IteratorState::AT_RECORD) {
      EXPECT_FALSE(it->accessedUnderReplicatedRegion());
      it->next();
    }
    EXPECT_TRUE(it->accessedUnderReplicatedRegion());
  }

  // Reading any LSN value on an under-replicated store that has no records
  // for a log should indicate underreplication.
  for (auto lsn_kv : lsn_to_partition) {
    auto it = store_->read(no_records_logid, read_options);
    it->seek(lsn_kv.first);
    EXPECT_EQ(it->state(), IteratorState::AT_END);
    EXPECT_TRUE(it->accessedUnderReplicatedRegion());
  }

  // Seeking to an LSN between two fully-replicated partitions, with no
  // underreplicated partitions between them, should report no underreplication.
  {
    auto it = store_->read(logid, read_options);
    it->seek(lsn_t(19));
    EXPECT_EQ(it->state(), IteratorState::AT_RECORD);
    EXPECT_EQ(it->getLSN(), lsn_t(20));
    EXPECT_FALSE(it->accessedUnderReplicatedRegion());
  }
}

/**
 * Test Iterator behavior when crossing dirty partitions in reverse.
 *
 * First seeking to any partition with under-replicated partitions:
 *   - Iterator indicates underreplication.
 * Prev:
 *   - Stays under-replicated if under-replicated regardless
 *     of partition under-replication status.
 *   - Sets under-replicated if it touches or crosses an
 *     under-replicated partition.
 * Subsequent seek:
 *   - Resets to fully-replicated if the range from start to end
 *     position of seek does not intersect an underreplicated
 *     partition.
 *
 * Test Partition Layout:
 *
 *    Partition            | 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 |
 *    Underreplicated      | X |   |   |   |   | X |   |   |   | X |
 *    Has Records          |   | X |   | X | X |   | X | X | X |   |
 *    Has Multiple Records |   | X |   | X |   |   |   | X | X |   |
 */
TEST_F(PartitionedRocksDBStoreTest, ReverseIterateOnDirtyPartitions) {
  // Create a store that matches the state of the diagram above.
  // Partition 0
  const logid_t logid(1);
  const logid_t no_records_logid(2);

  LsnToPartitionMap lsn_to_partition{// Partition 1
                                     {10, ID0 + 1},
                                     {11, ID0 + 1},
                                     {13, ID0 + 1},

                                     // Partition 3
                                     {15, ID0 + 3},
                                     {16, ID0 + 3},

                                     // Partition 4
                                     {20, ID0 + 4},

                                     // Partition 5
                                     {25, ID0 + 5},

                                     // Partition 7
                                     {30, ID0 + 7},
                                     {32, ID0 + 7},
                                     {33, ID0 + 7},

                                     // Partition 8
                                     {40, ID0 + 8},
                                     {41, ID0 + 8},
                                     {42, ID0 + 8}};
  PartitionSet under_replicated_partitions{ID0, ID0 + 4, ID0 + 9};

  populateUnderReplicatedStore(
      logid, lsn_to_partition, under_replicated_partitions, ID0, ID0 + 9);

  LocalLogStore::ReadOptions read_options("ReverseIterateOnDirtyPartitions");
  for (auto lsn_kv : lsn_to_partition) {
    lsn_t lsn = lsn_kv.first;
    SCOPED_TRACE(lsn);
    auto it = store_->read(logid, read_options);
    it->seekForPrev(lsn);
    EXPECT_EQ(it->state(), IteratorState::AT_RECORD);
    // Initial seek to any under-replicated partition should be flagged
    // as under-replicated.
    EXPECT_EQ(under_replicated_partitions.count(lsn_kv.second) != 0,
              it->accessedUnderReplicatedRegion());

    // Check stickiness
    if (it->accessedUnderReplicatedRegion()) {
      while (it->state() == IteratorState::AT_RECORD) {
        ASSERT_TRUE(it->accessedUnderReplicatedRegion());
        it->prev();
      }
    }
  }

  // If an initial seek doesn't mark the iterator as under-replicated,
  // a prev operation that accesses or crosses an under-replicated partition
  // should set the flag.
  for (auto lsn_kv : lsn_to_partition) {
    auto it = store_->read(logid, read_options);
    it->seekForPrev(lsn_kv.first);
    if (it->accessedUnderReplicatedRegion()) {
      continue;
    }
    partition_id_t prev_partition = lsn_kv.second;
    while (it->state() == IteratorState::AT_RECORD) {
      lsn_t lsn = it->getLSN();
      partition_id_t cur_partition = lsn_to_partition[lsn];
      for (; prev_partition != cur_partition; --prev_partition) {
        if (under_replicated_partitions.count(prev_partition)) {
          ASSERT_TRUE(it->accessedUnderReplicatedRegion());
        }
      }
      it->prev();
    }
  }

  // If we iterate to the end of the log, and prev'ing to the last
  // record didn't flag under-replication, hitting the end should
  // if there are under-replicated partitions after the partition
  // that held the last record.
  {
    auto it = store_->read(logid, read_options);
    auto lsn_kv = lsn_to_partition.begin();
    lsn_t first_lsn = lsn_kv->first;
    lsn_t start_lsn = lsn_kv->first;
    lsn_t start_partition = lsn_kv->second;
    while (++lsn_kv != lsn_to_partition.end()) {
      if (lsn_kv->second != start_partition) {
        break;
      }
      start_lsn = lsn_kv->first;
    }
    // Must have under-replicated partitions after our stop point.
    ASSERT_GT(start_partition, *under_replicated_partitions.begin());
    EXPECT_EQ(under_replicated_partitions.count(start_partition), 0);
    it->seekForPrev(start_lsn);
    while (it->state() == IteratorState::AT_RECORD) {
      EXPECT_FALSE(it->accessedUnderReplicatedRegion());
      it->prev();
    }
    EXPECT_TRUE(it->accessedUnderReplicatedRegion());
  }

  // Reading any LSN value on an under-replicated store that has no records
  // for a log should indicate underreplication.
  for (auto lsn_kv : lsn_to_partition) {
    auto it = store_->read(no_records_logid, read_options);
    it->seekForPrev(lsn_kv.first);
    EXPECT_EQ(it->state(), IteratorState::AT_END);
    EXPECT_TRUE(it->accessedUnderReplicatedRegion());
  }

  // Seeking for prev to an LSN that is after the last record
  // in a fully-replicated partition will indicate underreplicateion
  // if there are any under-replicated partitions after the partition
  // of the returned record. This is the positive test.
  {
    auto it = store_->read(logid, read_options);
    it->seekForPrev(lsn_t(34));
    EXPECT_EQ(it->state(), IteratorState::AT_RECORD);
    EXPECT_EQ(it->getLSN(), lsn_t(33));
    EXPECT_TRUE(it->accessedUnderReplicatedRegion());
  }
}

/**
 * Test IteratorState::AT_END behavior outside of global range of partitions
 * that are under-replicated.
 *
 * Hitting IteratorState::AT_END when the iterator did not report
 * under-replication during next()/prev() to the last record should not
 * report under replication if there are no more under-replicated partitions
 * in the direction of iterator traversal.
 *
 * Test Partition Layout:
 *
 *    Partition            | 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 |
 *    Underreplicated      |   |   | X |   | X |   |   | X |   |   |
 *    Has Records          |   | X |   | X | X |   | X | X | X |   |
 *    Has Multiple Records |   | X |   | X |   |   | X | X |   |   |
 */
TEST_F(PartitionedRocksDBStoreTest, IterateAtEndOnDirtyPartitions) {
  // Create a store that matches the state of the diagram above.
  // Partition 0
  const logid_t logid(1);
  const logid_t no_records_logid(2);

  LsnToPartitionMap lsn_to_partition{
      // Partition 1
      {10, ID0 + 1},
      {11, ID0 + 1},
      {13, ID0 + 1},

      // Partition 3
      {15, ID0 + 3},
      {16, ID0 + 3},

      // Partition 4
      {20, ID0 + 4},

      // Partition 6
      {25, ID0 + 6},
      {27, ID0 + 6},

      // Partition 7
      {30, ID0 + 7},
      {32, ID0 + 7},
      {33, ID0 + 7},

      // Partition 8
      {40, ID0 + 8},
  };
  PartitionSet under_replicated_partitions{ID0 + 2, ID0 + 4, ID0 + 7};

  populateUnderReplicatedStore(
      logid, lsn_to_partition, under_replicated_partitions, ID0, ID0 + 9);

  LocalLogStore::ReadOptions read_options("IterateAtEndOnDirtyPartitions");

  // If we iterate to the end of the log, and next'ing to the last
  // record didn't flag under-replication, hitting the end should
  // not either since there are no more under-replicated partitions.
  {
    auto it = store_->read(logid, read_options);
    auto lsn_kv = lsn_to_partition.rbegin();
    lsn_t last_lsn = lsn_kv->first;
    lsn_t start_lsn = lsn_kv->first;
    lsn_t start_partition = lsn_kv->second;
    while (++lsn_kv != lsn_to_partition.rend()) {
      if (lsn_kv->second != start_partition) {
        break;
      }
      start_lsn = lsn_kv->first;
    }
    // Must not have under-replicated partitions after our stop point.
    ASSERT_GT(start_partition, *under_replicated_partitions.rbegin());
    EXPECT_EQ(under_replicated_partitions.count(start_partition), 0);
    it->seek(start_lsn);
    while (it->state() == IteratorState::AT_RECORD) {
      EXPECT_FALSE(it->accessedUnderReplicatedRegion());
      it->next();
    }
    EXPECT_FALSE(it->accessedUnderReplicatedRegion());
  }

  // Seeking above the last record shouldn't report under-replication if no
  // partitions above last record's partition are under-replicated.
  {
    auto it = store_->read(logid, read_options);
    auto lsn_kv = lsn_to_partition.rbegin();
    lsn_t lsn = lsn_kv->first + 1;
    lsn_t start_partition = lsn_kv->second;
    // Must not have under-replicated partitions after our stop point.
    ASSERT_GT(start_partition, *under_replicated_partitions.rbegin());
    EXPECT_EQ(under_replicated_partitions.count(start_partition), 0);
    it->seek(lsn);
    EXPECT_EQ(IteratorState::AT_END, it->state());
    EXPECT_FALSE(it->accessedUnderReplicatedRegion());
  }

  // If we iterate to the end of the log, and prev'ing to the last
  // record didn't flag under-replication, hitting the end should
  // not either since there are no more under-replicated partitions.
  {
    auto it = store_->read(logid, read_options);
    auto lsn_kv = lsn_to_partition.begin();
    lsn_t first_lsn = lsn_kv->first;
    lsn_t start_lsn = lsn_kv->first;
    lsn_t start_partition = lsn_kv->second;
    while (++lsn_kv != lsn_to_partition.end()) {
      if (lsn_kv->second != start_partition) {
        break;
      }
      start_lsn = lsn_kv->first;
    }
    // Must not have under-replicated partitions after our stop point.
    ASSERT_LT(start_partition, *under_replicated_partitions.begin());
    EXPECT_EQ(under_replicated_partitions.count(start_partition), 0);
    it->seekForPrev(start_lsn);
    while (it->state() == IteratorState::AT_RECORD) {
      EXPECT_FALSE(it->accessedUnderReplicatedRegion());
      it->prev();
    }
    EXPECT_FALSE(it->accessedUnderReplicatedRegion());
  }

  // Reading any LSN value on an under-replicated store that has no records
  // for a log should indicate underreplication.
  for (auto lsn_kv : lsn_to_partition) {
    auto it = store_->read(no_records_logid, read_options);
    it->seek(lsn_kv.first);
    EXPECT_EQ(it->state(), IteratorState::AT_END);
    EXPECT_TRUE(it->accessedUnderReplicatedRegion());
  }

  // Seeking to an LSN that is before the beginning of the first record
  // in a fully-replicated partition will indicate underreplication
  // if there are any under-replicated partitions before the partition
  // of the returned record. These are the negative tests.
  {
    auto it = store_->read(logid, read_options);
    it->seek(lsn_t(9));
    EXPECT_EQ(it->state(), IteratorState::AT_RECORD);
    EXPECT_EQ(it->getLSN(), lsn_t(10));
    EXPECT_FALSE(it->accessedUnderReplicatedRegion());

    it->seekForPrev(lsn_t(41));
    EXPECT_EQ(it->state(), IteratorState::AT_RECORD);
    EXPECT_EQ(it->getLSN(), lsn_t(40));
    EXPECT_FALSE(it->accessedUnderReplicatedRegion());
    it->seekForPrev(lsn_t());
  }
}

TEST_F(PartitionedRocksDBStoreTest, AnticipatoryDirty) {
  logid_t logid(1);
  lsn_t lsn = 1;

  auto cur_cleaner_scans = [this]() {
    return stats_.aggregate().partition_cleaner_scans;
  };
  auto base_cleaner_scans = cur_cleaner_scans();

  auto cur_cleaned_partitions = [this]() {
    return stats_.aggregate().partition_marked_clean;
  };
  auto base_cleaned_partitions = cur_cleaned_partitions();

  // The latest partition is held dirty for appends even if no append
  // records have been written to the partition.
  PartitionedRocksDBStore::PartitionPtr latest_partition =
      store_->getLatestPartition();
  EXPECT_EQ(latest_partition->dirty_state_.dirtied_by_nodes.size(), 1);
  EXPECT_EQ(latest_partition->dirty_state_.dirtied_by_nodes.count(
                PartitionedRocksDBStore::DirtyState::SENTINEL_KEY),
            1);
  // When the last partition advances, the dirty hold is released.
  PartitionedRocksDBStore::PartitionPtr prev_latest_partition =
      latest_partition;
  store_->createPartition();
  latest_partition = store_->getLatestPartition();
  EXPECT_TRUE(prev_latest_partition->dirty_state_.dirtied_by_nodes.empty());
  EXPECT_EQ(latest_partition->dirty_state_.dirtied_by_nodes.size(), 1);
  EXPECT_EQ(latest_partition->dirty_state_.dirtied_by_nodes.count(
                PartitionedRocksDBStore::DirtyState::SENTINEL_KEY),
            1);

  // A cleaner pass should be requested due to the partition dirty hold
  // being released. One background thread run to schedule the clean...
  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::HI_PRI)
      .wait();
  EXPECT_EQ(cur_cleaner_scans(), base_cleaner_scans);
  EXPECT_EQ(cur_cleaned_partitions(), base_cleaned_partitions);

  // And another once the redirty delay expires.
  std::chrono::milliseconds period =
      RocksDBSettings::defaultTestSettings().partition_redirty_grace_period;
  setTime(BASE_TIME + period.count() + 10);
  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::HI_PRI)
      .wait();
  EXPECT_EQ(cur_cleaner_scans(), base_cleaner_scans + 1);
  EXPECT_EQ(cur_cleaned_partitions(), base_cleaned_partitions + 1);

  // Creating a new partition after the latest partition is only dirtied
  // by rebuild traffic will yield a partition held dirty just for
  // appends.
  put({TestRecord(
      logid, lsn++, Durability::MEMORY, TestRecord::StoreType::REBUILD)});
  EXPECT_EQ(latest_partition->dirty_state_.dirtied_by_nodes.size(), 2);
  prev_latest_partition = latest_partition;
  store_->createPartition();
  latest_partition = store_->getLatestPartition();
  EXPECT_EQ(latest_partition->dirty_state_.dirtied_by_nodes.size(), 1);
  EXPECT_EQ(latest_partition->dirty_state_.dirtied_by_nodes.count(
                PartitionedRocksDBStore::DirtyState::SENTINEL_KEY),
            1);
  EXPECT_EQ(prev_latest_partition->dirty_state_.dirtied_by_nodes.size(), 1);
  EXPECT_EQ(prev_latest_partition->dirty_state_.dirtied_by_nodes.count(
                PartitionedRocksDBStore::DirtyState::SENTINEL_KEY),
            0);

  // Creating a new partition after the latest partition is dirtied
  // by append traffic will yield a dirty new partition.
  put({TestRecord(
      logid, lsn++, Durability::MEMORY, TestRecord::StoreType::APPEND)});
  EXPECT_EQ(latest_partition->dirty_state_.dirtied_by_nodes.size(), 2);
  prev_latest_partition = latest_partition;
  latest_partition = store_->getLatestPartition();
  EXPECT_EQ(latest_partition->dirty_state_.dirtied_by_nodes.size(), 2);

  // Creating a new partition after the latest partition is dirtied
  // by both append and rebuild traffic will yield a new partition dirtied
  // only for appends.
  put({TestRecord(
      logid, lsn++, Durability::MEMORY, TestRecord::StoreType::APPEND)});
  put({TestRecord(
      logid, lsn++, Durability::MEMORY, TestRecord::StoreType::REBUILD)});
  EXPECT_EQ(latest_partition->dirty_state_.dirtied_by_nodes.size(), 3);
  prev_latest_partition = latest_partition;
  store_->createPartition();
  latest_partition = store_->getLatestPartition();
  EXPECT_EQ(latest_partition->dirty_state_.dirtied_by_nodes.size(), 2);
  for (auto& dn_kv :
       store_->getLatestPartition()->dirty_state_.dirtied_by_nodes) {
    EXPECT_EQ(dn_kv.first.second, DataClass::APPEND);
  }

  latest_partition.reset();
  prev_latest_partition.reset();

  readAndCheck();
}

TEST_F(PartitionedRocksDBStoreTest, DirectoryCleanupAfterCompaction) {
  ServerConfig::SettingsConfig s;
  s["rocksdb-partition-duration"] = "8h";
  closeStore();
  setTime(BASE_TIME + DAY * 4 + MINUTE);
  openStore(s);
  updateSetting("rocksdb-partition-duration", "0s");
  // Pre-created partitions covering 4 days.
  EXPECT_EQ(13, store_->getPartitionList()->size());

  // Convention: lsn/10 = partition_id - ID0 + 1

  // Log with 1-day retention.
  put({TestRecord(logid_t(10), 20, BASE_TIME + DAY / 2)});            // drop
  put({TestRecord(logid_t(10), 50, BASE_TIME + DAY + DAY / 2)});      // drop
  put({TestRecord(logid_t(10), 80, BASE_TIME + DAY * 2 + DAY / 2)});  // compact
  put({TestRecord(logid_t(10), 110, BASE_TIME + DAY * 3 + DAY / 2)}); // keep
  // Log with 1-day retention.
  put({TestRecord(logid_t(20), 80, BASE_TIME + DAY * 2 + DAY / 2)}); // compact
  // Log with 2-day retention.
  put({TestRecord(logid_t(110), 20, BASE_TIME + DAY / 2)});             // drop
  put({TestRecord(logid_t(110), 50, BASE_TIME + DAY + DAY / 2)});       // drop
  put({TestRecord(logid_t(110), 80, BASE_TIME + DAY * 2 + DAY / 2)});   // keep
  put({TestRecord(logid_t(110), 90, BASE_TIME + DAY * 2 + HOUR * 23)}); // keep
  put({TestRecord(logid_t(110), 92, BASE_TIME + DAY * 2 + HOUR * 23)}); // keep
  put({TestRecord(logid_t(110), 110, BASE_TIME + DAY * 3 + DAY / 2)});  // keep

  lsn_t lsn;
  EXPECT_EQ(0, store_->getHighestInsertedLSN(logid_t(20), &lsn));
  EXPECT_EQ(80, lsn);

  // Run automatic drops and compactions.
  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::LO_PRI)
      .wait();

  auto stats = stats_.aggregate();
  EXPECT_EQ(7, stats.partitions_dropped);
  EXPECT_EQ(1, stats.partitions_compacted);
  EXPECT_EQ(ID0 + 7, store_->getPartitionList()->firstID());

  // Log 20 was emptied by compaction. Check that LogState was updated.
  EXPECT_EQ(0, store_->getHighestInsertedLSN(logid_t(20), &lsn));
  EXPECT_EQ(LSN_INVALID, lsn);

  // Check that records are in the partitions we expect.
  auto data = readAndCheck();
  EXPECT_EQ(6, data.size());
  EXPECT_EQ(1, data[0].size());
  EXPECT_EQ(1, data[1].size());
  EXPECT_EQ(2, data[3].size());
  for (size_t p = 0; p < data.size(); ++p) {
    for (auto lg : data[p]) {
      if (lg.first.val_ == 10) {
        EXPECT_EQ(3, p);
        EXPECT_EQ(std::vector<lsn_t>({110}), lg.second.records);
      } else if (lg.first.val_ == 110) {
        EXPECT_TRUE(p == 0 || p == 1 || p == 3);
        if (p == 1) {
          EXPECT_EQ(std::vector<lsn_t>({90, 92}), lg.second.records);
        } else {
          EXPECT_EQ(std::vector<lsn_t>({(p + 8) * 10}), lg.second.records);
        }
      } else {
        ADD_FAILURE() << p << ' ' << lg.first.val_;
      }
    }
  }
  openStore();

  // Trim and compact manually.
  updateTrimPoint(logid_t(110), 92);
  store_->performCompaction(ID0 + 8);

  // Put a record inside a former directory entry but in a different partition.
  put({TestRecord(logid_t(110), 91, BASE_TIME + DAY * 2 + HOUR)});

  data = readAndCheck();
  EXPECT_EQ(6, data.size());
  EXPECT_EQ(1, data[0].size());
  EXPECT_EQ(2, data[3].size());
  for (size_t p = 0; p < data.size(); ++p) {
    for (auto lg : data[p]) {
      if (lg.first.val_ == 110) {
        EXPECT_TRUE(p == 0 || p == 3);
        if (p == 0) {
          EXPECT_EQ(std::vector<lsn_t>({80, 91}), lg.second.records);
        } else {
          EXPECT_EQ(std::vector<lsn_t>({110}), lg.second.records);
        }
      } else {
        EXPECT_EQ(10, lg.first.val_);
        EXPECT_EQ(std::vector<lsn_t>({110}), lg.second.records);
      }
    }
  }
}

// Simulate server being stopped for a long time and check that on next startup
// we create partitions to cover the downtime. Also check that we don't create
// too many if downtime was too long.
TEST_F(PartitionedRocksDBStoreTest, PrecreatingPartitions) {
  {
    auto p = store_->getPartitionList();
    EXPECT_EQ(1, p->size());
    EXPECT_EQ(
        BASE_TIME, p->get(ID0)->starting_timestamp.toMilliseconds().count());
  }
  closeStore();

  // 10.5 hours passed. Expect 10 partitions to be created.
  setTime(BASE_TIME + HOUR * 10 + HOUR / 2);
  openStore({{"rocksdb-partition-duration", "1h"}});
  {
    auto p = store_->getPartitionList();
    EXPECT_EQ(11, p->size());
    for (int i = 0; i < p->size(); ++i) {
      EXPECT_EQ(BASE_TIME + HOUR * i,
                p->get(ID0 + i)->starting_timestamp.toMilliseconds().count());
    }
  }
  closeStore();

  // 7 more hours passed, and we've hit the limit on number of partitions.
  setTime(BASE_TIME + HOUR * 17 + HOUR / 2);
  openStore({{"rocksdb-partition-duration", "1h"},
             {"rocksdb-partition-count-soft-limit", "14"}});
  {
    auto p = store_->getPartitionList();
    EXPECT_EQ(14, p->size());
    EXPECT_EQ(ID0, p->front()->id_);
    for (int i = 0; i < 3; ++i) {
      EXPECT_EQ(
          BASE_TIME + HOUR * (17 - i) + HOUR / 2,
          p->get(ID0 + 13 - i)->starting_timestamp.toMilliseconds().count());
    }
  }

  // Also test that hi-pri thread is more reluctant at creating new partitions.
  // Add 2h, no partition created.
  setTime(BASE_TIME + HOUR * 19 + HOUR / 2);
  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::HI_PRI)
      .wait();
  EXPECT_EQ(14, store_->getPartitionList()->size());
  // Add another 2h, partition is created.
  setTime(BASE_TIME + HOUR * 21 + HOUR / 2);
  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::HI_PRI)
      .wait();
  EXPECT_EQ(15, store_->getPartitionList()->size());
}

TEST_F(PartitionedRocksDBStoreTest, Prepending) {
  // No partitions are prepended if partition-duration is zero.
  put({TestRecord(logid_t(9), 10, BASE_TIME - HOUR * 10)});
  EXPECT_EQ(1, store_->getPartitionList()->size());

  // ... unless you prepend them manually.
  store_->prependPartitions(2);
  EXPECT_EQ(2, stats_.aggregate().partitions_prepended);
  {
    // Now we have 3 partitions with the same starting_timestamp.
    auto p = store_->getPartitionList();
    EXPECT_EQ(3, p->size());
    for (int i = 0; i < 3; ++i) {
      EXPECT_EQ(BASE_TIME,
                p->get(ID0 - i)->starting_timestamp.toMilliseconds().count());
    }
  }

  closeStore();
  openStore({{"rocksdb-partition-duration", "1h"},
             {"rocksdb-partition-count-soft-limit", "10"}});

  put({TestRecord(logid_t(9), 15, BASE_TIME + MINUTE)});
  EXPECT_EQ(3, store_->getPartitionList()->size());

  // Write a slightly old record, expect one partition to be prepended.
  put({TestRecord(logid_t(9), 5, BASE_TIME - MINUTE)});
  EXPECT_EQ(3, stats_.aggregate().partitions_prepended);
  {
    auto p = store_->getPartitionList();
    EXPECT_EQ(4, p->size());
    EXPECT_EQ(BASE_TIME - HOUR,
              p->get(ID0 - 3)->starting_timestamp.toMilliseconds().count());
  }

  auto check_readable = [&](logid_t log, std::vector<lsn_t> lsns) {
    LocalLogStore::ReadOptions read_options("Test");
    auto it = store_->read(log, read_options);
    it->seek(0);
    for (size_t i = 0; i < lsns.size(); ++i) {
      EXPECT_EQ(IteratorState::AT_RECORD, it->state());
      EXPECT_EQ(lsns[i], it->getLSN());
      it->next();
    }
    EXPECT_EQ(IteratorState::AT_END, it->state());
  };

  // Check that all records are readable.
  {
    SCOPED_TRACE("called from here");
    check_readable(logid_t(9), {5, 10, 15});
  }

  // Write an old record with LSN above an existing record. Current
  // implementation will prepend a partition but won't actually use it.
  put({TestRecord(logid_t(9), 6, BASE_TIME - HOUR - HOUR / 2)});
  EXPECT_EQ(4, stats_.aggregate().partitions_prepended);
  {
    auto p = store_->getPartitionList();
    EXPECT_EQ(5, p->size());
    EXPECT_EQ(BASE_TIME - HOUR * 2,
              p->get(ID0 - 4)->starting_timestamp.toMilliseconds().count());
  }

  {
    SCOPED_TRACE("called from here");
    check_readable(logid_t(9), {5, 6, 10, 15});
  }

  // The oldest partition is empty, so lo-pri background thread will drop it,
  // but only after prepended_partition_min_lifetime_ has elapsed.
  setTime(BASE_TIME +
          RocksDBSettings::defaultTestSettings()
              .prepended_partition_min_lifetime_.count() -
          10);
  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::LO_PRI)
      .wait();
  EXPECT_EQ(0, stats_.aggregate().partitions_dropped);
  EXPECT_EQ(5, store_->getPartitionList()->size());

  setTime(BASE_TIME +
          RocksDBSettings::defaultTestSettings()
              .prepended_partition_min_lifetime_.count() +
          10);
  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::LO_PRI)
      .wait();
  EXPECT_EQ(1, stats_.aggregate().partitions_dropped);
  EXPECT_EQ(4, store_->getPartitionList()->size());

  // Write a very old record, hit the limit on number of partitions.
  put({TestRecord(logid_t(20), 42, BASE_TIME - HOUR * 100 - HOUR / 2)});
  EXPECT_EQ(10, stats_.aggregate().partitions_prepended);
  {
    auto p = store_->getPartitionList();
    EXPECT_EQ(10, p->size());
    for (int i = 0; i < 6; ++i) {
      EXPECT_EQ(
          BASE_TIME - HOUR * (i + 2),
          p->get(ID0 - 4 - i)->starting_timestamp.toMilliseconds().count());
    }
  }

  {
    SCOPED_TRACE("called from here");
    check_readable(logid_t(20), {42});
  }

  auto data = readAndCheck();
  ASSERT_EQ(10, data.size());
  EXPECT_EQ(1, data[0].size());
  EXPECT_EQ(std::vector<lsn_t>({42}), data[0][logid_t(20)].records);
  EXPECT_EQ(1, data[6].size());
  EXPECT_EQ(std::vector<lsn_t>({5, 6}), data[6][logid_t(9)].records);
  EXPECT_EQ(1, data[9].size());
  EXPECT_EQ(std::vector<lsn_t>({10, 15}), data[9][logid_t(9)].records);

  openStore();
  {
    SCOPED_TRACE("called from here");
    check_readable(logid_t(9), {5, 6, 10, 15});
  }
  {
    SCOPED_TRACE("called from here");
    check_readable(logid_t(20), {42});
  }
}

// A few threads do random operations:
//  * write a record to a random partition,
//  * read some records with an iterator,
//  * create partition,
//  * prepend partitions,
//  * drop partitions,
//  * background thread iteration.
TEST_F(PartitionedRocksDBStoreTest, PrependingSmokeTest) {
  single_threaded_ = increasing_lsns_ = false;

  ServerConfig::SettingsConfig sett;
  sett["rocksdb-partition-duration"] = "1h";
  closeStore();

  // For each partition: starting_timestamp = BASE_TIME + (id - ID0) * HOUR
  // For each record: timestamp = BASE_TIME + (lsn - ID0*R) * (HOUR/R)
  // R is the max number of records per log per partition.
  constexpr int64_t R = 6;

  // Writes will be generated so that target partitions are in this range.
  std::atomic<partition_id_t> first_partition{ID0};
  std::atomic<partition_id_t> last_partition{ID0};

  // True when threads need to stop.
  std::atomic<bool> shutdown{false};

  auto rnd = [] { return folly::Random::rand64(); };

  auto writing_func = [&] {
    size_t its = 0;
    while (!shutdown.load(std::memory_order_relaxed)) {
      ++its;
      std::vector<TestRecord> batch(rnd() % 5 + 1);
      partition_id_t p1 = first_partition.load(std::memory_order_relaxed);
      partition_id_t p2 = last_partition.load(std::memory_order_relaxed);
      p2 = std::max(p2, p1);
      for (TestRecord& r : batch) {
        logid_t log(rnd() % 10 + 1);
        partition_id_t id = p1 + rnd() % (p2 - p1 + 1);
        lsn_t lsn = id * R + rnd() % R;
        uint64_t ts = BASE_TIME + (lsn - ID0 * R) * (HOUR / R);
        r = TestRecord(log, lsn, ts);
      }
      put(std::move(batch));
    }
    ld_info("writing thread did %lu iterations", its);
  };

  auto reading_func = [&] {
    size_t its = 0;
    LocalLogStore::ReadOptions read_options("Test");
    while (!shutdown.load(std::memory_order_relaxed)) {
      ++its;
      partition_id_t p1 = first_partition.load(std::memory_order_relaxed);
      partition_id_t p2 = last_partition.load(std::memory_order_relaxed);
      p2 = std::max(p2, p1);

      logid_t log(rnd() % 10 + 1);
      partition_id_t id1 = p1 + rnd() % (p2 - p1 + 1);
      partition_id_t id2 = p1 + rnd() % (p2 - p1 + 1);
      lsn_t lsn1 = id1 * R + rnd() % R;
      lsn_t lsn2 = id2 * R + rnd() % R;
      if (lsn2 < lsn1) {
        std::swap(lsn1, lsn2);
      }

      auto it = store_->read(log, read_options);
      it->seek(lsn1);
      while (it->state() == IteratorState::AT_RECORD && it->getLSN() <= lsn2) {
        it->next();
      }
      EXPECT_TRUE(it->state() == IteratorState::AT_RECORD ||
                  it->state() == IteratorState::AT_END);
    }
    ld_info("reading thread did %lu iterations", its);
  };

  // Create/drop/prepend partitions, and update first_partition and
  // last_partition to approximately reflect the range of partitions store_ has.
  // It's especially approximate when partitions_func runs on multiple threads.
  auto partitions_func = [&] {
    size_t its = 0;
    while (!shutdown.load(std::memory_order_relaxed)) {
      ++its;
      partition_id_t p1 = first_partition.load(std::memory_order_relaxed);
      partition_id_t p2 = last_partition.load(std::memory_order_relaxed);
      if (rnd() % 3 == 0) {
        last_partition.fetch_add(1, std::memory_order_relaxed);
        setTime(BASE_TIME + (p2 + 1 - ID0) * HOUR);
        store_->createPartition();
      } else if (rnd() % 2 == 0) {
        size_t target_cnt;
        if (rnd() % 10 == 0) {
          target_cnt = rnd() % 3 + 1;
        } else {
          target_cnt = rnd() % 100 + 1;
        }
        partition_id_t new_p1 = p2 - target_cnt + 1;

        // We'll update first_partition and drop/prepend partitions.
        // Do these 2 operations in random order, defined by this boolean.
        bool order = rnd() % 2 == 0;

        if (order) {
          // The way this function treats first_partition and last_partition
          // is full of "race conditions". But they don't matter since these
          // first_partition and last_partition don't have to stay accurate,
          // and any possible call to
          // createPartition()/dropPartitionsUpTo()/prependPartitions()
          // is legal.
          first_partition.store(new_p1, std::memory_order_relaxed);
        }
        if (new_p1 >= p1) {
          // Need to drop partitions.
          store_->dropPartitionsUpTo(new_p1);
        } else {
          // Need to prepend partitions. Sometimes do it explicitly, sometimes
          // let store_ automatically create these partitions when writes arrive
          if (rnd() % 5 == 0) {
            store_->prependPartitions(p1 - new_p1);
          }
        }
        if (!order) {
          first_partition.store(new_p1, std::memory_order_relaxed);
        }
      } else {
        bool hi = rnd() % 2 == 0;
        ld_info("Doing a %s-pri thread iteration", hi ? "hi" : "lo");
        store_
            ->backgroundThreadIteration(
                hi ? PartitionedRocksDBStore::BackgroundThreadType::HI_PRI
                   : PartitionedRocksDBStore::BackgroundThreadType::LO_PRI)
            .wait();
        filter_factory_->flushTrimPoints();
      }
      if (rnd() % 2 == 0) {
        /* sleep override */
        std::this_thread::sleep_for(std::chrono::milliseconds(rnd() % 50 + 50));
      } else {
        /* sleep override */
        std::this_thread::sleep_for(std::chrono::milliseconds(rnd() % 10 + 1));
      }
    }
    ld_info("partitions thread did %lu iterations", its);
  };

  for (int era = 0; era < 3; ++era) {
    openStore(sett);
    shutdown.store(false);
    // Spawn some threads and let them run for a few seconds.
    std::vector<std::thread> ts;
    for (size_t i = 0; i < 3; ++i) {
      ts.emplace_back(writing_func);
    }
    for (size_t i = 0; i < 3; ++i) {
      ts.emplace_back(reading_func);
    }
    for (size_t i = 0; i < 2; ++i) {
      ts.emplace_back(partitions_func);
    }
    /* sleep override */
    std::this_thread::sleep_for(std::chrono::seconds(5));
    shutdown.store(true);
    for (std::thread& t : ts) {
      t.join();
    }
    readAndCheck();
  }
}

TEST_F(PartitionedRocksDBStoreTest, NewPartitionTimestampMargin) {
  closeStore();
  openStore({{"rocksdb-new-partition-timestamp-margin", "10s"}});
  store_->createPartition();
  EXPECT_EQ(2, store_->getPartitionList()->size());
  EXPECT_EQ(BASE_TIME,
            store_->getPartitionList()
                ->front()
                ->starting_timestamp.toMilliseconds()
                .count());
  EXPECT_EQ(BASE_TIME + 10000,
            store_->getPartitionList()
                ->back()
                ->starting_timestamp.toMilliseconds()
                .count());
}

TEST_F(PartitionedRocksDBStoreTest, WrittenByRecovery) {
  closeStore();
  openStore({{"rocksdb-partition-duration", "1h"}});

  // Record with FLAG_WRITTEN_BY_RECOVERY doesn't prepend partitions.
  put({TestRecord(logid_t(1),
                  90,
                  Durability::ASYNC_WRITE,
                  TestRecord::StoreType::RECOVERY,
                  BASE_TIME - HOUR * 5 - HOUR / 2)});
  EXPECT_EQ(0, stats_.aggregate().partitions_prepended);

  // Record without the flag does prepend partitions.
  put({TestRecord(logid_t(1),
                  50,
                  Durability::ASYNC_WRITE,
                  TestRecord::StoreType::APPEND,
                  BASE_TIME - HOUR * 5 - HOUR / 2)});
  EXPECT_EQ(6, stats_.aggregate().partitions_prepended);

  put({TestRecord(logid_t(1),
                  10,
                  Durability::ASYNC_WRITE,
                  TestRecord::StoreType::REBUILD,
                  BASE_TIME - HOUR * 6 - HOUR / 2)});
  EXPECT_EQ(7, stats_.aggregate().partitions_prepended);

  auto data = readAndCheck();
  ASSERT_EQ(8, data.size());
  EXPECT_EQ(std::vector<lsn_t>({10}), data[0][logid_t(1)].records);
  EXPECT_EQ(std::vector<lsn_t>({50}), data[1][logid_t(1)].records);
  EXPECT_EQ(std::vector<lsn_t>({90}), data[7][logid_t(1)].records);
}

#ifdef LOGDEVICED_ROCKSDB_BLOOM_UNBROKEN
TEST_F(PartitionedRocksDBStoreTest, Bloom) {
  // Note: this test is somewhat sensitive to the details of rocksdb bloom
  // filter checks: it expects that BLOOM_FILTER_PREFIX_CHECKED will be
  // incremented exactly once for every sst file on every seek. This seems like
  // a reasonable assumption; if it starts failing, it's worth investigating
  // why rocksdb behavior changed.

  // Disable bloom filters.
  closeStore();
  ServerConfig::SettingsConfig s;
  s["rocksdb-bloom-bits-per-key"] = "0";
  openStore(s);

  put({TestRecord(logid_t(10), 100)});
  store_->flushAllMemtables(); // now we have 1 sst file

  // Seek iterator. Bloom filter not used.
  LocalLogStore::ReadOptions read_options("PartitionedRocksDBStoreTest.Bloom");
  read_options.allow_copyset_index = true;
  auto it = store_->read(logid_t(10), read_options);
  it->seek(50);
  EXPECT_EQ(IteratorState::AT_RECORD, it->state());
  EXPECT_EQ(100, it->getLSN());
  EXPECT_EQ(0,
            store_->getStatsTickerCount(
                rocksdb::Tickers::BLOOM_FILTER_PREFIX_CHECKED));
  EXPECT_EQ(0,
            store_->getStatsTickerCount(
                rocksdb::Tickers::BLOOM_FILTER_PREFIX_USEFUL));
  it.reset();

  // Enable bloom filters.
  closeStore();
  s["rocksdb-bloom-bits-per-key"] = "100";
  openStore(s);

  // Seek iterator. Bloom filter still not used because it's an old sst file.
  // (But BLOOM_FILTER_PREFIX_CHECKED gets incremented anyway.)
  it = store_->read(logid_t(10), read_options);
  it->seek(50);
  EXPECT_EQ(IteratorState::AT_RECORD, it->state());
  EXPECT_EQ(100, it->getLSN());
  // One sst file - two bloom check: one for CSI, one for data.
  // The ticker is incremented even if the file doesn't have bloom filter.
  EXPECT_EQ(2,
            store_->getStatsTickerCount(
                rocksdb::Tickers::BLOOM_FILTER_PREFIX_CHECKED));
  EXPECT_EQ(0,
            store_->getStatsTickerCount(
                rocksdb::Tickers::BLOOM_FILTER_PREFIX_USEFUL));

  put({TestRecord(logid_t(20), 200)});
  store_->flushAllMemtables(); // now we have 2 sst files

  // Bloom filter is checked for second sst but not useful because this log
  // exists there.
  it = store_->read(logid_t(20), read_options);
  it->seek(100);
  EXPECT_EQ(IteratorState::AT_RECORD, it->state());
  EXPECT_EQ(200, it->getLSN());
  // Two sst files - 4 more checks (even though one of them doesn't have bloom
  // filter).
  EXPECT_EQ(6,
            store_->getStatsTickerCount(
                rocksdb::Tickers::BLOOM_FILTER_PREFIX_CHECKED));
  EXPECT_EQ(0,
            store_->getStatsTickerCount(
                rocksdb::Tickers::BLOOM_FILTER_PREFIX_USEFUL));

  // Bloom filter is checked. For second sst it's useful.
  it = store_->read(logid_t(10), read_options);
  it->seek(50);
  EXPECT_EQ(IteratorState::AT_RECORD, it->state());
  EXPECT_EQ(100, it->getLSN());
  // 4 more checks, one useful.
  EXPECT_EQ(10,
            store_->getStatsTickerCount(
                rocksdb::Tickers::BLOOM_FILTER_PREFIX_CHECKED));
  EXPECT_EQ(2,
            store_->getStatsTickerCount(
                rocksdb::Tickers::BLOOM_FILTER_PREFIX_USEFUL));

  // Bloom filter not checked because log doesn't exist in partition.
  it = store_->read(logid_t(30), read_options);
  it->seek(150);
  EXPECT_EQ(IteratorState::AT_END, it->state());
  EXPECT_EQ(10,
            store_->getStatsTickerCount(
                rocksdb::Tickers::BLOOM_FILTER_PREFIX_CHECKED));
  EXPECT_EQ(2,
            store_->getStatsTickerCount(
                rocksdb::Tickers::BLOOM_FILTER_PREFIX_USEFUL));
}
#endif // LOGDEVICED_ROCKSDB_BLOOM_UNBROKEN

TEST_F(PartitionedRocksDBStoreTest, PartialCompactionStallTrigger) {
  closeStore();
  ServerConfig::SettingsConfig s;
  // Any partition with 3 or more files will be eligible for partial
  // compactions. If there are two or more such partitions, low-pri writers will
  // be stalled.
  s["rocksdb-partition-partial-compaction-file-num-threshold-old"] = "3";
  s["rocksdb-partition-partial-compaction-file-num-threshold-recent"] = "3";
  s["rocksdb-partition-partial-compaction-file-size-threshold"] = "10000000000";
  s["rocksdb-partition-partial-compaction-largest-file-share"] = "1.0";
  s["rocksdb-partition-partial-compaction-stall-trigger"] = "2";
  openStore(s);

  // Create 3 l0 files in the first 2 partitions.
  for (int p = 0; p < 2; ++p) {
    for (int r = 0; r < 3; ++r) {
      put({TestRecord(logid_t(1), p * 3 + r + 1)});
      store_->flushAllMemtables();
    }
    store_->createPartition();
  }
  // The last two pactitions are exempted from partial compactions, so we need
  // two empty partitions at the end here.
  store_->createPartition();

  // No stalling until lo-pri thread runs.
  EXPECT_EQ(WriteThrottleState::NONE, store_->getWriteThrottleState());

  // Stall compactions and start a background thread iteration. Don't wait
  // for the iteration to finish because it'll stall.
  auto compactions_staller = store_->stallCompaction();
  auto bg_iteration = store_->backgroundThreadIteration(
      PartitionedRocksDBStore::BackgroundThreadType::LO_PRI);

  // Wait for the background thread to finish planning compactions and
  // stall writers. If the test times out here, stalling is probably broken.
  ld_info("Waiting for writes to get stalled.");
  while (store_->getWriteThrottleState() == WriteThrottleState::NONE) {
    store_
        ->backgroundThreadIteration(
            PartitionedRocksDBStore::BackgroundThreadType::FLUSH)
        .wait();
    /* sleep override */
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }

  // Make a thread and get it actually stalled waiting for compactions.
  std::thread writer_thread([&] { store_->stallLowPriWrite(); });
  // Give it a little time to reach the stall.
  /* sleep override */
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // Finish the compactions.
  compactions_staller.set_value();
  ld_info("Waiting for compactions to finish.");
  bg_iteration.wait();

  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::FLUSH)
      .wait();

  // Check that the stalled writer was woken up. If the test times out here,
  // unstalling is probably broken.
  ld_info("Waiting for writes to get unstalled.");
  writer_thread.join();

  EXPECT_EQ(WriteThrottleState::NONE, store_->getWriteThrottleState());
}

TEST_F(PartitionedRocksDBStoreTest, MetadataCompactions) {
  std::chrono::milliseconds period =
      RocksDBSettings::defaultTestSettings().metadata_compaction_period;
  ASSERT_GT(period.count(), 0);

  // Flush 2 memtables in metadata CF.
  store_->flushAllMemtables();
  EXPECT_EQ(1, store_->getNumL0Files(store_->getMetadataCFHandle()));
  store_->createPartition();
  store_->flushAllMemtables();
  EXPECT_EQ(2, store_->getNumL0Files(store_->getMetadataCFHandle()));

  // Background thread should compact.
  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::LO_PRI)
      .wait();
  EXPECT_EQ(1, store_->getNumL0Files(store_->getMetadataCFHandle()));

  // Flush one more.
  store_->createPartition();
  store_->flushAllMemtables();
  EXPECT_EQ(2, store_->getNumL0Files(store_->getMetadataCFHandle()));

  // Background thread shouldn't compact because not enough time has elapsed.
  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::LO_PRI)
      .wait();
  EXPECT_EQ(2, store_->getNumL0Files(store_->getMetadataCFHandle()));

  // Still not enough time.
  setTime(BASE_TIME + period.count() - 10);
  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::LO_PRI)
      .wait();
  EXPECT_EQ(2, store_->getNumL0Files(store_->getMetadataCFHandle()));

  // Now it should compact.
  setTime(BASE_TIME + period.count() + 10);
  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::LO_PRI)
      .wait();
  EXPECT_EQ(1, store_->getNumL0Files(store_->getMetadataCFHandle()));
}

// TODO(T44746268): replace NDEBUG with folly::kIsDebug
#ifdef NDEBUG
#define IF_DEBUG(...)
#else
#define IF_DEBUG(...) __VA_ARGS__
#endif

TEST_F(PartitionedRocksDBStoreTest, FilteredIterators) {
  updateSetting("rocksdb-partition-timestamp-granularity", "0ms");

  // 5 partitions, each with 2 records. For each record:
  // lsn = timestamp - BASE_TIME = copyset[0].node().
  // First partition has records 10 and 90, second partition 110 and 190,
  // third 210 and 290 and so on.
  for (int i = 0; i < 5; ++i) {
    if (i) {
      setTime(BASE_TIME + i * 100);
      store_->createPartition();
    }
    for (int j : {i * 100 + 10, i * 100 + 90}) {
      std::vector<StoreChainLink> cs = {
          StoreChainLink{ShardID(j, 0), ClientID::INVALID}};
      put({TestRecord(logid_t(42), j, BASE_TIME + j)}, cs);
    }
  }

  TestReadFilter filter;
  LocalLogStore::ReadStats stats;
  LocalLogStore::ReadOptions read_options("FilteredIterators");
  read_options.allow_copyset_index = true;
  auto it = store_->read(logid_t(42), read_options);

  using C = TestReadFilter::Call;

  // All partitions are filtered out.
  it->seek(0, &filter, &stats);
  EXPECT_EQ(IteratorState::AT_END, it->state());
  EXPECT_EQ(std::vector<C>({C::timeRange(10, 90),
                            C::timeRange(110, 190),
                            C::timeRange(210, 290),
                            C::timeRange(310, 390),
                            C::timeRange(410, 490)}),
            filter.history);
  filter.history.clear();

  // Read from partitions 1 and 4 (second and last).
  // 2 records pass the filter.
  filter.time_ranges = {110, 410};
  filter.records = {190, 410};
  // Seek.
  it->seek(0, &filter, &stats);
  EXPECT_EQ(IteratorState::AT_RECORD, it->state());
  EXPECT_EQ(190, it->getLSN());
  EXPECT_GT(it->getRecord().size, 0);
  // For a record that passes the filter, filter() is called 3 times:
  // for CSI entry (with partition's timestamp range), for data record
  // (with exact timestamp), for CSI entry again for an assert in CSIWrapper.
  EXPECT_EQ(std::vector<C>({C::timeRange(10, 90),
                            C::timeRange(110, 190),
                            C::call(110),
                            C::call(190),
                            C::call(190, true),
                            IF_DEBUG(C::call(190))}),
            filter.history);
  filter.history.clear();
  // Next.
  it->next(&filter, &stats);
  EXPECT_EQ(IteratorState::AT_RECORD, it->state());
  EXPECT_EQ(std::vector<C>({C::timeRange(210, 290),
                            C::timeRange(310, 390),
                            C::timeRange(410, 490),
                            C::call(410),
                            C::call(410, true),
                            IF_DEBUG(C::call(410))}),
            filter.history);
  filter.history.clear();
  EXPECT_EQ(410, it->getLSN());
  EXPECT_GT(it->getRecord().size, 0);
  // Next.
  it->next(&filter, &stats);
  EXPECT_EQ(IteratorState::AT_END, it->state());
  EXPECT_EQ(std::vector<C>({C::call(490)}), filter.history);
  filter.history.clear();

  // Read from partitions 2 and 4.
  // All records are filtered out.
  filter.time_ranges = {210, 410};
  filter.records = {};
  // Seek to LSN 150.
  it->seek(150, &filter, &stats);
  EXPECT_EQ(IteratorState::AT_END, it->state());
  EXPECT_EQ(std::vector<C>({C::timeRange(110, 190),
                            C::timeRange(210, 290),
                            C::call(210),
                            C::call(290),
                            C::timeRange(310, 390),
                            C::timeRange(410, 490),
                            C::call(410),
                            C::call(490)}),
            filter.history);
  filter.history.clear();

  // Combining filtering and byte limit.
  filter.time_ranges = {10, 210, 410};
  filter.records = {10, 90, 490};
  // Seek to LSN 50.
  it->seek(50, &filter, &stats);
  EXPECT_EQ(IteratorState::AT_RECORD, it->state());
  EXPECT_EQ(90, it->getLSN());
  EXPECT_GT(it->getRecord().size, 0);
  EXPECT_EQ(std::vector<C>({C::timeRange(10, 90),
                            C::call(90),
                            C::call(90, true),
                            IF_DEBUG(C::call(90))}),
            filter.history);
  filter.history.clear();
  // Read limit = now + 1 byte.
  stats.max_bytes_to_read = stats.read_record_bytes + stats.read_csi_bytes + 1;
  // Next.
  it->next(&filter, &stats);
  EXPECT_EQ(IteratorState::LIMIT_REACHED, it->state());
  EXPECT_EQ(211, it->getLSN());
  EXPECT_EQ(std::vector<C>(
                {C::timeRange(110, 190), C::timeRange(210, 290), C::call(210)}),
            filter.history);
  filter.history.clear();
  // Read limit = infinity.
  stats.max_bytes_to_read = std::numeric_limits<size_t>::max();
  // Next.
  it->seek(211, &filter, &stats);
  EXPECT_EQ(IteratorState::AT_RECORD, it->state());
  EXPECT_EQ(490, it->getLSN());
  EXPECT_GT(it->getRecord().size, 0);
  EXPECT_EQ(std::vector<C>({C::timeRange(210, 290),
                            C::call(290),
                            C::timeRange(310, 390),
                            C::timeRange(410, 490),
                            C::call(410),
                            C::call(490),
                            C::call(490, true),
                            IF_DEBUG(C::call(490))}),
            filter.history);
  filter.history.clear();
  // Next.
  it->next(&filter, &stats);
  EXPECT_EQ(IteratorState::AT_END, it->state());
  EXPECT_EQ(std::vector<C>({}), filter.history);
}

// Verify that interleaving writes at different durability levels doesn't
// cause directory inconsistency if memtables are lost.
// In particular, if a directory write with ASYNC_WRITE durability depends
// on a previous directory entry that wasn't written to WAL, the previous entry
// should be re-written with WAL enabled. Also, deletions should always use WAL.
TEST_F(PartitionedRocksDBStoreTest, DirectoryDurabilityPromotion) {
  // Prevent flushes on shutdown to simulate a crash.
  closeStore();
  openStore({}, [](RocksDBLogStoreConfig& cfg) {
    cfg.options_.avoid_flush_during_shutdown = true;
  });

  logid_t log(42);

  // Durable directory write, followed by "MEMORY" deletion and write,
  // followed by promotion to ASYNC_WRITE. Since the deletion can't be promoted,
  // it should be done with WAL in the first place.
  put({TestRecord(
      log, 2, Durability::ASYNC_WRITE, TestRecord::StoreType::APPEND)});
  put({TestRecord(log, 1, Durability::MEMORY, TestRecord::StoreType::APPEND)});
  put({TestRecord(
      log, 1, Durability::ASYNC_WRITE, TestRecord::StoreType::APPEND)});

  store_->createPartition();

  // Non-durable, promoted to durable.
  put({TestRecord(log, 3, Durability::MEMORY, TestRecord::StoreType::APPEND)});
  put({TestRecord(
      log, 3, Durability::ASYNC_WRITE, TestRecord::StoreType::APPEND)});

  // Non-durable, should be lost.
  put({TestRecord(log, 4, Durability::MEMORY, TestRecord::StoreType::APPEND)});

  auto data = readAndCheck();
  ASSERT_EQ(2, data.size());
  EXPECT_EQ(std::vector<lsn_t>({1, 2}), data[0][log].records);
  EXPECT_EQ(std::vector<lsn_t>({3}), data[1][log].records);
}

TEST_F(PartitionedRocksDBStoreTest, TimestampDurability) {
  closeStore();
  openStore({{"rocksdb-partition-duration", "15min"},
             {"rocksdb-partition-timestamp-granularity", "5s"}});
  EXPECT_EQ(0, stats_.aggregate().partition_sync_write_promotion_for_timstamp);

  // Records with timestamps that fit within the latest partition
  // should not block waiting for timestamp updates.
  logid_t logid(1);
  put({TestRecord(logid,
                  10,
                  Durability::MEMORY,
                  TestRecord::StoreType::APPEND,
                  BASE_TIME)});
  EXPECT_EQ(0, stats_.aggregate().partition_sync_write_promotion_for_timstamp);

  put({TestRecord(logid,
                  11,
                  Durability::MEMORY,
                  TestRecord::StoreType::APPEND,
                  BASE_TIME - SECOND)});
  EXPECT_EQ(0, stats_.aggregate().partition_sync_write_promotion_for_timstamp);

  // For the latest partition, any timestamp within partition-duration of
  // the start timestamp is accepted, plus the timestamp-granularity slop.
  put({TestRecord(logid,
                  12,
                  Durability::MEMORY,
                  TestRecord::StoreType::APPEND,
                  BASE_TIME + MINUTE * 15 + SECOND)});
  EXPECT_EQ(0, stats_.aggregate().partition_sync_write_promotion_for_timstamp);

  // But going outside of the auto-expanded dirty range will require
  // blocking.
  //
  // NOTE: Previous writes have caused timestamp over-estimation by
  //       partition-timestamp-granularity. We have to be at least
  //       partition-timestamp-granularity away from the over-estimated
  //       values.
  put({TestRecord(logid_t(1),
                  13,
                  Durability::MEMORY,
                  TestRecord::StoreType::APPEND,
                  BASE_TIME - SECOND * 12)});
  EXPECT_EQ(1, stats_.aggregate().partition_sync_write_promotion_for_timstamp);

  put({TestRecord(logid,
                  14,
                  Durability::MEMORY,
                  TestRecord::StoreType::APPEND,
                  BASE_TIME + MINUTE * 15 + SECOND * 12)});
  EXPECT_EQ(2, stats_.aggregate().partition_sync_write_promotion_for_timstamp);
}

// Write two records to each of two partitions. Modify directory entry in
// for the first partition to simulate a missing maxLSN update for the
// second record in the first partition. Write this second record again
// to simulate that record being rebuilt. The rebuilt record should be
// seen in the second partition, and the original record should be skipped
// due to having an lsn > maxLSN for the first partition.
TEST_F(PartitionedRocksDBStoreTest, RecordWithoutMaxLSNUpdate) {
  const logid_t logid(1);
  time_ = SystemTimestamp(std::chrono::milliseconds(FUTURE + /*lsn*/ 5 * 2));
  put({TestRecord(logid, 9)});
  put({TestRecord(logid, 15)});

  // Set ID1's starting timestamp to be less than the timestamp for for lsn 15
  // by simulating LSN 14 on another log being being written into the new
  // partition (see timmestamps_are_lsn_ above).
  time_ = SystemTimestamp(std::chrono::milliseconds(FUTURE + /*lsn*/ 14 * 2));
  store_->createPartition();
  const logid_t logid_2(2);
  put({TestRecord(logid_2, 14)});

  // And some more records for logid.
  put({TestRecord(logid, 20), TestRecord(logid, 30)});

  {
    auto data = readAndCheck();
    EXPECT_EQ(2, data.size());
    EXPECT_EQ(std::vector<lsn_t>({9, 15}), data[0][logid].records);
    EXPECT_EQ(std::vector<lsn_t>({20, 30}), data[1][logid].records);
    EXPECT_EQ(std::vector<lsn_t>({14}), data[1][logid_2].records);
  }

  // Simulate the loss of lsn 15's directory update by rewinding it
  // to what should have been stored by lsn 9.
  auto rewound_key = PartitionDirectoryKey(logid, lsn_t(9), ID0);
  auto rewound_value = PartitionDirectoryValue(
      lsn_t(9), /*flags*/ 0, /*approximate_size_bytes*/ 0);
  putDirectoryEntry(rewound_key, rewound_value);

  // Now that we've poked a hole in the lsn sequence, we can no longer assume
  // during validations that lsns are written nicely in order. For example,
  // the starting timestamp of partition 1 will not match the timestamp
  // of the last seen records in partition 0.
  increasing_lsns_ = false;

  OrphanedData orphans{
      {partition_id_t(0), PartitionOrphans{{logid, std::vector<lsn_t>{15}}}}};

  // Verify that we have orphaned the correct record.
  {
    auto data = readAndCheck(&orphans);
    EXPECT_EQ(2, data.size());
    EXPECT_EQ(std::vector<lsn_t>({9}), data[0][logid].records);
    EXPECT_EQ(std::vector<lsn_t>({20, 30}), data[1][logid].records);
    EXPECT_EQ(std::vector<lsn_t>({14}), data[1][logid_2].records);
  }

  // Read via an iterator on the store and verify that the orphaned
  // record is not returned.
  {
    openStore();
    auto read_opts = LocalLogStore::ReadOptions("orphaned_in_partition0");
    read_opts.tailing = true;
    auto it = store_->read(logid, read_opts);
    it->seek(LSN_OLDEST);
    std::vector<lsn_t> found_lsns;
    while (it->state() == IteratorState::AT_RECORD) {
      found_lsns.push_back(it->getLSN());
      it->next();
    }
    EXPECT_EQ(found_lsns, std::vector<lsn_t>({9, 20, 30}));

    // Same results if iterating in reverse.
    read_opts.tailing = false;
    it = store_->read(logid, read_opts);
    it->seekForPrev(LSN_MAX);
    found_lsns.clear();
    while (it->state() == IteratorState::AT_RECORD) {
      found_lsns.insert(found_lsns.begin(), it->getLSN());
      it->prev();
    }
    EXPECT_EQ(found_lsns, std::vector<lsn_t>({9, 20, 30}));

    // Seeking to the record give the right results
    it->seek(lsn_t(15));
    EXPECT_EQ(it->getLSN(), lsn_t(20));
    it->seekForPrev(lsn_t(15));
    EXPECT_EQ(it->getLSN(), lsn_t(9));
  }

  // Add back the orphaned record. It should go into partition 1, not 0.
  put({TestRecord(logid, 15)});
  {
    auto data = readAndCheck(&orphans);
    EXPECT_EQ(2, data.size());
    EXPECT_EQ(std::vector<lsn_t>({9}), data[0][logid].records);
    EXPECT_EQ(std::vector<lsn_t>({15, 20, 30}), data[1][logid].records);
    EXPECT_EQ(std::vector<lsn_t>({14}), data[1][logid_2].records);
  }

  // Verify that the store agrees with the manually read data.
  openStore();
  {
    auto read_opts =
        LocalLogStore::ReadOptions("orphan_returned_to_partition1");
    read_opts.tailing = true;
    auto it = store_->read(logid, read_opts);
    it->seek(LSN_OLDEST);
    std::vector<lsn_t> found_lsns;
    while (it->state() == IteratorState::AT_RECORD) {
      found_lsns.push_back(it->getLSN());
      it->next();
    }
    EXPECT_EQ(found_lsns, std::vector<lsn_t>({9, 15, 20, 30}));

    // Same results if iterating in reverse.
    read_opts.tailing = false;
    it = store_->read(logid, read_opts);
    it->seekForPrev(LSN_MAX);
    found_lsns.clear();
    while (it->state() == IteratorState::AT_RECORD) {
      found_lsns.insert(found_lsns.begin(), it->getLSN());
      it->prev();
    }
    EXPECT_EQ(found_lsns, std::vector<lsn_t>({9, 15, 20, 30}));

    // Seeking to the record give the right results
    it->seek(lsn_t(15));
    EXPECT_EQ(it->getLSN(), lsn_t(15));
    it->seekForPrev(lsn_t(15));
    EXPECT_EQ(it->getLSN(), lsn_t(15));
  }

  // Verify that adding a record past maxLSN in partition 0 is picked
  // up by an active iterator after it is seeked. This simulates rebuilding
  // re-replicating a record.
  {
    auto read_opts = LocalLogStore::ReadOptions("active_iterator");
    read_opts.tailing = true;
    auto it = store_->read(logid, read_opts);
    it->seek(LSN_OLDEST);

    // The iterator should miss this record added after the seek.
    put({TestRecord(logid, 10)});
    std::vector<lsn_t> found_lsns;
    while (it->state() == IteratorState::AT_RECORD) {
      found_lsns.push_back(it->getLSN());
      it->next();
    }
    EXPECT_EQ(found_lsns, std::vector<lsn_t>({9, 15, 20, 30}));

    // But should pick it up if we seek again.
    it->seek(LSN_OLDEST);
    found_lsns.clear();
    while (it->state() == IteratorState::AT_RECORD) {
      found_lsns.push_back(it->getLSN());
      it->next();
    }
    EXPECT_EQ(found_lsns, std::vector<lsn_t>({9, 10, 15, 20, 30}));
  }
}

TEST_F(PartitionedRocksDBStoreTest, AllLogsIter) {
  updateSetting("rocksdb-partition-timestamp-granularity", "0ms");
  std::set<std::tuple<partition_id_t, logid_t, lsn_t>> records;

  // `id` is placed in copyset as node index, and also encoded in timestamp.
  // We'll use it in ReadFilter to filter out specific records.
  auto write = [&](logid_t log, lsn_t lsn, int id) {
    std::vector<StoreChainLink> copyset(4);
    for (int i = 0; i < 4; ++i) {
      copyset[i] = StoreChainLink{ShardID(id + i, 1), ClientID()};
    }

    partition_id_t partition = (MetaDataLog::isMetaDataLog(log) ||
                                configuration::InternalLogs::isInternal(log))
        ? PARTITION_INVALID
        : store_->getPartitionList()->back()->id_;
    auto key = std::make_tuple(partition, log, lsn);

    auto rv = records.insert(key);
    ld_assert(rv.second);

    put({TestRecord(log, lsn, BASE_TIME + id * 10)}, copyset);
  };

  ld_info("Writing some records to data logs.");
  logid_t log1(10), log2(20), log3(30);
  // Partition ID0 + 0, time range [10, 30]
  write(log1, 100, 1);
  write(log1, 200, 2);
  write(log2, 100, 3);
  // Partition ID0 + 1, time range [+inf, -inf]
  setTime(BASE_TIME + 100);
  store_->createPartition();
  // Partition ID0 + 2, time range [210, 230]
  setTime(BASE_TIME + 200);
  store_->createPartition();
  write(log1, 300, 21);
  write(log2, 200, 22);
  write(log2, 300, 23);
  // Partition ID0 + 3, time range [310, 310]
  setTime(BASE_TIME + 300);
  store_->createPartition();
  write(log3, 100, 31);

  ld_info("Writing some records to metadata and internal logs.");
  logid_t meta_log1 = MetaDataLog::metaDataLogID(logid_t(24));
  logid_t meta_log2 = MetaDataLog::metaDataLogID(logid_t(42));
  logid_t internal_log = configuration::InternalLogs::EVENT_LOG_DELTAS;
  write(internal_log, 100, 1001);
  write(internal_log, 200, 1002);
  write(meta_log1, 100, 2001);
  write(meta_log1, 200, 2002);
  write(meta_log2, 100, 3001);

  ld_info("Reading everything sequentially.");

  LocalLogStore::ReadOptions read_options(
      "PartitionedRocksDBStoreTest.AllLogsIter");
  read_options.allow_copyset_index = true;
  auto it = store_->readAllLogs(read_options, folly::none);

  auto set_it = records.begin();
  std::unique_ptr<LocalLogStore::AllLogsIterator::Location> mid_location,
      last_location, location_21;
  for (it->seek(*it->minLocation()); it->state() == IteratorState::AT_RECORD;
       it->next()) {
    if (set_it == records.end()) {
      ADD_FAILURE();
    } else {
      EXPECT_EQ(std::get<1>(*set_it), it->getLogID());
      EXPECT_EQ(std::get<2>(*set_it), it->getLSN());
      ++set_it;
    }

    verifyRecord(it->getLogID(), it->getLSN(), it->getRecord());

    // Save some Location's along the way.
    if (it->getLogID() == log1 && it->getLSN() == 300) {
      location_21 = it->getLocation();
    }
    if (it->getLogID() == log2 && it->getLSN() == 300) {
      mid_location = it->getLocation();
    }
    if (it->getLogID() == log3 && it->getLSN() == 100) {
      last_location = it->getLocation();
    }
  }
  EXPECT_EQ(IteratorState::AT_END, it->state());
  EXPECT_TRUE(set_it == records.end());
  ASSERT_FALSE(mid_location == nullptr);
  ASSERT_FALSE(last_location == nullptr);

  ld_info("Writing a record to a new partition and checking that iterator "
          "doesn't see it.");
  setTime(BASE_TIME + 400);
  store_->createPartition();
  write(log1, 400, 41);
  it->invalidate();
  it->seek(*last_location);
  ASSERT_EQ(IteratorState::AT_RECORD, it->state());
  it->next();
  EXPECT_EQ(IteratorState::AT_END, it->state());

  ld_info("Seeking a new iterator to a location returned by old iterator. "
          "Checking that the seek goes to the right record and that iterator "
          "sees new record.");
  auto it2 = store_->readAllLogs(read_options, folly::none);
  it2->seek(*mid_location);
  ASSERT_EQ(IteratorState::AT_RECORD, it2->state());
  EXPECT_EQ(log2, it2->getLogID());
  EXPECT_EQ(300, it2->getLSN());
  set_it = records.find(
      std::make_tuple(store_->getPartitionList()->back()->id_ - 2, log2, 300));
  ASSERT_FALSE(set_it == records.end());
  std::unique_ptr<LocalLogStore::AllLogsIterator::Location> new_location;
  for (; it2->state() == IteratorState::AT_RECORD; it2->next()) {
    if (set_it == records.end()) {
      ADD_FAILURE();
    } else {
      EXPECT_EQ(std::get<1>(*set_it), it2->getLogID());
      EXPECT_EQ(std::get<2>(*set_it), it2->getLSN());
      ++set_it;
    }

    verifyRecord(it2->getLogID(), it2->getLSN(), it2->getRecord());

    if (it2->getLogID() == log1 && it2->getLSN() == 400) {
      new_location = it2->getLocation();
    }
  }
  EXPECT_EQ(IteratorState::AT_END, it->state());
  EXPECT_TRUE(set_it == records.end());
  ASSERT_FALSE(new_location == nullptr);

  ld_info("Seeking old iterator to new location.");
  it->seek(*new_location);
  EXPECT_EQ(IteratorState::AT_END, it->state());

  TestReadFilter filter;
  LocalLogStore::ReadStats stats;
  using C = TestReadFilter::Call;

  ld_info("Reading with all partitions filtered out.");
  it->seek(*it->minLocation(), &filter, &stats);
  EXPECT_EQ(IteratorState::AT_END, it->state());
  // Note that, as currently implemented, the time range filter is not applied
  // to unpartitioned CF.
  EXPECT_EQ(std::vector<C>({C::fullRange(),
                            C::timeRange(10, 30),
                            C::emptyRange(),
                            C::timeRange(210, 230),
                            C::timeRange(310, 310)}),
            filter.history);
  filter.history.clear();

  ld_info("Reading with filter accepting 3 records from unpartitioned and "
          "partition 2.");
  filter.time_ranges = {C::FULL_TIME_RANGE, C::EMPTY_TIME_RANGE, 210};
  filter.records = {21, 23, 2002};
  it->seek(*it->minLocation(), &filter, &stats);
  EXPECT_EQ(std::vector<C>({C::fullRange(),
                            C::call(1001),
                            C::call(1002),
                            C::call(2001),
                            C::call(2002),
                            C::call(2002, true),
                            IF_DEBUG(C::call(2002))}),
            filter.history);
  filter.history.clear();
  ASSERT_EQ(IteratorState::AT_RECORD, it->state());
  EXPECT_EQ(meta_log1, it->getLogID());
  EXPECT_EQ(200, it->getLSN());
  verifyRecord(it->getLogID(), it->getLSN(), it->getRecord());
  it->next(&filter, &stats);
  EXPECT_EQ(std::vector<C>({C::call(3001),
                            C::timeRange(10, 30),
                            C::emptyRange(),
                            C::timeRange(210, 230),
                            C::call(21),
                            C::call(21, true),
                            IF_DEBUG(C::call(21))}),
            filter.history);
  filter.history.clear();
  ASSERT_EQ(IteratorState::AT_RECORD, it->state());
  EXPECT_EQ(log1, it->getLogID());
  EXPECT_EQ(300, it->getLSN());
  verifyRecord(it->getLogID(), it->getLSN(), it->getRecord());
  it->next(&filter, &stats);
  EXPECT_EQ(
      std::vector<C>(
          {C::call(22), C::call(23), C::call(23, true), IF_DEBUG(C::call(23))}),
      filter.history);
  filter.history.clear();
  ASSERT_EQ(IteratorState::AT_RECORD, it->state());
  EXPECT_EQ(log2, it->getLogID());
  EXPECT_EQ(300, it->getLSN());
  verifyRecord(it->getLogID(), it->getLSN(), it->getRecord());
  it->next(&filter, &stats);
  EXPECT_EQ(std::vector<C>({C::timeRange(310, 310)}), filter.history);
  filter.history.clear();
  ASSERT_EQ(IteratorState::AT_END, it->state());

  ld_info("Reading with filtering and byte limit.");
  filter.time_ranges.insert(310);
  // Read limit = now + 1 byte.
  stats.max_bytes_to_read = stats.read_record_bytes + stats.read_csi_bytes + 1;
  it->seek(*mid_location, &filter, &stats);
  EXPECT_EQ(std::vector<C>({C::timeRange(210, 230),
                            C::call(23),
                            C::call(23, true),
                            IF_DEBUG(C::call(23))}),
            filter.history);
  filter.history.clear();
  ASSERT_EQ(IteratorState::AT_RECORD, it->state());
  EXPECT_EQ(log2, it->getLogID());
  EXPECT_EQ(300, it->getLSN());
  verifyRecord(it->getLogID(), it->getLSN(), it->getRecord());
  auto limit_reached_location = it->getLocation();
  it->next(&filter, &stats);
  EXPECT_EQ(
      std::vector<C>({C::timeRange(310, 310), C::call(31)}), filter.history);
  filter.history.clear();
  ASSERT_EQ(IteratorState::LIMIT_REACHED, it->state());
  EXPECT_EQ(log3, it->getLogID());
  EXPECT_EQ(101, it->getLSN()); // 100 was filtered out
  limit_reached_location = it->getLocation();
  // Read limit = infinity.
  stats.max_bytes_to_read = std::numeric_limits<size_t>::max();
  it->next(&filter, &stats);
  EXPECT_EQ(std::vector<C>({}), filter.history);
  filter.history.clear();
  ASSERT_EQ(IteratorState::AT_END, it->state());

  ld_info("Reading from metadataLogsBegin()");
  it->seek(*it->metadataLogsBegin());
  ASSERT_EQ(IteratorState::AT_RECORD, it->state());
  EXPECT_EQ(meta_log1, it->getLogID());
  EXPECT_EQ(100, it->getLSN());
  verifyRecord(it->getLogID(), it->getLSN(), it->getRecord());
  it->next();
  ASSERT_EQ(IteratorState::AT_RECORD, it->state());
  EXPECT_EQ(meta_log1, it->getLogID());
  EXPECT_EQ(200, it->getLSN());
  verifyRecord(it->getLogID(), it->getLSN(), it->getRecord());
  it->next();
  ASSERT_EQ(IteratorState::AT_RECORD, it->state());
  EXPECT_EQ(meta_log2, it->getLogID());
  EXPECT_EQ(100, it->getLSN());
  verifyRecord(it->getLogID(), it->getLSN(), it->getRecord());
  it->next();
  if (it->state() != IteratorState::AT_END) {
    EXPECT_EQ(IteratorState::AT_RECORD, it->state());
    EXPECT_FALSE(MetaDataLog::isMetaDataLog(it->getLogID()));
  }

  ld_info("Reading an empty subset of logs filtering out all metadata records");
  it = store_->readAllLogs(
      read_options, std::unordered_map<logid_t, std::pair<lsn_t, lsn_t>>({}));
  filter.records.clear();
  filter.time_ranges = {C::FULL_TIME_RANGE};
  it->seek(*it->minLocation(), &filter, &stats);
  EXPECT_EQ(IteratorState::AT_END, it->state());
  EXPECT_EQ(std::vector<C>({C::fullRange(),
                            C::call(1001),
                            C::call(1002),
                            C::call(2001),
                            C::call(2002),
                            C::call(3001)}),
            filter.history);
  filter.history.clear();

  ld_info("Reading an empty subset of logs and some metadata records");
  filter.records = {1002, 2002};
  it->seek(*it->minLocation(), &filter, &stats);
  EXPECT_EQ(IteratorState::AT_RECORD, it->state());
  EXPECT_EQ(internal_log, it->getLogID());
  EXPECT_EQ(200, it->getLSN());
  verifyRecord(it->getLogID(), it->getLSN(), it->getRecord());
  EXPECT_EQ(std::vector<C>({C::fullRange(),
                            C::call(1001),
                            C::call(1002),
                            C::call(1002, true),
                            IF_DEBUG(C::call(1002))}),
            filter.history);
  filter.history.clear();
  it->next(&filter, &stats);
  EXPECT_EQ(std::vector<C>({C::call(2001),
                            C::call(2002),
                            C::call(2002, true),
                            IF_DEBUG(C::call(2002))}),
            filter.history);
  EXPECT_EQ(IteratorState::AT_RECORD, it->state());
  EXPECT_EQ(meta_log1, it->getLogID());
  EXPECT_EQ(200, it->getLSN());
  verifyRecord(it->getLogID(), it->getLSN(), it->getRecord());
  filter.history.clear();
  it->next(&filter, &stats);
  EXPECT_EQ(std::vector<C>({C::call(3001)}), filter.history);
  EXPECT_EQ(IteratorState::AT_END, it->state());
  filter.history.clear();

  ld_info("Reading a subset of logs and no metadata records");
  it = store_->readAllLogs(
      read_options,
      std::unordered_map<logid_t, std::pair<lsn_t, lsn_t>>(
          {{log1, {LSN_INVALID, LSN_MAX}}, {log2, {LSN_INVALID, LSN_MAX}}}));
  filter.time_ranges = {10, 210};
  filter.record_ranges = {std::make_pair(log1, 100), std::make_pair(log2, 200)};
  filter.records = {1, 23};
  it->seek(*it->minLocation(), &filter, &stats);
  EXPECT_EQ(IteratorState::AT_RECORD, it->state());
  EXPECT_EQ(log1, it->getLogID());
  EXPECT_EQ(100, it->getLSN());
  verifyRecord(it->getLogID(), it->getLSN(), it->getRecord());
  EXPECT_EQ(std::vector<C>({C::fullRange(),
                            C::timeRange(10, 30),
                            C::recordRange(log1, 100, 200),
                            C::call(1),
                            C::call(1, true),
                            IF_DEBUG(C::call(1))}),
            filter.history);
  filter.history.clear();
  it->next(&filter, &stats);
  EXPECT_EQ(IteratorState::AT_RECORD, it->state());
  EXPECT_EQ(log2, it->getLogID());
  EXPECT_EQ(300, it->getLSN());
  verifyRecord(it->getLogID(), it->getLSN(), it->getRecord());
  EXPECT_EQ(std::vector<C>({C::call(2),
                            C::recordRange(log2, 100, 100),
                            C::timeRange(210, 230),
                            C::recordRange(log1, 300, 300),
                            C::recordRange(log2, 200, 300),
                            C::call(22),
                            C::call(23),
                            C::call(23, true),
                            IF_DEBUG(C::call(23))}),
            filter.history);
  filter.history.clear();
  it->next(&filter, &stats);
  EXPECT_EQ(IteratorState::AT_END, it->state());
  EXPECT_EQ(std::vector<C>({C::timeRange(410, 410)}), filter.history);
  filter.history.clear();

  ld_info("Reading a metadata record and a data record");
  filter.time_ranges = {C::FULL_TIME_RANGE, 210, 310};
  filter.record_ranges = {std::make_pair(log1, 300)};
  filter.records = {2002, 21};
  it->seek(*it->minLocation(), &filter, &stats);
  EXPECT_EQ(IteratorState::AT_RECORD, it->state());
  EXPECT_EQ(meta_log1, it->getLogID());
  EXPECT_EQ(200, it->getLSN());
  verifyRecord(it->getLogID(), it->getLSN(), it->getRecord());
  EXPECT_EQ(std::vector<C>({C::fullRange(),
                            C::call(1001),
                            C::call(1002),
                            C::call(2001),
                            C::call(2002),
                            C::call(2002, true),
                            IF_DEBUG(C::call(2002))}),
            filter.history);
  filter.history.clear();
  it->next(&filter, &stats);
  EXPECT_EQ(IteratorState::AT_RECORD, it->state());
  EXPECT_EQ(log1, it->getLogID());
  EXPECT_EQ(300, it->getLSN());
  verifyRecord(it->getLogID(), it->getLSN(), it->getRecord());
  EXPECT_EQ(std::vector<C>({C::call(3001),
                            C::timeRange(10, 30),
                            C::timeRange(210, 230),
                            C::recordRange(log1, 300, 300),
                            C::call(21),
                            C::call(21, true),
                            IF_DEBUG(C::call(21))}),
            filter.history);
  filter.history.clear();
  it->next(&filter, &stats);
  EXPECT_EQ(IteratorState::AT_END, it->state());
  EXPECT_EQ(
      std::vector<C>({C::recordRange(log2, 200, 300), C::timeRange(410, 410)}),
      filter.history);
  filter.history.clear();

  ld_info("Seeking to the middle with directory filtering");
  filter.time_ranges = {210};
  filter.record_ranges = {std::make_pair(log2, 200)};
  filter.records = {23};
  it->seek(*mid_location, &filter, &stats);
  EXPECT_EQ(IteratorState::AT_RECORD, it->state());
  EXPECT_EQ(log2, it->getLogID());
  EXPECT_EQ(300, it->getLSN());
  verifyRecord(it->getLogID(), it->getLSN(), it->getRecord());
  EXPECT_EQ(std::vector<C>({C::timeRange(210, 230),
                            C::recordRange(log2, 200, 300),
                            C::call(23),
                            C::call(23, true),
                            IF_DEBUG(C::call(23))}),
            filter.history);
  filter.history.clear();
  it->next(&filter, &stats);
  EXPECT_EQ(IteratorState::AT_END, it->state());
  EXPECT_EQ(std::vector<C>({C::timeRange(410, 410)}), filter.history);
  filter.history.clear();

  ld_info("Now filter out the record");
  it = store_->readAllLogs(
      read_options,
      std::unordered_map<logid_t, std::pair<lsn_t, lsn_t>>(
          {{log2, {LSN_INVALID, LSN_MAX}}, {log3, {LSN_INVALID, LSN_MAX}}}));
  filter.time_ranges = {210};
  filter.record_ranges = {std::make_pair(log2, 200)};
  filter.records = {};
  it->seek(*mid_location, &filter, &stats);
  EXPECT_EQ(IteratorState::AT_END, it->state());
  EXPECT_EQ(std::vector<C>({C::timeRange(210, 230),
                            C::recordRange(log2, 200, 300),
                            C::call(23),
                            C::timeRange(310, 310)}),
            filter.history);
  filter.history.clear();

  ld_info("Now filter out the directory entry");
  filter.time_ranges = {210};
  filter.record_ranges = {};
  it->seek(*mid_location, &filter, &stats);
  EXPECT_EQ(IteratorState::AT_END, it->state());
  EXPECT_EQ(std::vector<C>({C::timeRange(210, 230),
                            C::recordRange(log2, 200, 300),
                            C::timeRange(310, 310)}),
            filter.history);
  filter.history.clear();

  ld_info("Now filter out the partition");
  filter.time_ranges = {};
  filter.record_ranges = {};
  it->seek(*mid_location, &filter, &stats);
  EXPECT_EQ(IteratorState::AT_END, it->state());
  EXPECT_EQ(std::vector<C>({C::timeRange(210, 230), C::timeRange(310, 310)}),
            filter.history);
  filter.history.clear();

  ld_info("Now filter out the log");
  it = store_->readAllLogs(
      read_options,
      std::unordered_map<logid_t, std::pair<lsn_t, lsn_t>>(
          {{log1, {LSN_INVALID, LSN_MAX}}, {log3, {LSN_INVALID, LSN_MAX}}}));
  filter.time_ranges = {310};
  filter.record_ranges = {std::make_pair(log3, 100)};
  it->seek(*mid_location, &filter, &stats);
  EXPECT_EQ(IteratorState::AT_END, it->state());
  EXPECT_EQ(std::vector<C>({C::timeRange(310, 310),
                            C::recordRange(log3, 100, 100),
                            C::call(31),
                            C::timeRange(410, 410)}),
            filter.history);
  filter.history.clear();

  ld_info("Seeking to a filtered out record");
  it = store_->readAllLogs(
      read_options,
      std::unordered_map<logid_t, std::pair<lsn_t, lsn_t>>(
          {{log1, {LSN_INVALID, LSN_MAX}}, {log2, {LSN_INVALID, LSN_MAX}}}));
  filter.time_ranges = {210};
  filter.record_ranges = {std::make_pair(log2, 200)};
  it->seek(*location_21, &filter, &stats);
  EXPECT_EQ(IteratorState::AT_END, it->state());
  EXPECT_EQ(std::vector<C>({C::timeRange(210, 230),
                            C::recordRange(log1, 300, 300),
                            C::recordRange(log2, 200, 300),
                            C::call(22),
                            C::call(23),
                            C::timeRange(410, 410)}),
            filter.history);
  filter.history.clear();

  ld_info("Stepping to a newly written record");
  it = store_->readAllLogs(read_options,
                           std::unordered_map<logid_t, std::pair<lsn_t, lsn_t>>(
                               {{log3, {LSN_INVALID, LSN_MAX}}}));
  filter.time_ranges = {310};
  filter.record_ranges = {std::make_pair(log3, 100)};
  write(log3, 200, 32);
  it->seek(*location_21, &filter, &stats);
  EXPECT_EQ(IteratorState::AT_END, it->state());
  EXPECT_EQ(std::vector<C>({C::timeRange(310, 320),
                            C::recordRange(log3, 100, 100),
                            C::call(31)}),
            filter.history);
  filter.history.clear();

  ld_info("End of log and byte limit hit simultaneously.");
  it = store_->readAllLogs(
      read_options,
      std::unordered_map<logid_t, std::pair<lsn_t, lsn_t>>(
          {{log1, {LSN_INVALID, LSN_MAX}}, {log2, {LSN_INVALID, LSN_MAX}}}));
  filter.time_ranges = {210};
  filter.record_ranges = {std::make_pair(log1, 300), std::make_pair(log2, 200)};
  filter.records = {21, 23};
  it->seek(*location_21, &filter, &stats);
  EXPECT_EQ(IteratorState::AT_RECORD, it->state());
  EXPECT_EQ(log1, it->getLogID());
  EXPECT_EQ(300, it->getLSN());
  verifyRecord(it->getLogID(), it->getLSN(), it->getRecord());
  EXPECT_EQ(std::vector<C>({C::timeRange(210, 230),
                            C::recordRange(log1, 300, 300),
                            C::call(21),
                            C::call(21, true),
                            IF_DEBUG(C::call(21))}),
            filter.history);
  filter.history.clear();
  // Read limit = now + 1 byte.
  stats.max_bytes_to_read = stats.read_record_bytes + stats.read_csi_bytes + 1;
  it->next(&filter, &stats);
  EXPECT_EQ(IteratorState::LIMIT_REACHED, it->state());
  EXPECT_EQ(log2, it->getLogID());
  EXPECT_EQ(201, it->getLSN());
  EXPECT_EQ(std::vector<C>({C::recordRange(log2, 200, 300), C::call(22)}),
            filter.history);
  filter.history.clear();
  // Read limit = infinity.
  stats.max_bytes_to_read = std::numeric_limits<size_t>::max();
  it->next(&filter, &stats);
  EXPECT_EQ(IteratorState::AT_RECORD, it->state());
  EXPECT_EQ(log2, it->getLogID());
  EXPECT_EQ(300, it->getLSN());
  EXPECT_EQ(
      std::vector<C>({C::call(23), C::call(23, true), IF_DEBUG(C::call(23))}),
      filter.history);
  filter.history.clear();
  it->next(&filter, &stats);
  EXPECT_EQ(IteratorState::AT_END, it->state());
  EXPECT_EQ(std::vector<C>({C::timeRange(410, 410)}), filter.history);
  filter.history.clear();
}

// Reproduces a former bug where time-based-trimmed records weren't compacted
// away if there are older non-time-based-trimmed records.
TEST_F(PartitionedRocksDBStoreTest, CompactionsWithManualRetention) {
  // Record with infinite retention.
  put({TestRecord(logid_t(400), 10)});
  // Record with 1-day retention.
  put({TestRecord(logid_t(10), 20)});
  setTime(BASE_TIME + HOUR);
  store_->createPartition();
  setTime(BASE_TIME + DAY + 2 * HOUR);
  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::LO_PRI)
      .wait();
  auto stats = stats_.aggregate();
  EXPECT_EQ(0, stats.partitions_dropped);
  EXPECT_EQ(1, stats.partitions_compacted);
  {
    auto it = store_->read(
        logid_t(10),
        LocalLogStore::ReadOptions("CompactionsWithManualRetention"));
    it->seek(0);
    EXPECT_EQ(IteratorState::AT_END, it->state());
  }
  {
    auto it = store_->read(
        logid_t(400),
        LocalLogStore::ReadOptions("CompactionsWithManualRetention"));
    it->seek(0);
    ASSERT_EQ(IteratorState::AT_RECORD, it->state());
    EXPECT_EQ(10, it->getLSN());
  }
}

// Reproduces a former bug where partitions weren't compacted if a log was
// removed from config recently.
TEST_F(PartitionedRocksDBStoreTest, CompactionsWithRemovedLogs) {
  // Record in a log we're going to remove.
  put({TestRecord(logid_t(300), 10)});
  // Record with 1-day retention.
  put({TestRecord(logid_t(10), 20)});
  setTime(BASE_TIME + HOUR);
  store_->createPartition();
  setTime(BASE_TIME + DAY + 2 * HOUR);

  // Remove log 300.
  auto log_group = processor_->config_->getLocalLogsConfig()->getLogGroup(
      "/logs_can_disappear");
  ASSERT_NE(nullptr, log_group);
  ASSERT_TRUE(processor_->config_->getLocalLogsConfig()->replaceLogGroup(
      "/logs_can_disappear",
      log_group->withRange(logid_range_t({logid_t(301), logid_t(399)}))));

  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::LO_PRI)
      .wait();
  auto stats = stats_.aggregate();
  EXPECT_EQ(0, stats.partitions_dropped);
  EXPECT_EQ(1, stats.partitions_compacted);
  {
    auto it = store_->read(
        logid_t(10),
        LocalLogStore::ReadOptions("CompactionsWithManualRetention"));
    it->seek(0);
    EXPECT_EQ(IteratorState::AT_END, it->state());
  }
  {
    auto it = store_->read(
        logid_t(300),
        LocalLogStore::ReadOptions("CompactionsWithManualRetention"));
    it->seek(0);
    ASSERT_EQ(IteratorState::AT_RECORD, it->state());
    EXPECT_EQ(10, it->getLSN());
  }
}

TEST_F(PartitionedRocksDBStoreTest, DataSize) {
  closeStore();
  openStore({{"rocksdb-partition-duration", "15min"},
             {"rocksdb-partition-compaction-schedule", "1d,2d"}});

  logid_t one_day_log = logid_t(1);
  logid_t infinite_log = logid_t(400);
  uint64_t time_raw = BASE_TIME;
  size_t result = 0;
  size_t old_size = 0;
  auto dataSize =
      [&](logid_t log_id,
          std::chrono::milliseconds lo = std::chrono::milliseconds::min(),
          std::chrono::milliseconds hi = std::chrono::milliseconds::max()) {
        old_size = result;
        return store_->dataSize(log_id, lo, hi, &result);
      };

  // Write a few records to a log and see that the approximate size in the
  // DirectoryEntry increases as expected.

  ASSERT_EQ(dataSize(one_day_log), 0);
  ASSERT_EQ(result, 0);
  put({TestRecord(one_day_log, 10, time_raw, std::string(36, 'x'))});
  ASSERT_EQ(dataSize(one_day_log), 0);
  ASSERT_NE(result, 0);

  time_raw += 2 * MINUTE;
  setTime(time_raw);
  put({TestRecord(one_day_log, 20, time_raw, std::string(51, 'x')),
       TestRecord(one_day_log, 30, time_raw, std::string(33, 'x'))});
  ASSERT_EQ(dataSize(one_day_log), 0);
  ASSERT_GT(result, old_size);

  // Write to another partition
  ASSERT_EQ(1, store_->getPartitionList()->size());
  time_raw += 15 * MINUTE;
  setTime(time_raw);
  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::HI_PRI)
      .wait();
  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::LO_PRI)
      .wait();
  ASSERT_EQ(2, store_->getPartitionList()->size());
  ASSERT_EQ(dataSize(one_day_log), 0);
  ASSERT_EQ(result, old_size);

  // Add some records to new partition
  time_raw += 3 * MINUTE;
  setTime(time_raw);
  put({TestRecord(one_day_log, 33, time_raw, std::string(32, 'x')),
       TestRecord(one_day_log, 35, time_raw)});
  ASSERT_EQ(dataSize(one_day_log), 0);
  ASSERT_GT(result, old_size);

  // Add some records to old partition
  time_raw += 2 * MINUTE;
  setTime(time_raw);
  put({TestRecord(one_day_log, 22, time_raw, std::string(42, 'x')),
       TestRecord(one_day_log, 29, time_raw, std::string(63, 'x'))});
  ASSERT_EQ(dataSize(one_day_log), 0);
  ASSERT_GT(result, old_size);

  // Add a few records of a log with infinite retention to the newer partition
  // to prevent it from being dropped except by manually trimming that log.
  put({TestRecord(infinite_log, 22, time_raw, std::string(44, 'x')),
       TestRecord(infinite_log, 29, time_raw, std::string(76, 'x'))});
  ASSERT_EQ(dataSize(one_day_log), 0);
  ASSERT_EQ(result, old_size);

  // Get the oldest partition dropped by trimming
  ld_assert(time_raw = BASE_TIME + 22 * MINUTE);
  // Go to 3 minutes before first partition can be dropped
  time_raw += 1 * DAY - 10 * MINUTE;
  setTime(time_raw);
  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::HI_PRI)
      .wait();
  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::LO_PRI)
      .wait();
  auto partition_list = store_->getPartitionList();
  ASSERT_EQ(ID0, partition_list->front()->id_);
  ASSERT_EQ(dataSize(one_day_log), 0);
  ASSERT_EQ(result, old_size);

  // Go to 2 minutes after first partition can be dropped
  time_raw += 5 * MINUTE;
  setTime(time_raw);
  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::HI_PRI)
      .wait();
  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::LO_PRI)
      .wait();
  partition_list = store_->getPartitionList();
  ASSERT_EQ(ID0 + 1, partition_list->front()->id_);
  ASSERT_EQ(dataSize(one_day_log), 0);
  ASSERT_LT(result, old_size);

  // Go forward till records of log 1 in the second oldest partition should get
  // compacted away (as per the 2d schedule).
  time_raw += 1 * DAY + 15 * MINUTE;
  setTime(time_raw);
  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::HI_PRI)
      .wait();
  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::LO_PRI)
      .wait();
  partition_list = store_->getPartitionList();
  ASSERT_EQ(ID0 + 1, partition_list->front()->id_);
  ASSERT_EQ(dataSize(one_day_log), 0);
  ASSERT_LT(result, old_size);
}

// Check that, given a time range that spans a fraction of a single partition,
// we give a reasonable estimate.
TEST_F(PartitionedRocksDBStoreTest, DataSize2) {
  closeStore();
  openStore({{"rocksdb-partition-duration", "15min"}});

  logid_t one_day_log = logid_t(1);
  logid_t two_day_log = logid_t(400);
  uint64_t time_raw = BASE_TIME;
  size_t result = 0;
  size_t old_size = 0;
  auto dataSize = [&](logid_t log_id,
                      uint64_t lo = 0,
                      uint64_t hi = std::chrono::milliseconds::max().count()) {
    old_size = result;
    int rv = store_->dataSize(log_id,
                              std::chrono::milliseconds(lo),
                              std::chrono::milliseconds(hi),
                              &result);
    ld_info("dataSize: log %lu, time range [%lu,%lu]: rv %d and size %lu",
            log_id.val_,
            lo,
            hi,
            rv,
            result);
    return rv;
  };

  auto update_time = [&](uint64_t new_time) {
    time_raw = new_time;
    setTime(time_raw);
    store_
        ->backgroundThreadIteration(
            PartitionedRocksDBStore::BackgroundThreadType::HI_PRI)
        .wait();
    store_
        ->backgroundThreadIteration(
            PartitionedRocksDBStore::BackgroundThreadType::LO_PRI)
        .wait();
  };

  // Write a few records to a log and see that the approximate size in the
  // DirectoryEntry increases as expected. Use larger payloads as it's easier
  // to figure out what to expect when headers a smaller part.

  ASSERT_EQ(dataSize(one_day_log), 0);
  ASSERT_EQ(result, 0);
  put({TestRecord(one_day_log, 10, time_raw, std::string(120, 'x'))});
  ASSERT_EQ(dataSize(one_day_log), 0);
  ASSERT_NE(result, 0);
  update_time(BASE_TIME + 12 * MINUTE);
  put({TestRecord(one_day_log, 20, time_raw, std::string(302, 'x')),
       TestRecord(one_day_log, 30, time_raw)});
  ASSERT_EQ(dataSize(one_day_log), 0);
  ASSERT_GT(result, old_size);

  // Make a second partition
  ASSERT_EQ(1, store_->getPartitionList()->size());
  update_time(BASE_TIME + 15 * MINUTE + 25 * SECOND);
  ASSERT_EQ(2, store_->getPartitionList()->size());
  ASSERT_EQ(dataSize(one_day_log), 0);
  ASSERT_GT(result, 0);
  ASSERT_EQ(result, old_size);

  // Add some records to new partition
  ld_check_gt(BASE_TIME + 16 * MINUTE + 25 * SECOND, time_raw);
  update_time(BASE_TIME + 16 * MINUTE + 25 * SECOND);
  put({TestRecord(one_day_log, 33, time_raw, std::string(617, 'x')),
       TestRecord(one_day_log, 35, time_raw, std::string(462, 'x'))});
  update_time(BASE_TIME + 29 * MINUTE);
  // Large amount of data to make sure only correct log is considered
  put({TestRecord(two_day_log, 43, time_raw, std::string(5000000, 'x'))});
  put({TestRecord(one_day_log, 43, time_raw, std::string(50, 'x'))});
  ASSERT_EQ(dataSize(one_day_log), 0);
  ASSERT_GT(result, old_size);

  // Make a third partition
  ASSERT_EQ(2, store_->getPartitionList()->size());
  update_time(BASE_TIME + 30 * MINUTE + 37 * SECOND);
  ASSERT_EQ(3, store_->getPartitionList()->size());
  ASSERT_EQ(dataSize(one_day_log), 0);
  ASSERT_GT(result, 0);
  ASSERT_EQ(result, old_size);

  // Add some records to new partition
  update_time(BASE_TIME + 31 * MINUTE + 2 * SECOND);
  put({TestRecord(one_day_log, 45, time_raw, std::string(333, 'x')),
       TestRecord(one_day_log, 54, time_raw)});
  update_time(BASE_TIME + 39 * MINUTE);
  put({TestRecord(one_day_log, 79, time_raw, std::string(79, 'x')),
       TestRecord(one_day_log, 119, time_raw, std::string(179, 'x'))});
  ASSERT_EQ(dataSize(one_day_log), 0);
  ASSERT_GT(result, old_size);

  update_time(time_raw + 1 * MINUTE);

  // Comparing various ranges

  // 1st partition should start at around BASE_TIME
  // 2nd partition should start at about BASE + 16m 25s
  // 3rd partition shuold start at about BASE + 31m + 2s

  // 1st partition should have ballpark 450 bytes of data
  // 2nd partition should have ballpark 1200 bytes of data
  // 3rd partition should have ballpark 700 bytes of data

  uint64_t beginning_first_partition = BASE_TIME + 10 * SECOND;
  uint64_t mid_first_partition = BASE_TIME + 7 * MINUTE + 43 * SECOND;
  uint64_t beginning_second_partition = BASE_TIME + 15 * MINUTE + 25 * SECOND;
  uint64_t mid_second_partition = BASE_TIME + 23 * MINUTE + SECOND;
  uint64_t beginning_third_partition = BASE_TIME + 30 * MINUTE + 37 * SECOND;
  uint64_t mid_third_partition = BASE_TIME + 35 * MINUTE;

  // End is wherever the next partition starts, except for the last partition,
  // which ends at the current time.
  uint64_t end_first_partition = beginning_second_partition;
  uint64_t end_second_partition = beginning_third_partition;
  update_time(BASE_TIME + 40 * MINUTE);
  uint64_t end_third_partition = time_raw;

  // Infinitesmall range
  ASSERT_EQ(
      dataSize(
          one_day_log, beginning_first_partition, beginning_first_partition),
      0);
  ASSERT_EQ(result, 0);

  // Larger in first vs smaller in first partition
  ASSERT_EQ(
      dataSize(one_day_log, beginning_first_partition, end_first_partition), 0);
  size_t res1 = result;
  ASSERT_EQ(dataSize(one_day_log, mid_first_partition, end_first_partition), 0);
  size_t res2 = result;
  ASSERT_GT(res1, res2);
  // First partition has something like 450 bytes; see if it's in the ballpark
  ASSERT_GT(res1, 300);
  ASSERT_LT(res1, 600);
  // Expect about half of that for half the range
  ASSERT_GT(res2, 150);
  ASSERT_LT(res2, 300);

  // Larger & smaller range of multiple partitions
  ASSERT_EQ(
      dataSize(one_day_log, beginning_first_partition, mid_second_partition),
      0);
  res1 = result;
  ASSERT_EQ(
      dataSize(one_day_log, mid_first_partition, mid_second_partition), 0);
  res2 = result;
  ASSERT_GT(res1, res2);

  // Expect about 450 + 600 = ~ 1k
  ASSERT_GT(res1, 800);
  ASSERT_LT(res1, 1200);
  // Minus about half the first partition, so something like 800 byte
  ASSERT_GT(res2, 650);
  ASSERT_LT(res2, 900);

  // Entire log should have about 450 + 1200 + 700 = 2350 byte
  ASSERT_EQ(dataSize(one_day_log), 0);
  ASSERT_GT(result, 2000);
  ASSERT_LT(result, 2600);

  // Larger bounds than the partition itself for partition 3: should get entire
  // partition size but no more
  ASSERT_EQ(
      dataSize(one_day_log, beginning_third_partition, end_third_partition), 0);
  ASSERT_GT(result, 600);
  ASSERT_LT(result, 900);

  // Let's try last partition, and past last partition
  ASSERT_EQ(
      dataSize(one_day_log, mid_third_partition, end_third_partition + 3 * DAY),
      0);
  ASSERT_LT(result, old_size);
  ASSERT_GT(result, 200);

  ASSERT_EQ(dataSize(one_day_log,
                     end_third_partition - 1 * MINUTE,
                     end_third_partition + 40 * MINUTE),
            0);
  ASSERT_LT(result, old_size);
  ASSERT_GT(result, 0);

  ASSERT_EQ(dataSize(one_day_log,
                     end_third_partition + 1 * MINUTE,
                     end_third_partition + 52 * MINUTE),
            0);
  ASSERT_EQ(result, 0);

  // Entire two-day log should have ~5000000 bytes
  ASSERT_EQ(dataSize(two_day_log), 0);
  ASSERT_GT(result, 4900000);
  ASSERT_LT(result, 5100000);

  // How much of other partitions are in the range shouldn't matter
  ASSERT_EQ(dataSize(two_day_log, mid_first_partition, mid_third_partition), 0);
  ASSERT_EQ(result, old_size);
  ASSERT_EQ(dataSize(two_day_log, mid_first_partition, end_third_partition), 0);
  ASSERT_EQ(result, old_size);
  ASSERT_EQ(
      dataSize(two_day_log, end_first_partition, beginning_third_partition), 0);
  ASSERT_EQ(result, old_size);

  // 2nd partition should have ballpark 5000000 bytes of data for log 2

  // Play with linear interpolation of the partition that has data for this log

  // Partition ranges 912s; a second should still give us a non-zero value but
  // 5000000/912 ~ 5k so should at least be less than 7k
  ld_check_eq(end_second_partition - beginning_second_partition, 912 * SECOND);
  ASSERT_EQ(dataSize(two_day_log,
                     beginning_second_partition + MINUTE,
                     beginning_second_partition + MINUTE + SECOND),
            0);
  ASSERT_GT(result, 3500);
  ASSERT_LT(result, 7500);
  // ~5M total
  ASSERT_EQ(
      dataSize(two_day_log, beginning_second_partition, end_second_partition),
      0);
  ASSERT_GT(result, 5000000);
  ASSERT_LT(result, 5250000);
  // With 440s, or 7min 20s, we should expect just under half the amount, ~48%.
  ASSERT_EQ(dataSize(two_day_log,
                     beginning_second_partition + 7 * MINUTE + 20 * SECOND,
                     end_second_partition),
            0);
  ASSERT_GT(result, 2400000);
  ASSERT_LT(result, 2600000);
}

TEST_F(PartitionedRocksDBStoreTest, DataSizeOnRestart) {
  closeStore();
  openStore({{"rocksdb-partition-duration", "15min"},
             {"rocksdb-partition-compaction-schedule", "1d"}});

  logid_t one_day_log = logid_t(1);
  uint64_t time_raw = BASE_TIME;
  size_t result = 0;
  size_t old_size = 0;
  auto dataSize = [&](logid_t log_id) {
    old_size = result;
    return store_->dataSize(log_id,
                            std::chrono::milliseconds::min(),
                            std::chrono::milliseconds::max(),
                            &result);
  };

  // Write a few records to a log and see that the approximate size in the
  // DirectoryEntry increases as expected.

  ASSERT_EQ(dataSize(one_day_log), 0);
  ASSERT_EQ(result, 0);
  put({TestRecord(one_day_log, 10, time_raw, std::string(36, 'x'))});
  ASSERT_EQ(dataSize(one_day_log), 0);
  ASSERT_NE(result, 0);

  time_raw += 2 * MINUTE;
  setTime(time_raw);
  put({TestRecord(one_day_log, 20, time_raw, std::string(1151, 'x')),
       TestRecord(one_day_log, 30, time_raw, std::string(33, 'x'))});
  ASSERT_EQ(dataSize(one_day_log), 0);
  ASSERT_GT(result, old_size);

  // Write to another partition
  ASSERT_EQ(1, store_->getPartitionList()->size());
  time_raw += 15 * MINUTE;
  setTime(time_raw);
  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::HI_PRI)
      .wait();
  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::LO_PRI)
      .wait();
  ASSERT_EQ(2, store_->getPartitionList()->size());
  ASSERT_EQ(dataSize(one_day_log), 0);
  ASSERT_EQ(result, old_size);

  // Reopen store
  closeStore();
  openStore({{"rocksdb-partition-duration", "15min"},
             {"rocksdb-partition-compaction-schedule", "1d"}});

  // Result should remain unchanged
  ASSERT_EQ(dataSize(one_day_log), 0);
  ASSERT_EQ(result, old_size);
}

TEST_F(PartitionedRocksDBStoreTest, IteratorStaleMaxLsnBug) {
  logid_t logid(3);

  put({TestRecord(logid, 10)});
  LocalLogStore::ReadOptions ropt("IteratorStaleMaxLsnBug");
  ropt.tailing = true;
  auto it = store_->read(logid, ropt);
  it->seek(10);
  ASSERT_EQ(IteratorState::AT_RECORD, it->state());
  EXPECT_EQ(10, it->getLSN());

  put({TestRecord(logid, 20)});
  it->seek(10);
  ASSERT_EQ(IteratorState::AT_RECORD, it->state());
  EXPECT_EQ(10, it->getLSN());

  store_->createPartition();
  put({TestRecord(logid, 30)});

  it->next();
  ASSERT_EQ(IteratorState::AT_RECORD, it->state());
  EXPECT_EQ(20, it->getLSN());
}

// Reproduces a former bug where a corrupted payload caused records to be
// unaccounted for, and thus be lost in practice.
TEST_F(PartitionedRocksDBStoreTest, CorruptionAndNewPartition) {
  closeStore();
  ServerConfig::SettingsConfig s;
  s["rocksdb-partition-compaction-schedule"] = "disabled";
  s["rocksdb-partition-duration"] = "15min";
  openStore(s);
  ASSERT_TRUE(store_->getPartitionList()->get(ID0));

  auto time_raw = BASE_TIME + 2 * MINUTE;
  ASSERT_TRUE(store_->getPartitionList()->get(ID0));
  ASSERT_FALSE(store_->getPartitionList()->get(ID0 + 1));

  // Write a few records
  put({TestRecord(logid_t(1), 10, time_raw),
       TestRecord(logid_t(1), 12, time_raw + MINUTE),
       TestRecord(logid_t(1), 17, time_raw + 2 * MINUTE)});
  time_raw += 2 * MINUTE + 2 * SECOND;

  // Write a batch with a record of lsn 22, which should not increase max_lsn
  // because it is corrupted. Before the fix, the directory update would be in
  // memory but not flushed, so subsequent writes to LSNs in (17, 22] would not
  // be known by the on-disk directory, until a record with a higher LSN would
  // be written. But if the node was restarted before that happened, those
  // records were lost.

  updateSetting("rocksdb-test-corrupt-stores", "true");
  put(/*ops=*/{TestRecord(logid_t(1), 22, time_raw + MINUTE)},
      /*csi_copyset=*/{},
      /*assert_write_fail=*/true);
  updateSetting("rocksdb-test-corrupt-stores", "false");
  time_raw += MINUTE + 15 * SECOND;

  // Before the fix, the in-memory max_lsn would've been updated at this point,
  // diverging from what's stored in the shard.
  ASSERT_FALSE(store_->getPartitionList()->get(ID0 + 1));
  std::vector<std::pair<logid_t, DirectoryEntry>> logsdb_directory;
  store_->getLogsDBDirectories(/*partitions=*/{},
                               /*logs=*/{},
                               logsdb_directory);
  ASSERT_EQ(logsdb_directory.size(), 1);
  auto pair = *(logsdb_directory.begin());
  ld_check_eq(pair.first, logid_t(1));
  DirectoryEntry de = pair.second;
  ASSERT_EQ(de.id, ID0);
  ASSERT_EQ(de.max_lsn, lsn_t(17));

  // Reopen the store and verify that on-disk directory has expected max_lsn
  closeStore();
  logsdb_directory.clear();
  openStore();
  store_->getLogsDBDirectories(/*partitions=*/{},
                               /*logs=*/{},
                               logsdb_directory);
  ASSERT_EQ(logsdb_directory.size(), 1);
  pair = *(logsdb_directory.begin());
  ld_check_eq(pair.first, logid_t(1));
  de = pair.second;
  ASSERT_EQ(de.id, ID0);
  ASSERT_EQ(de.max_lsn, lsn_t(17));
}

// unstall low priority writes during shutdown.
TEST_F(PartitionedRocksDBStoreTest, StallLowPriWritesShutdownTest) {
  store_->suggested_throttle_state_ = WriteThrottleState::STALL_LOW_PRI_WRITE;
  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::FLUSH)
      .wait();
  EXPECT_EQ(
      WriteThrottleState::STALL_LOW_PRI_WRITE, store_->getWriteThrottleState());

  std::atomic<bool> thread_started{false};
  std::atomic<bool> thread_done{false};
  std::thread write_thread([&]() {
    thread_started.store(true);
    store_->stallLowPriWrite();
    thread_done.store(true);
  });
  while (!thread_started.load()) {
    std::this_thread::yield();
  }
  /* sleep override */
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  EXPECT_FALSE(thread_done.load());

  store_->suggested_throttle_state_ = WriteThrottleState::NONE;
  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::FLUSH)
      .wait();
  EXPECT_EQ(WriteThrottleState::NONE, store_->getWriteThrottleState());

  write_thread.join();
}

TEST_F(PartitionedRocksDBStoreTest, InterleavingCompactions) {
  using PartitionToCompact = PartitionedRocksDBStore::PartitionToCompact;
  using Reason = PartitionedRocksDBStore::PartitionToCompact::Reason;
  using Partition = PartitionedRocksDBStore::Partition;
  using PartitionPtr = PartitionedRocksDBStore::PartitionPtr;

  std::vector<PartitionPtr> ps(10);
  for (partition_id_t i = 1; i < ps.size(); ++i) {
    ps[i] = std::make_shared<Partition>(i, nullptr, RecordTimestamp::min());
  }

  std::vector<PartitionToCompact> c;

  auto check = [&](size_t num_pairs,
                   std::initializer_list<std::pair<PartitionPtr, Reason>> il) {
    std::vector<std::pair<PartitionPtr, Reason>> ex(il.begin(), il.end());
    ASSERT_EQ(ex.size(), c.size());
    for (size_t i = 0; i < c.size(); ++i) {
      // Allow either order in each pair.
      if (i < num_pairs * 2 && i % 2 == 0 && ex[i].second != c[i].reason) {
        swap(ex[i], ex[i + 1]);
      }
      EXPECT_EQ(ex[i].first->id_, c[i].partition->id_);
      EXPECT_EQ(ex[i].second, c[i].reason);
    }
  };

  c = {{ps[1], Reason::MANUAL},
       {ps[2], Reason::MANUAL},
       {ps[3], Reason::PARTIAL},
       {ps[4], Reason::PARTIAL},
       {ps[5], Reason::PARTIAL},
       {ps[6], Reason::PROACTIVE}};
  PartitionToCompact::interleavePartialAndNormalCompactions(&c);
  check(2,
        {{ps[1], Reason::MANUAL},
         {ps[3], Reason::PARTIAL},
         {ps[2], Reason::MANUAL},
         {ps[4], Reason::PARTIAL},
         {ps[5], Reason::PARTIAL},
         {ps[6], Reason::PROACTIVE}});

  c = {{ps[1], Reason::MANUAL},
       {ps[2], Reason::MANUAL},
       {ps[3], Reason::MANUAL},
       {ps[4], Reason::PARTIAL},
       {ps[5], Reason::PARTIAL},
       {ps[6], Reason::PROACTIVE}};
  PartitionToCompact::interleavePartialAndNormalCompactions(&c);
  check(2,
        {{ps[1], Reason::MANUAL},
         {ps[4], Reason::PARTIAL},
         {ps[2], Reason::MANUAL},
         {ps[5], Reason::PARTIAL},
         {ps[3], Reason::MANUAL},
         {ps[6], Reason::PROACTIVE}});

  c = {{ps[1], Reason::MANUAL},
       {ps[2], Reason::MANUAL},
       {ps[3], Reason::PROACTIVE}};
  PartitionToCompact::interleavePartialAndNormalCompactions(&c);
  check(0,
        {{ps[1], Reason::MANUAL},
         {ps[2], Reason::MANUAL},
         {ps[3], Reason::PROACTIVE}});

  c = {};
  PartitionToCompact::interleavePartialAndNormalCompactions(&c);
  check(0, {});
}

// The shared_ptr pointer returned will be invalidated after the partition
// is dropped. This test makes sure that is the case.
TEST_F(PartitionedRocksDBStoreTest, RocksDBCFPtrInvalidateAfterPartitionDrop) {
  // Get the rocksdb cf shared_ptr similar to how memtables fetch it.
  auto latest_partition = store_->getLatestPartition();
  auto cf_id = latest_partition->cf_->getID();
  auto rocksdb_ptr = store_->getColumnFamilyPtr(cf_id);
  const logid_t logid(1);
  ASSERT_TRUE(rocksdb_ptr != nullptr);
  ASSERT_TRUE(rocksdb_ptr->getID() == cf_id);
  put({TestRecord(logid, 10)});
  // Create a new partition just so that previous partition can be dropped.
  store_->createPartition();
  store_->dropPartitionsUpTo(ID0 + 1);
  // Now try to fetch the cf_ptr again. This time the returned ptr should
  // be invalid.
  rocksdb_ptr = store_->getColumnFamilyPtr(cf_id);
  ASSERT_TRUE(rocksdb_ptr == nullptr);

  // Other threads who already got access to the partition ptr should still be
  // able to access the cf_ptr.
  ASSERT_TRUE(latest_partition->cf_ != nullptr);
  ASSERT_TRUE(latest_partition->cf_->getID() == cf_id);
}

// Test that there's no overflow when a log just has data in some partitions,
// and we pass dataSize() a starting timestamp in an unoccupied region.
TEST_F(PartitionedRocksDBStoreTest, DataSizeWithDirectoryGaps) {
  closeStore();
  openStore({{"rocksdb-partition-duration", "15min"},
             {"rocksdb-partition-compaction-schedule", "1d,2d"}});

  logid_t one_day_log = logid_t(1);
  logid_t infinite_log = logid_t(400);
  uint64_t time_raw = BASE_TIME + 2 * MINUTE;
  size_t result = 0;
  int num_partitions = 1;

  auto dataSize =
      [&](logid_t log_id,
          std::chrono::milliseconds lo = std::chrono::milliseconds::min(),
          std::chrono::milliseconds hi = std::chrono::milliseconds::max()) {
        return store_->dataSize(log_id, lo, hi, &result);
      };

  auto wait_for_new_partition = [&] {
    store_
        ->backgroundThreadIteration(
            PartitionedRocksDBStore::BackgroundThreadType::HI_PRI)
        .wait();
    store_
        ->backgroundThreadIteration(
            PartitionedRocksDBStore::BackgroundThreadType::LO_PRI)
        .wait();
    ASSERT_EQ(++num_partitions, store_->getPartitionList()->size());
  };

  // Write a few records to both logs and see that the approximate size in the
  // DirectoryEntry increases.
  int lsn_1d = 10;
  ASSERT_EQ(dataSize(one_day_log), 0);
  ASSERT_EQ(result, 0);
  put({TestRecord(one_day_log, lsn_1d, time_raw, std::string(36, 'x'))});
  ASSERT_EQ(dataSize(one_day_log), 0);
  ASSERT_NE(result, 0);
  size_t prev_1d_log_size = result;

  int lsn_inf = 22;
  ASSERT_EQ(dataSize(infinite_log), 0);
  ASSERT_EQ(result, 0);
  put({TestRecord(infinite_log, lsn_inf, time_raw, std::string(36, 'x'))});
  ASSERT_EQ(dataSize(infinite_log), 0);
  ASSERT_NE(result, 0);
  size_t prev_inf_log_size = result;

  // Create a new partition, and write some to the one-day log. Verify data
  // sizes.
  auto new_partition_for_1d_log = [&]() {
    time_raw += 15 * MINUTE;
    setTime(time_raw);
    wait_for_new_partition();
    put({TestRecord(one_day_log, ++lsn_1d, time_raw, std::string(51, 'x')),
         TestRecord(one_day_log, ++lsn_1d, time_raw, std::string(33, 'x'))});
    ASSERT_EQ(dataSize(one_day_log), 0);
    ASSERT_GT(result, prev_1d_log_size);
    prev_1d_log_size = result;
  };
  new_partition_for_1d_log();

  ASSERT_EQ(dataSize(infinite_log), 0);
  ASSERT_EQ(result, prev_inf_log_size);

  // Now, let's create a few more partitions and write only to the one-day log.
  // Pick a time somewhere here that we'll later use as lower time bound for a
  // dataSize() call for the infinite-retention log.
  new_partition_for_1d_log();
  std::chrono::milliseconds lo_timestamp_for_final_query(time_raw);
  new_partition_for_1d_log();

  ASSERT_EQ(dataSize(infinite_log), 0);
  ASSERT_EQ(result, prev_inf_log_size);
  // Now use the lower timestamp we picked earlier.
  ASSERT_EQ(dataSize(infinite_log, lo_timestamp_for_final_query), 0);
  ASSERT_EQ(result, 0);

  // Write some data for the infinite log and try again.
  put({TestRecord(infinite_log, ++lsn_inf, time_raw, std::string(36, 'x'))});

  ASSERT_EQ(dataSize(infinite_log), 0);
  ASSERT_GT(result, prev_inf_log_size);
  // Now use the lower timestamp we picked earlier.
  ASSERT_EQ(dataSize(infinite_log, lo_timestamp_for_final_query), 0);
  ASSERT_GT(result, 0);
  // Now, check that there was no overflow.
  ASSERT_LT(result, 10000);
}

// Get the latest partition for the store and check if new memtable allocated
// notifications are triggered as expected.
TEST_F(PartitionedRocksDBStoreTest, MemtableFlushNotifications) {
  auto latest_partition = store_->getLatestPartition();
  auto metadata_cf = store_->getMetadataCFPtr();
  EXPECT_EQ(
      latest_partition->cf_->activeMemtableFlushToken(), FlushToken_INVALID);
  // As part of store open, new memtable cf is written to install the
  // schema_version. At the end of initialization, we flush all memtables so
  // that all memtables written during initialization or recovery are persisted.
  // Next write will allocate two flush tokens first for metadata cf and next
  // will be for the data cf.
  auto maxFlushToken = store_->maxFlushToken();
  put({TestRecord(logid_t(1), 1)});
  // Verify expected flush token are assigned to memtables.
  EXPECT_EQ(metadata_cf->activeMemtableFlushToken(), maxFlushToken + 1);
  EXPECT_EQ(
      latest_partition->cf_->activeMemtableFlushToken(), maxFlushToken + 2);
  // Force flush out the existing memtable. Write another record and it should
  // create a new memtable for the latest partition. Verify that is the case.
  store_->flushMemtable(latest_partition->cf_);
  // Active memtable flush token is reset when a memtable is flushed.
  EXPECT_EQ(
      latest_partition->cf_->activeMemtableFlushToken(), FlushToken_INVALID);
  put({TestRecord(logid_t(1), 2)});
  EXPECT_EQ(
      latest_partition->cf_->activeMemtableFlushToken(), maxFlushToken + 3);
}

// Add a few writes and make sure the partitions have correct dependencies.
// Issue flushes so that metadata memtable gets updated and so does the
// dependency in the partition.
TEST_F(PartitionedRocksDBStoreTest, PartitionsDependencyTest) {
  auto latest_partition = store_->getLatestPartition();
  auto metadata_cf_holder = store_->getMetadataCFPtr();
  auto rnd = [] { return folly::Random::rand64(); };
  auto prev_metadata_flush_token = FlushToken_INVALID;
  for (auto i = 1; i <= 1000; ++i) {
    put({TestRecord(logid_t(1), i)});
    auto dependency = latest_partition->cf_->dependentMemtableFlushToken();
    EXPECT_NE(dependency, FlushToken_INVALID);
    EXPECT_EQ(dependency, metadata_cf_holder->activeMemtableFlushToken());
    EXPECT_TRUE(prev_metadata_flush_token == FlushToken_INVALID ||
                prev_metadata_flush_token < dependency);
    if (rnd() % 4 == 0) {
      prev_metadata_flush_token =
          metadata_cf_holder->activeMemtableFlushToken();
      store_->flushMetadataMemtables();
    }
  }
}

// Makes sure memory stats returned for active memtable reach a given target
// usage. Target usage should not include partitions that will be flushed
// because of max memtable trigger. Memory usage of all other partitions will
// sum to target usage.  This class also randomizes the flush trigger according
// to given discrete distribution amongst set of triggers.
class MemTableStatsGenerator {
 public:
  using Partition = PartitionedRocksDBStore::Partition;
  using PartitionPtr = PartitionedRocksDBStore::PartitionPtr;
  using RocksDBMemTableStats = PartitionedRocksDBStore::RocksDBMemTableStats;
  using CFData = PartitionedRocksDBStore::FlushEvaluator::CFData;
  using PartitionList = PartitionedRocksDBStore::PartitionList;
  enum class FlushTrigger : int {
    NONE,
    ZERO_SIZE,
    ABOVE_MAX_SIZE,
    IDLE,
    OLD_DATA
  };

  MemTableStatsGenerator(
      SystemTimestamp start_time,
      uint64_t num_partitions,
      std::vector<uint8_t> trigger_pc,
      uint64_t max_memtable_size,
      uint64_t target_usage,
      folly::Function<void(PartitionPtr, RocksDBMemTableStats)> cb = nullptr)
      : start_ts_(start_time),
        num_partitions_(num_partitions),
        max_memtable_size_(max_memtable_size),
        total_non_max_usage_(target_usage),
        cb_(std::move(cb)) {
    ld_check(trigger_pc.size() == 5);
    auto total = 0;
    auto partition_account = 0;
    // Distribution of triggers according to given percentages within all
    // partitions.
    for (auto val : trigger_pc) {
      total += val;
      int num_partitions_with_trigger = num_partitions_ * (val / 100.0);
      partition_account += num_partitions_with_trigger;
      trigger_values_.push_back(num_partitions_with_trigger);
    }

    ld_check(total == 100);
    ld_check(partition_account <= num_partitions_);
    if (partition_account < num_partitions_) {
      trigger_values_[rng_() % trigger_values_.size()] +=
          num_partitions_ - partition_account;
    }
  }

  // Randomly select a trigger and get the necessary final distribution.
  FlushTrigger getTrigger() {
    std::vector<uint8_t> possible_triggers;
    for (int i = 0; i < trigger_values_.size(); ++i) {
      if (trigger_values_[i] > 0) {
        possible_triggers.push_back(i);
      }
    }
    ld_check(possible_triggers.size() > 0);

    auto selected = possible_triggers[rng_() % possible_triggers.size()];
    trigger_values_[selected]--;
    return static_cast<FlushTrigger>(selected);
  }

  RocksDBMemTableStats getMemTableStats(PartitionPtr partition) {
    auto avg_non_max_memtable_size = total_non_max_usage_ / num_partitions_;
    auto active_memtable_size = 0;

    ld_debug(
        "avg:%lu rolled:%ld", avg_non_max_memtable_size, rolled_over_usage_);
    if (partition_num_ == 0 && rng_() % 2) {
      // For first partition to go beyond average and some fudge factor this
      // makes it unbiased.
      active_memtable_size =
          avg_non_max_memtable_size + rng_() % avg_non_max_memtable_size;
    } else if (partition_num_ + 1 == num_partitions_) {
      // Transfer everything to last partition.
      ld_check(rolled_over_usage_ >= 0 ||
               avg_non_max_memtable_size >= -1 * rolled_over_usage_);
      active_memtable_size = avg_non_max_memtable_size + rolled_over_usage_;
    } else {
      active_memtable_size =
          rng_() % (avg_non_max_memtable_size + rolled_over_usage_);
    }

    ++partition_num_;
    auto immutable_memtable_size = rng_() % avg_non_max_memtable_size;
    RocksDBMemTableStats stats{static_cast<uint64_t>(active_memtable_size),
                               immutable_memtable_size,
                               0};

    FlushTrigger result = getTrigger();
    switch (result) {
      case FlushTrigger::NONE:
      case FlushTrigger::IDLE:
      case FlushTrigger::OLD_DATA: {
        auto& mem_size = stats.active_memtable_size;
        // Make sure the size is non-zero.
        mem_size = mem_size == 0 ? 1 : mem_size;
        // Randomize the dirtied time for partitions.
        auto new_dirtied_time = start_ts_ + std::chrono::minutes{rng_() % 15};
        partition->cf_->first_dirtied_time_ =
            SteadyTimestamp(new_dirtied_time.time_since_epoch());
      } break;
      case FlushTrigger::ABOVE_MAX_SIZE:
        stats.active_memtable_size = max_memtable_size_;
        break;
      case FlushTrigger::ZERO_SIZE:
        stats.active_memtable_size = active_memtable_size = 0;
        break;
    }

    if (result != FlushTrigger::ABOVE_MAX_SIZE) {
      // In this case memtable_size is fixed and does not account towards
      // total_non_max_usage_.
      if (active_memtable_size < avg_non_max_memtable_size) {
        rolled_over_usage_ += avg_non_max_memtable_size - active_memtable_size;
      } else {
        rolled_over_usage_ -= active_memtable_size - avg_non_max_memtable_size;
      }
    }

    ld_info("FlushTrigger result:%d active:%lu imm:%lu avg:%lu rolled:%ld",
            static_cast<int>(result),
            stats.active_memtable_size,
            stats.immutable_memtable_size,
            avg_non_max_memtable_size,
            rolled_over_usage_);

    if (cb_) {
      cb_(partition, stats);
    }

    return stats;
  }

  std::vector<CFData> genStatsList(PartitionList& partitions) {
    std::vector<CFData> cf_data;
    cf_data.reserve(partitions->size());
    for (auto& p : *partitions) {
      p->cf_->first_dirtied_time_ = SteadyTimestamp::now();
      CFData cf = {p->cf_, getMemTableStats(p), SteadyTimestamp::now()};
      cf_data.push_back(std::move(cf));
    }
    return cf_data;
  }

  CFData genMetadataStats(RocksDBCFPtr& cf) {
    return {cf, RocksDBMemTableStats(), SteadyTimestamp::now()};
  }

  // Used to create idle partitions.
  SystemTimestamp start_ts_;
  // Number of times getMemTableStats will be called.
  uint64_t num_partitions_;
  // Discrete distribution. Helps in distribution flush triggers.
  // Eg: { 50, 20, 20, 5, 5},
  // 50 % memtables should be of type NONE.
  // 20 % memtables should be zero sized.
  // 20 % memtables should be above max memtable size.
  // 5 % memtables satisfy idle trigger.
  // 5 % memtables satisfy old data age trigger.
  std::vector<uint32_t> trigger_values_;
  folly::ThreadLocalPRNG rng_;
  // Max memtable size usually close half of entire store budget.
  // It should be way greater than total
  uint64_t max_memtable_size_;
  // Total usage of memtables which are less than max_memtable_size.
  // This value plus number of memtables above max_memtable_size gives value of
  // total_memory_usage before flush.
  uint64_t total_non_max_usage_;
  // Usage for non max memtables which was used extra or unused from previous
  // iteration.
  int64_t rolled_over_usage_{0};
  // Counts which partition is getting processed currently.
  uint64_t partition_num_{0};
  // Allows tests to get the stats returned to evaluator.
  folly::Function<void(PartitionPtr, RocksDBMemTableStats)> cb_;
};

TEST_F(PartitionedRocksDBStoreTest, PartitionFlushSimpleTests) {
  updateSetting("rocksdb-partition-data-age-flush-trigger", "0");
  updateSetting("rocksdb-partition-idle-flush-trigger", "0");
  using FlushEvaluator = PartitionedRocksDBStore::FlushEvaluator;
  using CFData = FlushEvaluator::CFData;
  auto max_memtable_size = 200;
  auto target = 100;
  auto total_budget = 100;
  // Mark the partition as dirtied so that it is selected for flushing.
  auto latest_partition = store_->getLatestPartition();
  latest_partition->cf_->first_dirtied_time_ = SteadyTimestamp::now();
  SCOPE_EXIT {
    auto partitions = store_->getPartitionList();
    for (auto& partition : *partitions) {
      partition->cf_->first_dirtied_time_ = SteadyTimestamp::min();
    }
  };

  auto get_candidates_and_evaluate = [this](MemTableStatsGenerator& generator,
                                            FlushEvaluator& evaluator) {
    auto partitions = store_->getPartitionList();
    auto metadata_cf = store_->getMetadataCFPtr();
    auto flush_candidates = generator.genStatsList(partitions);
    CFData metadata_cf_data = generator.genMetadataStats(metadata_cf);
    return evaluator.pickCFsToFlush(
        store_->currentSteadyTime(), metadata_cf_data, flush_candidates);
  };

  auto partitions = store_->getPartitionList();
  auto cf_id = partitions->get(ID0)->cf_->getID();
  auto shard_idx = store_->getShardIdx();
  ASSERT_EQ(partitions->size(), 1u);
  // Single partition of non-zero size but below total store budget hence no
  // partitions should be selected for flush.
  {
    MemTableStatsGenerator generator(time_,
                                     1,
                                     {100, 0, 0, 0, 0},
                                     max_memtable_size,
                                     50u /*should be less than total_budget */);
    FlushEvaluator evaluator(shard_idx,
                             total_budget,
                             max_memtable_size,
                             total_budget,
                             store_->getRocksDBLogStoreConfig());
    auto cf_data = get_candidates_and_evaluate(generator, evaluator);
    auto buf_stats = evaluator.getBufStats();
    ASSERT_TRUE(cf_data.empty());
    ASSERT_LT(0u, buf_stats.active_memory_usage);
  }

  // Single partition less than max memtable size selected because usage is
  // beyond total limit.
  {
    MemTableStatsGenerator generator(
        time_, 1, {100, 0, 0, 0, 0}, max_memtable_size, 150u /* target */);
    FlushEvaluator evaluator(shard_idx,
                             total_budget,
                             max_memtable_size,
                             total_budget,
                             store_->getRocksDBLogStoreConfig());
    auto cf_data = get_candidates_and_evaluate(generator, evaluator);
    auto buf_stats = evaluator.getBufStats();
    ASSERT_FALSE(cf_data.empty());
    ASSERT_EQ(cf_id, cf_data[0].cf->getID());
    ASSERT_EQ(0u, buf_stats.active_memory_usage);
    ASSERT_LT(0u, buf_stats.memory_being_flushed);
  }

  // Single partition above max_memtable_size is selected for flushing.
  {
    MemTableStatsGenerator generator(
        time_, 1, {0, 0, 100, 0, 0}, max_memtable_size, target);
    FlushEvaluator evaluator(shard_idx,
                             total_budget,
                             max_memtable_size,
                             total_budget,
                             store_->getRocksDBLogStoreConfig());
    auto cf_data = get_candidates_and_evaluate(generator, evaluator);
    auto buf_stats = evaluator.getBufStats();
    ASSERT_FALSE(cf_data.empty());
    ASSERT_EQ(cf_id, cf_data[0].cf->getID());
    ASSERT_EQ(0u, buf_stats.active_memory_usage);
    ASSERT_LT(0u, buf_stats.memory_being_flushed);
  }
}

// Latest two partitions have different policy compared to other older
// partitions. Run simple tests for them.
TEST_F(PartitionedRocksDBStoreTest, FlushLatestPartitionsTest) {
  updateSetting("rocksdb-partition-data-age-flush-trigger", "0");
  updateSetting("rocksdb-partition-idle-flush-trigger", "0");
  using PartitionPtr = PartitionedRocksDBStore::PartitionPtr;
  using RocksDBMemTableStats = PartitionedRocksDBStore::RocksDBMemTableStats;
  using FlushEvaluator = PartitionedRocksDBStore::FlushEvaluator;
  using CFData = FlushEvaluator::CFData;
  auto max_memtable_size = 150;
  auto target = 100;
  auto total_budget = 200;
  SCOPE_EXIT {
    auto partitions = store_->getPartitionList();
    for (auto& partition : *partitions) {
      partition->cf_->first_dirtied_time_ = SteadyTimestamp::min();
    }
  };
  // Mark the partition as dirtied so that it is selected for flushing.
  auto latest_partition = store_->getLatestPartition();
  auto shard_idx = store_->getShardIdx();
  latest_partition->cf_->first_dirtied_time_ = SteadyTimestamp::now();
  // Create another partition so that we have two partitions for the test.
  latest_partition = store_->createPartition();
  latest_partition->cf_->first_dirtied_time_ = SteadyTimestamp::now();
  ASSERT_EQ(2u, store_->getPartitionList()->size());
  auto get_candidates_and_evaluate = [this](MemTableStatsGenerator& generator,
                                            FlushEvaluator& evaluator) {
    auto partitions = store_->getPartitionList();
    auto metadata_cf = store_->getMetadataCFPtr();
    auto flush_candidates = generator.genStatsList(partitions);
    CFData metadata_cf_data = generator.genMetadataStats(metadata_cf);
    return evaluator.pickCFsToFlush(
        store_->currentSteadyTime(), metadata_cf_data, flush_candidates);
  };
  // Simple test with non-zero memtable size for partition but none of them get
  // selected for flush because memory budget is way higher than sum of size of
  // the memtables.
  {
    MemTableStatsGenerator generator(
        time_, 2, {100, 0, 0, 0, 0}, max_memtable_size, target);
    FlushEvaluator evaluator(shard_idx,
                             total_budget,
                             max_memtable_size,
                             total_budget,
                             store_->getRocksDBLogStoreConfig());
    auto cf_data = get_candidates_and_evaluate(generator, evaluator);
    ASSERT_TRUE(cf_data.empty());
  }

  // Test where all latest partitions have memtable size greater than
  // max_allowed leading to flush.
  {
    MemTableStatsGenerator generator(
        time_, 2, {0, 0, 100, 0, 0}, max_memtable_size, target);
    FlushEvaluator evaluator(shard_idx,
                             total_budget,
                             max_memtable_size,
                             total_budget,
                             store_->getRocksDBLogStoreConfig());
    auto cf_data = get_candidates_and_evaluate(generator, evaluator);
    auto buf_stats = evaluator.getBufStats();
    ASSERT_FALSE(cf_data.empty());
    ASSERT_EQ(2u, cf_data.size());
    ASSERT_LT(0u, buf_stats.memory_being_flushed);
    ASSERT_EQ(0u, buf_stats.active_memory_usage);
  }

  // NOTE: last 2 tests actually test the old flush logic, so disable the
  // heuristic.
  updateSetting("rocksdb-use-age-size-flush-heuristic", "false");
  // Test where single partition of two latest partition is selected for
  // flushing.
  {
    auto total_budget_override = 100;
    auto partition_flushed = 0;
    RocksDBMemTableStats max_stats;
    // Current policy selects maximum partition of the two.
    auto cb = [&partition_flushed, &max_stats](
                  PartitionPtr partition, RocksDBMemTableStats stats) {
      if (stats.active_memtable_size > max_stats.active_memtable_size) {
        max_stats = stats;
        partition_flushed = partition->cf_->getID();
      }
    };

    MemTableStatsGenerator generator(
        time_,
        2,
        {100, 0, 0, 0, 0},
        max_memtable_size,
        total_budget_override +
            10, // Setting this to total_budget will create two
                // memtables with one of them bigger than other.
        std::move(cb));
    FlushEvaluator evaluator(shard_idx,
                             total_budget_override,
                             max_memtable_size,
                             total_budget_override,
                             store_->getRocksDBLogStoreConfig());
    auto cf_data = get_candidates_and_evaluate(generator, evaluator);
    auto buf_stats = evaluator.getBufStats();
    ASSERT_FALSE(cf_data.empty());
    ASSERT_EQ(1u, cf_data.size());
    ASSERT_EQ(partition_flushed, cf_data[0].cf->getID());
    ASSERT_NE(0u, buf_stats.memory_being_flushed);
    ASSERT_GE(buf_stats.memory_being_flushed, max_stats.active_memtable_size);
  }

  // Test where all partitions of two latest partition are selected for
  // flushing.
  {
    auto total_budget_override = 100;
    auto partition_flushed = 0;
    RocksDBMemTableStats max_stats;
    // Current policy selects maximum partition of the two.
    auto cb = [&partition_flushed, &max_stats](
                  PartitionPtr partition, RocksDBMemTableStats stats) {
      if (stats.active_memtable_size > max_stats.active_memtable_size) {
        max_stats = stats;
        partition_flushed = partition->cf_->getID();
      }
    };

    MemTableStatsGenerator generator(time_,
                                     2,
                                     {100, 0, 0, 0, 0},
                                     max_memtable_size,
                                     total_budget_override + 10,
                                     std::move(cb));
    FlushEvaluator evaluator(
        shard_idx,
        total_budget_override,
        max_memtable_size,
        0, // Setting target to zero so that all partitions are selected.
        store_->getRocksDBLogStoreConfig());
    auto cf_data = get_candidates_and_evaluate(generator, evaluator);
    ASSERT_FALSE(cf_data.empty());
    ASSERT_EQ(2u, cf_data.size());
    ASSERT_EQ(partition_flushed, cf_data[0].cf->getID());
  }
}

// Work with four 4 partitions and select the right partitions to flush.
TEST_F(PartitionedRocksDBStoreTest,
       PickFlushCandidatesFromOldAndNewPartitions) {
  updateSetting("rocksdb-partition-data-age-flush-trigger", "0");
  updateSetting("rocksdb-partition-idle-flush-trigger", "0");
  setTime(100);
  using PartitionPtr = PartitionedRocksDBStore::PartitionPtr;
  using RocksDBMemTableStats = PartitionedRocksDBStore::RocksDBMemTableStats;
  using FlushEvaluator = PartitionedRocksDBStore::FlushEvaluator;
  using CFData = FlushEvaluator::CFData;
  auto max_memtable_size = 150;
  auto target = 100;
  auto total_budget = 200;
  auto partition = store_->getLatestPartition();
  SCOPE_EXIT {
    auto partitions = store_->getPartitionList();
    for (auto& p : *partitions) {
      p->cf_->first_dirtied_time_ = SteadyTimestamp::min();
    }
  };
  // Mark the partition as dirtied so that it is selected for flushing.
  partition->cf_->first_dirtied_time_ = SteadyTimestamp::now();
  auto create_partition = [this]() {
    auto part = store_->createPartition();
    // Mark the partition as dirtied so that it is selected for flushing.
    part->cf_->first_dirtied_time_ = SteadyTimestamp::now();
  };

  auto get_candidates_and_evaluate = [this](MemTableStatsGenerator& generator,
                                            FlushEvaluator& evaluator) {
    auto partitions = store_->getPartitionList();
    auto metadata_cf = store_->getMetadataCFPtr();
    auto flush_candidates = generator.genStatsList(partitions);
    CFData metadata_cf_data = generator.genMetadataStats(metadata_cf);
    return evaluator.pickCFsToFlush(
        store_->currentSteadyTime(), metadata_cf_data, flush_candidates);
  };
  // Create another partition so that we have more than two partitions for the
  // test.
  create_partition();
  create_partition();
  create_partition();
  auto partitions = store_->getPartitionList();
  ASSERT_EQ(4u, partitions->size());
  auto shard_idx = store_->getShardIdx();

  // Test with few partitions at max_memtable_size.
  {
    MemTableStatsGenerator generator(
        time_, 4, {25, 0, 75, 0, 0}, max_memtable_size, target);
    FlushEvaluator evaluator(shard_idx,
                             total_budget,
                             max_memtable_size,
                             total_budget,
                             store_->getRocksDBLogStoreConfig());
    auto cf_data = get_candidates_and_evaluate(generator, evaluator);
    ASSERT_FALSE(cf_data.empty());
    // Atleast 1 partition should be selected for flushing.
    ASSERT_LE(1u, cf_data.size());
  }

  // Test in multiple partitions selected for flush. The partitions with oldest
  // data is selected amongst older partition.
  {
    // 2 of 4 are considered old partitions.
    auto partition_counter = 2;
    SteadyTimestamp min_dirtied_time{SteadyTimestamp::max()};
    SteadyTimestamp second_min{SteadyTimestamp::min()};
    auto cb = [&partition_counter, &min_dirtied_time, &second_min](
                  PartitionPtr partition, RocksDBMemTableStats /* stats */) {
      if (partition_counter > 0) {
        partition_counter--;
        min_dirtied_time.storeMin(partition->cf_->first_dirtied_time_);
        second_min.storeMax(partition->cf_->first_dirtied_time_);
      }
    };
    MemTableStatsGenerator generator(
        time_, 4, {100, 0, 0, 0, 0}, 1000, total_budget + 10, std::move(cb));
    FlushEvaluator evaluator(shard_idx,
                             total_budget,
                             1000,
                             total_budget / 2,
                             store_->getRocksDBLogStoreConfig());
    auto cf_data = get_candidates_and_evaluate(generator, evaluator);
    // Make sure `age * size` is sorted correctly.
    // Relative time should have same compare result regardless of now.
    const auto time_now = SteadyTimestamp::now();

    for (int i = 1; i < cf_data.size(); ++i) {
      const auto age_a = time_now - cf_data[i - 1].cf->first_dirtied_time_;
      const auto age_b = time_now - cf_data[i].cf->first_dirtied_time_;
      ASSERT_GT(cf_data[i - 1].stats.active_memtable_size * age_a,
                cf_data[i].stats.active_memtable_size * age_b);
    }
  }
  {
    // NOTE: This came from the original test. Disable flush heuristic and make
    // sure sorting ordering is correct.
    updateSetting("rocksdb-use-age-size-flush-heuristic", "false");
    // 2 of 4 are considered old partitions.
    auto partition_counter = 2;
    SteadyTimestamp min_dirtied_time{SteadyTimestamp::max()};
    SteadyTimestamp second_min{SteadyTimestamp::min()};
    auto cb = [&partition_counter, &min_dirtied_time, &second_min](
                  PartitionPtr partition, RocksDBMemTableStats /* stats */) {
      if (partition_counter > 0) {
        partition_counter--;
        min_dirtied_time.storeMin(partition->cf_->first_dirtied_time_);
        second_min.storeMax(partition->cf_->first_dirtied_time_);
      }
    };
    MemTableStatsGenerator generator(
        time_, 4, {100, 0, 0, 0, 0}, 1000, total_budget + 10, std::move(cb));
    FlushEvaluator evaluator(shard_idx,
                             total_budget,
                             1000,
                             total_budget / 2,
                             store_->getRocksDBLogStoreConfig());
    auto cf_data = get_candidates_and_evaluate(generator, evaluator);
    ASSERT_FALSE(cf_data.empty());
    ASSERT_TRUE(min_dirtied_time == cf_data[0].cf->first_dirtied_time_);
    if (cf_data.size() > 1) {
      ASSERT_TRUE(second_min == cf_data[1].cf->first_dirtied_time_);
    }
  }
}

// Make sure metadata cf is picked up correctly for flushing.
TEST_F(PartitionedRocksDBStoreTest, PickMetadataCFForFlush) {
  updateSetting("rocksdb-partition-data-age-flush-trigger", "0");
  updateSetting("rocksdb-partition-idle-flush-trigger", "0");
  using FlushEvaluator = PartitionedRocksDBStore::FlushEvaluator;
  using CFData = FlushEvaluator::CFData;
  using RocksDBMemTableStats = PartitionedRocksDBStore::RocksDBMemTableStats;
  uint64_t max_memtable_size =
      100000; // Too huge that this should not come in question.
  auto latest_partition = store_->getLatestPartition();
  auto latest_cf_id = latest_partition->cf_->getID();
  auto metadata_cf_holder = store_->getMetadataCFPtr();
  auto metadata_cf_id = metadata_cf_holder->getID();
  std::string data(100, 'a');
  put({TestRecord(logid_t(1), 1, 0, data)});
  CFData metadata_cf_data{metadata_cf_holder,
                          store_->getMemTableStats(metadata_cf_holder->get()),
                          SteadyTimestamp::now()};
  std::vector<CFData> partition_data{
      {latest_partition->cf_,
       store_->getMemTableStats(latest_partition->cf_->get()),
       SteadyTimestamp::now()}};
  auto shard_idx = store_->getShardIdx();
  {
    std::vector<CFData> partition_data{
        {latest_partition->cf_,
         store_->getMemTableStats(latest_partition->cf_->get()),
         SteadyTimestamp::now()}};
    // Single write should mark metadata cf dependent and should be selected
    // when flushing data cf.
    FlushEvaluator evaluator(
        shard_idx, 0, max_memtable_size, 0, store_->getRocksDBLogStoreConfig());
    auto cf_data = evaluator.pickCFsToFlush(
        store_->currentSteadyTime(), metadata_cf_data, partition_data);
    ASSERT_FALSE(cf_data.empty());
    ASSERT_EQ(cf_data.size(), 2u);
    ASSERT_EQ(cf_data[0].cf->getID(), metadata_cf_id);
    ASSERT_EQ(cf_data[1].cf->getID(), latest_cf_id);
  }
  {
    std::vector<CFData> partition_data{
        {latest_partition->cf_,
         store_->getMemTableStats(latest_partition->cf_->get()),
         SteadyTimestamp::now()}};
    // Flush metadata cf and after that data cf is no longer dependent. Make
    // sure this is the case. The data cf still should be selected for flushing
    store_->flushMetadataMemtables();
    metadata_cf_data.stats =
        store_->getMemTableStats(metadata_cf_holder->get());
    FlushEvaluator evaluator(
        shard_idx, 0, max_memtable_size, 0, store_->getRocksDBLogStoreConfig());
    auto cf_data = evaluator.pickCFsToFlush(
        store_->currentSteadyTime(), metadata_cf_data, partition_data);
    ASSERT_FALSE(cf_data.empty());
    ASSERT_EQ(1u, cf_data.size());
    ASSERT_EQ(cf_data[0].cf->getID(), latest_cf_id);
  }
  {
    std::vector<CFData> partition_data{
        {latest_partition->cf_,
         store_->getMemTableStats(latest_partition->cf_->get()),
         SteadyTimestamp::now()}};
    // Select metadata column family even though there is nothing else to flush.
    metadata_cf_data.stats = RocksDBMemTableStats{
        max_memtable_size + 1, max_memtable_size, max_memtable_size};
    FlushEvaluator evaluator(shard_idx,
                             10000000, // Make sure total budget is huge.
                             max_memtable_size,
                             10000000, // Make sure target is huge.
                             store_->getRocksDBLogStoreConfig());
    auto cf_data = evaluator.pickCFsToFlush(
        store_->currentSteadyTime(), metadata_cf_data, partition_data);
    ASSERT_FALSE(cf_data.empty());
    ASSERT_EQ(1u, cf_data.size());
    ASSERT_EQ(cf_data[0].cf->getID(), metadata_cf_id);
  }
}

// Write into the latest partition and flush the write buf.
TEST_F(PartitionedRocksDBStoreTest, FlushWriteBufTest) {
  updateSetting("rocksdb-partition-data-age-flush-trigger", "0");
  updateSetting("rocksdb-partition-idle-flush-trigger", "0");
  updateSetting("rocksdb-memtable-size-per-node", "10G");
  updateSetting("rocksdb-write-buffer-size", "20K");
  auto latest_partition = store_->getLatestPartition();
  auto metadata_cf_holder = store_->getMetadataCFPtr();
  auto stats_before = store_->getMemTableStats(latest_partition->cf_->get());
  std::string data(100, 'a');
  for (auto i = 1; i <= 1000; ++i) {
    put({TestRecord(logid_t(1), i, 0, data)});
    stats_before.active_memtable_size += data.size();
  }
  auto dependency = latest_partition->cf_->dependentMemtableFlushToken();
  EXPECT_NE(dependency, FlushToken_INVALID);
  EXPECT_EQ(dependency, metadata_cf_holder->activeMemtableFlushToken());
  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::FLUSH)
      .wait();
  auto stats = store_->getLastFlushEvalStats();
  EXPECT_EQ(0u, stats.active_memory_usage);
  EXPECT_GE(stats.memory_being_flushed, stats_before.active_memtable_size);
  EXPECT_EQ(
      latest_partition->cf_->activeMemtableFlushToken(), FlushToken_INVALID);
  EXPECT_EQ(metadata_cf_holder->activeMemtableFlushToken(), FlushToken_INVALID);
  EXPECT_EQ(
      latest_partition->cf_->dependentMemtableFlushToken(), FlushToken_INVALID);
}

TEST_F(PartitionedRocksDBStoreTest, RocksDBMemTableStatsTest) {
  auto latest_partition = store_->getLatestPartition();
  auto metadata_cf_holder = store_->getMetadataCFPtr();
  auto metadata_stats = store_->getMemTableStats(metadata_cf_holder->get());
  std::atomic<uint64_t> total_usage_{0};
  TestRocksDBMemTableRep::allocate_forwarder =
      [&total_usage_, id = latest_partition->cf_->getID()](
          uint32_t cf_id, const size_t len, char** /* buf */) {
        if (cf_id == id) {
          total_usage_ += len;
        }
        return nullptr;
      };
  std::string data(10, 'a');
  for (auto i = 1; i <= 400; ++i) {
    put({TestRecord(logid_t(1), i, BASE_TIME, data)});
  }
  auto stats_after = store_->getMemTableStats(latest_partition->cf_->get());
  auto metadata_stats_after =
      store_->getMemTableStats(metadata_cf_holder->get());
  ASSERT_LE(total_usage_, stats_after.active_memtable_size);
  ASSERT_LE(metadata_stats.active_memtable_size,
            metadata_stats_after.active_memtable_size);
}

TEST_F(PartitionedRocksDBStoreTest, DataKeyFormatConversion) {
  // Cases tested in each log:
  //  * Full record in old format: lsn 10.
  //  * Full record in new format: lsn 15.
  //  * Full record in old format + amend in new format: lsn 20.
  //  * Full record in new format + amend in old format: lsn 25.
  //  * Amend in old format: lsn 30.
  //  * Amend in new format: lsn 35.
  //  * Amend in old format + amend in new format: lsn 40.

  have_dangling_amends_ = true;
  logid_t data_log(1);
  logid_t meta_log = MetaDataLog::metaDataLogID(data_log);
  std::vector<StoreChainLink> old_format_cs = {
      StoreChainLink{ShardID(19, 0), ClientID::INVALID}};
  std::vector<StoreChainLink> new_format_cs = {
      StoreChainLink{ShardID(42, 0), ClientID::INVALID}};

  for (logid_t log : {data_log, meta_log}) {
    // Records in old format.
    put({TestRecord(log, 10),
         TestRecord(log, 20),
         TestRecord(log, 25).amend(),
         TestRecord(log, 30).amend(),
         TestRecord(log, 40).amend()},
        old_format_cs,
        false,
        DataKeyFormat::OLD);
    // Records in new format.
    put({TestRecord(log, 15),
         TestRecord(log, 20).amend(),
         TestRecord(log, 25),
         TestRecord(log, 35).amend(),
         TestRecord(log, 40).amend()},
        new_format_cs,
        false,
        DataKeyFormat::NEW);
  }

  auto stats = stats_.aggregate();
  EXPECT_EQ(0, stats.data_key_format_migration_steps);
  EXPECT_EQ(0, stats.data_key_format_migration_merges);

  auto check_record =
      [&](LocalLogStore::ReadIterator& it, lsn_t lsn, bool new_cs) {
        SCOPED_TRACE(std::to_string(lsn));
        ASSERT_EQ(lsn, it.getLSN());
        Slice record = it.getRecord();
        LocalLogStoreRecordFormat::flags_t flags;
        copyset_size_t cs_size;
        std::array<ShardID, COPYSET_SIZE_MAX> copyset;
        int rv = LocalLogStoreRecordFormat::parse(record,
                                                  nullptr,
                                                  nullptr,
                                                  &flags,
                                                  nullptr,
                                                  &cs_size,
                                                  &copyset[0],
                                                  copyset.size(),
                                                  nullptr,
                                                  nullptr,
                                                  nullptr,
                                                  THIS_SHARD);
        ASSERT_EQ(0, rv);
        ASSERT_EQ(1, cs_size);
        if (new_cs) {
          EXPECT_EQ(new_format_cs[0].destination, copyset[0]);
        } else {
          EXPECT_EQ(old_format_cs[0].destination, copyset[0]);
        }
      };

  auto check = [&](logid_t log, bool converted = false) {
    SCOPED_TRACE(std::to_string(log.val()));
    auto it = store_->read(
        log,
        LocalLogStore::ReadOptions(
            "PartitionedRocksDBStoreTest.DataKeyFormatConversion"));

    it->seek(5);
    ASSERT_EQ(IteratorState::AT_RECORD, it->state());
    check_record(*it, 10, false);

    it->next();
    ASSERT_EQ(IteratorState::AT_RECORD, it->state());
    check_record(*it, 15, true);

    it->next();
    ASSERT_EQ(IteratorState::AT_RECORD, it->state());
    check_record(*it, 20, !converted);

    it->next();
    ASSERT_EQ(IteratorState::AT_RECORD, it->state());
    check_record(*it, 25, !converted);

    it->next();
    EXPECT_EQ(IteratorState::AT_END, it->state());
  };

  {
    SCOPED_TRACE("called from here");
    check(data_log);
    check(meta_log);
  }

  stats = stats_.aggregate();
  EXPECT_EQ(6, stats.data_key_format_migration_steps);
  EXPECT_EQ(4, stats.data_key_format_migration_merges);

  auto data = readAndCheck();
  EXPECT_EQ(std::vector<lsn_t>({10, 15, 20, 20, 25, 25, 30, 35, 40, 40}),
            data.at(0).at(data_log).records);
  openStore();

  stats = stats_.aggregate();
  EXPECT_EQ(6, stats.data_key_format_migration_steps);
  EXPECT_EQ(4, stats.data_key_format_migration_merges);

  {
    SCOPED_TRACE("called from here");
    check(data_log);
    check(meta_log, true);
  }

  stats = stats_.aggregate();
  EXPECT_EQ(12, stats.data_key_format_migration_steps);
  EXPECT_EQ(6, stats.data_key_format_migration_merges);

  data = readAndCheck();
  EXPECT_EQ(std::vector<lsn_t>({10, 15, 20, 20, 25, 25, 30, 35, 40, 40}),
            data.at(0).at(data_log).records);
}

TEST_F(PartitionedRocksDBStoreTest, TestClampBacklog) {
  put({TestRecord(logid_t(1), 2)});
  store_->createPartition();
  setTime(BASE_TIME + HOUR * 2);
  store_->createPartition();
  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::LO_PRI)
      .wait();
  auto it = store_->read(logid_t(1),
                         LocalLogStore::ReadOptions(
                             "PartitionedRocksDBStoreTest.TestClampBacklog"));

  it->seek(1);
  ASSERT_EQ(IteratorState::AT_RECORD, it->state());
  EXPECT_EQ(2, it->getLSN());
  it.reset();

  updateSetting("rocksdb-test-clamp-backlog", "1h");
  store_
      ->backgroundThreadIteration(
          PartitionedRocksDBStore::BackgroundThreadType::LO_PRI)
      .wait();
  it = store_->read(logid_t(1),
                    LocalLogStore::ReadOptions(
                        "PartitionedRocksDBStoreTest.TestClampBacklog"));

  it->seek(1);
  ASSERT_EQ(IteratorState::AT_END, it->state());
}

// Test write throttle logic in throttleIOIfNeeded.
TEST_F(PartitionedRocksDBStoreTest, ThrottleWrites) {
  uint64_t memory_limit = 2000;
  double low_pri_write_stall_threshold =
      store_->getSettings()->low_pri_write_stall_threshold_percent / 100;
  ASSERT_GT(low_pri_write_stall_threshold, 1e-3);

  // active memory usage and unflushed memory usage below per shard limit, write
  // should not be throttled.
  {
    WriteBufStats stats;
    stats.active_memory_usage = 900;
    // memory_being_flushed = 0
    store_->throttleIOIfNeeded(stats, memory_limit);
    EXPECT_EQ(store_->getWriteThrottleState(),
              LocalLogStore::WriteThrottleState::NONE);
  }
  // unflushed memory usage is non-zero but the sum still does not go beyond per
  // shard limit.
  {
    WriteBufStats stats;
    stats.active_memory_usage = 500;
    stats.memory_being_flushed = 400;
    store_->throttleIOIfNeeded(stats, memory_limit);
    EXPECT_EQ(store_->getWriteThrottleState(),
              LocalLogStore::WriteThrottleState::NONE);
  }
  // unflushed + active memory usage goes beyond per shard limit but active
  // memory usage is still less than low_pri_write_stall_threshold_percent of
  // per shard limit.
  {
    WriteBufStats stats;
    stats.active_memory_usage = static_cast<uint64_t>(
        memory_limit / 2 * low_pri_write_stall_threshold * .9);
    stats.memory_being_flushed = 1000;
    store_->throttleIOIfNeeded(stats, memory_limit);
    EXPECT_EQ(store_->getWriteThrottleState(),
              LocalLogStore::WriteThrottleState::NONE);
  }
  // unflushed + active memory usage goes beyond per shard limit and active
  // memory usage is beyond low_pri_write_stall_threshold_percent of per shard
  // limit.
  {
    WriteBufStats stats;
    stats.active_memory_usage = static_cast<uint64_t>(
        memory_limit / 2 * low_pri_write_stall_threshold * 1.1);
    stats.memory_being_flushed = 1000;
    store_->throttleIOIfNeeded(stats, memory_limit);
    EXPECT_EQ(store_->getWriteThrottleState(),
              LocalLogStore::WriteThrottleState::STALL_LOW_PRI_WRITE);
  }
  // Reset throttle state for the store
  store_->throttleIOIfNeeded(WriteBufStats(), memory_limit);
  EXPECT_EQ(
      store_->getWriteThrottleState(), LocalLogStore::WriteThrottleState::NONE);
  // unflushed memory usage itself is equal to per shard limit.
  {
    WriteBufStats stats;
    stats.memory_being_flushed = memory_limit + 1;
    store_->throttleIOIfNeeded(stats, memory_limit);
    EXPECT_EQ(store_->getWriteThrottleState(),
              LocalLogStore::WriteThrottleState::REJECT_WRITE);
  }
  // Reset throttle state for the store
  store_->throttleIOIfNeeded(WriteBufStats(), memory_limit);
  EXPECT_EQ(
      store_->getWriteThrottleState(), LocalLogStore::WriteThrottleState::NONE);
  // unflushed memory usage + active memory usage is beyond per shard limit and
  // active memory is above per shard limit.
  {
    WriteBufStats stats;
    stats.active_memory_usage = memory_limit / 2;
    stats.memory_being_flushed = memory_limit - stats.active_memory_usage + 1;
    store_->throttleIOIfNeeded(stats, memory_limit);
    EXPECT_EQ(store_->getWriteThrottleState(),
              LocalLogStore::WriteThrottleState::REJECT_WRITE);
  }

  // Disable ld-managed flushes for the rest of the test.
  openStoreWithoutLDManagedFlushes();

  // When ld-managed flushes are disabled, we should never reject appends and
  // should stall low-pri writes during flushes.

  {
    WriteBufStats stats;
    stats.active_memory_usage = memory_limit * 10;
    store_->throttleIOIfNeeded(stats, memory_limit);
    EXPECT_EQ(store_->getWriteThrottleState(),
              LocalLogStore::WriteThrottleState::NONE);
  }

  {
    WriteBufStats stats;
    stats.pinned_buffer_usage = memory_limit * 10;
    store_->throttleIOIfNeeded(stats, memory_limit);
    EXPECT_EQ(store_->getWriteThrottleState(),
              LocalLogStore::WriteThrottleState::NONE);
  }

  {
    WriteBufStats stats;
    stats.memory_being_flushed = 1;
    store_->throttleIOIfNeeded(stats, memory_limit);
    EXPECT_EQ(store_->getWriteThrottleState(),
              LocalLogStore::WriteThrottleState::STALL_LOW_PRI_WRITE);
  }
}

TEST_F(PartitionedRocksDBStoreTest, FlushBlockPolicyTest) {
  std::vector<StoreChainLink> cs1 = {
      StoreChainLink{ShardID(0, 0), ClientID::INVALID},
      StoreChainLink{ShardID(1, 0), ClientID::INVALID}};
  std::vector<StoreChainLink> cs2 = {
      StoreChainLink{ShardID(0, 0), ClientID::INVALID},
      StoreChainLink{ShardID(2, 0), ClientID::INVALID}};

  closeStore();
  openStore({{"rocksdb-flush-block-policy", "each_log"},
             {"rocksdb-min-block-size", "16000"}});

  Stats stats = stats_.aggregate();
  EXPECT_EQ(0, stats.sst_record_blocks_written);
  EXPECT_EQ(0, stats.sst_record_blocks_bytes);

  lsn_t lsn = 1;
  auto write_and_flush = [&] {
    put({TestRecord(logid_t(1), lsn++)}, cs1);
    put({TestRecord(logid_t(1), lsn++)}, cs2);
    put({TestRecord(logid_t(2), lsn++)}, cs1);
    put({TestRecord(logid_t(2), lsn++)}, cs2);
    store_->flushAllMemtables();
  };

  // Everyone goes into the same block because it's smaller than
  // rocksdb-min-block-size.
  write_and_flush();
  stats = stats_.aggregate();
  EXPECT_EQ(1, stats.sst_record_blocks_written);
  EXPECT_GE(stats.sst_record_blocks_bytes, 40);
  EXPECT_LE(stats.sst_record_blocks_bytes, 400);
  stats_.reset();

  closeStore();
  openStore({{"rocksdb-flush-block-policy", "each_log"},
             {"rocksdb-min-block-size", "10"}});

  // Each log goes into its own block.
  write_and_flush();
  stats = stats_.aggregate();
  EXPECT_EQ(2, stats.sst_record_blocks_written);
  stats_.reset();

  closeStore();
  openStore({{"rocksdb-flush-block-policy", "each_copyset"},
             {"rocksdb-min-block-size", "10"}});

  // Each log+copyset goes into its own block.
  write_and_flush();
  stats = stats_.aggregate();
  EXPECT_EQ(4, stats.sst_record_blocks_written);
}

class PassAllReadFilter : public LocalLogStore::ReadFilter {
 public:
  bool operator()(logid_t,
                  lsn_t,
                  const ShardID*,
                  const copyset_size_t,
                  const csi_flags_t,
                  RecordTimestamp,
                  RecordTimestamp) override {
    return true;
  }
};

TEST_F(PartitionedRocksDBStoreTest,
       SeekBetweenPartitionsTouchesOnlyOnePartition) {
  put({TestRecord(logid_t(1), 10)});
  store_->createPartition();
  put({TestRecord(logid_t(1), 20)});

  auto it = store_->read(logid_t(1),
                         LocalLogStore::ReadOptions(
                             "PartitionedRocksDBStoreTest."
                             "SeekBetweenPartitionsTouchesOnlyOnePartition"));
  PassAllReadFilter filter;
  LocalLogStore::ReadStats stats;

  it->seek(15, &filter, &stats);
  EXPECT_EQ(IteratorState::AT_RECORD, it->state());
  EXPECT_EQ(1, stats.seen_logsdb_partitions);

  stats.seen_logsdb_partitions = 0;
  it->seek(25, &filter, &stats);
  EXPECT_EQ(IteratorState::AT_END, it->state());
  EXPECT_EQ(0, stats.seen_logsdb_partitions);
}
