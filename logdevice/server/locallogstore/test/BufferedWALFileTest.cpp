/**
 * Copyright (c) 2020-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <folly/Random.h>
#include <gtest/gtest.h>

#include "logdevice/common/test/TestUtil.h"
#include "logdevice/server/locallogstore/RocksDBEnv.h"

using namespace facebook::logdevice;

namespace {

constexpr size_t kAlignment = 128;

class MockFile : public rocksdb::WritableFile {
 public:
  std::atomic<uint64_t> size{0};
  std::atomic<uint64_t> flushedTo{0};
  std::atomic<uint64_t> rangeSyncedTo{0};
  std::atomic<uint64_t> syncedTo{0};
  std::string content;

  // Test options, set from outside this class.
  bool isSyncThreadSafe = false;
  bool appendsOnForegroundThread = false;
  bool syncsOnForegroundThread = false;
  bool expectAlignedAppends = true;
  std::chrono::microseconds appendMaxDelay{0};
  std::chrono::microseconds syncMaxDelay{0};
  std::atomic<bool> stallAppend{false};
  std::atomic<bool> stallSync{false};
  rocksdb::Status status;

  folly::ThreadLocal<bool> isForegroundThread;

  MockFile() {
    *isForegroundThread = true;
  }

  rocksdb::Status Append(const rocksdb::Slice& data) override {
    if (expectAlignedAppends) {
      EXPECT_EQ(0, reinterpret_cast<uint64_t>(data.data()) % kAlignment);
    }
    EXPECT_EQ(appendsOnForegroundThread, *isForegroundThread);
    while (stallAppend.load()) {
      std::this_thread::yield();
    }
    if (appendMaxDelay.count() > 0) {
      /* sleep override */
      std::this_thread::sleep_for(std::chrono::microseconds(
          folly::Random::rand64() % (appendMaxDelay.count() + 1)));
    }
    size += data.size();
    content.append(data.data(), data.size());
    return status;
  }

  rocksdb::Status Flush() override {
    EXPECT_EQ(appendsOnForegroundThread, *isForegroundThread);
    flushedTo.store(size.load());
    return status;
  }

  rocksdb::Status PositionedAppend(const rocksdb::Slice& /* data */,
                                   uint64_t /* offset */) override {
    ld_check(false);
    return status;
  }
  rocksdb::Status Truncate(uint64_t s) override {
    EXPECT_EQ(s, size);
    EXPECT_TRUE(*isForegroundThread);
    return status;
  }
  rocksdb::Status Close() override {
    EXPECT_TRUE(*isForegroundThread);
    return status;
  }
  rocksdb::Status Sync() override {
    EXPECT_EQ(syncsOnForegroundThread, *isForegroundThread);
    uint64_t s = flushedTo.load();
    while (stallSync.load()) {
      std::this_thread::yield();
    }
    if (syncMaxDelay.count() > 0) {
      /* sleep override */
      std::this_thread::sleep_for(std::chrono::microseconds(
          folly::Random::rand64() % (syncMaxDelay.count() + 1)));
    }
    syncedTo.store(s);
    return status;
  }

  rocksdb::Status Fsync() override {
    return Sync();
  }

  bool IsSyncThreadSafe() const override {
    return isSyncThreadSafe;
  }

  bool use_direct_io() const override {
    return false;
  }

  size_t GetRequiredBufferAlignment() const override {
    return kAlignment;
  }

  uint64_t GetFileSize() override {
    return size;
  }

  rocksdb::Status RangeSync(uint64_t offset, uint64_t nbytes) override {
    EXPECT_EQ(offset, rangeSyncedTo.load());
    EXPECT_LE(offset + nbytes, size.load());
    EXPECT_FALSE(*isForegroundThread);
    rangeSyncedTo.store(offset + nbytes);
    return status;
  }
};

class BufferedWALFileTest : public ::testing::Test {
 public:
  // Timer to kill the test if it takes too long
  Alarm alarm_{DEFAULT_TEST_TIMEOUT};

  std::unique_ptr<rocksdb::WritableFile> filePtr;
  MockFile* file; // points to filePtr, even after it's been moved out

  void createMockFile() {
    file = new MockFile();
    filePtr.reset(static_cast<rocksdb::WritableFile*>(file));
  }

  BufferedWALFileTest() {
    createMockFile();
  }
};

TEST_F(BufferedWALFileTest, Unbuffered) {
  file->isSyncThreadSafe = true;
  file->appendsOnForegroundThread = true;
  file->syncsOnForegroundThread = true;
  // The test calls Append() with unaligned buffers.
  file->expectAlignedAppends = false;

  // Use invalidly huge buffer size to make sure it's not allocated when
  // buffering is disabled.
  RocksDBBufferedWALFile f(std::move(filePtr),
                           nullptr,
                           FileTracingInfo{},
                           /* defer_writes */ false,
                           /* flush_eagerly */ false,
                           /* buffer_size */ 1ul << 60);

  // Append.
  EXPECT_TRUE(f.Append("pika").ok());
  EXPECT_EQ(4, file->size.load());
  EXPECT_EQ("pika", file->content);

  // Append.
  EXPECT_TRUE(f.Append("chu").ok());
  EXPECT_EQ(7, file->size.load());
  EXPECT_EQ("pikachu", file->content);
  EXPECT_EQ(7, f.GetFileSize());

  // Flush.
  EXPECT_EQ(0, file->flushedTo.load());
  EXPECT_TRUE(f.Flush().ok());
  EXPECT_EQ(7, file->flushedTo.load());

  // Range sync.
  EXPECT_EQ(0, file->rangeSyncedTo.load());
  EXPECT_TRUE(f.RangeSync(0, 6).ok());
  while (file->rangeSyncedTo.load() != 6) {
    std::this_thread::yield();
  }

  // Sync.
  EXPECT_EQ(0, file->syncedTo.load());
  EXPECT_TRUE(f.Sync().ok());
  EXPECT_EQ(7, file->syncedTo.load());

  // Append while syncing.
  EXPECT_TRUE(f.Append("-").ok());
  EXPECT_TRUE(f.Flush().ok());
  file->stallSync.store(true);
  std::atomic<bool> sync_done{false};
  std::thread t([&] {
    *file->isForegroundThread = true;
    EXPECT_TRUE(f.Sync().ok());
    sync_done.store(true);
  });
  /* sleep override */
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  EXPECT_TRUE(f.Append("choo").ok());
  EXPECT_TRUE(f.Flush().ok());
  EXPECT_TRUE(f.Append("chooo").ok());
  EXPECT_TRUE(f.Flush().ok());
  EXPECT_TRUE(f.RangeSync(6, 11).ok());
  EXPECT_EQ(17, file->size.load());
  EXPECT_EQ(17, file->flushedTo.load());
  EXPECT_EQ(7, file->syncedTo.load());
  EXPECT_FALSE(sync_done.load());
  file->stallSync.store(false);
  t.join();
  EXPECT_GE(file->syncedTo.load(), 8);

  // Finish up.

  EXPECT_TRUE(f.Sync().ok());
  EXPECT_EQ(17, file->syncedTo.load());

  EXPECT_EQ("pikachu-choochooo", file->content);

  EXPECT_TRUE(f.Truncate(17).ok());
  EXPECT_TRUE(f.Close().ok());
}

TEST_F(BufferedWALFileTest, BufferedEager) {
  RocksDBBufferedWALFile f(std::move(filePtr),
                           nullptr,
                           FileTracingInfo{},
                           /* defer_writes */ true,
                           /* flush_eagerly */ true,
                           /* buffer_size */ 5);

  // Append, shouldn't be flushed.
  EXPECT_TRUE(f.Append("lo").ok());
  /* sleep override */
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  EXPECT_EQ(0, file->size.load());

  // Flush, should go to file.
  EXPECT_TRUE(f.Flush().ok());
  while (file->flushedTo.load() != 2) {
    std::this_thread::yield();
  }

  // Append while file appends are stuck.
  file->stallAppend.store(true);
  EXPECT_TRUE(f.Append("rem").ok());
  EXPECT_TRUE(f.Flush().ok());
  /* sleep override */
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  // Now background thread is stuck on append. We should still be able to take
  // one buffer's worth of appends without stalling.
  for (int i = 0; i < 5; ++i) {
    EXPECT_TRUE(f.Append("-").ok());
    EXPECT_TRUE(f.Flush().ok());
  }
  EXPECT_EQ(2, file->flushedTo.load());
  file->stallAppend.store(false);
  while (file->flushedTo.load() != 10) {
    std::this_thread::yield();
  }

  // Sync.
  EXPECT_TRUE(f.Append("ips").ok());
  EXPECT_TRUE(f.Flush().ok());
  EXPECT_TRUE(f.Sync().ok());
  EXPECT_EQ(13, file->flushedTo.load());
  EXPECT_EQ(13, file->syncedTo.load());

  // Finish up.

  EXPECT_EQ("lorem-----ips", file->content);

  EXPECT_TRUE(f.Truncate(13).ok());
  EXPECT_TRUE(f.Close().ok());
}

TEST_F(BufferedWALFileTest, BufferedDelayed) {
  RocksDBBufferedWALFile f(std::move(filePtr),
                           nullptr,
                           FileTracingInfo{},
                           /* defer_writes */ true,
                           /* flush_eagerly */ false,
                           /* buffer_size */ 5);

  // Append and flush, shouldn't go to file.
  EXPECT_TRUE(f.Append("hel").ok());
  EXPECT_TRUE(f.Flush().ok());
  EXPECT_TRUE(f.Append("l").ok());
  EXPECT_TRUE(f.Flush().ok());
  /* sleep override */
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  EXPECT_EQ(0, file->size.load());

  // Another append, buffer gets full and is written out.
  EXPECT_TRUE(f.Append("o w").ok());
  EXPECT_TRUE(f.Flush().ok());
  while (file->flushedTo.load() != 5) {
    std::this_thread::yield();
  }
  EXPECT_EQ("hello", file->content);
  // buffer size: 2

  // Sync.
  EXPECT_EQ(0, file->syncedTo.load());
  EXPECT_TRUE(f.Sync().ok());
  EXPECT_EQ(7, file->syncedTo.load());
  // buffer size: 0

  // Range sync for unflushed range.
  EXPECT_TRUE(f.Append("or").ok());
  EXPECT_TRUE(f.Flush().ok());
  EXPECT_EQ(0, file->rangeSyncedTo.load());
  EXPECT_TRUE(f.RangeSync(0, 9).ok());
  while (file->rangeSyncedTo.load() != 7) {
    std::this_thread::yield();
  }
  // buffer size: 2

  // Append.
  EXPECT_TRUE(f.Append("ld!-").ok());
  EXPECT_TRUE(f.Flush().ok());
  while (file->rangeSyncedTo.load() != 9) {
    std::this_thread::yield();
  }
  // buffer size: 1

  // Finish up.

  EXPECT_TRUE(f.Close().ok());
  EXPECT_EQ(13, file->flushedTo.load());
  EXPECT_EQ("hello world!-", file->content);
}

TEST_F(BufferedWALFileTest, SyncAlreadyThreadSafe) {
  file->isSyncThreadSafe = true;
  file->syncsOnForegroundThread = true;

  RocksDBBufferedWALFile f(std::move(filePtr),
                           nullptr,
                           FileTracingInfo{},
                           /* defer_writes */ true,
                           /* flush_eagerly */ true,
                           /* buffer_size */ 3);

  // If underlying file's Sync() is thread safe, our appends during a slow
  // sync are not limited by buffer size.

  // Append.
  EXPECT_TRUE(f.Append("T").ok());
  EXPECT_TRUE(f.Flush().ok());

  // Start a sync that will get stuck.
  file->stallSync.store(true);
  std::atomic<bool> sync_done{false};
  std::thread t([&] {
    *file->isForegroundThread = true;
    EXPECT_TRUE(f.Sync().ok());
    sync_done.store(true);
  });
  /* sleep override */
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // Append way more than buffer size.
  const std::string str = "o be fair, you have to have a very high IQ to "
                          "understand Rick and Morty";
  EXPECT_TRUE(f.Append(str.c_str()).ok());
  EXPECT_TRUE(f.Flush().ok());

  // Do some random range sync for good measure.
  EXPECT_TRUE(f.RangeSync(0, 13).ok());

  // Make sure the appends actually made it to the file.
  while (file->flushedTo.load() != str.size() + 1 ||
         file->rangeSyncedTo.load() != 13) {
    std::this_thread::yield();
  }

  // Unstall the sync.
  EXPECT_FALSE(sync_done.load());
  EXPECT_EQ(0, file->syncedTo.load());
  file->stallSync.store(false);
  t.join();
  EXPECT_GE(file->syncedTo.load(), 1);

  EXPECT_TRUE(f.Close().ok());
}

TEST_F(BufferedWALFileTest, NonOkStatus) {
  RocksDBBufferedWALFile f(std::move(filePtr),
                           nullptr,
                           FileTracingInfo{},
                           /* defer_writes */ true,
                           /* flush_eagerly */ true,
                           /* buffer_size */ 3);

  // Error should propagate from background operation to the next foreground
  // operation.
  file->status = rocksdb::Status::Corruption("henlo");
  EXPECT_TRUE(f.Append("a").ok());
  EXPECT_TRUE(f.Flush().ok());
  auto s = f.Sync();
  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsCorruption());

  EXPECT_FALSE(f.Close().ok());
}

// Do random operations for a few seconds, with injected random latency in
// various places.
TEST_F(BufferedWALFileTest, Stress) {
  for (int eager = 0; eager < 2; ++eager) {
    SCOPED_TRACE(std::to_string(eager));

    for (int epoch = 0; epoch < 5; ++epoch) {
      SCOPED_TRACE(std::to_string(epoch));

      createMockFile();
      file->appendMaxDelay = std::chrono::microseconds(200 * epoch);
      file->syncMaxDelay = std::chrono::microseconds(200 * epoch * epoch);

      RocksDBBufferedWALFile f(std::move(filePtr),
                               nullptr,
                               FileTracingInfo{},
                               /* defer_writes */ true,
                               /* flush_eagerly */ eager,
                               /* buffer_size */ 16);
      std::atomic<bool> stop{false};
      std::string appended;
      size_t appends_done = 0;
      size_t flushes_done = 0;
      size_t range_syncs_done = 0;
      size_t syncs_done = 0;

      std::thread append_thread([&] {
        *file->isForegroundThread = true;
        std::string s;
        uint64_t flushed = 0;
        uint64_t range_synced = 0;
        while (!stop.load() && appended.size() < (1ul << 26)) {
          if (folly::Random::rand32() % 4 != 0) {
            // Append.

            s.resize(folly::Random::rand32() % 8 + 1);
            if (folly::Random::rand32() % 3 == 0) {
              s.resize(s.size() * 4);
            }
            for (char& c : s) {
              c = 'a' + folly::Random::rand32() % 26;
            }

            EXPECT_TRUE(f.Append(s).ok());
            appended += s;
            ++appends_done;
          } else if (folly::Random::rand32() % 3 != 0) {
            // Flush.

            EXPECT_TRUE(f.Flush().ok());
            flushed = appended.size();
            ++flushes_done;
          } else {
            // RangeSync.

            uint64_t n = flushed - range_synced;
            if (n != 0) {
              if (folly::Random::rand32() % 2 == 0) {
                n = folly::Random::rand64() % n + 1;
              }
              EXPECT_TRUE(f.RangeSync(range_synced, n).ok());
              range_synced += n;
              ++range_syncs_done;
            }
          }
        }
        EXPECT_TRUE(f.Flush().ok());
      });

      std::thread sync_thread([&] {
        *file->isForegroundThread = true;
        while (!stop.load()) {
          EXPECT_TRUE(f.Sync().ok());
          ++syncs_done;

          if (folly::Random::rand32() % 2 == 0) {
            /* sleep override */
            std::this_thread::sleep_for(std::chrono::microseconds(
                folly::Random::rand32() % 50 + epoch * 50));
          }
        }
      });

      /* sleep override */
      std::this_thread::sleep_for(std::chrono::seconds(2));
      stop.store(true);
      sync_thread.join();
      append_thread.join();

      EXPECT_TRUE(f.Close().ok());
      EXPECT_EQ(file->size.load(), file->flushedTo.load());
      EXPECT_EQ(appended, file->content);

      ld_info("Done %lu appends, %lu flushes, %lu range syncs, %lu syncs",
              appends_done,
              flushes_done,
              range_syncs_done,
              syncs_done);
      EXPECT_NE(0, appends_done);
      EXPECT_NE(0, flushes_done);
      EXPECT_NE(0, range_syncs_done);
      EXPECT_NE(0, syncs_done);
    }
  }
}

} // namespace
