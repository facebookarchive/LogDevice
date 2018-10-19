/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/EpochMetaDataCache.h"

#include <cstdio>
#include <cstring>

#include <folly/Memory.h>
#include <gtest/gtest.h>

#include "logdevice/common/EpochMetaData.h"
#include "logdevice/common/MetaDataLogReader.h"

#define N3 ShardID(3, 0)
#define N4 ShardID(4, 0)
#define N5 ShardID(5, 0)

using namespace facebook::logdevice;

namespace {

using RecordSource = MetaDataLogReader::RecordSource;

class EpochMetaDataCacheTest : public ::testing::Test {
 public:
  static EpochMetaData genEpochMetaData(epoch_t epoch) {
    EpochMetaData m(StorageSet{N3, N4, N5},
                    ReplicationProperty(2, NodeLocationScope::NODE));
    m.h.epoch = m.h.effective_since = epoch;
    return m;
  }

  struct Result {
    epoch_t until;
    RecordSource source;
    EpochMetaData metadata;

    bool operator==(const Result& rhs) const {
      return until == rhs.until && source == rhs.source &&
          metadata == rhs.metadata;
    }
  };

  static constexpr logid_t LOG_ID{233};
  size_t capacity_ = 1024;
  std::unique_ptr<EpochMetaDataCache> cache_;
  Result result_;

  void setUp() {
    cache_ = std::make_unique<EpochMetaDataCache>(capacity_);
  }

  bool get(epoch_t epoch, bool require_consistent) {
    return cache_->getMetaData(LOG_ID,
                               epoch,
                               &result_.until,
                               &result_.metadata,
                               &result_.source,
                               require_consistent);
  }

  bool getNoPromotion(epoch_t epoch, bool require_consistent) {
    return cache_->getMetaDataNoPromotion(LOG_ID,
                                          epoch,
                                          &result_.until,
                                          &result_.metadata,
                                          &result_.source,
                                          require_consistent);
  }
};

constexpr logid_t EpochMetaDataCacheTest::LOG_ID;

TEST_F(EpochMetaDataCacheTest, BasicSetAndGet) {
  setUp();
  ASSERT_FALSE(get(epoch_t(1), true));
  ASSERT_FALSE(get(epoch_t(1), false));
  cache_->setMetaData(LOG_ID,
                      epoch_t(1),
                      epoch_t(10),
                      RecordSource::CACHED_CONSISTENT,
                      genEpochMetaData(epoch_t(1)));
  Result expected{epoch_t(10),
                  RecordSource::CACHED_CONSISTENT,
                  genEpochMetaData(epoch_t(1))};
  ASSERT_TRUE(get(epoch_t(1), true));
  ASSERT_EQ(expected, result_);
  ASSERT_TRUE(getNoPromotion(epoch_t(1), true));
  ASSERT_EQ(expected, result_);
  ASSERT_TRUE(get(epoch_t(1), false));
  ASSERT_EQ(expected, result_);
  ASSERT_TRUE(getNoPromotion(epoch_t(1), false));
  ASSERT_EQ(expected, result_);
}

TEST_F(EpochMetaDataCacheTest, ConsistencyRequirement) {
  setUp();
  // populate a `soft' entry
  cache_->setMetaData(LOG_ID,
                      epoch_t(1),
                      epoch_t(10),
                      RecordSource::CACHED_SOFT,
                      genEpochMetaData(epoch_t(1)));
  // should be a miss if consistent entry is required
  ASSERT_FALSE(get(epoch_t(1), true));
  ASSERT_FALSE(getNoPromotion(epoch_t(1), true));
  Result expected{
      epoch_t(10), RecordSource::CACHED_SOFT, genEpochMetaData(epoch_t(1))};
  ASSERT_TRUE(get(epoch_t(1), false));
  ASSERT_EQ(expected, result_);
  ASSERT_TRUE(getNoPromotion(epoch_t(1), false));
  ASSERT_EQ(expected, result_);
  // overwrite with a consistent one
  cache_->setMetaData(LOG_ID,
                      epoch_t(1),
                      epoch_t(20),
                      RecordSource::CACHED_CONSISTENT,
                      genEpochMetaData(epoch_t(1)));

  Result expected2{epoch_t(20),
                   RecordSource::CACHED_CONSISTENT,
                   genEpochMetaData(epoch_t(1))};
  ASSERT_TRUE(get(epoch_t(1), true));
  ASSERT_EQ(expected2, result_);
  ASSERT_TRUE(getNoPromotion(epoch_t(1), true));
  ASSERT_EQ(expected2, result_);
}

TEST_F(EpochMetaDataCacheTest, Overwrite) {
  setUp();
  // soft entry cannot overwrite consistent entry with the same epoch
  cache_->setMetaData(LOG_ID,
                      epoch_t(1),
                      epoch_t(20),
                      RecordSource::CACHED_CONSISTENT,
                      genEpochMetaData(epoch_t(1)));
  Result expected{epoch_t(20),
                  RecordSource::CACHED_CONSISTENT,
                  genEpochMetaData(epoch_t(1))};
  ASSERT_TRUE(get(epoch_t(1), false));
  ASSERT_EQ(expected, result_);
  cache_->setMetaData(LOG_ID,
                      epoch_t(1),
                      epoch_t(9999),
                      RecordSource::CACHED_SOFT,
                      genEpochMetaData(epoch_t(1)));
  ASSERT_TRUE(get(epoch_t(1), true));
  ASSERT_EQ(expected, result_);
  ASSERT_TRUE(get(epoch_t(1), false));
  ASSERT_EQ(expected, result_);
}

// TODO: add test(s) for eviction

} // namespace
