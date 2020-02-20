/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/MetaDataLogTrimmer.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "logdevice/common/Sequencer.h"
#include "logdevice/common/test/TestUtil.h"

using namespace facebook::logdevice;
using testing::_;

namespace {

class MockMetaDataLogTrimmer : public MetaDataLogTrimmer {
 public:
  explicit MockMetaDataLogTrimmer(Sequencer* sequencer)
      : MetaDataLogTrimmer(sequencer) {}

  ~MockMetaDataLogTrimmer() override = default;

  MOCK_METHOD1(sendTrimRequest, void(lsn_t));

  using MetaDataLogTrimmer::trim;
};

class MockSequencer : public Sequencer {
 public:
  explicit MockSequencer(logid_t log_id, UpdateableSettings<Settings> settings)
      : Sequencer(log_id, settings, nullptr) {}

  std::shared_ptr<const NodeSetFinder::MetaDataExtrasMap>
  getMetaDataExtrasMap() const override {
    return extras_map_;
  }

  void setTrimPoint(lsn_t trim_point) {
    updateTrimPoint(E::OK, trim_point);
  }

  void addMetaDataExtras(uint32_t epoch_num, uint32_t esn_num) {
    auto epoch = epoch_t(epoch_num);
    auto lsn = compose_lsn(epoch, esn_t(esn_num));
    NodeSetFinder::MetaDataExtras extras = {
        lsn, std::chrono::milliseconds::zero()};
    extras_map_->insert({epoch, extras});
  }

  ~MockSequencer() override {}

 private:
  std::shared_ptr<NodeSetFinder::MetaDataExtrasMap> extras_map_ =
      std::make_shared<NodeSetFinder::MetaDataExtrasMap>();
};

class MetaDataLogTrimmerTest : public ::testing::Test {
 public:
  void SetUp() override {
    sequencer_ = std::make_unique<MockSequencer>(log_id_, updateable_settings_);
    trimmer_ = std::make_unique<MockMetaDataLogTrimmer>(sequencer_.get());
  }

  const logid_t log_id_{1};
  Settings settings_ = create_default_settings<Settings>();
  UpdateableSettings<Settings> updateable_settings_ =
      UpdateableSettings<Settings>(settings_);
  std::unique_ptr<MockSequencer> sequencer_;
  std::unique_ptr<MockMetaDataLogTrimmer> trimmer_;
};

TEST_F(MetaDataLogTrimmerTest, TrimSent) {
  lsn_t data_trim_point = compose_lsn(epoch_t(8), esn_t(27));
  lsn_t metadata_trim_point = compose_lsn(epoch_t(7), esn_t(11));
  sequencer_->setTrimPoint(data_trim_point);
  sequencer_->addMetaDataExtras(1, 17); // effective range [1, 7]
  sequencer_->addMetaDataExtras(7, 11); // effective range [7, 8]
  sequencer_->addMetaDataExtras(8, 24); // effective range [8, 10]
  sequencer_->addMetaDataExtras(10, 7); // effective range [10, max)
  EXPECT_CALL(*trimmer_, sendTrimRequest(metadata_trim_point)).Times(1);
  trimmer_->trim();
}

TEST_F(MetaDataLogTrimmerTest, NothingToTrim) {
  lsn_t data_trim_point = compose_lsn(epoch_t(9), esn_t(17));
  sequencer_->setTrimPoint(data_trim_point);
  sequencer_->addMetaDataExtras(8, 24); // effective range [8, 10]
  sequencer_->addMetaDataExtras(10, 7); // effective range [10, max)
  trimmer_->trim();
  EXPECT_CALL(*trimmer_, sendTrimRequest(_)).Times(0);
}

// Checks the case when current epoch number was introduced without adding
// record to metadata log
TEST_F(MetaDataLogTrimmerTest, EpochFromPast) {
  lsn_t data_trim_point = compose_lsn(epoch_t(4), esn_t(17));
  // We should not trim record for epoch 3 or we will end up with record
  // starting current epoch
  lsn_t metadata_trim_point = compose_lsn(epoch_t(1), esn_t(17));
  sequencer_->setTrimPoint(data_trim_point);
  sequencer_->addMetaDataExtras(1, 17); // effective range [1, 3]
  sequencer_->addMetaDataExtras(3, 11); // effective range [7, 8]
  sequencer_->addMetaDataExtras(8, 24); // effective range [8, 10]
  sequencer_->addMetaDataExtras(10, 7); // effective range [10, max)
  EXPECT_CALL(*trimmer_, sendTrimRequest(metadata_trim_point)).Times(1);
  trimmer_->trim();
}

TEST_F(MetaDataLogTrimmerTest, NoTrimPoint) {
  sequencer_->addMetaDataExtras(1, 17); // effective range [1, 10]
  sequencer_->addMetaDataExtras(10, 7); // effective range [10, max)
  trimmer_->trim();
  EXPECT_CALL(*trimmer_, sendTrimRequest(_)).Times(0);
}

TEST_F(MetaDataLogTrimmerTest, NoExtrasLoaded) {
  lsn_t data_trim_point = compose_lsn(epoch_t(4), esn_t(17));
  sequencer_->setTrimPoint(data_trim_point);
  trimmer_->trim();
  EXPECT_CALL(*trimmer_, sendTrimRequest(_)).Times(0);
}
} // namespace
