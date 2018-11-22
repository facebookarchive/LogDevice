/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/LocalLogStoreRecordFormat.h"

#include <gtest/gtest.h>

#include "logdevice/common/RecordID.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/protocol/STORE_Message.h"

using namespace facebook::logdevice;

class LocalLogStoreRecordFormatTest
    : public ::testing::TestWithParam<
          std::tuple<bool, bool, bool, bool, bool, bool>> {
 public:
  LocalLogStoreRecordFormatTest() {
    dbg::assertOnData = true;
  }
};

TEST_P(LocalLogStoreRecordFormatTest, RoundTrip) {
  //
  // First test that formRecordHeader() works as expected
  //
  const std::chrono::milliseconds timestamp{496152000000};
  const esn_t last_known_good(100);
  const RecordID rid = {esn_t(111), epoch_t(222), logid_t(333)};
  const bool enable_offset = std::get<0>(GetParam());
  const bool written_by_recovery = std::get<1>(GetParam());
  const bool is_hole = std::get<2>(GetParam());
  const bool written_by_rebuilding = std::get<3>(GetParam());
  const bool shard_id_in_copyset = std::get<4>(GetParam());
  bool enable_offset_map = std::get<5>(GetParam());

  STORE_Header header;
  header.rid = rid;
  header.timestamp = timestamp.count();
  header.last_known_good = last_known_good;
  header.wave = 4321;
  header.flags = STORE_Header::CHECKSUM;
  STORE_Extra extra;

  if (enable_offset) {
    header.flags |= STORE_Header::OFFSET_WITHIN_EPOCH;
  }

  if (enable_offset_map) {
    header.flags |= STORE_Header::OFFSET_MAP;
  }

  if (written_by_recovery) {
    header.flags |= STORE_Header::RECOVERY;
    header.flags |= STORE_Header::WRITTEN_BY_RECOVERY;
    extra.recovery_epoch = epoch_t(1234);
  }

  if (written_by_rebuilding) {
    header.flags |= STORE_Header::REBUILDING;
  }

  if (is_hole) {
    header.flags |= STORE_Header::HOLE;
  }

  header.copyset_size = 2;
  OffsetMap om;
  om.setCounter(BYTE_OFFSET, 34);
  extra.offsets_within_epoch = om;

  // if shard_id_in_copyset is set, we use a different shard_index_t value for
  // each recipient.
  shard_index_t this_shard = 3;
  ShardID rec_1 = ShardID(88, shard_id_in_copyset ? this_shard : this_shard);
  ShardID rec_2 = ShardID(99, shard_id_in_copyset ? 64 : this_shard);
  const StoreChainLink copyset[] = {{rec_1, ClientID()}, {rec_2, ClientID()}};

  std::map<KeyType, std::string> optional_keys;
  optional_keys.insert(
      std::make_pair(KeyType::FINDKEY, std::string("1234567")));
  optional_keys.insert(
      std::make_pair(KeyType::FILTERABLE, std::string("abcd")));
  std::string buf;
  Slice header_blob = LocalLogStoreRecordFormat::formRecordHeader(
      header, copyset, &buf, shard_id_in_copyset, optional_keys, extra);

  size_t blob_size = 8 + 4 + 1 +
      4
      // varints encoded flags
      + 2
      // 8 bytes for offset within epoch
      + (enable_offset && !enable_offset_map ? 8 : 0)
      // 2 for optional_keys blob + 2 for optional_keys size + (1 for key type
      // + 2 for key length + 7 for key data) * 2
      + 2 + 2 + 1 + 2 + 7 + 1 + 2 +
      4
      // Copyset
      + 1 + 2 * (shard_id_in_copyset ? sizeof(ShardID) : sizeof(node_index_t)) +
      ((enable_offset && enable_offset_map)
           ? extra.offsets_within_epoch.sizeInLinearBuffer()
           : 0)
      // FLAG_OFFSET_MAP causes the `flag` varint to use one more byte
      + (enable_offset_map ? 1 : 0);

  ASSERT_EQ(blob_size, header_blob.size);

  //
  // Now check that we can parse it properly
  //

  const std::string blob_buf =
      std::string(
          reinterpret_cast<const char*>(header_blob.data), header_blob.size) +
      "data";
  Slice log_store_blob(blob_buf.data(), blob_buf.size());
  int rv;
  std::chrono::milliseconds timestamp_read;
  esn_t last_known_good_read;
  LocalLogStoreRecordFormat::flags_t flags;
  copyset_size_t copyset_size_read;
  std::map<KeyType, std::string> optional_keys_read;
  Payload payload_read;
  OffsetMap offsets_within_epoch_read;
  uint32_t wave_read;
  // If you don't know the copyset size in advance, you can pass in a null
  // copyset_arr_out on the first attempt ...
  rv = LocalLogStoreRecordFormat::parse(log_store_blob,
                                        &timestamp_read,
                                        &last_known_good_read,
                                        &flags,
                                        &wave_read,
                                        &copyset_size_read,
                                        nullptr,
                                        0,
                                        &offsets_within_epoch_read,
                                        &optional_keys_read,
                                        &payload_read,
                                        -1 /* unused */);

  ASSERT_EQ(0, rv);
  ASSERT_EQ(timestamp, timestamp_read);
  ASSERT_EQ(last_known_good, last_known_good_read);
  const LocalLogStoreRecordFormat::flags_t expected_flags =
      LocalLogStoreRecordFormat::FLAG_CHECKSUM |
      (shard_id_in_copyset ? LocalLogStoreRecordFormat::FLAG_SHARD_ID : 0) |
      (enable_offset ? LocalLogStoreRecordFormat::FLAG_OFFSET_WITHIN_EPOCH
                     : 0) |
      (written_by_recovery ? LocalLogStoreRecordFormat::FLAG_WRITTEN_BY_RECOVERY
                           : 0) |
      (is_hole ? LocalLogStoreRecordFormat::FLAG_HOLE : 0) |
      (written_by_rebuilding
           ? LocalLogStoreRecordFormat::FLAG_WRITTEN_BY_REBUILDING
           : 0) |
      LocalLogStoreRecordFormat::FLAG_CUSTOM_KEY |
      LocalLogStoreRecordFormat::FLAG_OPTIONAL_KEYS |
      (enable_offset_map ? LocalLogStoreRecordFormat::FLAG_OFFSET_MAP : 0);
  ASSERT_EQ(expected_flags, flags);
  ASSERT_EQ(2, copyset_size_read);
  ASSERT_EQ("data", payload_read.toString());
  if (written_by_recovery) {
    ASSERT_EQ(1234, wave_read);
  } else {
    ASSERT_EQ(4321, wave_read);
  }

  if (enable_offset) {
    ASSERT_EQ(om, offsets_within_epoch_read);
  } else {
    ASSERT_FALSE(offsets_within_epoch_read.isValid());
  }
  ASSERT_EQ(optional_keys.at(KeyType::FILTERABLE),
            optional_keys_read.at(KeyType::FILTERABLE));
  // Now get the copyset
  ShardID copyset_read[copyset_size_read];
  rv = LocalLogStoreRecordFormat::parse(log_store_blob,
                                        nullptr,
                                        nullptr,
                                        nullptr,
                                        nullptr,
                                        nullptr,
                                        copyset_read,
                                        copyset_size_read,
                                        nullptr,
                                        nullptr,
                                        nullptr,
                                        this_shard);

  ASSERT_EQ(0, rv);
  ASSERT_EQ(rec_1, copyset_read[0]);
  ASSERT_EQ(rec_2, copyset_read[1]);
}

TEST_P(LocalLogStoreRecordFormatTest, CSIRoundTrip) {
  //
  // First test that formCopySetIndexEntry() works as expected
  //
  const std::chrono::milliseconds timestamp{496152000000};
  const esn_t last_known_good(100);
  const RecordID rid = {esn_t(111), epoch_t(222), logid_t(333)};
  const bool is_hole = std::get<2>(GetParam());
  const bool written_by_rebuilding = std::get<3>(GetParam());
  const bool shard_id_in_copyset = std::get<4>(GetParam());

  STORE_Header header;
  header.rid = rid;
  header.timestamp = timestamp.count();
  header.last_known_good = last_known_good;
  header.wave = 4321;
  header.flags = STORE_Header::CHECKSUM;
  STORE_Extra extra;

  if (is_hole) {
    header.flags |= STORE_Header::HOLE;
  }

  if (written_by_rebuilding) {
    header.flags |= STORE_Header::REBUILDING;
  }

  header.copyset_size = 2;
  OffsetMap offsets_within_epoch;
  offsets_within_epoch.setCounter(BYTE_OFFSET, 34);
  extra.offsets_within_epoch = std::move(offsets_within_epoch);

  // if shard_id_in_copyset is set, we use a different shard_index_t value for
  // each recipient.
  shard_index_t this_shard = 3;
  ShardID rec_1 = ShardID(88, shard_id_in_copyset ? this_shard : this_shard);
  ShardID rec_2 = ShardID(99, shard_id_in_copyset ? 64 : this_shard);
  const StoreChainLink copyset[] = {{rec_1, ClientID()}, {rec_2, ClientID()}};

  std::map<KeyType, std::string> optional_keys;
  optional_keys.insert(
      std::make_pair(KeyType::FINDKEY, std::string("12345678")));
  std::string buf;
  Slice csi_blob = LocalLogStoreRecordFormat::formCopySetIndexEntry(
      header, STORE_Extra(), copyset, LSN_INVALID, shard_id_in_copyset, &buf);

  size_t blob_size = sizeof(header.wave) +
      sizeof(LocalLogStoreRecordFormat::csi_flags_t) + sizeof(copyset_size_t) +
      header.copyset_size *
          (shard_id_in_copyset ? sizeof(ShardID) : sizeof(node_index_t));
  ASSERT_EQ(blob_size, csi_blob.size);

  //
  // Now check that we can parse it properly
  //

  bool rv;
  LocalLogStoreRecordFormat::csi_flags_t flags;
  uint32_t wave_read = 0xffff;
  std::vector<ShardID> copyset_read;

  rv = LocalLogStoreRecordFormat::parseCopySetIndexSingleEntry(
      csi_blob, &copyset_read, &wave_read, &flags, this_shard);

  ASSERT_TRUE(rv);
  ASSERT_EQ(header.wave, wave_read);
  const LocalLogStoreRecordFormat::csi_flags_t expected_flags =
      (is_hole ? LocalLogStoreRecordFormat::CSI_FLAG_HOLE : 0) |
      (shard_id_in_copyset ? LocalLogStoreRecordFormat::CSI_FLAG_SHARD_ID : 0) |
      (written_by_rebuilding
           ? LocalLogStoreRecordFormat::CSI_FLAG_WRITTEN_BY_REBUILDING
           : 0);
  ASSERT_EQ(expected_flags, flags);
  ASSERT_EQ(2, copyset_read.size());
  ASSERT_EQ(rec_1, copyset_read[0]);
  ASSERT_EQ(rec_2, copyset_read[1]);
}

INSTANTIATE_TEST_CASE_P(LocalLogStoreRecordFormatTest,
                        LocalLogStoreRecordFormatTest,
                        ::testing::Combine(::testing::Bool(),
                                           ::testing::Bool(),
                                           ::testing::Bool(),
                                           ::testing::Bool(),
                                           ::testing::Bool(),
                                           ::testing::Bool()));
