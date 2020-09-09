/**
 * Copyright (c) 2019-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/epoch_store/LogMetaDataCodec.h"

#include <gtest/gtest.h>

namespace facebook { namespace logdevice {

namespace {
TailRecord create_dummy_tail_record() {
  static logid_t logid(10);
  static lsn_t lsn(1001);

  logid = logid_t(logid.val() + 1);
  lsn++;

  TailRecordHeader hdr{
      logid,
      lsn,
      static_cast<uint64_t>(SystemTimestamp::now().toMilliseconds().count()),
      {BYTE_OFFSET_INVALID /* deprecated, offsets_map used instead */},
      TailRecordHeader::GAP,
      {}};

  OffsetMap map;
  map.setCounter(123, 321);
  map.setCounter(124, 322);

  return TailRecord(std::move(hdr), std::move(map), nullptr);
}
} // namespace

// A test that creates a LogMetaData, converts it to thrift and back to make
// sure that there's no information that gets lost during the conversion.
TEST(LogMetaDataCodecTest, Basic) {
  LogMetaData log_metadata;
  log_metadata.current_epoch_metadata =
      EpochMetaData({ShardID(0, 1), ShardID(10, 10), ShardID(100, 2)},
                    ReplicationProperty{{NodeLocationScope::NODE, 2}},
                    epoch_t(101),
                    epoch_t(100),
                    {0.1, 0.2, 0.3});
  log_metadata.current_epoch_metadata.setEpochIncrementAt();
  log_metadata.current_epoch_metadata.nodeset_params.seed = 123;
  log_metadata.current_epoch_metadata.nodeset_params.signature = 321;
  log_metadata.current_epoch_metadata.nodeset_params.target_nodeset_size = 30;
  log_metadata.epoch_store_properties.last_writer_node_id = NodeID(10, 2);
  log_metadata.data_last_clean_epoch = epoch_t(2);
  log_metadata.data_tail_record = create_dummy_tail_record();
  log_metadata.metadata_last_clean_epoch = epoch_t(2);
  log_metadata.metadata_tail_record = create_dummy_tail_record();
  log_metadata.version = LogMetaData::Version(10);
  log_metadata.last_changed_timestamp =
      SystemTimestamp::from(std::chrono::seconds(100000));

  auto thrift = LogMetaDataThriftConverter::toThrift(log_metadata);
  auto new_log_metadata = LogMetaDataThriftConverter::fromThrift(thrift);

  ASSERT_NE(nullptr, new_log_metadata);
  EXPECT_EQ(log_metadata, *new_log_metadata)
      << "Expected: " << log_metadata.toString()
      << ", got: " << new_log_metadata->toString();
}

}} // namespace facebook::logdevice
