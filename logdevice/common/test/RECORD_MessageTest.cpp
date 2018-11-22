/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/protocol/RECORD_Message.h"

#include <numeric>

#include <folly/Memory.h>
#include <folly/ScopeGuard.h>
#include <gtest/gtest.h>

#include "event2/buffer.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/libevent/compat.h"
#include "logdevice/common/protocol/ProtocolReader.h"
#include "logdevice/common/protocol/ProtocolWriter.h"
#include "logdevice/common/util.h"

namespace facebook { namespace logdevice {

static RECORD_Header create_test_header() {
  RECORD_Header header = {
      logid_t(333),
      read_stream_id_t(444),
      lsn_t(555),
      uint64_t(666), // timestamp
  };
  return header;
}

static ExtraMetadata::Header
create_test_extra_metadata_header(copyset_size_t copyset_size) {
  ExtraMetadata::Header header = {
      esn_t(999),
      uint32_t(1111),
      copyset_size,
  };
  return header;
}

class RECORD_MessageTest : public ::testing::Test {
 public:
  RECORD_MessageTest() {
    dbg::assertOnData = true;
  }
  // Takes advantage of friend declaration in RECORD_Message to expose header
  const RECORD_Header& getHeader(const RECORD_Message& msg) {
    return msg.header_;
  }

  ExtraMetadata* getExtraMetadata(const RECORD_Message& msg) {
    return msg.extra_metadata_.get();
  }
};

TEST_F(RECORD_MessageTest, SerializationNoExtraMetadata) {
  RECORD_Header header = create_test_header();
  RECORD_Message orig(
      header, TrafficClass::READ_TAIL, Payload(nullptr, 0), nullptr);
  struct evbuffer* evbuf = LD_EV(evbuffer_new)();
  SCOPE_EXIT {
    LD_EV(evbuffer_free)(evbuf);
  };

  ProtocolWriter writer(
      orig.type_, evbuf, Compatibility::MAX_PROTOCOL_SUPPORTED);
  orig.serialize(writer);
  ASSERT_GT(writer.result(), 0);

  ProtocolReader reader(MessageType::RECORD,
                        evbuf,
                        LD_EV(evbuffer_get_length)(evbuf),
                        Compatibility::MAX_PROTOCOL_SUPPORTED);
  auto read = checked_downcast<std::unique_ptr<RECORD_Message>>(
      RECORD_Message::deserialize(reader).msg);
  ASSERT_NE(read.get(), nullptr);

  RECORD_Header expected_header = header;
  expected_header.flags = 0;
  ASSERT_EQ(
      0,
      memcmp(
          &expected_header, &this->getHeader(*read), sizeof expected_header));
}

TEST_F(RECORD_MessageTest, SerializationWithExtraMetadata) {
  RECORD_Header header = create_test_header();
  header.flags |= RECORD_Header::INCLUDES_EXTRA_METADATA;
  header.flags |= RECORD_Header::INCLUDE_OFFSET_WITHIN_EPOCH;
  OffsetMap offsets_within_epoch;
  offsets_within_epoch.setCounter(BYTE_OFFSET, 3);
  OffsetMap byte_offsets;
  ExtraMetadata::Header meta_header = create_test_extra_metadata_header(3);
  std::vector<ShardID> meta_copyset{
      ShardID(4, 0), ShardID(5, 0), ShardID(9, 0)};
  ld_check(meta_copyset.size() == meta_header.copyset_size);

  auto orig_meta = std::make_unique<ExtraMetadata>();
  orig_meta->header = meta_header;
  orig_meta->copyset.assign(meta_copyset.begin(), meta_copyset.end());
  orig_meta->offsets_within_epoch = offsets_within_epoch;

  RECORD_Message orig(header,
                      TrafficClass::REBUILD,
                      Payload(nullptr, 0),
                      std::move(orig_meta),
                      RECORD_Message::Source::LOCAL_LOG_STORE,
                      byte_offsets);
  struct evbuffer* evbuf = LD_EV(evbuffer_new)();
  SCOPE_EXIT {
    LD_EV(evbuffer_free)(evbuf);
  };

  ProtocolWriter writer(
      orig.type_, evbuf, Compatibility::MAX_PROTOCOL_SUPPORTED);
  orig.serialize(writer);
  ASSERT_GT(writer.result(), 0);

  ProtocolReader reader(MessageType::RECORD,
                        evbuf,
                        LD_EV(evbuffer_get_length)(evbuf),
                        Compatibility::MAX_PROTOCOL_SUPPORTED);
  auto read = checked_downcast<std::unique_ptr<RECORD_Message>>(
      RECORD_Message::deserialize(reader).msg);
  ASSERT_NE(read.get(), nullptr);
  RECORD_Header expected_header = header;
  expected_header.flags |= RECORD_Header::INCLUDES_EXTRA_METADATA;
  expected_header.flags |= RECORD_Header::INCLUDE_OFFSET_WITHIN_EPOCH;
  ASSERT_EQ(
      0,
      memcmp(
          &expected_header, &this->getHeader(*read), sizeof expected_header));

  ExtraMetadata* read_metadata = this->getExtraMetadata(*read);
  ASSERT_NE(read_metadata, nullptr);
  ASSERT_EQ(read_metadata->offsets_within_epoch, offsets_within_epoch);
  ASSERT_EQ(read->offsets_, orig.offsets_);
  ASSERT_EQ(
      0, memcmp(&meta_header, &read_metadata->header, sizeof meta_header));

  std::vector<ShardID> read_copyset(
      read_metadata->copyset.begin(), read_metadata->copyset.end());
  ASSERT_EQ(meta_copyset, read_copyset);
}

}} // namespace facebook::logdevice
