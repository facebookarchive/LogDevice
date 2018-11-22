/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/TailRecord.h"

#include <cstdio>
#include <cstring>
#include <memory>

#include <gtest/gtest.h>

#include "logdevice/common/OffsetMap.h"
#include "logdevice/include/Record.h"

namespace {

using namespace facebook::logdevice;

std::shared_ptr<PayloadHolder> createPayload(size_t size, char fill) {
  void* payload_flat = malloc(size);
  memset(payload_flat, fill, size);
  return std::make_shared<PayloadHolder>(payload_flat, size);
}

class TailRecordTest : public ::testing::Test {
 public:
  static constexpr logid_t LOG_ID = logid_t(233);
  static constexpr TailRecordHeader::flags_t NEW_FLAG = 1u << 30;

  TailRecordTest() {
    dbg::assertOnData = true;
  }

  TailRecord genTailRecord(bool include_payload) {
    TailRecordHeader::flags_t flags = TailRecordHeader::CHECKSUM_PARITY |
        (include_payload ? TailRecordHeader::HAS_PAYLOAD : 0);
    flags |= TailRecordHeader::OFFSET_WITHIN_EPOCH;
    flags |= TailRecordHeader::OFFSET_MAP;
    return TailRecord(
        TailRecordHeader{
            LOG_ID,
            compose_lsn(epoch_t(933), esn_t(3347)),
            1502502135,
            {BYTE_OFFSET_INVALID /* deprecated, OffsetMap used instead */},
            flags,
            {}},
        OffsetMap({{BYTE_OFFSET, 2349045994592}}),
        include_payload ? createPayload(2323, 't') : nullptr);
  }
};

constexpr logid_t TailRecordTest::LOG_ID;
constexpr TailRecordHeader::flags_t TailRecordTest::NEW_FLAG;

TEST_F(TailRecordTest, BasicSerialization) {
  const size_t n_records = 99;
  const size_t max_len = 1024 * 1024;
  std::unique_ptr<char[]> buf1(new char[max_len]);
  size_t written = 0;
  std::vector<size_t> record_size(n_records, 0);
  std::string buf_str;
  ProtocolWriter w(&buf_str, "TailRecordTest", 0);

  for (int i = 0; i < n_records; ++i) {
    TailRecord r = genTailRecord(i % 2 == 0);
    ASSERT_TRUE(r.isValid());
    record_size[i] = r.serialize(buf1.get() + written, max_len - written);
    ASSERT_GT(record_size[i], 0);
    written += record_size[i];
    // also serialize to the string
    r.serialize(w);
    ASSERT_FALSE(w.error());
  }
  ld_info("Wrote %lu records of %lu bytes.", n_records, written);
  ASSERT_EQ(written, buf_str.size());
  size_t n_read = 0;
  for (int i = 0; i < n_records; ++i) {
    TailRecord d;
    ASSERT_FALSE(d.isValid());
    int nbytes = d.deserialize({buf1.get() + n_read, max_len - n_read});

    TailRecord d2;
    int nbytes2 = d2.deserialize({buf_str.data() + n_read, max_len - n_read});
    ASSERT_EQ(nbytes, nbytes2);
    ASSERT_TRUE(TailRecord::sameContent(d, d2));

    ASSERT_EQ(record_size[i], nbytes);
    ASSERT_TRUE(d.isValid());
    // INCLUDE_BLOB only used in serialization format
    ASSERT_FALSE(d.header.flags & TailRecordHeader::INCLUDE_BLOB);
    ASSERT_TRUE(TailRecord::sameContent(d, genTailRecord(i % 2 == 0)));
    n_read += nbytes;
  }

  ASSERT_EQ(written, n_read);
}

TEST_F(TailRecordTest, EmptyPayload) {
  // tail record has payload but it is empty (size 0)
  TailRecordHeader::flags_t flags = TailRecordHeader::CHECKSUM_PARITY |
      TailRecordHeader::HAS_PAYLOAD | TailRecordHeader::OFFSET_WITHIN_EPOCH;
  OffsetMap offsets;
  offsets.setCounter(BYTE_OFFSET, 2349045994592);
  TailRecord r(
      TailRecordHeader{
          LOG_ID, compose_lsn(epoch_t(933), esn_t(3347)), 1502502135, {BYTE_OFFSET_INVALID /* deprecated, offsets_within_epoch used instead */}, flags, {}},
      std::move(offsets),
      std::make_shared<PayloadHolder>(nullptr, 0));

  std::unique_ptr<char[]> buf1(new char[512]);
  ASSERT_TRUE(r.isValid());
  int written = r.serialize(buf1.get(), 512);
  ASSERT_GT(written, 0);

  TailRecord d;
  ASSERT_FALSE(d.isValid());
  int nbytes = d.deserialize({buf1.get(), 512});
  ASSERT_EQ(written, nbytes);
  ASSERT_TRUE(d.isValid());
  ASSERT_TRUE(TailRecord::sameContent(d, r));
}

TEST_F(TailRecordTest, AssignmentAndMove) {
  const size_t max_len = 1024 * 1024;
  std::unique_ptr<char[]> buf1(new char[max_len]);
  for (int i = 0; i < 2; ++i) {
    TailRecord r = genTailRecord(i % 2 == 0);
    ASSERT_TRUE(r.isValid());
    // copy assignment
    TailRecord r2 = r;
    ASSERT_TRUE(TailRecord::sameContent(r, r2));
    int s1 = r.serialize(buf1.get(), max_len);
    int s2 = r2.serialize(buf1.get() + s1, max_len - s1);
    ASSERT_EQ(s1, s2);
    ASSERT_EQ(0, memcmp(buf1.get(), buf1.get() + s1, s1));

    TailRecord r3 = std::move(r);
    ASSERT_FALSE(r.isValid());
    ASSERT_FALSE(r.hasPayload());
    ASSERT_TRUE(TailRecord::sameContent(r2, r3));
  }
}

TEST_F(TailRecordTest, SerializationError) {
  TailRecord tr;
  ASSERT_FALSE(tr.isValid());
  std::unique_ptr<char[]> buf(new char[1]);
  int rv = tr.serialize(buf.get(), 1);
  ASSERT_EQ(-1, rv);
  ASSERT_EQ(E::INVALID_PARAM, err);
  tr = genTailRecord(true);
  rv = tr.serialize(buf.get(), 1);
  ASSERT_EQ(-1, rv);
  ASSERT_EQ(E::NOBUFS, err);
}

TEST_F(TailRecordTest, RemovePayload) {
  TailRecord r = genTailRecord(true);
  auto header = r.header;
  ASSERT_TRUE(r.isValid());
  ASSERT_TRUE(r.hasPayload());
  r.removePayload();
  ASSERT_TRUE(r.isValid());
  ASSERT_FALSE(r.hasPayload());
  auto header2 = r.header;
  header2.flags |= TailRecordHeader::HAS_PAYLOAD;
  int rv = memcmp(&header, &header2, sizeof(header));
  ASSERT_EQ(0, rv);
}

namespace {
int writeFutureRecord(const TailRecord& tr,
                      void* payload,
                      size_t size,
                      bool add_flag = true) {
  ld_check(tr.isValid());
  EXPECT_EQ(0, TailRecordTest::NEW_FLAG & TailRecordHeader::ALL_KNOWN_FLAGS);
  int written = tr.serialize(payload, size);
  if (written < 0) {
    return written;
  }

  TailRecordHeader::flags_t* flags_ptr = &((TailRecordHeader*)payload)->flags;
#define WRITE_BYTES(_data, _nbytes)                     \
  do {                                                  \
    if (written + _nbytes > size) {                     \
      return -1;                                        \
    }                                                   \
    memcpy((char*)payload + written, (_data), _nbytes); \
    written += _nbytes;                                 \
  } while (0)

  std::string w(13, 'w');
  if (tr.hasPayload()) {
    // if tr has payload, append 13 bytes after the payload
    TailRecordHeader::blob_size_t* bsize_ptr =
        (TailRecordHeader::blob_size_t*)((char*)payload +
                                         sizeof(TailRecordHeader) +
                                         tr.offsets_map_.sizeInLinearBuffer());
    (*bsize_ptr) += 13;
    WRITE_BYTES(w.data(), w.size());
  } else {
    // append a new blob of 13 bytes
    *flags_ptr |= TailRecordHeader::INCLUDE_BLOB;
    TailRecordHeader::blob_size_t bsize = 13;
    WRITE_BYTES(&bsize, sizeof(bsize));
    WRITE_BYTES(w.data(), w.size());
  }

  if (add_flag) {
    *flags_ptr |= TailRecordTest::NEW_FLAG;
  }

  return written;
#undef WRITE_BYTES
}
} // namespace

TEST_F(TailRecordTest, ForwardCompatibility) {
  const size_t max_len = 1024 * 1024;
  std::unique_ptr<char[]> buf1(new char[max_len]);

  for (int i = 0; i < 2; ++i) {
    TailRecord r = genTailRecord(i % 2 == 0);
    int written = writeFutureRecord(r, buf1.get(), max_len, false);
    ASSERT_GT(written, 0);
    // the record should failed to deserialze because it has no unknown flag
    TailRecord d;
    ASSERT_FALSE(d.isValid());
    int nbytes = d.deserialize({buf1.get(), max_len});
    ASSERT_EQ(-1, nbytes);
    ASSERT_EQ(E::TOOBIG, err);
  }

  // try to deserialize multiple future records
  const size_t n_records = 29;
  size_t written = 0;
  std::vector<size_t> record_size(n_records, 0);
  for (int i = 0; i < n_records; ++i) {
    TailRecord r = genTailRecord(i % 2 == 0);
    ASSERT_TRUE(r.isValid());
    record_size[i] =
        writeFutureRecord(r, buf1.get() + written, max_len - written, true);
    ASSERT_GT(record_size[i], 0);
    written += record_size[i];
  }
  ld_info("Wrote %lu future records of %lu bytes.", n_records, written);
  size_t n_read = 0;
  for (int i = 0; i < n_records; ++i) {
    TailRecord d;
    ASSERT_FALSE(d.isValid());
    int nbytes = d.deserialize({buf1.get() + n_read, max_len - n_read});
    ASSERT_EQ(record_size[i], nbytes);
    ASSERT_TRUE(d.isValid());
    // mask out the unknown flags before comparison
    d.header.flags &= ~TailRecordTest::NEW_FLAG;
    ASSERT_TRUE(TailRecord::sameContent(d, genTailRecord(i % 2 == 0)));
    n_read += nbytes;
  }

  ASSERT_EQ(written, n_read);
}

} // anonymous namespace
