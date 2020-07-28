/**
 * Copyright (c) 2020-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/buffered_writer/BufferedWriteCodec.h"

#include <folly/Varint.h>

#include "logdevice/common/Checksum.h"
#include "logdevice/common/buffered_writer/BufferedWriteDecoderImpl.h"
#include "logdevice/common/debug.h"

namespace facebook { namespace logdevice {

namespace {

size_t calculateHeaderSize(int checksum_bits, size_t appends_count) {
  size_t header_size =
      // Any bytes for the checksum.  This goes first since it gets stripped
      // first on the read path.
      checksum_bits / 8 +
      // 2 bytes for header (magic marker and header)
      2 +
      // The batch size.
      folly::encodeVarintSize(appends_count);
  return header_size;
}

} // namespace

BufferedWriteCodec::Encoder::Encoder(int checksum_bits,
                                     size_t appends_count,
                                     size_t capacity)
    : checksum_bits_(checksum_bits),
      appends_count_(appends_count),
      header_size_(calculateHeaderSize(checksum_bits_, appends_count_)),
      blob_(folly::IOBuf::CREATE, capacity),
      appender_(&blob_, /* growth */ 0) {
  appender_.append(header_size_);
}

void BufferedWriteCodec::Encoder::append(const folly::IOBuf& payload) {
  size_t len = folly::encodeVarint(
      payload.computeChainDataLength(), appender_.writableData());
  ld_check(len <= appender_.length());
  appender_.append(len);

  // TODO this makes a copy of payload to make sure result is contiguous,
  // once non-contiguous IOBufs are supported payload can appended as is
  for (const auto& bytes : payload) {
    size_t appended = appender_.pushAtMost(bytes);
    ld_check(appended == bytes.size());
  }
}

void BufferedWriteCodec::Encoder::encode(folly::IOBufQueue& out) {
  // TODO add compression support
  encodeHeader();
  out.append(std::move(blob_));
}

// Format of the header:
// * 0-8 bytes reserved for checksum -- this is not really part of the
//   BufferedWriter format, see BufferedWriterImpl::prependChecksums()
// * 1 magic marker byte
// * 1 flags byte
// * 0-9 bytes varint batch size
void BufferedWriteCodec::Encoder::encodeHeader() {
  using batch_flags_t = BufferedWriteDecoderImpl::flags_t;

  const batch_flags_t flags = BufferedWriteDecoderImpl::Flags::SIZE_INCLUDED |
      static_cast<batch_flags_t>(Compression::NONE);

  uint8_t* out = blob_.writableData();
  // Skip checksum
  out += checksum_bits_ / 8;
  // Magic marker & flags
  *out++ = 0xb1;
  *out++ = flags;

  size_t len = folly::encodeVarint(appends_count_, out);
  out += len;
  ld_check(blob_.writableData() + header_size_ == out);

  if (checksum_bits_ > 0) {
    // Update checksum
    size_t nbytes = checksum_bits_ / 8;
    Slice checksummed(blob_.writableData() + nbytes, blob_.length() - nbytes);
    checksum_bytes(checksummed,
                   checksum_bits_,
                   reinterpret_cast<char*>(blob_.writableData()));
  }
}

void BufferedWriteCodec::Estimator::append(const folly::IOBuf& payload) {
  appends_count_++;
  const size_t len = payload.computeChainDataLength();
  encoded_payloads_size_ += folly::encodeVarintSize(len) + len;
}

size_t BufferedWriteCodec::Estimator::calculateSize(int checksum_bits) const {
  return calculateHeaderSize(checksum_bits, appends_count_) +
      encoded_payloads_size_;
}

}} // namespace facebook::logdevice
