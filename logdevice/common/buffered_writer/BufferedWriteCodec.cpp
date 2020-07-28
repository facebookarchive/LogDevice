/**
 * Copyright (c) 2020-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/buffered_writer/BufferedWriteCodec.h"

#include <lz4.h>
#include <lz4hc.h>
#include <zstd.h>

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

void BufferedWriteCodec::Encoder::encode(folly::IOBufQueue& out,
                                         Compression compression,
                                         int zstd_level) {
  bool compressed = compress(compression, zstd_level);
  if (!compressed) {
    compression = Compression::NONE;
  }
  encodeHeader(compression);
  out.append(std::move(blob_));
}

bool BufferedWriteCodec::Encoder::compress(Compression compression,
                                           int zstd_level) {
  if (compression == Compression::NONE) {
    // Nothing to do.
    return true;
  }
  ld_check(compression == Compression::ZSTD ||
           compression == Compression::LZ4 ||
           compression == Compression::LZ4_HC);

  // Skip the uncompressed blob header.
  ld_check(header_size_ > 0);
  ld_check(blob_.length() > header_size_);
  const Slice to_compress(
      blob_.data() + header_size_, blob_.length() - header_size_);

  const size_t compressed_data_bound = compression == Compression::ZSTD
      ? ZSTD_compressBound(to_compress.size)
      : LZ4_compressBound(to_compress.size);

  const size_t compressed_buf_size = header_size_ + // header
      folly::kMaxVarintLength64 +                   // uncompressed length
      compressed_data_bound                         // compressed bytes
      ;
  folly::IOBuf compress_buf(folly::IOBuf::CREATE, compressed_buf_size);
  uint8_t* out = compress_buf.writableTail();
  uint8_t* const end = out + compressed_buf_size;

  out += header_size_;

  // Append uncompressed size so that the decoding path knows how much memory
  // to allocate
  out += folly::encodeVarint(to_compress.size, out);

  size_t compressed_size;
  if (compression == Compression::ZSTD) {
    ld_check(zstd_level > 0);
    compressed_size = ZSTD_compress(out,              // dst
                                    end - out,        // dstCapacity
                                    to_compress.data, // src
                                    to_compress.size, // srcSize
                                    zstd_level);      // level
    if (ZSTD_isError(compressed_size)) {
      ld_critical(
          "ZSTD_compress() failed: %s", ZSTD_getErrorName(compressed_size));
      ld_check(false);
      return false;
    }
  } else {
    // LZ4
    int rv;
    if (compression == Compression::LZ4) {
      rv = LZ4_compress_default(reinterpret_cast<const char*>(to_compress.data),
                                reinterpret_cast<char*>(out),
                                to_compress.size,
                                end - out);
    } else {
      rv = LZ4_compress_HC(reinterpret_cast<const char*>(to_compress.data),
                           reinterpret_cast<char*>(out),
                           to_compress.size,
                           end - out,
                           0);
    }
    ld_spew("LZ4_compress() returned %d", rv);
    ld_check(rv > 0);
    compressed_size = rv;
  }
  out += compressed_size;
  ld_check(out <= end);

  const size_t compressed_len = out - compress_buf.data();
  ld_spew(
      "original size is %zu, compressed %zu", blob_.length(), compressed_len);
  if (compressed_len < blob_.length()) {
    // Compression was a win.  Replace the uncompressed blob.
    compress_buf.append(compressed_len);
    blob_ = std::move(compress_buf);
    return true;
  } else {
    return false;
  }
}

// Format of the header:
// * 0-8 bytes reserved for checksum -- this is not really part of the
//   BufferedWriter format, see BufferedWriterImpl::prependChecksums()
// * 1 magic marker byte
// * 1 flags byte
// * 0-9 bytes varint batch size
void BufferedWriteCodec::Encoder::encodeHeader(Compression compression) {
  using batch_flags_t = BufferedWriteDecoderImpl::flags_t;

  const batch_flags_t flags = BufferedWriteDecoderImpl::Flags::SIZE_INCLUDED |
      static_cast<batch_flags_t>(compression);

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
