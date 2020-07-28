/**
 * Copyright (c) 2020-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/buffered_writer/BufferedWriteCodec.h"

#include <iterator>
#include <lz4.h>
#include <lz4hc.h>
#include <zstd.h>

#include <folly/Varint.h>
#include <folly/io/Cursor.h>

#include "logdevice/common/Checksum.h"
#include "logdevice/common/buffered_writer/BufferedWriteDecoderImpl.h"
#include "logdevice/common/debug.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

BufferedWriteSinglePayloadsCodec::Encoder::Encoder(size_t capacity,
                                                   size_t headroom)
    : blob_(folly::IOBuf::CREATE, headroom + capacity),
      appender_(&blob_, /* growth */ 0) {
  blob_.advance(headroom);
}

void BufferedWriteSinglePayloadsCodec::Encoder::append(
    const folly::IOBuf& payload) {
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

void BufferedWriteSinglePayloadsCodec::Encoder::encode(folly::IOBufQueue& out,
                                                       Compression& compression,
                                                       int zstd_level) {
  bool compressed = compress(compression, zstd_level);
  if (!compressed) {
    compression = Compression::NONE;
  }
  out.append(std::move(blob_));
}

bool BufferedWriteSinglePayloadsCodec::Encoder::compress(
    Compression compression,
    int zstd_level) {
  if (compression == Compression::NONE) {
    // Nothing to do.
    return true;
  }
  ld_check(compression == Compression::ZSTD ||
           compression == Compression::LZ4 ||
           compression == Compression::LZ4_HC);

  const Slice to_compress(blob_.data(), blob_.length());

  const size_t compressed_data_bound = compression == Compression::ZSTD
      ? ZSTD_compressBound(to_compress.size)
      : LZ4_compressBound(to_compress.size);

  // Preserve headroom (reserved for header)
  const size_t compressed_buf_size = blob_.headroom() + // header
      folly::kMaxVarintLength64 +                       // uncompressed length
      compressed_data_bound                             // compressed bytes
      ;
  folly::IOBuf compress_buf(folly::IOBuf::CREATE, compressed_buf_size);
  compress_buf.advance(blob_.headroom());
  uint8_t* out = compress_buf.writableTail();
  uint8_t* const end = out + compressed_buf_size - blob_.headroom();

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

namespace {
folly::Optional<folly::IOBuf> uncompress(const Slice& slice,
                                         const Compression compression) {
  if (compression == Compression::NONE) {
    return folly::IOBuf::wrapBufferAsValue(slice.data, slice.size);
  }

  const uint8_t *ptr = (const uint8_t*)slice.data, *end = ptr + slice.size;

  // Blob should start with a varint containing the uncompressed size
  uint64_t uncompressed_size;
  try {
    folly::ByteRange range(ptr, end);
    uncompressed_size = folly::decodeVarint(range);
    ptr = range.begin();
  } catch (...) {
    RATELIMIT_ERROR(std::chrono::seconds(1), 1, "Failed to decode varint");
    return folly::none;
  }

  ld_spew("uncompressed length in header is %lu", uncompressed_size);
  if (uncompressed_size > MAX_PAYLOAD_SIZE_INTERNAL) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    1,
                    "Compressed buffered write header says uncompressed length "
                    "is %lu, should be at most MAX_PAYLOAD_SIZE_INTERNAL (%zu)",
                    uncompressed_size,
                    MAX_PAYLOAD_SIZE_INTERNAL);
    return folly::none;
  }

  ld_spew("decompressing blob of size %ld", end - ptr);
  folly::IOBuf out{folly::IOBuf::CREATE, uncompressed_size};
  switch (compression) {
    case Compression::NONE:
      ld_check(false);
      return folly::none;
    case Compression::ZSTD: {
      size_t rv = ZSTD_decompress(out.writableTail(), // dst
                                  uncompressed_size,  // dstCapacity
                                  ptr,                // src
                                  end - ptr);         // compressedSize
      if (ZSTD_isError(rv)) {
        RATELIMIT_ERROR(std::chrono::seconds(1),
                        1,
                        "ZSTD_decompress() failed: %s",
                        ZSTD_getErrorName(rv));
        return folly::none;
      }
      if (rv != uncompressed_size) {
        RATELIMIT_ERROR(std::chrono::seconds(1),
                        1,
                        "Zstd decompression length %zu does not match %lu found"
                        "in header",
                        rv,
                        uncompressed_size);
        return folly::none;
      }
      out.append(uncompressed_size);
      return out;
    }
    case Compression::LZ4:
    case Compression::LZ4_HC: {
      int rv = LZ4_decompress_safe(reinterpret_cast<const char*>(ptr),
                                   reinterpret_cast<char*>(out.writableTail()),
                                   end - ptr,
                                   uncompressed_size);

      if (rv < 0) {
        RATELIMIT_ERROR(std::chrono::seconds(1),
                        1,
                        "LZ4 decompression failed with error %d",
                        rv);
        return folly::none;
      }
      if (rv != uncompressed_size) {
        RATELIMIT_ERROR(std::chrono::seconds(1),
                        1,
                        "LZ4 decompression length %d does not match %lu found"
                        "in header",
                        rv,
                        uncompressed_size);
        return folly::none;
      }
      out.append(uncompressed_size);
      return out;
    }
  }
  RATELIMIT_ERROR(std::chrono::seconds(1),
                  1,
                  "Unknown compression %d",
                  static_cast<int>(compression));
  return folly::none;
}

folly::Expected<uint64_t, folly::DecodeVarintError>
decodeVarint(folly::io::Cursor& cursor) {
  std::array<uint8_t, folly::kMaxVarintLength64> buffer;
  folly::ByteRange bytes = cursor.peekBytes();
  if (UNLIKELY(bytes.size() < buffer.size())) {
    bytes = {buffer.data(), cursor.pullAtMost(buffer.data(), buffer.size())};
    cursor.retreat(bytes.size());
  }
  auto begin = bytes.data();
  auto result = folly::tryDecodeVarint(bytes);
  if (!result) {
    return result;
  }
  cursor.skip(bytes.data() - begin);
  return result;
}

} // namespace

size_t BufferedWriteSinglePayloadsCodec::decode(
    const Slice& binary,
    Compression compression,
    std::vector<folly::IOBuf>& payloads_out,
    bool allow_buffer_sharing) {
  auto uncompressed = uncompress(binary, compression);
  if (!uncompressed) {
    return 0;
  }

  // uncompressed can share buffer with binary (e.g. in case of NO_COMPRESSION)
  // If sharing is not allowed, ensure that uncompressed manages its own copy
  if (!allow_buffer_sharing) {
    uncompressed->makeManaged();
  }

  std::vector<folly::IOBuf> payloads;
  payloads.reserve(payloads_out.capacity() - payloads_out.size());
  folly::io::Cursor cursor{uncompressed.get_pointer()};
  while (!cursor.isAtEnd()) {
    auto len = decodeVarint(cursor);
    if (!len) {
      RATELIMIT_ERROR(std::chrono::seconds(1), 1, "Failed to decode varint");
      return 0;
    }
    folly::IOBuf payload;
    size_t available = cursor.cloneAtMost(payload, *len);
    if (available != *len) {
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      1,
                      "Expected %lu more bytes based on length varint but "
                      "there are only %zu left",
                      *len,
                      available);
      return 0;
    }
    payloads.push_back(std::move(payload));
  }

  payloads_out.insert(payloads_out.end(),
                      std::make_move_iterator(payloads.begin()),
                      std::make_move_iterator(payloads.end()));
  return cursor.getCurrentPosition();
}

void BufferedWriteSinglePayloadsCodec::Estimator::append(
    const folly::IOBuf& payload) {
  const size_t len = payload.computeChainDataLength();
  encoded_payloads_size_ += folly::encodeVarintSize(len) + len;
}

size_t BufferedWriteSinglePayloadsCodec::Estimator::calculateSize() const {
  return encoded_payloads_size_;
}

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

template <>
BufferedWriteCodec::Encoder<BufferedWriteSinglePayloadsCodec::Encoder>::Encoder(
    int checksum_bits,
    size_t appends_count,
    size_t capacity)
    : checksum_bits_(checksum_bits),
      appends_count_(appends_count),
      header_size_(calculateHeaderSize(checksum_bits_, appends_count_)),
      payloads_encoder_(capacity - header_size_, header_size_) {}

template <>
BufferedWriteCodec::Encoder<PayloadGroupCodec::Encoder>::Encoder(
    int checksum_bits,
    size_t appends_count,
    // not used by PayloadGroupCodec::Encoder
    size_t /* capacity */)
    : checksum_bits_(checksum_bits),
      appends_count_(appends_count),
      header_size_(calculateHeaderSize(checksum_bits_, appends_count_)),
      payloads_encoder_(appends_count_) {}

template <typename PayloadsEncoder>
void BufferedWriteCodec::Encoder<PayloadsEncoder>::append(
    folly::IOBuf&& payload) {
  payloads_encoder_.append(std::move(payload));
}

template <>
void BufferedWriteCodec::Encoder<PayloadGroupCodec::Encoder>::append(
    const PayloadGroup& payload_group) {
  payloads_encoder_.append(payload_group);
}

template <>
void BufferedWriteCodec::Encoder<BufferedWriteSinglePayloadsCodec::Encoder>::
    append(const PayloadGroup& /* payload_group */) {
  // this should never be called: if there's at least one PayloadGroup appended,
  // then PayloadGroupCodec must be used
  ld_check(false);
}

template <typename PayloadsEncoder>
void BufferedWriteCodec::Encoder<PayloadsEncoder>::encode(
    folly::IOBufQueue& out,
    Compression compression,
    int zstd_level) {
  folly::IOBufQueue queue;
  if constexpr (std::is_same_v<PayloadsEncoder, PayloadGroupCodec::Encoder>) {
    // Make sure there's headroom reserved
    // Initial buffer size is based on kDesiredGrowth in thrift
    // BinaryProtocolWriter (16Kb - IOBuf overhead)
    auto iobuf = folly::IOBuf::create((2 << 14) - 64);
    iobuf->advance(header_size_);
    queue.append(std::move(iobuf));
  }

  payloads_encoder_.encode(queue, compression, zstd_level);

  auto blob = queue.move();
  if constexpr (std::is_same_v<PayloadsEncoder, PayloadGroupCodec::Encoder>) {
    // TODO checksumming requires a contiguous blob, so coalesce the blob
    // this can be removed once non-contiguous IOBufs are fully supported
    blob->coalesceWithHeadroomTailroom(header_size_, 0);

    // Compression for payloads in payload groups is encoded separately.
    // This compression can be tratead as compression used for the whole batch,
    // which is not compressed in case of payload groups encoder.
    compression = Compression::NONE;
  } else {
    ld_check(!blob->isChained());
  }
  ld_check(blob->headroom() >= header_size_);
  blob->prepend(header_size_);
  encodeHeader(*blob, compression);
  out.append(std::move(blob));
}

namespace {
/** Returns format based on encoder type. */
template <typename PayloadsEncoder>
BufferedWriteCodec::Format getFormat();

template <>
BufferedWriteCodec::Format
getFormat<BufferedWriteSinglePayloadsCodec::Encoder>() {
  return BufferedWriteCodec::Format::SINGLE_PAYLOADS;
}

template <>
BufferedWriteCodec::Format getFormat<PayloadGroupCodec::Encoder>() {
  return BufferedWriteCodec::Format::PAYLOAD_GROUPS;
}

} // namespace

// Format of the header:
// * 0-8 bytes reserved for checksum -- this is not really part of the
//   BufferedWriter format, see BufferedWriterImpl::prependChecksums()
// * 1 magic marker byte
// * 1 flags byte
// * 0-9 bytes varint batch size
template <typename PayloadsEncoder>
void BufferedWriteCodec::Encoder<PayloadsEncoder>::encodeHeader(
    folly::IOBuf& blob,
    Compression compression) {
  using batch_flags_t = BufferedWriteDecoderImpl::flags_t;

  const batch_flags_t flags = BufferedWriteDecoderImpl::Flags::SIZE_INCLUDED |
      static_cast<batch_flags_t>(compression);

  uint8_t* out = blob.writableData();
  // Skip checksum
  out += checksum_bits_ / 8;
  // Magic marker & flags
  *out++ = static_cast<uint8_t>(getFormat<PayloadsEncoder>());
  *out++ = flags;

  size_t len = folly::encodeVarint(appends_count_, out);
  out += len;
  ld_check(blob.writableData() + header_size_ == out);

  if (checksum_bits_ > 0) {
    // Update checksum
    size_t nbytes = checksum_bits_ / 8;
    Slice checksummed(blob.writableData() + nbytes, blob.length() - nbytes);
    checksum_bytes(checksummed,
                   checksum_bits_,
                   reinterpret_cast<char*>(blob.writableData()));
  }
}

// Instantiate Encoder with all supported variants of payload encoders
template class BufferedWriteCodec::Encoder<
    BufferedWriteSinglePayloadsCodec::Encoder>;
template class BufferedWriteCodec::Encoder<PayloadGroupCodec::Encoder>;

void BufferedWriteCodec::Estimator::append(const folly::IOBuf& payload) {
  // For single payloads format we should update payload groups format too
  // in case payload group is appended. However once format is switched to
  // payload groups, there's no need to do single payloads estimates, since they
  // will be discarded.
  switch (format_) {
    case Format::SINGLE_PAYLOADS:
      single_payloads_estimator_.append(payload);
      FOLLY_FALLTHROUGH;
    case Format::PAYLOAD_GROUPS:
      payload_groups_estimator_.append(payload);
      break;
  }
  appends_count_++;
}

void BufferedWriteCodec::Estimator::append(const PayloadGroup& payload_group) {
  // PayloadGroup encoding requires PAYLOAD_GROUPS format
  format_ = Format::PAYLOAD_GROUPS;
  payload_groups_estimator_.append(payload_group);
  appends_count_++;
}

size_t BufferedWriteCodec::Estimator::calculateSize(int checksum_bits) const {
  size_t size = calculateHeaderSize(checksum_bits, appends_count_);
  switch (format_) {
    case Format::SINGLE_PAYLOADS:
      size += single_payloads_estimator_.calculateSize();
      break;
    case Format::PAYLOAD_GROUPS:
      size += payload_groups_estimator_.calculateSize();
      break;
  }
  return size;
}

namespace {
// A helper function to extract flags and the number of records in the batch.
// Updates the slice to point to data directly following the header.
size_t decodeHeader(Slice& blob,
                    BufferedWriteDecoderImpl::flags_t* flags_out,
                    BufferedWriteCodec::Format* format_out,
                    size_t* size_out) {
  const uint8_t* ptr = reinterpret_cast<const uint8_t*>(blob.data);
  const uint8_t* end = ptr + blob.size;

  if (ptr == end) {
    RATELIMIT_ERROR(
        std::chrono::seconds(1), 1, "Reached end looking for marker byte 0xb1");
    return 0;
  }

  using Format = BufferedWriteCodec::Format;
  auto format = static_cast<Format>(*ptr++);
  if (format != Format::SINGLE_PAYLOADS && format != Format::PAYLOAD_GROUPS) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    1,
                    "Got unexpected marker byte 0x%02x",
                    static_cast<uint8_t>(format));
    return 0;
  }

  if (ptr == end) {
    RATELIMIT_ERROR(
        std::chrono::seconds(1), 1, "Reached end looking for flags");
    return 0;
  }
  BufferedWriteDecoderImpl::flags_t flags = *ptr++;

  size_t batch_size;
  if (flags & BufferedWriteDecoderImpl::Flags::SIZE_INCLUDED) {
    if (ptr == end) {
      RATELIMIT_ERROR(
          std::chrono::seconds(1), 1, "Reached end looking for the batch size");
      return 0;
    }
    try {
      folly::ByteRange range(ptr, end);
      batch_size = folly::decodeVarint(range);
      ptr = range.begin();
    } catch (...) {
      RATELIMIT_ERROR(std::chrono::seconds(1), 1, "Failed to decode varint");
      return 0;
    }
  } else {
    // If size is not included in the blob, we'll treat the whole batch as a
    // single record.
    batch_size = 1;
  }

  const size_t header_size = ptr - reinterpret_cast<const uint8_t*>(blob.data);
  blob = Slice(ptr, end - ptr);
  if (flags_out != nullptr) {
    *flags_out = flags;
  }
  if (format_out != nullptr) {
    *format_out = format;
  }
  if (size_out != nullptr) {
    *size_out = batch_size;
  }
  return header_size;
}
} // namespace

FOLLY_NODISCARD
bool BufferedWriteCodec::decodeBatchSize(Slice binary, size_t* size_out) {
  return decodeHeader(binary, nullptr, nullptr, size_out) != 0;
}

FOLLY_NODISCARD
bool BufferedWriteCodec::decodeCompression(Slice binary,
                                           Compression* compression_out) {
  BufferedWriteDecoderImpl::flags_t flags;
  const size_t header_size = decodeHeader(binary, &flags, nullptr, nullptr);
  if (header_size == 0) {
    return false;
  }
  if (compression_out) {
    *compression_out = static_cast<Compression>(
        flags & BufferedWriteDecoderImpl::Flags::COMPRESSION_MASK);
  }
  return true;
}

FOLLY_NODISCARD
bool BufferedWriteCodec::decodeFormat(Slice binary, Format* format_out) {
  return decodeHeader(binary, nullptr, format_out, nullptr) != 0;
}

FOLLY_NODISCARD
size_t BufferedWriteCodec::decode(Slice binary,
                                  std::vector<folly::IOBuf>& payloads_out,
                                  bool allow_buffer_sharing) {
  BufferedWriteDecoderImpl::flags_t flags;
  Format format;
  size_t batch_size;
  const size_t header_size = decodeHeader(binary, &flags, &format, &batch_size);
  if (header_size == 0) {
    return 0;
  }
  if (binary.size == 0) {
    // Nothing else to decode. Just the header.
    return header_size;
  }
  const Compression compression = static_cast<Compression>(
      flags & BufferedWriteDecoderImpl::Flags::COMPRESSION_MASK);
  switch (format) {
    case Format::SINGLE_PAYLOADS: {
      size_t bytes_decoded = BufferedWriteSinglePayloadsCodec::decode(
          binary, compression, payloads_out, allow_buffer_sharing);
      if (bytes_decoded == 0) {
        return 0;
      }
      return header_size + bytes_decoded;
    }
    case Format::PAYLOAD_GROUPS:
      RATELIMIT_ERROR(
          std::chrono::seconds(1),
          1,
          "Batch containing PayloadGroups cannot be decoded using this API");
      return 0;
  }
  ld_check(false); // decodeHeader should check format
  return 0;
}

namespace {
void convert(std::vector<folly::IOBuf>&& payloads,
             std::vector<PayloadGroup>& payload_groups_out) {
  payload_groups_out.reserve(payload_groups_out.size() + payloads.size());
  for (auto& payload : payloads) {
    PayloadGroup payload_group{{0, std::move(payload)}};
    payload_groups_out.emplace_back(std::move(payload_group));
  }
}
} // namespace

FOLLY_NODISCARD
size_t BufferedWriteCodec::decode(Slice binary,
                                  std::vector<PayloadGroup>& payload_groups_out,
                                  bool allow_buffer_sharing) {
  BufferedWriteDecoderImpl::flags_t flags;
  Format format;
  size_t batch_size;
  const size_t header_size = decodeHeader(binary, &flags, &format, &batch_size);
  if (header_size == 0) {
    return 0;
  }
  const Compression compression = static_cast<Compression>(
      flags & BufferedWriteDecoderImpl::Flags::COMPRESSION_MASK);
  switch (format) {
    case Format::SINGLE_PAYLOADS: {
      if (binary.size == 0) {
        // Nothing else to decode. Just the header.
        return header_size;
      }
      std::vector<folly::IOBuf> payloads;
      const size_t bytes_decoded = BufferedWriteSinglePayloadsCodec::decode(
          binary, compression, payloads, allow_buffer_sharing);
      if (bytes_decoded == 0) {
        return 0;
      }
      convert(std::move(payloads), payload_groups_out);
      return header_size + bytes_decoded;
    }
    case Format::PAYLOAD_GROUPS: {
      const size_t bytes_decoded = PayloadGroupCodec::decode(
          binary, payload_groups_out, allow_buffer_sharing);
      if (bytes_decoded == 0) {
        return 0;
      }
      return header_size + bytes_decoded;
    }
  }
  ld_check(false); // decodeHeader should check format
  return 0;
}

}} // namespace facebook::logdevice
