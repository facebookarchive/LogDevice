/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/buffered_writer/BufferedWriteDecoderImpl.h"

#include <lz4.h>
#include <zstd.h>

#include <folly/Varint.h>

#include "logdevice/common/DataRecordOwnsPayload.h"
#include "logdevice/common/debug.h"

namespace facebook { namespace logdevice {

using Compression = BufferedWriter::Options::Compression;

namespace {
// A helper function to extract flags and the number of records in the batch.
// Updates the slice to point to data directly following the header.
int decodeHeader(Slice& blob,
                 BufferedWriteDecoderImpl::flags_t* flags_out,
                 size_t* size_out) {
  const uint8_t* ptr = (const uint8_t*)blob.data;
  const uint8_t* end = ptr + blob.size;

  if (ptr == end) {
    RATELIMIT_ERROR(
        std::chrono::seconds(1), 1, "Reached end looking for marker byte 0xb1");
    return -1;
  }
  uint8_t marker = *ptr++;
  if (marker != 0xb1) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    1,
                    "Expected marker byte 0xb1, got 0x%02x",
                    marker);
    return -1;
  }

  if (ptr == end) {
    RATELIMIT_ERROR(
        std::chrono::seconds(1), 1, "Reached end looking for flags");
    return -1;
  }
  BufferedWriteDecoderImpl::flags_t flags = *ptr++;

  size_t batch_size;
  if (flags & BufferedWriteDecoderImpl::Flags::SIZE_INCLUDED) {
    if (ptr == end) {
      RATELIMIT_ERROR(
          std::chrono::seconds(1), 1, "Reached end looking for the batch size");
      return -1;
    }
    try {
      folly::ByteRange range(ptr, end);
      batch_size = folly::decodeVarint(range);
      ptr = range.begin();
    } catch (...) {
      RATELIMIT_ERROR(std::chrono::seconds(1), 1, "Failed to decode varint");
      return -1;
    }
  } else {
    // If size is not included in the blob, we'll treat the whole batch as a
    // single record.
    batch_size = 1;
  }

  blob = Slice(ptr, end - ptr);
  if (flags_out != nullptr) {
    *flags_out = flags;
  }
  if (size_out != nullptr) {
    *size_out = batch_size;
  }
  return 0;
}
} // namespace

int BufferedWriteDecoderImpl::decode(
    std::vector<std::unique_ptr<DataRecord>>&& records,
    std::vector<Payload>& payloads_out) {
  // We'll decode into this vector first to avoid partially filling
  // `payloads_out' with a batch that ends up failing to decode.
  std::vector<Payload> payloads_tmp;
  int rv = 0;
  for (auto& recordptr : records) {
    payloads_tmp.clear();
    if (decodeOne(std::move(recordptr), payloads_tmp) == 0) {
      payloads_out.insert(
          payloads_out.end(), payloads_tmp.begin(), payloads_tmp.end());
    } else {
      rv = -1;
    }
  }
  return rv;
}

int BufferedWriteDecoderImpl::decodeOne(std::unique_ptr<DataRecord>&& record,
                                        std::vector<Payload>& payloads_out) {
  Slice slice(record->payload);
  return decodeOne(slice,
                   payloads_out,
                   std::move(record),
                   /* copy_blob_if_uncompressed */ false);
};

int BufferedWriteDecoderImpl::decodeOne(const DataRecord& record,
                                        std::vector<Payload>& payloads_out) {
  return decodeOne(Slice(record.payload),
                   payloads_out,
                   nullptr,
                   /* copy_blob_if_uncompressed */ true);
};

int BufferedWriteDecoderImpl::decodeOne(Slice blob,
                                        std::vector<Payload>& payloads_out,
                                        std::unique_ptr<DataRecord>&& record,
                                        bool copy_blob_if_uncompressed) {
  if (record) {
    // For the memory ownership transfer to work as intended, `recordptr'
    // needs to be a DataRecordOwnsPayload under the hood.
    ld_assert(dynamic_cast<DataRecordOwnsPayload*>(record.get()) != nullptr);
  }

  flags_t flags;
  if (decodeHeader(blob, &flags, nullptr) != 0) {
    return -1;
  }

  Compression compression = (Compression)(flags & Flags::COMPRESSION_MASK);
  switch (compression) {
    case Compression::NONE: {
      std::unique_ptr<uint8_t[]> buf;
      if (copy_blob_if_uncompressed) {
        buf = std::make_unique<uint8_t[]>(blob.size);
        memcpy(buf.get(), blob.data, blob.size);
        blob = Slice(buf.get(), blob.size);
      }

      int rv = decodeUnowned(blob, payloads_out);
      if (rv == 0) {
        if (copy_blob_if_uncompressed) {
          pinned_buffers_.push_back(std::move(buf));
        } else {
          pinned_data_records_.push_back(std::move(record));
        }
      }
      return rv;
    }

    case Compression::ZSTD:
    case Compression::LZ4:
    case Compression::LZ4_HC: {
      int rv = decodeCompressed(blob, compression, payloads_out);
      // If we succeeded, steal the DataRecordOwnsPayload from the client to
      // be consistent with the uncompressed case.
      if (rv == 0) {
        record.reset();
      }
      return rv;
    }

    default:
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      1,
                      "Invalid compression flag value 0x%02x",
                      (uint8_t)compression);
      return -1;
  }
}

int BufferedWriteDecoderImpl::getBatchSize(const DataRecord& record,
                                           size_t* size_out) {
  Slice blob(record.payload);
  return decodeHeader(blob, nullptr, size_out);
}

int BufferedWriteDecoderImpl::getCompression(const DataRecord& record,
                                             Compression* compression_out) {
  Slice blob(record.payload);
  BufferedWriteDecoderImpl::flags_t flags;
  int rv = decodeHeader(blob, &flags, nullptr);
  if (rv == 0) {
    *compression_out =
        static_cast<Compression>(flags & Flags::COMPRESSION_MASK);
    return 0;
  }
  return -1;
}

int BufferedWriteDecoderImpl::decodeUnowned(
    const Slice& slice,
    std::vector<Payload>& payloads_out) {
  const uint8_t *ptr = (const uint8_t*)slice.data, *end = ptr + slice.size;
  for (; ptr < end;) {
    uint64_t len;
    try {
      folly::ByteRange range(ptr, end);
      len = folly::decodeVarint(range);
      ptr = range.begin();
    } catch (...) {
      RATELIMIT_ERROR(std::chrono::seconds(1), 1, "Failed to decode varint");
      return -1;
    }
    if (ptr + len > end) {
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      1,
                      "Expected %lu more bytes based on length varint but "
                      "there are only %zd left",
                      len,
                      end - ptr);
      return -1;
    }
    payloads_out.push_back(Payload(ptr, len));
    ptr += len;
  }
  ld_check(ptr == end);
  return 0;
}

int BufferedWriteDecoderImpl::decodeCompressed(
    const Slice& slice,
    const Compression compression,
    std::vector<Payload>& payloads_out) {
  const uint8_t *ptr = (const uint8_t*)slice.data, *end = ptr + slice.size;

  // Blob should start with a varint containing the uncompressed size
  uint64_t uncompressed_size;
  try {
    folly::ByteRange range(ptr, end);
    uncompressed_size = folly::decodeVarint(range);
    ptr = range.begin();
  } catch (...) {
    RATELIMIT_ERROR(std::chrono::seconds(1), 1, "Failed to decode varint");
    return -1;
  }

  ld_spew("uncompressed length in header is %lu", uncompressed_size);
  if (uncompressed_size > MAX_PAYLOAD_SIZE_INTERNAL) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    1,
                    "Compressed buffered write header says uncompressed length "
                    "is %lu, should be at most MAX_PAYLOAD_SIZE_INTERNAL (%zu)",
                    uncompressed_size,
                    MAX_PAYLOAD_SIZE_INTERNAL);
    return -1;
  }

  ld_spew("decompressing blob of size %ld", end - ptr);
  std::unique_ptr<uint8_t[]> buf(new uint8_t[uncompressed_size]);
  if (compression == Compression::ZSTD) {
    size_t rv = ZSTD_decompress(buf.get(),         // dst
                                uncompressed_size, // dstCapacity
                                ptr,               // src
                                end - ptr);        // compressedSize
    if (ZSTD_isError(rv)) {
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      1,
                      "ZSTD_decompress() failed: %s",
                      ZSTD_getErrorName(rv));
      return -1;
    }
    if (rv != uncompressed_size) {
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      1,
                      "Zstd decompression length %zu does not match %lu found"
                      "in header",
                      rv,
                      uncompressed_size);
      return -1;
    }
  }
  if (compression == Compression::LZ4 || compression == Compression::LZ4_HC) {
    int rv = LZ4_decompress_safe(
        (char*)ptr, (char*)buf.get(), end - ptr, uncompressed_size);

    if (rv < 0) {
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      1,
                      "LZ4 decompression failed with error %d",
                      rv);
      return -1;
    }
    if (rv != uncompressed_size) {
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      1,
                      "LZ4 decompression length %d does not match %lu found"
                      "in header",
                      rv,
                      uncompressed_size);
      return -1;
    }
  }

  if (decodeUnowned(Slice(buf.get(), uncompressed_size), payloads_out) != 0) {
    return -1;
  }

  // Decoding succeeded.  Pin the decompressed buffer.
  pinned_buffers_.push_back(std::move(buf));
  return 0;
}
}} // namespace facebook::logdevice
