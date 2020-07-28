/**
 * Copyright (c) 2020-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <string>

#include <folly/io/Cursor.h>
#include <folly/io/IOBuf.h>

#include "logdevice/common/PayloadGroupCodec.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

/** Codec for batches of single payloads */
class BufferedWriteSinglePayloadsCodec {
 public:
  class Encoder {
   public:
    /**
     * Creates encoder with specified capacity for the encoding buffer
     * (uncompressed). In addition to capacity, a headroom can be reserved.
     * encode ensures that returned IOBuf has this headroom available.
     */
    Encoder(size_t capacity, size_t headroom);

    /** Appends single payload to the batch. */
    void append(const folly::IOBuf& payload);

    /*
     * Encodes and compressess payloads. If compressing payloads with requested
     * compresssion doesn't improve required space, then it can be left
     * uncompressed. compression parameter is updated accordingly.
     */
    void encode(folly::IOBufQueue& out,
                Compression& compression,
                int zstd_level = 0);

   private:
    /**
     * Replaces blob with compressed blob if compression saves some space and
     * returns true. Otherwise leaves blob as is and returns false.
     */
    bool compress(Compression compression, int zstd_level);

    // Payloads are appended to the blob_ using appender_ */
    folly::IOBuf blob_;
    folly::io::Appender appender_;
  };

  /** Estimator for uncompressed batch size. */
  class Estimator {
   public:
    /** Appends payload to the batch */
    void append(const folly::IOBuf& payload);

    /** Returns size of current batch in encoded form */
    size_t calculateSize() const;

   private:
    // Number of bytes required to encode payloads.
    size_t encoded_payloads_size_ = 0;
  };

  /**
   * Uncompress and decode payloads stored in batch.
   * Resulting payloads can optionally share data with input (for example in
   * case it's uncompressed).
   * Returns number of bytes consumed, or 0 if decoding fails.
   */
  static size_t decode(const Slice& binary,
                       Compression compression,
                       std::vector<folly::IOBuf>& payloads_out,
                       bool allow_buffer_sharing);
};

/**
 * Codec for encoding/decoding buffered writes.
 */
class BufferedWriteCodec {
 public:
  // Enum values are persisted in storage to identify encoding.
  enum class Format : uint8_t { SINGLE_PAYLOADS = 0xb1, PAYLOAD_GROUPS = 0xb2 };

  /** Supports encoding of the payloads. */
  template <typename PayloadsEncoder>
  class Encoder {
   public:
    /**
     * Creates encoder. Encoder requires number of appends and capacity for
     * the buffer to be specified beforehand. Capacity must be calculated using
     * Estimator on the same sequence of appends.
     */
    Encoder(int checksum_bits, size_t appends_count, size_t capacity);

    /** Appends single payload to the batch. */
    void append(folly::IOBuf&& payload);
    /** Appends payload group to the batch. */
    void append(const PayloadGroup& payload_group);

    /**
     * Encodes added appends into output specified in constructor.
     * Output queue will be appended with a single contigous IOBuf containing
     * encoded payloads.
     * Encoder must not be re-used after calling this.
     * zstd_level must be specified if ZSTD compression is used.
     */
    void encode(folly::IOBufQueue& out,
                Compression compression,
                int zstd_level = 0);

   private:
    /** Writes header (checksum, flags, etc) to the blob's headroom */
    void encodeHeader(folly::IOBuf& blob, Compression compression);

    int checksum_bits_;
    size_t appends_count_;
    size_t header_size_;

    PayloadsEncoder payloads_encoder_;
  };

  /**
   * Supports estimation of encoded buffered writes batch size.
   */
  class Estimator {
   public:
    /** Appends single payload to the batch. */
    void append(const folly::IOBuf& payload);
    /**
     * Appends payload group to the batch. This can change required format for
     * the encoding.
     */
    void append(const PayloadGroup& payload_group);

    /**
     * Returns resulting encoded uncompressed blob size, including space for
     * the header. Result of this call can be used to specify capacity for the
     * Encoder. Passing same sequence of appends to encoder is guaranteed to fit
     * into a buffer of size calculated by this function.
     */
    size_t calculateSize(int checksum_bits) const;

    /**
     * Returns format required for the batch encoding. Format is updated
     * dynamically based on perormed appends.
     */
    Format getFormat() const {
      return format_;
    }

   private:
    Format format_ = Format::SINGLE_PAYLOADS;
    // Appends count is required to calculate header size correctly
    size_t appends_count_ = 0;
    BufferedWriteSinglePayloadsCodec::Estimator single_payloads_estimator_;
    PayloadGroupCodec::Estimator payload_groups_estimator_;
  };

  /** Decodes number of records stored in batch */
  FOLLY_NODISCARD
  static bool decodeBatchSize(Slice binary, size_t* size_out);

  /** Decodes batch compression in use */
  FOLLY_NODISCARD
  static bool decodeCompression(Slice binary, Compression* compression_out);

  /** Decodes format in use. Fails is format is unknown. */
  FOLLY_NODISCARD
  static bool decodeFormat(Slice binary, Format* format_out);

  /**
   * Decodes payloads stored in batch.
   * Resulting payloads can optionally share data with input (for example in
   * case it's uncompressed).
   * Returns number of bytes consumed, or 0 if decoding fails.
   */
  FOLLY_NODISCARD
  static size_t decode(Slice binary,
                       std::vector<folly::IOBuf>& payloads_out,
                       bool allow_buffer_sharing);

  /**
   * Decodes payloads stored in batch.
   * Resulting payloads can optionally share data with input (for example in
   * case it's uncompressed).
   * Returns number of bytes consumed, or 0 if decoding fails.
   */
  FOLLY_NODISCARD
  static size_t decode(Slice binary,
                       std::vector<PayloadGroup>& payload_groups_out,
                       bool allow_buffer_sharing);
};

}} // namespace facebook::logdevice
