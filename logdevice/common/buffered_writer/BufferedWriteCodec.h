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
};

/**
 * Codec for encoding/decoding buffered writes.
 */
class BufferedWriteCodec {
 public:
  /** Supports encoding of the payloads. */
  class Encoder {
   public:
    /**
     * Creates encoder. Encoder requires number of appends and capacity for
     * the buffer to be specified beforehand. Capacity must be calculated using
     * Estimator on the same sequence of appends.
     */
    Encoder(int checksum_bits, size_t appends_count, size_t capacity);

    /** Appends single payload to the batch. */
    void append(const folly::IOBuf& payload);

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

    BufferedWriteSinglePayloadsCodec::Encoder payloads_encoder_;
  };

  /**
   * Supports estimation of encoded buffered writes batch size.
   */
  class Estimator {
   public:
    /** Appends single payload to the batch. */
    void append(const folly::IOBuf& payload);

    /**
     * Returns resulting encoded uncompressed blob size, including space for
     * the header. Result of this call can be used to specify capacity for the
     * Encoder. Passing same sequence of appends to encoder is guaranteed to fit
     * into a buffer of size calculated by this function.
     */
    size_t calculateSize(int checksum_bits) const;

   private:
    // Appends count is required to calculate header size correctly
    size_t appends_count_ = 0;
    BufferedWriteSinglePayloadsCodec::Estimator payloads_estimator_;
  };
};

}} // namespace facebook::logdevice
