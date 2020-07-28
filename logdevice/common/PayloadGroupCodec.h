/**
 * Copyright (c) 2020-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <string>
#include <unordered_set>
#include <vector>

#include <folly/Portability.h>
#include <folly/io/IOBufQueue.h>

#include "logdevice/common/if/gen-cpp2/payload_types.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

/**
 * Allows encoding/decoding PayloadGroup objects to/from binary
 * representation.
 */
class PayloadGroupCodec {
 public:
  /**
   * Supports encoding of a sequence of payloads.
   */
  class Encoder {
   public:
    /** Initializes encoder with expected number of appends */
    explicit Encoder(size_t expected_appends_count);

    /** Appends payload group. */
    void append(const PayloadGroup& payload_group);

    /**
     * Appends single payload. Single payload is encoded as a PayloadGroup with
     * payload at key 0.
     */
    void append(folly::IOBuf&& payload);

    /** Encodes all currently added payloads to a binary representation. */
    void encode(folly::IOBufQueue& out);

   private:
    /**
     * Updates key in the last added payload group. During append, an empty
     * payload group is appended first, and then this group is updated for
     * each key.
     * iobuf parameter can be null, indicating that key has no payload (used to
     * append empty payload groups).
     */
    void update(PayloadKey key, const folly::IOBuf* iobuf);

    const size_t expected_appends_count_;
    size_t appends_count_ = 0;
    bool contains_only_empty_groups_ = false;
    thrift::CompressedPayloadGroups encoded_payload_groups_;
  };

  /**
   * Calculates encoded payloads size without doing actual encoding.
   */
  class Estimator {
   public:
    Estimator();

    /** Updates estimate for payload group */
    void append(const PayloadGroup& payload_group);
    /** Updates estimate for single payload */
    void append(const folly::IOBuf& payload);

    /** Claculates current size of the encoded blob */
    size_t calculateSize() const {
      return encoded_bytes_;
    }

   private:
    /**
     * Updates key in the last added payload group. During append, an empty
     * payload group is appended first, and then this group is updated for
     * each key.
     * iobuf parameter can be null, indicating that key has no payload (used to
     * append empty payload groups).
     */
    void update(PayloadKey key, const folly::IOBuf* iobuf);

    size_t appends_count_ = 0;
    bool contains_only_empty_groups_ = false;
    size_t encoded_bytes_;
    std::unordered_set<PayloadKey> payload_keys_;
  };

  /**
   * Encodes single PayloadGroup to binary representation.
   */
  static void encode(const PayloadGroup& payload_group, folly::IOBufQueue& out);

  /**
   * Encoding several PayloadGroups to binary representation.
   */
  static void encode(const std::vector<PayloadGroup>& payload_groups,
                     folly::IOBufQueue& out);

  /**
   * Decodes single PayloadGroup from binary representation.
   * Returns number of bytes consumed, or 0 in case of error.
   */
  FOLLY_NODISCARD
  static size_t decode(Slice binary, PayloadGroup& payload_group_out);

  /**
   * Decodes PayloadGroups from binary representation.
   * Decoded PayloadGroups are appended to payload_groups_out on success.
   * Returns number of bytes consumed, or 0 in case of error.
   */
  FOLLY_NODISCARD
  static size_t decode(Slice binary,
                       std::vector<PayloadGroup>& payload_groups_out);
};

}} // namespace facebook::logdevice
