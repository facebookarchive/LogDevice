/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <deque>
#include <memory>
#include <vector>

#include <folly/FBVector.h>

#include "logdevice/common/types_internal.h"
#include "logdevice/include/BufferedWriteDecoder.h"
#include "logdevice/include/BufferedWriter.h"

namespace facebook { namespace logdevice {

class BufferedWriteDecoderImpl : public BufferedWriteDecoder {
 public:
  // Represents a bitset of flags included in every record constructed by
  // BufferedWriter.
  typedef uint8_t flags_t;

  struct Flags {
    // Covers flag bits that denote the compression type; see
    // logdevice/include/BufferedWriter.h.
    static constexpr flags_t COMPRESSION_MASK = 0x7;
    // A flag bit which indicates that the blob composed by BufferedWriter
    // contains the size of the batch before individual payloads.
    static constexpr flags_t SIZE_INCLUDED = 1 << 3;
  };

  int decode(std::vector<std::unique_ptr<DataRecord>>&& records,
             std::vector<Payload>& payloads_out);
  int decode(std::vector<std::unique_ptr<DataRecord>>&& records,
             std::vector<PayloadGroup>& payload_groups_out);
  // Decodes a single DataRecord.  Claims ownership of the DataRecord if
  // successful.  Allowed to partially fill `payloads_out' in case of failed
  // decoding.  If necessary, caller will ensure atomicity in appending to the
  // client-supplied output vector.
  int decodeOne(std::unique_ptr<DataRecord>&& record,
                std::vector<Payload>& payloads_out);
  int decodeOne(std::unique_ptr<DataRecord>&& record,
                std::vector<PayloadGroup>& payload_groups_out);
  // Variant that does not consume the input DataRecord.  Instead, the blob is
  // copied if uncompressed.  This is useful when the caller cannot afford to
  // unconditionally relinquish ownership of the DataRecord.
  int decodeOne(const DataRecord& record, std::vector<Payload>& payloads_out);
  int decodeOne(const DataRecord& record,
                std::vector<PayloadGroup>& payload_groups_out);

  // Internal variant of decodeOne() where `record' is optional (`blob' may
  // point into a manually managed piece of memory).
  int decodeOne(Slice blob,
                std::vector<Payload>& payloads_out,
                std::unique_ptr<DataRecord>&& record,
                bool allow_buffer_sharing);
  int decodeOne(Slice blob,
                std::vector<PayloadGroup>& payload_groups_out,
                std::unique_ptr<DataRecord>&& record,
                bool allow_buffer_sharing);

  // Returns the number of individual records stored in a single DataRecord.
  static int getBatchSize(const DataRecord& record, size_t* size_out);

 private:
  template <typename T>
  int decodeImpl(std::vector<std::unique_ptr<DataRecord>>&& records,
                 std::vector<T>& payload_groups_out);

  // DataRecord instances we decoded and assumed ownership of from the client
  folly::fbvector<std::unique_ptr<DataRecord>> pinned_data_records_;
  // Buffers used for decompression; Payload instances we returned to the
  // client point into these buffers.
  folly::fbvector<folly::IOBuf> pinned_buffers_;
};
}} // namespace facebook::logdevice
