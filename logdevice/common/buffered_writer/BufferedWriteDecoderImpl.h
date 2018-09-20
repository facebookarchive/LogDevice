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
  // Decodes a single DataRecord.  Claims ownership of the DataRecord if
  // successful.  Allowed to partially fill `payloads_out' in case of failed
  // decoding.  If necessary, caller will ensure atomicity in appending to the
  // client-supplied output vector.
  int decodeOne(std::unique_ptr<DataRecord>&& record,
                std::vector<Payload>& payloads_out);
  // Variant that does not consume the input DataRecord.  Instead, the blob is
  // copied if uncompressed.  This is useful when the caller cannot afford to
  // unconditionally relinquish ownership of the DataRecord.
  int decodeOne(const DataRecord& record, std::vector<Payload>& payloads_out);

  // Internal variant of decodeOne() where `record' is optional (`blob' may
  // point into a manually managed piece of memory).
  int decodeOne(Slice blob,
                std::vector<Payload>& payloads_out,
                std::unique_ptr<DataRecord>&& record,
                bool copy_blob_if_uncompressed);

  // Returns the number of individual records stored in a single DataRecord.
  static int getBatchSize(const DataRecord& record, size_t* size_out);

  // Returns compression codec of a single DataRecord.
  static int getCompression(const DataRecord& record,
                            Compression* compression_out);

 private:
  // Decodes an uncompressed blob without claiming ownership of the memory.
  int decodeUnowned(const Slice& slice, std::vector<Payload>& payloads_out);
  // Decodes a compressed blob.  In case of successful decoding, adds the
  // buffer containing uncompressed data to pinned_buffers_; the source
  // DataRecord is no longer needed.
  int decodeCompressed(const Slice& slice,
                       BufferedWriter::Options::Compression compression,
                       std::vector<Payload>& payloads_out);

  // DataRecord instances we decoded and assumed ownership of from the client
  std::deque<std::unique_ptr<DataRecord>> pinned_data_records_;
  // Buffers used for decompression; Payload instances we returned to the
  // client point into these buffers.
  std::deque<std::unique_ptr<uint8_t[]>> pinned_buffers_;
};
}} // namespace facebook::logdevice
