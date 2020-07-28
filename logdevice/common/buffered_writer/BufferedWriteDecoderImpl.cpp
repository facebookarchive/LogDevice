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
#include "logdevice/common/buffered_writer/BufferedWriteCodec.h"
#include "logdevice/common/debug.h"

namespace facebook { namespace logdevice {

using Compression = BufferedWriter::Options::Compression;

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
                   /* allow_buffer_sharing */ true);
};

int BufferedWriteDecoderImpl::decodeOne(const DataRecord& record,
                                        std::vector<Payload>& payloads_out) {
  return decodeOne(Slice(record.payload),
                   payloads_out,
                   nullptr,
                   /* allow_buffer_sharing */ false);
};

int BufferedWriteDecoderImpl::decodeOne(Slice blob,
                                        std::vector<Payload>& payloads_out,
                                        std::unique_ptr<DataRecord>&& record,
                                        bool allow_buffer_sharing) {
  if (record) {
    // For the memory ownership transfer to work as intended, `recordptr'
    // needs to be a DataRecordOwnsPayload under the hood.
    ld_assert(dynamic_cast<DataRecordOwnsPayload*>(record.get()) != nullptr);
  }

  std::vector<folly::IOBuf> payloads;
  size_t bytes_decoded =
      BufferedWriteCodec::decode(blob, payloads, allow_buffer_sharing);
  if (bytes_decoded == 0) {
    return -1;
  }

  payloads_out.reserve(payloads_out.size() + payloads.size());

  bool has_unmanaged_buffers = false;
  for (auto& iobuf : payloads) {
    iobuf.coalesce();
    const size_t len = iobuf.length();
    payloads_out.emplace_back(len ? iobuf.data() : nullptr, len);
    if (iobuf.isManaged()) {
      // Data is not shared with blob, so it must be pinned, otherwise payload
      // will point to deallocated memory
      pinned_buffers_.push_back(std::move(iobuf));
    } else {
      has_unmanaged_buffers = true;
    }
  }
  if (has_unmanaged_buffers) {
    // There are payloads which share buffer blob. This can only happen if
    // allow_buffer_sharing was true.
    ld_check(allow_buffer_sharing);
    if (record) {
      // Blob belongs to the record, so it must be pined to preserve blob
      pinned_data_records_.push_back(std::move(record));
    }
  }
  // If we succeeded, steal the DataRecordOwnsPayload from the client to
  // be consistent with the uncompressed case.
  record.reset();
  return 0;
}

int BufferedWriteDecoderImpl::getBatchSize(const DataRecord& record,
                                           size_t* size_out) {
  Slice blob(record.payload);
  return BufferedWriteCodec::decodeBatchSize(blob, size_out) ? 0 : -1;
}

}} // namespace facebook::logdevice
