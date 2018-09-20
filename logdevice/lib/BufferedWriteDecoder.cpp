/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/include/BufferedWriteDecoder.h"

#include <folly/Memory.h>

#include "logdevice/common/buffered_writer/BufferedWriteDecoderImpl.h"

namespace facebook { namespace logdevice {

std::unique_ptr<BufferedWriteDecoder> BufferedWriteDecoder::create() {
  return std::make_unique<BufferedWriteDecoderImpl>();
}

int BufferedWriteDecoder::getBatchSize(const DataRecord& record,
                                       size_t* size_out) {
  return BufferedWriteDecoderImpl::getBatchSize(record, size_out);
}

int BufferedWriteDecoder::decode(
    std::vector<std::unique_ptr<DataRecord>>&& records,
    std::vector<Payload>& payloads_out) {
  return impl()->decode(std::move(records), payloads_out);
}

int BufferedWriteDecoder::decodeOne(std::unique_ptr<DataRecord>&& record,
                                    std::vector<Payload>& payloads_out) {
  return impl()->decodeOne(std::move(record), payloads_out);
}

BufferedWriteDecoderImpl* BufferedWriteDecoder::impl() {
  return static_cast<BufferedWriteDecoderImpl*>(this);
}

}} // namespace facebook::logdevice
