/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/include/Record.h"

namespace facebook { namespace logdevice {

namespace {

PayloadGroup makePayloadGroup(const Payload& payload) {
  return PayloadGroup{
      {0, folly::IOBuf::wrapBufferAsValue(payload.data(), payload.size())}};
}

Payload makePrimaryPayload(PayloadGroup& payload_group) {
  auto payload = payload_group.find(0);
  if (payload != payload_group.end()) {
    auto& iobuf = payload->second;
    // Since payload requires contiguous memory range, IOBuf needs to be
    // coalesced
    iobuf.coalesce();
    return Payload(iobuf.empty() ? nullptr : iobuf.data(), iobuf.length());
  } else {
    return {};
  }
}

} // namespace

DataRecord::DataRecord(logid_t log_id,
                       const Payload& pl,
                       lsn_t lsn,
                       std::chrono::milliseconds timestamp,
                       int batch_offset,
                       RecordOffset offsets)
    : LogRecord(log_id),
      payload(pl),
      payloads(makePayloadGroup(payload)),
      attrs(lsn, timestamp, batch_offset, std::move(offsets)) {}

DataRecord::DataRecord(logid_t log_id,
                       Payload&& pl,
                       lsn_t lsn,
                       std::chrono::milliseconds timestamp,
                       int batch_offset,
                       RecordOffset offsets)
    : LogRecord(log_id),
      payload(std::move(pl)),
      payloads(makePayloadGroup(payload)),
      attrs(lsn, timestamp, batch_offset, std::move(offsets)) {}

DataRecord::DataRecord(logid_t log_id,
                       PayloadGroup&& pl,
                       lsn_t lsn,
                       std::chrono::milliseconds timestamp,
                       int batch_offset,
                       RecordOffset offsets)
    : LogRecord(log_id),
      payload(makePrimaryPayload(pl)),
      payloads(std::move(pl)),
      attrs(lsn, timestamp, batch_offset, std::move(offsets)) {}

std::string gapTypeToString(GapType type) {
#define LD_GAP_TYPE_TO_STRING(t) \
  case GapType::t:               \
    return #t;
  switch (type) {
    LD_GAP_TYPE_TO_STRING(UNKNOWN)
    LD_GAP_TYPE_TO_STRING(BRIDGE)
    LD_GAP_TYPE_TO_STRING(HOLE)
    LD_GAP_TYPE_TO_STRING(DATALOSS)
    LD_GAP_TYPE_TO_STRING(TRIM)
    LD_GAP_TYPE_TO_STRING(ACCESS)
    LD_GAP_TYPE_TO_STRING(NOTINCONFIG)
    LD_GAP_TYPE_TO_STRING(FILTERED_OUT)
    LD_GAP_TYPE_TO_STRING(MAX)
    default:
      return "UNDEFINED";
  }
#undef LD_GAP_TYPE_TO_STRING
}

}} // namespace facebook::logdevice
