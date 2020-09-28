/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/server/epoch_store/LogMetaData.h"

#include "logdevice/common/MetaDataLog.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

LogMetaData::Version LogMetaData::getVersion() const {
  return version;
}

bool LogMetaData::operator==(const LogMetaData& rhs) const {
  return current_epoch_metadata == rhs.current_epoch_metadata &&
      epoch_store_properties == rhs.epoch_store_properties &&
      data_last_clean_epoch == rhs.data_last_clean_epoch &&
      data_tail_record.sameContent(rhs.data_tail_record) &&
      metadata_last_clean_epoch == rhs.metadata_last_clean_epoch &&
      metadata_tail_record.sameContent(rhs.metadata_tail_record) &&
      version == rhs.version &&
      last_changed_timestamp == rhs.last_changed_timestamp;
}

bool LogMetaData::operator!=(const LogMetaData& rhs) const {
  return !(*this == rhs);
}

std::string LogMetaData::toString() const {
  return folly::sformat(
      "[CurrentEpoch: {}, EpochStoreProperties: {}, DataLCE: {}, DataTail: {}, "
      "MetaLCE:{}, MetaTail:{}, Version:{}, LastChangedTimestamp: {}]",
      current_epoch_metadata.toString(),
      epoch_store_properties.toString(),
      data_last_clean_epoch.val(),
      data_tail_record.toString(),
      metadata_last_clean_epoch.val(),
      metadata_tail_record.toString(),
      version.val(),
      last_changed_timestamp.toMilliseconds().count());
}

void LogMetaData::touch() {
  version = Version(version.val_ + 1);
  last_changed_timestamp = SystemTimestamp::now();
}

/* static */ LogMetaData LogMetaData::forNewLog(logid_t log_id) {
  // For backward comptability, new logs should have a valid dummy TailRecord.
  const auto build_dummy_tail_record = [](logid_t logid) {
    OffsetMap offsets;
    offsets.setCounter(BYTE_OFFSET, EPOCH_INVALID.val());
    return TailRecord{TailRecordHeader{logid,
                       LSN_INVALID,
                       EPOCH_INVALID.val(),
                       {BYTE_OFFSET_INVALID /* deprecated, offsets_within_epoch used instead */},
                       /*flags*/ 0,
                       {}},
                       std::move(offsets),
                       PayloadHolder()};
  };

  LogMetaData log_metadata;

  log_metadata.data_last_clean_epoch = EPOCH_INVALID;
  log_metadata.data_tail_record =
      build_dummy_tail_record(MetaDataLog::dataLogID(log_id));

  log_metadata.metadata_last_clean_epoch = EPOCH_INVALID;
  log_metadata.metadata_tail_record =
      build_dummy_tail_record(MetaDataLog::metaDataLogID(log_id));

  return log_metadata;
}

}} // namespace facebook::logdevice
