/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/server/epoch_store/LogMetaData.h"

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

}} // namespace facebook::logdevice
