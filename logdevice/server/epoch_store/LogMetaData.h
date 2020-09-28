/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/EpochMetaData.h"
#include "logdevice/common/EpochStore.h"

namespace facebook { namespace logdevice {

/**
 * A class that holds all the metadata for a single log. When possible, prefer
 * passing around the sub components of this class rather than the whole class.
 */
class LogMetaData {
 public:
  using Version = vcs_config_version_t;

  EpochMetaData current_epoch_metadata;
  EpochStoreMetaProperties epoch_store_properties;

  epoch_t data_last_clean_epoch;
  TailRecord data_tail_record;

  epoch_t metadata_last_clean_epoch;
  TailRecord metadata_tail_record;

  Version version;
  SystemTimestamp last_changed_timestamp;

  LogMetaData::Version getVersion() const;
  bool operator==(const LogMetaData& rhs) const;
  bool operator!=(const LogMetaData& rhs) const;
  std::string toString() const;

  /**
   * For backward comptability with existing deserializers, we need to guarantee
   * that for a newly provisioned LogMetaData, we have a valid LCE information
   * (more specifically valid dummy TailRecord).
   */
  static LogMetaData forNewLog(logid_t);

  /**
   * Increments the version and updates the last changed timestamp.
   */
  void touch();
};

}} // namespace facebook::logdevice
