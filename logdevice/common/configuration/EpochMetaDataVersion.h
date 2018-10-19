/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>

#include <folly/Optional.h>

namespace facebook { namespace logdevice {

/**
 * @file: EpochMetaData has several formats of metadata it supports. These are
 *        versioned by prepending a version field to the metadata. This file
 *        describes the version type and bounds on it, as well as the version
 *        currently used.
 */

class ServerConfig;

namespace epoch_metadata_version {
typedef uint32_t type;

const type CURRENT{2};
const type MIN_SUPPORTED{1};

inline bool validToRead(type v) {
  return v >= MIN_SUPPORTED;
}
inline bool validToWrite(type v) {
  return v >= MIN_SUPPORTED && v <= CURRENT;
}
// returns cfg->getMetaDataLogsConfig().metadata_version_to_write if it is
// set, otherwise returns CURRENT. Metadata writes should use this method to
// select the version of metadata to write. It has to be used instead of the
// metadata_version_to_write directly, as we want to avoid dumping
// metadata_version_to_write into the config if it hasn't been explicitly
// specified.
type versionToWrite(const std::shared_ptr<ServerConfig>& cfg);
}; // namespace epoch_metadata_version

}} // namespace facebook::logdevice
