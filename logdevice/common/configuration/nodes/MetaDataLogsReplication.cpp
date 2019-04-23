/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/configuration/nodes/MetaDataLogsReplication.h"

#include <folly/Format.h>

#include "logdevice/common/debug.h"
#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice { namespace configuration {
namespace nodes {

MetaDataLogsReplication::MetaDataLogsReplication()
    : version_(membership::MembershipVersion::EMPTY_VERSION) {
  ld_check(replication_.isEmpty());
}

bool MetaDataLogsReplication::Update::isValid() const {
  return replication.isValid();
}

std::string MetaDataLogsReplication::Update::toString() const {
  return folly::sformat(
      "[V:{},R:{}]", logdevice::toString(base_version), replication.toString());
}

int MetaDataLogsReplication::applyUpdate(
    const Update& update,
    MetaDataLogsReplication* config_out) const {
  if (!update.isValid()) {
    err = E::INVALID_PARAM;
    return -1;
  }

  if (update.base_version != version_) {
    err = E::VERSION_MISMATCH;
    return -1;
  }

  MetaDataLogsReplication new_config(*this);
  new_config.replication_ = update.replication;
  new_config.version_ = membership::MembershipVersion::Type(version_.val() + 1);
  ld_assert(new_config.validate());
  if (config_out != nullptr) {
    *config_out = new_config;
  }
  return 0;
}

bool MetaDataLogsReplication::validate() const {
  if (version_ == membership::MembershipVersion::EMPTY_VERSION) {
    return replication_.isEmpty();
  }
  return replication_.isValid();
}

}}}} // namespace facebook::logdevice::configuration::nodes
