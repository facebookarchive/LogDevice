/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/configuration/ReplicationProperty.h"
#include "logdevice/common/membership/types.h"

namespace facebook { namespace logdevice { namespace configuration {
namespace nodes {

class MetaDataLogsReplication {
 public:
  struct Update {
    membership::MembershipVersion::Type base_version{
        membership::MembershipVersion::EMPTY_VERSION};
    ReplicationProperty replication{};
    explicit Update(membership::MembershipVersion::Type base)
        : base_version(base), replication() {}
    bool isValid() const;
    std::string toString() const;
  };

  explicit MetaDataLogsReplication();

  ReplicationProperty getReplicationProperty() const {
    return replication_;
  }

  int applyUpdate(const Update& update,
                  MetaDataLogsReplication* config_out) const;

  bool validate() const;

  membership::MembershipVersion::Type getVersion() const {
    return version_;
  }

  bool operator==(const MetaDataLogsReplication& rhs) const {
    return version_ == rhs.version_ && replication_ == rhs.replication_;
  }

 private:
  membership::MembershipVersion::Type version_{
      membership::MembershipVersion::EMPTY_VERSION};
  ReplicationProperty replication_{};

  friend class NodesConfigLegacyConverter;
  friend class NodesConfigurationThriftConverter;
};

}}}} // namespace facebook::logdevice::configuration::nodes
