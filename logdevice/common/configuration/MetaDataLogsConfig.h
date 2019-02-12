/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <vector>

#include "logdevice/common/NodeSetSelectorType.h"
#include "logdevice/common/ShardID.h"
#include "logdevice/common/configuration/EpochMetaDataVersion.h"
#include "logdevice/common/configuration/logs/LogsConfigTree.h"
#include "logdevice/include/LogAttributes.h"

namespace facebook { namespace logdevice { namespace logsconfig {
class LogGroupNode;
}}} // namespace facebook::logdevice::logsconfig

namespace facebook { namespace logdevice { namespace configuration {

struct MetaDataLogsConfig {
  /* Some APIs create this as an empty object and populate it afterwards,
   * e.g. config parsing */
  MetaDataLogsConfig() {}

  /* We need an explicit copy/move constructor here in order to fix the
   * pointers in LogGroupInDirectory to always point to the log within this
   * object, since the directory object doesn't own the log itself */
  MetaDataLogsConfig(MetaDataLogsConfig&& o) noexcept
      : metadata_nodes(std::move(o.metadata_nodes)),
        nodeset_selector_type(o.nodeset_selector_type),
        metadata_version_to_write(o.metadata_version_to_write),
        sequencers_write_metadata_logs(o.sequencers_write_metadata_logs),
        sequencers_provision_epoch_store(o.sequencers_provision_epoch_store) {
    setMetadataLogGroup(std::move(o.metadata_log_group));
  }

  MetaDataLogsConfig(const MetaDataLogsConfig& o) noexcept
      : metadata_nodes(o.metadata_nodes),
        nodeset_selector_type(o.nodeset_selector_type),
        metadata_version_to_write(o.metadata_version_to_write),
        sequencers_write_metadata_logs(o.sequencers_write_metadata_logs),
        sequencers_provision_epoch_store(o.sequencers_provision_epoch_store) {
    setMetadataLogGroup(o.metadata_log_group);
  }

  MetaDataLogsConfig& operator=(const MetaDataLogsConfig& other) {
    metadata_nodes = other.metadata_nodes;
    metadata_log_group = other.metadata_log_group;
    nodeset_selector_type = other.nodeset_selector_type;
    metadata_version_to_write = other.metadata_version_to_write;
    sequencers_write_metadata_logs = other.sequencers_write_metadata_logs;
    sequencers_provision_epoch_store = other.sequencers_provision_epoch_store;
    setMetadataLogGroup(other.metadata_log_group);
    return *this;
  }
  // indices of nodes that store metadata logs
  std::vector<node_index_t> metadata_nodes;

  // log group property of metadata logs
  std::shared_ptr<logsconfig::LogGroupNode> metadata_log_group;

  logsconfig::LogGroupInDirectory metadata_log_group_in_dir;

  void setMetadataLogGroup(const logsconfig::LogGroupNode& log_group) {
    setMetadataLogGroup(std::make_shared<logsconfig::LogGroupNode>(log_group));
  }

  void
  setMetadataLogGroup(std::shared_ptr<logsconfig::LogGroupNode> log_group) {
    if (log_group != nullptr) {
      metadata_log_group = std::move(log_group);
      metadata_log_group_in_dir =
          logsconfig::LogGroupInDirectory(metadata_log_group, nullptr);
    }
  }

  // The default nodeset selector type is CONSISTENT_HASHING.
  NodeSetSelectorType nodeset_selector_type =
      NodeSetSelectorType::CONSISTENT_HASHING;

  // Version of metadata that should be written by the nodes to the epoch store.
  // This can be used to ensure backwards compatibility when upgrading a cluster
  folly::Optional<epoch_metadata_version::type> metadata_version_to_write;

  // if false, sequencers in logdeviced will not be writing metadata logs.
  bool sequencers_write_metadata_logs = true;

  // if false, sequencers in logdeviced will be re-provisioning metadata in the
  // epoch store.
  bool sequencers_provision_epoch_store = true;
};

}}} // namespace facebook::logdevice::configuration
