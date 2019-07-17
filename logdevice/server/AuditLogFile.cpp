/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/server/AuditLogFile.h"

#include <folly/json.h>

#include "logdevice/common/BuildInfo.h"
#include "logdevice/common/Timestamp.h"
#include "logdevice/common/commandline_util_chrono.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/plugin/PluginRegistry.h"
#include "logdevice/server/LocalLogFile.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/locallogstore/PartitionedRocksDBStore.h"

namespace facebook { namespace logdevice {

size_t to_log_entry(const TrimAuditLogEntry& data, char* buf, size_t size) {
  folly::dynamic obj = folly::dynamic::object("operation_type", "TRIM")(
      "log_id", data.log_id.val_)("timestamp", format_time(data.timestamp))(
      "new_lsn", data.new_lsn)("log_group", data.log_group)(
      "host_address", data.host_address)("cluster_name", data.cluster_name)(
      "build_info", data.build_info)(
      "partition_timestamp", format_time(data.partition_timestamp));

  // Log client request or retention policy for automatic trim
  if (!data.client.empty()) {
    obj["client"] = data.client;
    obj["client_address"] = data.client_address;
    folly::dynamic identities = folly::dynamic::array();
    for (auto identity : data.identity.identities) {
      folly::dynamic ident = folly::dynamic::object();
      ident["name"] = identity.first;
      ident["identity"] = identity.second;
      identities.push_back(std::move(ident));
    }
    obj["identities"] = std::move(identities);
  } else {
    obj["retention"] = data.retention;
  }

  auto res = folly::toJson(obj);
  return snprintf(buf, size, "%s\n", res.c_str());
}

void log_trim_movement(ServerProcessor& processor,
                       LocalLogStore& store,
                       logid_t log_id,
                       lsn_t trim_lsn,
                       const std::string& client,
                       const std::string& client_address,
                       const PrincipalIdentity& identity) {
  const std::shared_ptr<LocalLogFile>& auditLog = processor.getAuditLog();
  if (!auditLog) {
    return;
  }

  TrimAuditLogEntry entry;
  entry.timestamp = std::chrono::system_clock::now();
  entry.log_id = log_id;
  entry.new_lsn = trim_lsn;
  entry.client = client;
  entry.client_address = client_address;
  entry.identity = identity;

  static auto build_info_plugin =
      processor.getPluginRegistry()->getSinglePlugin<BuildInfo>(
          PluginType::BUILD_INFO);
  ld_check(build_info_plugin);
  static std::string build_info = build_info_plugin->version();
  entry.build_info = build_info;

  std::shared_ptr<Configuration> config = processor.config_->get();
  const std::shared_ptr<ServerConfig>& server_config = config->serverConfig();
  const auto& service_discovery =
      processor.config_->getNodesConfiguration()->getServiceDiscovery();

  entry.cluster_name = server_config->getClusterName();
  auto nodeIdx = processor.getMyNodeID().index();
  auto node_config = service_discovery->getNodeAttributes(nodeIdx);
  entry.host_address = node_config->address.toStringNoPort();
  std::shared_ptr<configuration::LocalLogsConfig> logs_config =
      config->localLogsConfig();
  if (!MetaDataLog::isMetaDataLog(log_id)) {
    auto log_group = logs_config->getLogGroupByIDShared(log_id);
    if (log_group) {
      entry.log_group = log_group->name();
      auto retention = *log_group->attrs().backlogDuration();
      entry.retention =
          retention.hasValue() ? chrono_string(retention.value()) : "infinity";
    }
  }

  auto partitioned_store = dynamic_cast<PartitionedRocksDBStore*>(&store);
  if (partitioned_store) {
    entry.partition_timestamp =
        partitioned_store->getPartitionTimestampForLSNIfReadilyAvailable(
            log_id, trim_lsn);
  }
  auditLog->write(entry);
}
}} // namespace facebook::logdevice
