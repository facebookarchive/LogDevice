/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/configuration/ConfigParser.h"

#include <numeric>
#include <string>

#include <folly/Memory.h>
#include <folly/dynamic.h>

#include "logdevice/common/ConstructorFailed.h"
#include "logdevice/common/EpochMetaData.h"
#include "logdevice/common/SlidingWindow.h"
#include "logdevice/common/commandline_util_chrono.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/nodes/utils.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/Err.h"
#include "logdevice/include/LogAttributes.h"

using namespace facebook::logdevice::configuration;
using namespace facebook::logdevice::logsconfig;
namespace facebook { namespace logdevice { namespace configuration {
namespace parser {

static bool parseMetaDataLogNodes(const folly::dynamic& nodes,
                                  MetaDataLogsConfig& output);
bool parseTraceLogger(const folly::dynamic& clusterMap,
                      TraceLoggerConfig& output) {
  auto iter = clusterMap.find("trace-logger");
  if (iter == clusterMap.items().end()) {
    return true; // trace-logger is optional and have defaults
  }
  const folly::dynamic& tracerSection = iter->second;
  if (!tracerSection.isObject()) {
    ld_error("\"trace-logger\" entry is not a JSON object");
    err = E::INVALID_CONFIG;
    return false;
  }

  auto def_iter = tracerSection.find("default-sampling-percentage");
  if (def_iter != tracerSection.items().end()) {
    output.default_sampling = def_iter->second.asDouble();
  }

  iter = tracerSection.find("tracers");
  if (iter == clusterMap.items().end()) {
    return true; // trace-logger.tracers is optional and have defaults
  }

  const folly::dynamic& tracers = iter->second;
  if (!tracers.isObject()) {
    ld_error("\"trace-logger.tracers\" entry is not a JSON object");
    err = E::INVALID_CONFIG;
    return false;
  }

  output.percentages.clear();
  for (auto& pair : tracers.items()) {
    if (!pair.first.isString()) {
      ld_error("a key in \"tracers\" section is not a string"
               ". Expected a map from String -> Double(0, 100.0)");
      err = E::INVALID_CONFIG;
      return false;
    }
    if (!(pair.second.isDouble() || pair.second.isInt())) {
      ld_error("a 'value' in \"tracers\" section is not a number"
               ". Expected a map from String -> Double(0, 100.0)");
      err = E::INVALID_CONFIG;
      return false;
    }
    output.percentages.insert(
        std::make_pair(pair.first.asString(), pair.second.asDouble()));
  }
  return true;
}

bool parseInternalLogs(const folly::dynamic& clusterMap,
                       configuration::InternalLogs& internalLogs) {
  auto iter = clusterMap.find("internal_logs");
  if (iter == clusterMap.items().end()) {
    // Defining the internal logs is not mandatory.
    return true;
  }

  const folly::dynamic& section = iter->second;
  if (!section.isObject()) {
    ld_error("\"internal_logs\" entry is not a JSON object");
    err = E::INVALID_CONFIG;
    return false;
  }

  for (auto& pair : section.items()) {
    if (!pair.first.isString()) {
      ld_error("a key in \"internal_logs\" section is not a string");
      err = E::INVALID_CONFIG;
      return false;
    }
    if (!pair.second.isObject()) {
      ld_error("a 'value' in \"internal_logs\" section is not a JSON object");
      err = E::INVALID_CONFIG;
      return false;
    }
    folly::Optional<LogAttributes> attrs =
        parseAttributes(pair.second, pair.first.asString(), false);
    if (!attrs.hasValue()) {
      return false;
    }
    if (!internalLogs.insert(pair.first.asString(), attrs.value())) {
      return false;
    }
  }

  if (!internalLogs.isValid()) {
    return false;
  }

  return true;
}

// A copy-paste of EpochMetaData::nodesetToStorageSet(). It's a quick hack to
// avoid having to slightly reorganize build targets.
// TODO(TT15517759): Remove.
static StorageSet nodesetToStorageSet(const NodeSetIndices& indices) {
  StorageSet set;
  set.reserve(indices.size());
  for (node_index_t nid : indices) {
    set.push_back(ShardID(nid, 0));
  }
  return set;
}

bool validateNodeCount(const ServerConfig& server_cfg,
                       const LocalLogsConfig* logs_cfg) {
  // number of nodes with positive weights which can be used to store records
  int writable_node_cnt = 0;
  // number of nodes that can run sequencers
  int sequencer_node_cnt = 0;

  for (const auto& it : server_cfg.getNodes()) {
    const Node& node_cfg = it.second;
    if (node_cfg.isWritableStorageNode()) {
      writable_node_cnt++;
    }
    if (node_cfg.isSequencingEnabled()) {
      sequencer_node_cnt++;
    }
  }

  if (sequencer_node_cnt == 0) {
    ld_error("the 'sequencer' attribute must be set to 'true' for at least "
             "one node in the cluster. Found no such nodes.");
    err = E::INVALID_CONFIG;
    return false;
  }

  if (!logs_cfg) {
    // no log config available locally - skipping log config validation.
    return true;
  }

  const auto& nodes = server_cfg.getNodes();
  for (auto it = logs_cfg->getLogMap().begin();
       it != logs_cfg->getLogMap().end();
       it++) {
    const LogGroupInDirectory& logcfg = it->second;
    int replication_factor =
        ReplicationProperty::fromLogAttributes(logcfg.log_group->attrs())
            .getReplicationFactor();
    if (writable_node_cnt < replication_factor) {
      ld_error("the cluster does not have enough writable storage nodes "
               "for logs in the interval [%lu..%lu]. The log(s) in that "
               "interval require %d node(s). Found %i node(s).",
               it->first.lower(),
               it->first.upper() - 1,
               replication_factor,
               writable_node_cnt);
      err = E::INVALID_CONFIG;
      return false;
    }
  }

  const auto& meta_cfg = server_cfg.getMetaDataLogsConfig();
  auto storage_set = nodesetToStorageSet(meta_cfg.metadata_nodes);
  // ensure metadata_logs section is consistent with nodes section
  if (!configuration::nodes::validStorageSet(
          *server_cfg.getNodesConfigurationFromServerConfigSource(),
          storage_set,
          ReplicationProperty::fromLogAttributes(
              meta_cfg.metadata_log_group->attrs()),
          true // check the nodes exist
          )) {
    ld_error("Nodeset for metadata_logs is not compatible with its "
             "configuration, please check if the metadata nodes satisfy "
             "replication requirements on its replication scope. "
             "nodeset size: %lu, replication property: %s, "
             "total nodes in cluster: %lu.",
             meta_cfg.metadata_nodes.size(),
             ReplicationProperty::fromLogAttributes(
                 meta_cfg.metadata_log_group->attrs())
                 .toString()
                 .c_str(),
             nodes.size());
    err = E::INVALID_CONFIG;
    return false;
  }

  return true;
}

// Sets the permissions such that only an admin user can modify the the
// metadata log, while all other users only have read permissions.
void setMetaDataLogsPermission(MetaDataLogsConfig& config) {
  std::array<bool, static_cast<int>(ACTION::MAX)> default_principals;
  default_principals[static_cast<int>(ACTION::APPEND)] = false;
  default_principals[static_cast<int>(ACTION::READ)] = true;
  default_principals[static_cast<int>(ACTION::TRIM)] = false;

  config.setMetadataLogGroup(std::make_shared<LogGroupNode>(
      config.metadata_log_group->name(),
      config.metadata_log_group->attrs().with_permissions(
          folly::F14FastMap<std::string,
                            std::array<bool, static_cast<int>(ACTION::MAX)>>(
              {std::make_pair(Principal::DEFAULT, default_principals)})),
      config.metadata_log_group->range()));
}

bool parseMetaDataLog(const folly::dynamic& clusterMap,
                      const SecurityConfig& securityConfig,
                      MetaDataLogsConfig& output) {
  auto iter = clusterMap.find("metadata_logs");
  if (iter == clusterMap.items().end()) {
    ld_error("missing \"metadata_logs\" entry for cluster");
    err = E::INVALID_CONFIG;
    return false;
  }

  const folly::dynamic& metaDataLogSection = iter->second;
  if (!metaDataLogSection.isObject()) {
    ld_error("\"metadata_logs\" entry for cluster is not a JSON object");
    err = E::INVALID_CONFIG;
    return false;
  }

  iter = metaDataLogSection.find("nodeset");
  if (iter == metaDataLogSection.items().end()) {
    ld_error("\"nodeset\" is missing in \"metadata_logs\" section");
    err = E::INVALID_CONFIG;
    return false;
  }

  if (!parseMetaDataLogNodes(iter->second, output)) {
    return false;
  }

  folly::Optional<LogAttributes> log_attrs =
      parseAttributes(metaDataLogSection,
                      "metadata_logs",
                      /* permissions */ false,
                      /* metadata_logs */ true);
  if (!log_attrs.hasValue()) {
    err = E::INVALID_CONFIG;
    return false;
  }

  std::string replication_error;
  if (ReplicationProperty::validateLogAttributes(
          log_attrs.value(), &replication_error) != 0) {
    ld_error("Invalid replication settings in \"metadata_logs\" section: %s",
             replication_error.c_str());
    err = E::INVALID_CONFIG;
    return false;
  }

  // nodeset_selector_type is optional, default value should be
  // consistent-hashing
  ld_check(output.nodeset_selector_type ==
           NodeSetSelectorType::CONSISTENT_HASHING);
  std::string nodeset_selector_type_str;

  bool success = getStringFromMap(
      metaDataLogSection, "nodeset_selector", nodeset_selector_type_str);
  if (success) {
    NodeSetSelectorType parsed_type =
        NodeSetSelectorTypeFromString(nodeset_selector_type_str);
    if (parsed_type == NodeSetSelectorType::INVALID) {
      success = false;
      err = E::INVALID_PARAM;
    }
    output.nodeset_selector_type = parsed_type;
  }
  if (!success && err != E::NOTFOUND) {
    ld_error("Invalid value of \"nodeset_selector\" attribute in "
             "\"metadata_logs\": \"%s\"",
             nodeset_selector_type_str.c_str());
    err = E::INVALID_CONFIG;
    return false;
  }
  ld_check(output.nodeset_selector_type != NodeSetSelectorType::INVALID);

  // metadata version override is optional, default should be unspecified
  ld_check(!output.metadata_version_to_write.hasValue());
  epoch_metadata_version::type metadata_version;
  success = getIntFromMap<epoch_metadata_version::type>(
      metaDataLogSection, "metadata_version", metadata_version);
  if ((!success && err != E::NOTFOUND) ||
      (success && !epoch_metadata_version::validToWrite(metadata_version))) {
    ld_error("Invalid value of \"metadata_version\" attribute in "
             "\"metadata_logs\" section, should be an int between %u and %u",
             epoch_metadata_version::MIN_SUPPORTED,
             epoch_metadata_version::CURRENT);
    err = E::INVALID_CONFIG;
    return false;
  } else if (success) {
    output.metadata_version_to_write.assign(metadata_version);
  }

  // sequencers_write_metadata_logs is optional, default value should be true
  ld_check(output.sequencers_write_metadata_logs);
  success = getBoolFromMap(metaDataLogSection,
                           "sequencers_write_metadata_logs",
                           output.sequencers_write_metadata_logs);
  if (!success && err != E::NOTFOUND) {
    ld_error("Invalid value of \"sequencers_write_metadata_logs\" attribute "
             "in \"metadata_logs\" section. Expected a bool.");
    err = E::INVALID_CONFIG;
    return false;
  }

  // sequencers_provision_epoch_store is optional, default value should be true
  ld_check(output.sequencers_provision_epoch_store);
  success = getBoolFromMap(metaDataLogSection,
                           "sequencers_provision_epoch_store",
                           output.sequencers_provision_epoch_store);
  if (!success && err != E::NOTFOUND) {
    ld_error("Invalid value of \"sequencers_provision_epoch_store\" attribute "
             "in \"metadata_logs\" section. Expected a bool.");
    err = E::INVALID_CONFIG;
    return false;
  }

  if (output.sequencers_provision_epoch_store &&
      !output.sequencers_write_metadata_logs) {
    ld_error(
        "\"sequencers_provision_epoch_store\" attribute cannot be set to "
        "'true' (default) if \"sequencers_write_metadata_logs\" are set to "
        "'false' in \"metadata_logs\" section.");
    err = E::INVALID_CONFIG;
    return false;
  }

  output.setMetadataLogGroup(
      LogGroupNode("metadata logs",
                   log_attrs.value(),
                   logid_range_t(LOGID_INVALID, LOGID_INVALID)));

  // The MetaData log permissions cannot be changed. If permissions are stored
  // in the configuration file then set the metadata log permissions.
  if (securityConfig.allowPermissionsInConfig()) {
    setMetaDataLogsPermission(output);
  }

  int replication_factor =
      ReplicationProperty::fromLogAttributes(log_attrs.value())
          .getReplicationFactor();
  if (replication_factor + (*log_attrs).extraCopies().value() >
      COPYSET_SIZE_MAX) {
    ld_error("the sum (%d) of replicationFactor and extraCopies "
             "for metadata logs exceeds COPYSET_SIZE_MAX %zu",
             replication_factor + (*log_attrs).extraCopies().value(),
             COPYSET_SIZE_MAX);
    // shouldn't happend in default config
    ld_check(false);
    err = E::INVALID_CONFIG;
    return false;
  }

  // consistency of metadata nodeset and replication_factor is checked later
  // in validateNodeCount()

  return true;
}

static bool parseMetaDataLogNodes(const folly::dynamic& nodes,
                                  MetaDataLogsConfig& output) {
  ld_check(output.metadata_nodes.empty());
  if (nodes.isArray()) {
    // nodes are represented as an array of indices
    for (const folly::dynamic& item : nodes) {
      if (!item.isInt()) {
        ld_error("Item in the metadata_logs nodeset section is not an "
                 "integer. Expected a list of node indexes");
        err = E::INVALID_CONFIG;
        return false;
      }
      node_index_t idx = node_index_t(item.asInt());
      if (idx < 0) {
        ld_error("Invalid index %d in the metadata_logs nodeset section", idx);
        err = E::INVALID_CONFIG;
        return false;
      }
      output.metadata_nodes.push_back(idx);
    }
    // sort the array and check for duplicates
    std::sort(output.metadata_nodes.begin(), output.metadata_nodes.end());
    if (std::unique(
            output.metadata_nodes.begin(), output.metadata_nodes.end()) !=
        output.metadata_nodes.end()) {
      ld_error("Duplicate indices in metadata_logs nodeset section");
      err = E::INVALID_CONFIG;
      return false;
    }
    return true;
  }

  if (nodes.isString()) {
    // nodes are represented as an interval string
    std::string interval_string = nodes.asString();
    interval_t interval_raw;
    if (parse_interval(interval_string.c_str(), &interval_raw) != 0 ||
        interval_raw.hi > std::numeric_limits<node_index_t>::max()) {
      ld_error("invalid nodeset index interval \"%s\" in metadata_logs",
               interval_string.c_str());
      err = E::INVALID_CONFIG;
      return false;
    }

    ld_check(interval_raw.hi >= interval_raw.lo);
    size_t nodeset_size = interval_raw.hi - interval_raw.lo + 1;
    output.metadata_nodes.resize(nodeset_size);
    std::iota(output.metadata_nodes.begin(),
              output.metadata_nodes.end(),
              node_index_t(interval_raw.lo));
    return true;
  }

  ld_error("Invalid \"nodeset\" entry for \"metadata_logs\" section");
  err = E::INVALID_CONFIG;
  return false;
}

}}}} // namespace facebook::logdevice::configuration::parser
