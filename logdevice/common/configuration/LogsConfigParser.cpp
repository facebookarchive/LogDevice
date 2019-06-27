/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/configuration/LogsConfigParser.h"

#include <folly/Conv.h>
#include <folly/MapUtil.h>
#include <folly/Range.h>
#include <folly/dynamic.h>
#include <folly/json.h>
#include <google/dense_hash_set>

#include "logdevice/common/SlidingWindow.h"
#include "logdevice/common/commandline_util_chrono.h"
#include "logdevice/common/configuration/ParsingHelpers.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/Err.h"

using namespace facebook::logdevice::logsconfig;

namespace facebook { namespace logdevice { namespace configuration {
namespace parser {

//
// Forward declarations of helpers
//

class LogsConfigNamespace {
 public:
  explicit LogsConfigNamespace(const std::string& delim,
                               const std::string& name = "")
      : delim_(delim), s_(name) {}

  LogsConfigNamespace(const LogsConfigNamespace& src)
      : delim_(src.delim_), s_(src.s_) {}

  LogsConfigNamespace& operator+=(const std::string& token) {
    if (s_.size() > 0) {
      s_ += delim_;
    }
    s_.append(token.data(), token.size());
    return *this;
  }

  LogsConfigNamespace operator+(const std::string& token) const {
    LogsConfigNamespace res = *this;
    res += token;
    return res;
  }

  const std::string& toString() const {
    return s_;
  }

  std::string toStringPrefix() const {
    return s_.empty() ? s_ : s_ + delim_;
  }

  bool empty() const {
    return s_.empty();
  }

  std::string getDelimiter() const {
    return delim_;
  }

 private:
  const std::string delim_;
  std::string s_;
};

static bool parseOneLogEntry(
    LocalLogsConfig::DirectoryNode* parent_ns,
    const LogsConfigNamespace& namespace_prefix,
    const folly::dynamic& logMap,
    const SecurityConfig& securityConfig,
    const ConfigParserOptions& options,
    std::shared_ptr<facebook::logdevice::configuration::LocalLogsConfig>&
        output);

// If you add a new key here, make sure to add it to Log::toFollyDynamic() and
// Log::operator==() as well.
static const std::set<std::string> logs_config_recognized_keys = {
    "id",
    "name",

    REPLICATION_FACTOR,
    EXTRA_COPIES,
    SYNCED_COPIES,
    MAX_WRITES_IN_FLIGHT,
    SINGLE_WRITER,
    SYNC_REPLICATION_SCOPE,
    REPLICATE_ACROSS,
    NODESET_SIZE,
    BACKLOG,
    DELIVERY_LATENCY,
    SCD_ENABLED,
    LOCAL_SCD_ENABLED,
    WRITE_TOKEN,
    PERMISSIONS,
    ACLS,
    ACLS_SHADOW,
    STICKY_COPYSETS,
    MUTABLE_PER_EPOCH_LOG_METADATA_ENABLED,
    SEQUENCER_AFFINITY,
    SEQUENCER_BATCHING,
    SEQUENCER_BATCHING_TIME_TRIGGER,
    SEQUENCER_BATCHING_SIZE_TRIGGER,
    SEQUENCER_BATCHING_COMPRESSION,
    SEQUENCER_BATCHING_PASSTHRU_THRESHOLD,
    SHADOW,
    TAIL_OPTIMIZED};

static const std::set<std::string> logs_config_non_defaultable_keys = {
    "id",
    "name"};

static bool parseSubLogs(LocalLogsConfig::DirectoryNode* parent_ns,
                         const LogsConfigNamespace& namespace_prefix,
                         const std::string& ns_name,
                         const folly::dynamic& namespaceMap,
                         const SecurityConfig& securityConfig,
                         std::shared_ptr<LocalLogsConfig>& output,
                         LoadFileCallback loadFileCallback,
                         const ConfigParserOptions& options) {
  std::string scope = namespace_prefix.empty()
      ? "cluster"
      : "namespace \"" + namespace_prefix.toString() + "\"";

  // start with empty set of attributes
  LogAttributes ns_attrs;

  const folly::dynamic* localDefaults = namespaceMap.get_ptr("defaults");
  if (localDefaults) {
    if (!localDefaults->isObject()) {
      ld_error("\"defaults\" entry for %s not a JSON object", scope.c_str());
      err = E::INVALID_CONFIG;
      return false;
    }
    for (const auto& key : localDefaults->keys()) {
      if (logs_config_non_defaultable_keys.count(key.asString())) {
        ld_error("unexpected key %s in %s defaults",
                 key.asString().c_str(),
                 scope.c_str());
        err = E::INVALID_CONFIG;
        return false;
      }
    }
    if (localDefaults->size() > 0) {
      folly::Optional<LogAttributes> attrs =
          parseAttributes(*localDefaults,
                          std::string(),
                          securityConfig.allowPermissionsInConfig());
      if (!attrs) {
        return false;
      }
      ns_attrs = attrs.value();
    }
  }

  // log list could either be specified inline or included from an external
  // file.
  auto inline_iter = namespaceMap.find("logs");
  auto include_iter = namespaceMap.find("include_log_config");

  if (include_iter != namespaceMap.items().end() &&
      inline_iter != namespaceMap.items().end()) {
    ld_error("both \"logs\" and \"include_log_config\" "
             "entries specified for cluster");
    err = E::INVALID_CONFIG;
    return false;
  }

  folly::dynamic included_json = folly::dynamic::object;
  if (include_iter != namespaceMap.items().end()) {
    if (!namespace_prefix.empty()) {
      ld_error("include_log_config only supported for the whole logs config, "
               "not allowed under %s",
               scope.c_str());
      err = E::INVALID_CONFIG;
      return false;
    }

    if (!loadFileCallback) {
      // found valid include_log_config entry, but no callback passed -
      // indicates that we shouldn't follow up with the actual include.
      // Resetting the LogsConfig pointer to signal that we aren't able to
      // resolve log configuration locally.
      output.reset();
      return true;
    }
    std::string json_file;
    Status rv =
        loadFileCallback(include_iter->second.asString().c_str(), &json_file);
    if (rv == E::NOTREADY) {
      // Callback indicated that the included config isn't ready yet.
      // Silently abort parsing.
      err = E::NOTREADY;
      return false;
    }
    if (rv != E::OK) {
      ld_error("unable to load includable file \"%s\": %s",
               include_iter->second.asString().c_str(),
               error_name(rv));
      err = E::INVALID_CONFIG;
      return false;
    }
    included_json = parseJson(json_file);
    if (included_json == nullptr) {
      // assuming parseJson() set the error code and message already.
      return false;
    }
    auto sub_iter = included_json.find("logs");
    if (sub_iter != included_json.items().end()) {
      inline_iter = sub_iter;
    }
  }

  // This will be triggered if there is no entry in the included config file
  // above.
  if (inline_iter == namespaceMap.items().end()) {
    ld_debug("missing \"logs\" entry for %s", scope.c_str());
    err = E::LOGS_SECTION_MISSING;
    return false;
  }

  const folly::dynamic& logsList = inline_iter->second;
  if (!logsList.isArray()) {
    ld_error("\"logs\" entry for %s not an array", scope.c_str());
    err = E::INVALID_CONFIG;
    return false;
  }

  // Create the namespace in the tree
  // if this is the first namespace then
  // we need to create a Directory node and use this defaults (if not
  // defined we use the DefaultLogAttributes())
  LocalLogsConfig::DirectoryNode* namespace_dir = nullptr;
  if (parent_ns == nullptr) {
    std::unique_ptr<LocalLogsConfig::LogsConfigTree> tree =
        LocalLogsConfig::LogsConfigTree::create(
            namespace_prefix.getDelimiter(),
            LogAttributes(ns_attrs, LocalLogsConfig::DefaultLogAttributes()));
    output->setLogsConfigTree(std::move(tree));
    namespace_dir = output->getRootNamespace();
  } else {
    LocalLogsConfig::DirectoryNode* dir_node =
        output->insertNamespace(parent_ns, ns_name, ns_attrs);
    if (dir_node == nullptr) {
      ld_error("Namespace \"%s\" is already defined in the config",
               namespace_prefix.toString().c_str());
      err = E::INVALID_CONFIG;
      return false;
    }
    namespace_dir = dir_node;
  }

  for (const folly::dynamic& item : logsList) {
    if (!parseOneLogEntry(namespace_dir,
                          namespace_prefix + ns_name,
                          item,
                          securityConfig,
                          options,
                          output)) {
      // NOTE: one invalid log entry fails the entire config
      output->clearLogsConfigTree();
      return false;
    }
  }

  return true;
}

bool parseLogs(const folly::dynamic& clusterMap,
               std::shared_ptr<LocalLogsConfig>& output,
               const SecurityConfig& securityConfig,
               LoadFileCallback loadFileCallback,
               const std::string& ns_delimiter,
               const ConfigParserOptions& options) {
  ld_check(output->logsBegin() == output->logsEnd());

  if (!parseSubLogs(
          nullptr, // this has no parent, this means that it's the root
          LogsConfigNamespace(ns_delimiter),
          std::string(), // name of this namespace is just nothing!
          clusterMap,
          securityConfig,
          output,
          loadFileCallback,
          options)) {
    return false;
  }
  return true;
}

static bool parseSyncReplicationScope(const folly::dynamic& logMap,
                                      Attribute<NodeLocationScope>& scope_out,
                                      const std::string& interval_string) {
  NodeLocationScope scope;
  // Commented out until T23232554 lands.
  // It doesn't make sense to replicate across the top most 'ROOT' scope.
  // EnumFilter<NodeLocationScope>* scope_filter = [](const auto& v) {
  //   return v != NodeLocationScope::ROOT;
  // };
  bool success = parseEnum<NodeLocationScope>(
      logMap, SYNC_REPLICATION_SCOPE, scope, nullptr);

  if (success) {
    scope_out = scope;
  } else {
    // Optional field.
    if (err == E::NOTFOUND) {
      return true;
    }
    // Augment error from parseEnum to make the incorrect entry
    // easier to find and correct.
    ld_error("invalid value of \"sync_replicate_across\" was found "
             "for log(s) %s",
             interval_string.c_str());
  }
  return success;
}

static bool parseReplicateAcross(
    const folly::dynamic& logMap,
    Attribute<LogAttributes::ScopeReplicationFactors>& replication_out,
    const std::string& interval_string) {
  auto it = logMap.find(REPLICATE_ACROSS);
  if (it == logMap.items().end()) {
    return true;
  }

  const folly::dynamic& val = it->second;
  if (!val.isObject()) {
    ld_error("Invalid (non-object) value of \"replicate_across\" attribute for "
             "log(s) %s",
             interval_string.c_str());
    return false;
  }

  LogAttributes::ScopeReplicationFactors replication_factors;

  for (const auto& item : val.items()) {
    if (!item.first.isString()) {
      ld_error("Non-string key inside \"replicate_across\" attribute "
               "for log(s) %s",
               interval_string.c_str());
      return false;
    }
    if (!item.second.isInt()) {
      ld_error("Non-integer replication factor inside "
               "\"replicate_across\" attribute for log(s) %s",
               interval_string.c_str());
      return false;
    }

    std::string scope_str = item.first.getString();
    std::transform(
        scope_str.begin(), scope_str.end(), scope_str.begin(), ::toupper);
    NodeLocationScope scope =
        NodeLocation::scopeNames().reverseLookup(scope_str);
    static_assert(
        (int)NodeLocationScope::NODE == 0,
        "Did you add a location "
        "scope smaller than NODE? Update this validation code to allow it.");
    if (scope < NodeLocationScope::NODE || scope >= NodeLocationScope::ROOT) {
      ld_error("Invalid sync_replication_scope %s for log(s) %s",
               scope_str.c_str(),
               interval_string.c_str());
      return false;
    }

    int64_t r = item.second.getInt();
    if (r <= 0 || r > COPYSET_SIZE_MAX) {
      ld_error("Replication factor %ld for scope %s is out of range for log(s) "
               "%s",
               r,
               scope_str.c_str(),
               interval_string.c_str());
      return false;
    }

    replication_factors.emplace_back(scope, r);
  }

  // replication_factors is an std::vector formed from a json object.
  // Order of iteration of a json object is undefined.
  // Let's sort the vector to make parsing deterministic.
  // Sort from bigger to smaller scopes, like
  // ReplicationProperty::getDistinctReplicationFactors().
  std::sort(replication_factors.begin(),
            replication_factors.end(),
            std::greater<std::pair<NodeLocationScope, int>>());

  replication_out = Attribute<LogAttributes::ScopeReplicationFactors>(
      std::move(replication_factors));
  return true;
}

static bool parseNodeSetSize(const folly::dynamic& logMap,
                             Attribute<folly::Optional<int>>& nodeset_size_attr,
                             const std::string& interval_string) {
  int nodeset_size = 0;
  bool success =
      getIntFromMap<int>(logMap, "nodeset_size", nodeset_size, nullptr);
  if ((!success && err != E::NOTFOUND) || (success && nodeset_size <= 0)) {
    ld_error("invalid value of \"nodeset_size\" attribute "
             "for log(s) %s",
             interval_string.c_str());
    err = E::INVALID_CONFIG;
    return false;
  }

  if (success) {
    nodeset_size_attr = nodeset_size;
  }
  return true;
}

bool parseLogInterval(const folly::dynamic& logMap,
                      std::string& interval_string,
                      interval_t& interval_raw,
                      bool limit_logid_range) {
  int64_t logid;
  if (getIntFromMap<int64_t>(logMap, "id", logid)) {
    if (logid <= 0 || logid > USER_LOGID_MAX.val_) {
      ld_error("invalid \"id\" attribute: %lu. Expected a positive %zu-bit int",
               logid,
               LOGID_BITS);
      err = E::INVALID_CONFIG;
      return false;
    }
    // convert to string for use in error messages
    interval_string = folly::to<std::string>(logid);
  } else if (!getStringFromMap(logMap, "id", interval_string)) {
    ld_error("missing \"id\" entry for log");
    err = E::INVALID_CONFIG;
    return false;
  }

  if (parse_interval(interval_string.c_str(), &interval_raw) != 0 ||
      interval_raw.lo == 0 ||
      (limit_logid_range && interval_raw.hi > USER_LOGID_MAX.val_)) {
    ld_error("invalid format of log id attribute: \"%s\". Expected a single "
             "positive %zu-bit log id or lo..hi where lo and hi are the low "
             "and high ends of a closed log id interval",
             interval_string.c_str(),
             LOGID_BITS);
    err = E::INVALID_CONFIG;
    return false;
  }
  return true;
}

static bool parseOneLogRange(LocalLogsConfig::DirectoryNode* parent_ns,
                             const folly::dynamic& logMap,
                             const SecurityConfig& securityConfig,
                             const ConfigParserOptions& options,
                             LocalLogsConfig& output) {
  std::string interval_string;
  interval_t interval_raw;

  // The name is required
  std::string name;
  if (!getStringFromMap(logMap, "name", name)) {
    ld_error(
        "Invalid or missing value for \"name\" attribute of log range '%s'. "
        "Expected a string.",
        interval_string.c_str());
    err = E::INVALID_CONFIG;
    return false;
  }

  // TODO(T14426956): The following should be removed once all configurations
  // for production tiers don't have this log group anymore.
  static const char* event_log_group_name = "logdevice_event_log";
  if (name == event_log_group_name ||
      name == output.getDelimiter() + event_log_group_name) {
    ld_warning("User log configuration contains log group with name \"%s\", "
               "skipping it since this is an internal log and internal logs "
               "are now configured in the main config",
               name.c_str());
    return true;
  }

  if (!parseLogInterval(logMap, interval_string, interval_raw, true)) {
    return false;
  }

  // this and the sanity check in the if statement above ensure that
  // interval_raw.hi+1 does not overflow
  static_assert((uint64_t)USER_LOGID_MAX.val_ <
                    std::numeric_limits<logid_t::raw_type>::max(),
                "USER_LOGID_MAX must be smaller than LONG_LONG_MAX");

  logid_range_t logid_interval(
      logid_t(interval_raw.lo), logid_t(interval_raw.hi));

  folly::Optional<LogAttributes> attrs = parseAttributes(
      logMap, interval_string, securityConfig.allowPermissionsInConfig());
  if (!attrs) {
    return false;
  }

  std::shared_ptr<LocalLogsConfig::LogGroupNode> log_group_node =
      output.insert(parent_ns, logid_interval, name, attrs.value());
  if (!log_group_node) {
    return false;
  }

  return true;
}

static bool parseOneLogEntry(LocalLogsConfig::DirectoryNode* parent_dir,
                             const LogsConfigNamespace& namespace_prefix,
                             const folly::dynamic& logMap,
                             const SecurityConfig& securityConfig,
                             const ConfigParserOptions& options,
                             std::shared_ptr<LocalLogsConfig>& output) {
  if (!logMap.isObject()) {
    ld_error("log config must be a map");
    err = E::INVALID_CONFIG;
    return false;
  }

  std::string ns_name;
  if (!getStringFromMap(logMap, "namespace", ns_name)) {
    // This is a log range
    return parseOneLogRange(
        parent_dir, logMap, securityConfig, options, *output);
  } else {
    // this is a namespace
    return parseSubLogs(parent_dir,
                        namespace_prefix,
                        ns_name,
                        logMap,
                        securityConfig,
                        output,
                        LoadFileCallback(),
                        options);
  }
}

folly::Optional<LogAttributes>
parseAttributes(const folly::dynamic& attrs,
                const std::string& interval_string,
                bool allow_permissions,
                bool metadata_logs) {
  ld_check(attrs.isObject());
  Attribute<int> replicationFactor;
  getIntAttributeFromMap<int>(
      attrs, REPLICATION_FACTOR, replicationFactor, nullptr);

  Attribute<int> syncedCopies;
  getIntAttributeFromMap<int>(attrs, SYNCED_COPIES, syncedCopies, nullptr);

  // sync_replicate_across is optional
  Attribute<NodeLocationScope> syncReplicationScope;
  parseSyncReplicationScope(attrs, syncReplicationScope, interval_string);

  Attribute<LogAttributes::ScopeReplicationFactors> replicateAcross;
  parseReplicateAcross(attrs, replicateAcross, interval_string);

  if (metadata_logs) {
    // The rest of the fields are not configurable for metadata logs.
    return LogAttributes(
        replicationFactor,
        0, /* extraCopies */
        syncedCopies.hasValue() ? syncedCopies : Attribute<int>(0),
        SLIDING_WINDOW_MIN_CAPACITY, /* maxWritesInFlight */
        false,                       /* singleWriter */
        syncReplicationScope,
        replicateAcross,
        /* backlogDuration [Unset means INF] */
        Attribute<folly::Optional<std::chrono::seconds>>(
            folly::Optional<std::chrono::seconds>()),
        /* nodeSetSize */
        Attribute<folly::Optional<int>>(folly::Optional<int>()),
        Attribute<folly::Optional<std::chrono::milliseconds>>(
            folly::Optional<std::chrono::milliseconds>()), /* deliveryLatency */
        false,                                             /* scdEnabled */
        false,                                             /* localScdEnabled */
        /* writeToken */
        Attribute<folly::Optional<std::string>>(folly::Optional<std::string>()),
        false, /* stickyCopySets */
        true,  /* mutablePerEpochLogMetadataEnabled */
        Attribute<LogAttributes::PermissionsMap>(), /* permissions */
        Attribute<LogAttributes::ACLList>(),        /* acls */
        Attribute<LogAttributes::ACLList>(),        /* acls_shadow*/
        Attribute<folly::Optional<std::string>>(
            folly::Optional<std::string>()),    /* sequencerAffinity */
        false,                                  /* sequencerBatching */
        Attribute<std::chrono::milliseconds>(), /* sequencerBatchingTimeTrigger
                                                 */
        Attribute<ssize_t>(),     /* sequencerBatchingSizeTrigger */
        Attribute<Compression>(), /* sequencerBatchingCompression */
        Attribute<ssize_t>(),     /* sequencerBatchingPassthruThreshold */
        Attribute<LogAttributes::Shadow>(),     /* shadow */
        false,                                  /* tail optimized */
        Attribute<LogAttributes::ExtrasMap>()); /* extras */
  }

  Attribute<int> extraCopies;
  getIntAttributeFromMap<int>(attrs, EXTRA_COPIES, extraCopies, nullptr);

  Attribute<int> maxWritesInFlight;
  getIntAttributeFromMap<int>(
      attrs, MAX_WRITES_IN_FLIGHT, maxWritesInFlight, nullptr);

  Attribute<bool> singleWriter;
  bool singleWriter_;
  // single_writer is optional, defaults to false (Defined in
  // DefaultLogAttributes.h)
  bool success = getBoolFromMap(attrs, SINGLE_WRITER, singleWriter_, nullptr);
  if (success) {
    singleWriter = singleWriter_;
  } else if (!success && err != E::NOTFOUND) {
    ld_error(
        "Invalid value of \"%s\" attribute for log(s) %s. Expected a bool.",
        SINGLE_WRITER,
        interval_string.c_str());
    err = E::INVALID_CONFIG;
    return folly::none;
  }

  // folly::dynamic does not have a default constructor.
  folly::dynamic acls_dynamic = folly::dynamic::array;

  success = getArrayFromMap(attrs, ACLS, acls_dynamic, nullptr);
  // acl field is optional
  if (!success && err != E::NOTFOUND) {
    ld_error("Invalid value of \"acls\" attribute for log(s) %s. "
             "Expected a list.",
             interval_string.c_str());
    err = E::INVALID_CONFIG;
    return folly::none;
  }

  Attribute<LogAttributes::ACLList> acls;
  // If the config contains a acls field, parse it.
  if (success && !parseLogACLs(acls_dynamic, ACLS, acls)) {
    // parseLogACLs takes cares of the error code and message
    return folly::none;
  }

  success = getArrayFromMap(attrs, ACLS_SHADOW, acls_dynamic, nullptr);
  // acl field is optional
  if (!success && err != E::NOTFOUND) {
    ld_error("Invalid value of \"acls_shadow\" attribute for log(s) %s. "
             "Expected a list.",
             interval_string.c_str());
    err = E::INVALID_CONFIG;
    return folly::none;
  }

  Attribute<LogAttributes::ACLList> acls_shadow;
  // If the config contains a acls_shadow field, parse it.
  if (success && !parseLogACLs(acls_dynamic, ACLS_SHADOW, acls_shadow)) {
    // parseLogACLs takes cares of the error code and message
    return folly::none;
  }

  // folly::dynamic does not have a default constructor.
  folly::dynamic permissions_dynamic = folly::dynamic::array;
  success = getObjectFromMap(attrs, PERMISSIONS, permissions_dynamic, nullptr);

  if (!allow_permissions && success) {
    ld_error("unexpected \"permissions\" entry found in log(s) %s. "
             "\"permissions\" is only allowed when \"permission_checker_type\""
             " in \"security_information\" is set to \"config\"",
             interval_string.c_str());
    err = E::INVALID_CONFIG;
    return folly::none;
  }

  // Permissions field is optional
  if (!success && err != E::NOTFOUND) {
    ld_error("Invalid value of \"%s\" attribute for log(s) %s. Expected a map.",
             PERMISSIONS,
             interval_string.c_str());
    err = E::INVALID_CONFIG;
    return folly::none;
  }

  Attribute<LogAttributes::PermissionsMap> permissions;
  // If the config contains a permissions field, parse it.
  if (success && !parseLogPermissions(permissions_dynamic, permissions)) {
    // parseLogPermissions takes cares of the error code and message
    return folly::none;
  }

  // parse the optional nodesetsize parameter
  Attribute<folly::Optional<int>> nodesetSize;
  if (!parseNodeSetSize(attrs, nodesetSize, interval_string)) {
    return folly::none;
  }

  // parse optional backlog and delivery_latency parameters
  Attribute<folly::Optional<std::chrono::seconds>> backlogDuration;
  if (!parseChronoValueOptAttribute(
          attrs, BACKLOG, "log(s): " + interval_string, backlogDuration)) {
    return folly::none;
  }

  Attribute<folly::Optional<std::chrono::milliseconds>> deliveryLatency;
  std::chrono::milliseconds latency;
  if (parseChronoValue(attrs,
                       DELIVERY_LATENCY,
                       "log(s): " + interval_string,
                       &latency,
                       nullptr)) {
    deliveryLatency = latency;
  } else if (err != E::NOTFOUND) {
    return folly::none;
  }

  // sticky_copysets is optional, defaults to false
  Attribute<bool> stickyCopySets;
  bool sticky_copy_sets_bool;
  success =
      getBoolFromMap(attrs, STICKY_COPYSETS, sticky_copy_sets_bool, nullptr);
  if (success) {
    stickyCopySets = sticky_copy_sets_bool;
  } else if (!success && err != E::NOTFOUND) {
    ld_error(
        "Invalid value of \"%s\" attribute for log(s) %s. Expected a bool.",
        STICKY_COPYSETS,
        interval_string.c_str());
    err = E::INVALID_CONFIG;
    return folly::none;
  }

  // mutable_per_epoch_log_metadata_enabled is optional, defaults to true
  Attribute<bool> mutablePerEpochLogMetadataEnabled;
  bool mutable_per_epoch_log_metadata_enabled_bool;
  success = getBoolFromMap(attrs,
                           MUTABLE_PER_EPOCH_LOG_METADATA_ENABLED,
                           mutable_per_epoch_log_metadata_enabled_bool,
                           nullptr);
  if (success) {
    mutablePerEpochLogMetadataEnabled =
        mutable_per_epoch_log_metadata_enabled_bool;
  } else if (err != E::NOTFOUND) {
    ld_error(
        "Invalid value of \"%s\" attribute for log(s) %s. Expected a bool.",
        MUTABLE_PER_EPOCH_LOG_METADATA_ENABLED,
        interval_string.c_str());
    err = E::INVALID_CONFIG;
    return folly::none;
  }

  // Optional, defaults to false.
  Attribute<bool> scdEnabled;
  bool scdEnabled_bool = false;
  success = getBoolFromMap(attrs, SCD_ENABLED, scdEnabled_bool, nullptr);
  if (success) {
    scdEnabled = scdEnabled_bool;
  } else if (!success && err != E::NOTFOUND) {
    ld_error("Invalid value for \"%s\" attribute of log range '%s'. Expected "
             "a bool.",
             SCD_ENABLED,
             interval_string.c_str());
    err = E::INVALID_CONFIG;
    return folly::none;
  }

  // Optional, defaults to false.
  Attribute<bool> localScdEnabled;
  bool localScdEnabled_bool = false;
  success =
      getBoolFromMap(attrs, LOCAL_SCD_ENABLED, localScdEnabled_bool, nullptr);
  if (success) {
    localScdEnabled = localScdEnabled_bool;
  } else if (!success && err != E::NOTFOUND) {
    ld_error("Invalid value for \"%s\" attribute of log range '%s'. Expected "
             "a bool.",
             LOCAL_SCD_ENABLED,
             interval_string.c_str());
    err = E::INVALID_CONFIG;
    return folly::none;
  }

  Attribute<folly::Optional<std::string>> writeToken;
  std::string write_token;
  if (getStringFromMap(attrs, WRITE_TOKEN, write_token)) {
    writeToken = write_token;
  }

  success = false;
  Attribute<folly::Optional<std::string>> sequencerAffinity;
  std::string sequencer_affinity;
  if (getStringFromMap(attrs, SEQUENCER_AFFINITY, sequencer_affinity)) {
    if (NodeLocation::validDomain(sequencer_affinity)) {
      sequencerAffinity = sequencer_affinity;
      success = true;
    }
  } else if (err == E::NOTFOUND) {
    success = true;
  }
  if (!success) {
    ld_error("Invalid value for \"%s\" attribute of log range '%s'. Expected "
             "a valid domain string.",
             SEQUENCER_AFFINITY,
             interval_string.c_str());
    err = E::INVALID_CONFIG;
    return folly::none;
  }

  Attribute<bool> sequencerBatching;
  bool sequencerBatching_bool = false;
  success = getBoolFromMap(
      attrs, SEQUENCER_BATCHING, sequencerBatching_bool, nullptr);
  if (success) {
    sequencerBatching = sequencerBatching_bool;
  } else if (!success && err != E::NOTFOUND) {
    ld_error("Invalid value for \"%s\" attribute of log range '%s'. Expected "
             "a bool.",
             SEQUENCER_BATCHING,
             interval_string.c_str());
    err = E::INVALID_CONFIG;
    return folly::none;
  }

  Attribute<std::chrono::milliseconds> sequencerBatchingTimeTrigger;
  std::chrono::milliseconds sequencerBatchingTimeTrigger_chrono;
  if (parseChronoValue(attrs,
                       SEQUENCER_BATCHING_TIME_TRIGGER,
                       "log(s): " + interval_string,
                       &sequencerBatchingTimeTrigger_chrono,
                       nullptr)) {
    sequencerBatchingTimeTrigger = sequencerBatchingTimeTrigger_chrono;
  } else if (err != E::NOTFOUND) {
    return folly::none;
  }

  Attribute<ssize_t> sequencerBatchingSizeTrigger;
  ssize_t sequencerBatchingSizeTrigger_ssize_t;
  if (getIntFromMap<ssize_t>(attrs,
                             SEQUENCER_BATCHING_SIZE_TRIGGER,
                             sequencerBatchingSizeTrigger_ssize_t,
                             nullptr)) {
    sequencerBatchingSizeTrigger = sequencerBatchingSizeTrigger_ssize_t;
  } else if (err != E::NOTFOUND) {
    ld_error("Invalid value for \"%s\" attribute of log range '%s'. "
             "Integer expected.",
             SEQUENCER_BATCHING_SIZE_TRIGGER,
             interval_string.c_str());
    err = E::INVALID_CONFIG;
    return folly::none;
  }

  Attribute<Compression> sequencerBatchingCompression;
  std::string sequencerBatchingCompression_string;
  if (getStringFromMap(attrs,
                       SEQUENCER_BATCHING_COMPRESSION,
                       sequencerBatchingCompression_string)) {
    Compression compression;
    auto rv = parseCompression(
        sequencerBatchingCompression_string.c_str(), &compression);
    if (rv == -1) {
      ld_error("Invalid value for \"%s\" attribute of log range '%s'. "
               "Valid compression codec expected, got '%s'",
               SEQUENCER_BATCHING_COMPRESSION,
               interval_string.c_str(),
               sequencerBatchingCompression_string.c_str());
      err = E::INVALID_CONFIG;
      return folly::none;
    }
    sequencerBatchingCompression = compression;
  } else if (err != E::NOTFOUND) {
    ld_error("Invalid value for \"%s\" attribute of log range '%s'. "
             "String expected.",
             SEQUENCER_BATCHING_COMPRESSION,
             interval_string.c_str());
    err = E::INVALID_CONFIG;
    return folly::none;
  }

  Attribute<ssize_t> sequencerBatchingPassthruThreshold;
  ssize_t sequencerBatchingPassthruThreshold_ssize_t;
  if (getIntFromMap<ssize_t>(attrs,
                             SEQUENCER_BATCHING_PASSTHRU_THRESHOLD,
                             sequencerBatchingPassthruThreshold_ssize_t,
                             nullptr)) {
    sequencerBatchingPassthruThreshold =
        sequencerBatchingPassthruThreshold_ssize_t;
  } else if (err != E::NOTFOUND) {
    ld_error("Invalid value for \"%s\" attribute of log range '%s'. "
             "Integer expected.",
             SEQUENCER_BATCHING_PASSTHRU_THRESHOLD,
             interval_string.c_str());
    err = E::INVALID_CONFIG;
    return folly::none;
  }

  Attribute<LogAttributes::Shadow> shadow;
  folly::dynamic shadow_dynamic = folly::dynamic::object();
  success = getObjectFromMap(attrs, SHADOW, shadow_dynamic, nullptr);
  if (success && !parseShadow(shadow_dynamic, shadow)) {
    return folly::none;
  }

  // Optional, defaults to false in logs/DefaultLogAttributes.h.
  Attribute<bool> tailOptimized;
  bool tailOptimized_bool = false;
  success = getBoolFromMap(attrs, TAIL_OPTIMIZED, tailOptimized_bool, nullptr);
  if (success) {
    tailOptimized = tailOptimized_bool;
  } else if (!success && err != E::NOTFOUND) {
    ld_error("Invalid value for \"%s\" attribute of log range '%s'. Expected "
             "a bool.",
             TAIL_OPTIMIZED,
             interval_string.c_str());
    err = E::INVALID_CONFIG;
    return folly::none;
  }

  // Adding fields that logdevice doesn't recognize
  Attribute<LogAttributes::ExtrasMap> extras;
  LogAttributes::ExtrasMap extras_map;
  for (auto& pair : attrs.items()) {
    if (logs_config_recognized_keys.find(pair.first.asString()) !=
        logs_config_recognized_keys.end()) {
      // This key is supposed to be parsed by logdevice
      continue;
    }
    // We only support extra attributes that are "string" typed, everything
    // else will fail to parse to avoid confusion.
    if (!pair.second.isString()) {
      ld_warning("Invalid type of an extra attribute \"%s\". "
                 "Expected a String. Attribute will be ignored!",
                 pair.first.asString().c_str());
      continue;
    }
    extras_map[pair.first.asString()] = pair.second.getString();
  }
  if (extras_map.size() > 0) {
    extras = extras_map;
  }

  LogAttributes output{replicationFactor,
                       extraCopies,
                       syncedCopies,
                       maxWritesInFlight,
                       singleWriter,
                       syncReplicationScope,
                       replicateAcross,
                       backlogDuration,
                       nodesetSize,
                       deliveryLatency,
                       scdEnabled,
                       localScdEnabled,
                       writeToken,
                       stickyCopySets,
                       mutablePerEpochLogMetadataEnabled,
                       permissions,
                       acls,
                       acls_shadow,
                       sequencerAffinity,
                       sequencerBatching,
                       sequencerBatchingTimeTrigger,
                       sequencerBatchingSizeTrigger,
                       sequencerBatchingCompression,
                       sequencerBatchingPassthruThreshold,
                       shadow,
                       tailOptimized,
                       extras};
  return folly::Optional<LogAttributes>(std::move(output));
}

}}}} // namespace facebook::logdevice::configuration::parser
