/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/configuration/ParsingHelpers.h"

#include <algorithm>
#include <fcntl.h>
#include <unordered_set>

#include <folly/FileUtil.h>
#include <folly/dynamic.h>
#include <folly/json.h>

#include "logdevice/common/PriorityMap.h"
#include "logdevice/common/SecurityInformation.h"
#include "logdevice/common/commandline_util_chrono.h"
#include "logdevice/common/configuration/NodeLocation.h"
#include "logdevice/common/configuration/ZookeeperConfig.h"
#include "logdevice/common/util.h"

using namespace facebook::logdevice::configuration;

namespace facebook { namespace logdevice { namespace configuration {
namespace parser {

EnumFilter<TrafficClass>* read_tc_filter = [](const auto& v) {
  return std::find(read_traffic_classes.begin(),
                   read_traffic_classes.end(),
                   v) != read_traffic_classes.end();
};

/**
 * Helper method to extract a string value from a map, given a key.
 */
bool getStringFromMap(const folly::dynamic& map,
                      const char* key,
                      std::string& output,
                      const folly::dynamic* defaults) {
  auto f = [&](const folly::dynamic& val) {
    if (!val.isString()) {
      return false;
    }
    output = val.asString();
    return true;
  };

  return getFromMap(map, key, defaults, f);
}

bool getObjectFromMap(const folly::dynamic& map,
                      const char* key,
                      folly::dynamic& outputMap,
                      const folly::dynamic* defaults) {
  auto f = [&](const folly::dynamic& val) {
    if (!val.isObject()) {
      return false;
    }
    outputMap = val;
    return true;
  };

  return getFromMap(map, key, defaults, f);
}

bool getArrayFromMap(const folly::dynamic& map,
                     const char* key,
                     folly::dynamic& outputMap,
                     const folly::dynamic* defaults) {
  auto f = [&](const folly::dynamic& val) {
    if (!val.isArray()) {
      return false;
    }
    outputMap = val;
    return true;
  };

  return getFromMap(map, key, defaults, f);
}

/**
 * Helper method to extract a bool value from a map, given a key.
 *
 * NOTE: return value indicates success, output will contain the extracted bool.
 */
bool getBoolFromMap(const folly::dynamic& map,
                    const char* key,
                    bool& output,
                    const folly::dynamic* defaults) {
  auto f = [&](const folly::dynamic& val) {
    if (!val.isBool()) {
      return false;
    }
    output = val.asBool();
    return true;
  };

  return getFromMap(map, key, defaults, f);
}

/**
 * Helper method to extract a double value from a map, given a key.
 */
bool getDoubleFromMap(const folly::dynamic& map,
                      const char* key,
                      double& output,
                      const folly::dynamic* defaults) {
  auto f = [&](const folly::dynamic& val) {
    if (!val.isNumber()) {
      return false;
    }
    output = val.asDouble();
    return true;
  };

  return getFromMap(map, key, defaults, f);
}

std::string readFileIntoString(const char* path) {
  int fd = open(path, O_RDONLY);
  if (fd == -1) {
    ld_error("failed to open config file \"%s\" for reading"
             ", errno=%d (%s)",
             path,
             errno,
             strerror(errno));
    err = E::FILE_OPEN;
    return "";
  }

  off_t filesize = lseek(fd, 0, SEEK_END);
  if (filesize == (off_t)-1) {
    ld_error(
        "lseek failed unexpectedly, errno=%d (%s)", errno, strerror(errno));
    err = E::FILE_READ;
    close(fd);
    return "";
  }

  std::unique_ptr<char[]> data(new char[filesize]);

  lseek(fd, 0, SEEK_SET);
  if (folly::readFull(fd, data.get(), filesize) != filesize) {
    ld_error("failed to read contents of config file \"%s\""
             ", errno=%d (%s)",
             path,
             errno,
             strerror(errno));
    err = E::FILE_READ;
    close(fd);
    return "";
  }

  if (close(fd) != 0) {
    ld_warning("failed to close config file \"%s\" after reading"
               ", errno=%d (%s)",
               path,
               errno,
               strerror(errno));
  }
  return std::string(data.get(), filesize);
}

folly::dynamic parseJson(const std::string& jsonPiece) {
  folly::dynamic parsed = folly::dynamic::object;
  try {
    folly::json::serialization_opts opts;
    opts.allow_trailing_comma = true;
    parsed = folly::parseJson(jsonPiece, opts);
  } catch (std::exception& e) {
    ld_error("error parsing JSON from config file: %s", e.what());
    err = E::INVALID_CONFIG;
    return nullptr;
  }

  if (!parsed.isObject()) {
    ld_error("top-level JSON item is not a map");
    err = E::INVALID_CONFIG;
    return nullptr;
  }
  return parsed;
}

bool parseClusterName(const folly::dynamic& clusterMap, std::string& output) {
  if (!getStringFromMap(clusterMap, "cluster", output)) {
    ld_error("\"cluster\" entry is missing or is not a string");
    return false;
  }

  if (!validClusterName(output)) {
    ld_error("\"cluster\" entry has an invalid format. Expected "
             " a non-empty string of printable characters with no / and "
             "no longer than %zu chars. Got \"%s\"",
             ZookeeperConfig::MAX_CLUSTER_NAME,
             output.c_str());
    return false;
  }

  return true;
}

bool parseClusterCreationTime(
    const folly::dynamic& clusterMap,
    ServerConfig::OptionalTimestamp& clusterCreationTime) {
  int64_t creationTime;
  if (getIntFromMap<int64_t>(
          clusterMap, "cluster_creation_time", creationTime)) {
    clusterCreationTime.assign(std::chrono::seconds(creationTime));
  } else if (err != E::NOTFOUND) {
    ld_error("Invalid value for the cluster_creation_time property");
    return false;
  }

  return true;
}

bool parseVersion(const folly::dynamic& clusterMap, config_version_t& output) {
  int64_t version;
  folly::dynamic defaults = folly::dynamic::object("version", 1);
  if (!getIntFromMap<int64_t>(clusterMap, "version", version, &defaults) ||
      version < 1 || version > UINT32_MAX) {
    ld_error("\"version\" entry must be a positive integer no greater than"
             " UINT32_MAX");
    return false;
  }
  output = config_version_t(version);
  return true;
}

bool parseOneSetting(std::string key,
                     const folly::dynamic& value,
                     const std::string& element_name,
                     ServerConfig::SettingsConfig& output) {
  std::string val;
  try {
    val = value.asString();
  } catch (const folly::TypeError& e) {
    ld_error("Cannot parse value for setting \"%s\" in \"%s\": %s",
             key.c_str(),
             element_name.c_str(),
             e.what());
    err = E::INVALID_CONFIG;
    return false;
  }

  if (output.count(key)) {
    ld_error("Duplicate entry for setting \"%s\" in \"%s\"",
             element_name.c_str(),
             key.c_str());
    err = E::INVALID_CONFIG;
    return false;
  }
  output[key] = std::move(val);

  return true;
}

bool parseSettings(const folly::dynamic& map,
                   const std::string& element_name,
                   ServerConfig::SettingsConfig& output) {
  auto iter = map.find(element_name);
  if (iter == map.items().end()) {
    // No settings seem to be defined in the config file.
    return true;
  }

  const folly::dynamic& settingsSection = iter->second;
  if (!settingsSection.isObject()) {
    ld_error("\"%s\" entry is not a JSON object", element_name.c_str());
    err = E::INVALID_CONFIG;
    return false;
  }

  for (const auto& p : settingsSection.items()) {
    if (!p.first.isString()) {
      ld_error("Key for entry in \"%s\" section is not a string",
               element_name.c_str());
      err = E::INVALID_CONFIG;
      return false;
    }
    if (!parseOneSetting(p.first.asString(), p.second, element_name, output)) {
      return false;
    }
  }

  return true;
}

bool validClusterName(std::string name) {
  if (name.length() == 0 || name.length() > ZookeeperConfig::MAX_CLUSTER_NAME) {
    return false;
  }

  for (auto ch : name) {
    if (!isprint(ch) || ch == '/') {
      return false;
    }
  }

  return true;
}

template <typename Enum, typename EnumMapType>
bool parseEnumImpl(const char* enum_name,
                   EnumMapType& enum_name_map,
                   const folly::dynamic& map,
                   const char* key,
                   Enum& value,
                   const folly::dynamic* defaults,
                   EnumFilter<Enum>* filter) {
  ld_check(enum_name_map.size() != 0);

  std::string output;
  if (!getStringFromMap(map, key, output, defaults)) {
    return false;
  }

  std::transform(output.begin(), output.end(), output.begin(), ::toupper);
  Enum e = enum_name_map.reverseLookup(output);

  if (e == enum_name_map.invalidEnum() || (filter != nullptr && !filter(e))) {
    std::string allowed_values;
    bool first = true;

    for (int i = 0; i < enum_name_map.size(); i++) {
      const std::string& name = enum_name_map[i];
      if (name != enum_name_map.invalidValue() &&
          (filter == nullptr || filter(static_cast<Enum>(i)))) {
        allowed_values += first ? "\"" : "\", \"";
        allowed_values += name;
        first = false;
      }
    }
    // If this assert fires, the filter is bad since it excludes all values
    // from the EnumMap.
    ld_check(!first);
    allowed_values += '"';

    ld_error("Invalid %s \"%s\" found in key \"%s\". Allowed values: %s.",
             enum_name,
             output.c_str(),
             key,
             allowed_values.c_str());
    err = E::INVALID_CONFIG;
    return false;
  }

  value = e;
  return true;
}

#define PARSE_ENUM_INST(Type, Name, EnumMap)                   \
  template <>                                                  \
  bool parseEnum<Type>(const folly::dynamic& dynamic,          \
                       const char* key,                        \
                       Type& value,                            \
                       const folly::dynamic* defaults,         \
                       EnumFilter<Type>* filter) {             \
    return parseEnumImpl<Type, decltype(EnumMap)>(             \
        Name, EnumMap, dynamic, key, value, defaults, filter); \
  }

PARSE_ENUM_INST(Priority, "priority", PriorityMap::toName())
PARSE_ENUM_INST(TrafficClass, "traffic class", trafficClasses())
PARSE_ENUM_INST(NodeLocationScope, "scope", NodeLocation::scopeNames())

static const std::unordered_set<std::string> validPermissionActions{
    "READ",
    "APPEND",
    "TRIM",
};

bool parseLogPermissions(
    const folly::dynamic& permissions,
    logsconfig::Attribute<logsconfig::LogAttributes::PermissionsMap>& output) {
  if (!permissions.isObject()) {
    ld_error("items with in \"permissions\" should be a map");
    err = E::INVALID_CONFIG;
    return false;
  }

  logsconfig::LogAttributes::PermissionsMap permissionMap;
  for (const auto& item : permissions.items()) {
    if (!item.first.isString()) {
      ld_error("Principals must be defined as a string in \"permissions\"");
      err = E::INVALID_CONFIG;
      return false;
    }

    std::string principal = item.first.asString();

    if (!Principal::isAllowedInConfig(principal)) {
      ld_error("%s is a reserved principal", principal.c_str());
      err = E::INVALID_CONFIG;
      return false;
    }

    const auto& user_permission = item.second;
    if (!user_permission.isArray()) {
      ld_error("items within \"permissions\" must be mapped to an array");
      err = E::INVALID_CONFIG;
      return false;
    }

    std::array<bool, static_cast<int>(ACTION::MAX)> permission_array;
    permission_array.fill(false);

    for (const auto& action : user_permission) {
      if (!action.isString()) {
        ld_error("items within the permissions array must be strings");
        err = E::INVALID_CONFIG;
        return false;
      }

      auto iter = validPermissionActions.find(action.asString());
      if (iter == validPermissionActions.end()) {
        ld_error("invalid action \"%s\" found in permission array for \"%s\". "
                 "Valid actions are \"READ\" \"APPEND\" or \"TRIM\"",
                 action.asString().c_str(),
                 principal.c_str());
        err = E::INVALID_CONFIG;
        return false;
      }

      if (*iter == "READ") {
        permission_array[static_cast<int>(ACTION::READ)] = true;
      } else if (*iter == "APPEND") {
        permission_array[static_cast<int>(ACTION::APPEND)] = true;
      } else if (*iter == "TRIM") {
        permission_array[static_cast<int>(ACTION::TRIM)] = true;
      }
    }

    permissionMap.insert(std::make_pair(principal, permission_array));
  }

  output = permissionMap;
  return true;
}

bool parseLogACLs(
    const folly::dynamic& acls,
    const std::string& field,
    logsconfig::Attribute<logsconfig::LogAttributes::ACLList>& output) {
  if (!acls.isArray()) {
    ld_error("items with in \"%s\" should be a array", field.c_str());
    err = E::INVALID_CONFIG;
    return false;
  }

  logsconfig::LogAttributes::ACLList acl_list;
  for (const auto& item : acls) {
    if (!item.isString()) {
      ld_error(
          "ACL array item must be of string type in \"%s\"", field.c_str());
      err = E::INVALID_CONFIG;
      return false;
    }
    acl_list.emplace_back(item.asString());
  }

  output = acl_list;
  return true;
}

bool parseShadow(
    const folly::dynamic& shadow,
    logsconfig::Attribute<logsconfig::LogAttributes::Shadow>& output) {
  if (!shadow.isObject()) {
    ld_error("\"%s\" should be a map", logsconfig::SHADOW);
    err = E::INVALID_CONFIG;
    return false;
  }

  std::string destination;
  bool success = getStringFromMap(shadow, logsconfig::SHADOW_DEST, destination);
  if (!success) {
    ld_error("\"%s\" must be a string entry within shadow map",
             logsconfig::SHADOW_DEST);
    err = E::INVALID_CONFIG;
    return false;
  }

  double ratio;
  success = getDoubleFromMap(shadow, logsconfig::SHADOW_RATIO, ratio);
  if (!success) {
    ld_error("\"%s\" must be a double entry within shadow map",
             logsconfig::SHADOW_RATIO);
    err = E::INVALID_CONFIG;
    return false;
  }

  output = logsconfig::LogAttributes::Shadow(destination, ratio);
  return true;
}

bool parsePrincipals(const folly::dynamic& clusterMap,
                     PrincipalsConfig& output) {
  auto iter = clusterMap.find("principals");
  if (iter == clusterMap.items().end()) {
    // Optional section.
    return true;
  }

  const auto& principals = iter->second;
  if (!principals.isArray()) {
    ld_error("\"principals\" entry for cluster must be an array");
    err = E::INVALID_CONFIG;
    return false;
  }

  auto& principals_map = output.principals;

  for (const auto& principal : principals) {
    if (!principal.isObject()) {
      ld_error("A principal entry in the \"principals\" array is not a map");
      err = E::INVALID_CONFIG;
      return false;
    }

    std::string name;
    bool successful = getStringFromMap(principal, "name", name);
    if (!successful) {
      ld_error("\"name\" must be a string entry within a principal entry");
      err = E::INVALID_CONFIG;
      return false;
    }

    // Allow default principals to be modified?
    if (principals_map.find(name) != principals_map.end()) {
      ld_error("Duplicate principal \"%s\" found in the \"principals\" array",
               name.c_str());
      err = E::INVALID_CONFIG;
      return false;
    }

    auto map_entry = std::make_shared<Principal>(name);
    successful = parseEnum(principal,
                           "max_read_traffic_class",
                           map_entry->max_read_traffic_class,
                           /*defaults*/ nullptr,
                           read_tc_filter);
    // "max_read_traffic_class" is an optional field, so ignore NOTFOUND
    // errors.
    if (!successful && err != E::NOTFOUND) {
      ld_error("While processing principal \"%s\".", name.c_str());
      err = E::INVALID_CONFIG;
      return false;
    }

    successful =
        getIntFromMap(principal, "egress_dscp", map_entry->egress_dscp);
    // "egress_dscp" is an optional field, so ignore NOTFOUND errors.
    if (!successful && err != E::NOTFOUND) {
      ld_error("While processing principal \"%s\".", name.c_str());
      err = E::INVALID_CONFIG;
      return false;
    }

    principals_map.insert({name, map_entry});
  }

  return true;
}

bool parseSecurityInfo(const folly::dynamic& clusterMap,
                       SecurityConfig& securityConfig) {
  auto security_info_iter = clusterMap.find("security_information");
  if (security_info_iter == clusterMap.items().end()) {
    // Not a required Field
    return true;
  }

  auto security_info = security_info_iter->second;
  if (!security_info.isObject()) {
    ld_error("\"security_information\" must be a map");
    err = E::INVALID_CONFIG;
    return false;
  }

  std::string auth_type_string;
  bool success =
      getStringFromMap(security_info, "authentication_type", auth_type_string);
  if (!success) {
    ld_error("\"authentication_type\" must be a string entry within "
             "\"security_information\"");
    err = E::INVALID_CONFIG;
    return false;
  }

  auto authType =
      AuthenticationTypeTranslator::toAuthenticationType(auth_type_string);
  if (authType == AuthenticationType::MAX) {
    ld_error(
        "unrecognized \"authentication_type\" %s.", auth_type_string.c_str());
    err = E::INVALID_CONFIG;
    return false;
  }
  if (authType == AuthenticationType::NONE) {
    ld_error("\"authentication_type\" should not be of type \"NONE\".");
    err = E::INVALID_CONFIG;
    return false;
  }

  securityConfig.authenticationType = authType;

  // Check that the translation is consistent
  ld_check(AuthenticationTypeTranslator::toString(authType) ==
           auth_type_string);

  success = getBoolFromMap(security_info,
                           "allow_unauthenticated",
                           securityConfig.allowUnauthenticated);

  // Not a required field, default value for allowUnauthenticated is true.
  if (!success && err != E::NOTFOUND) {
    ld_error("Invalid value of \"allow_unauthenticated\" attribute within "
             "\"security_information\", bool expected");
    err = E::INVALID_CONFIG;
    return false;
  }

  success = getBoolFromMap(security_info,
                           "enable_server_ip_authentication",
                           securityConfig.enableServerIpAuthentication);

  // Not a required field, default value is true
  if (!success && err != E::NOTFOUND) {
    ld_error("Invalid value of \"enable_server_ip_authentication\" attribute "
             "within \"security_information\", bool expected");
    err = E::INVALID_CONFIG;
    return false;
  }

  success = getBoolFromMap(security_info,
                           "enable_permission_checking",
                           securityConfig.enablePermissionChecking);

  if (!success) {
    ld_error("Missing or invalid \"enable_permission_checking\" attribute. "
             "\"enable_permission_checking\" must be a bool entry within "
             "\"security_information\"");
    err = E::INVALID_CONFIG;
    return false;
  }

  success = getBoolFromMap(
      security_info, "allow_acl_not_found", securityConfig.allowIfACLNotFound);

  // Not a required field, default value is true
  if (!success && err != E::NOTFOUND) {
    ld_error("Invalid value of \"allow_acl_not_found\" attribute "
             "within \"security_information\", bool expected");
    err = E::INVALID_CONFIG;
    return false;
  }

  std::string perm_checker_type_string;
  bool has_permission_checker_type = getStringFromMap(
      security_info, "permission_checker_type", perm_checker_type_string);

  if (!has_permission_checker_type && err != E::NOTFOUND) {
    ld_error("Invalid \"permission_checker_type\" attribute within "
             "\"security_information\", string expected");
    err = E::INVALID_CONFIG;
    return false;
  }

  if (securityConfig.enablePermissionChecking && !has_permission_checker_type) {
    ld_error("\"permission_checker_type\" must be an attribute within "
             "\"security_information\" since \"enable_permission_checking\" "
             "is true");
    err = E::INVALID_CONFIG;
    return false;
  }

  success = getStringFromMap(security_info, "acl_type", securityConfig.aclType);

  // Not a required field, default value for acl_type is empty.
  if (!success && err != E::NOTFOUND) {
    ld_error("Invalid value of \"acl_type\" attribute within "
             "\"security_information\", string expected");
    err = E::INVALID_CONFIG;
    return false;
  }

  // This is done regardless of "enable_permission_checking" to allow
  // permission checking to be disable and enabled without having to remove
  // the permission information stored within the configuration file
  if (has_permission_checker_type) {
    auto pcType = PermissionCheckerTypeTranslator::toPermissionCheckerType(
        perm_checker_type_string);

    if (pcType == PermissionCheckerType::MAX ||
        pcType == PermissionCheckerType::NONE) {
      ld_error("unrecognized \"permission_checker_type\" %s.",
               perm_checker_type_string.c_str());
      err = E::INVALID_CONFIG;
      return false;
    }
    securityConfig.permissionCheckerType = pcType;

    // Check that the translation is consistent
    ld_check(PermissionCheckerTypeTranslator::toString(pcType) ==
             perm_checker_type_string);
  }

  // admin_list is not a required field.
  if (!parseList(
          security_info, "admin_list", securityConfig, securityConfig.admins)) {
    return false;
  }
  // domain_list is not a required field.
  if (!parseList(security_info,
                 "domain_list",
                 securityConfig,
                 securityConfig.domains)) {
    return false;
  }

  return true;
}

bool parseList(const folly::dynamic& map,
               const std::string& list_name,
               SecurityConfig& securityConfig,
               std::unordered_set<std::string>& values) {
  auto list_iter = map.find(list_name);
  if (list_iter != map.items().end()) {
    // admin_list is only valid for config based and permission store
    // permission checking.
    if (securityConfig.permissionCheckerType != PermissionCheckerType::CONFIG &&
        securityConfig.permissionCheckerType !=
            PermissionCheckerType::PERMISSION_STORE) {
      ld_error("Invalid \"%s\" attribute within "
               "\"security_information\", this attribute should only be in "
               "the config if \"permission_checker_type\" is set to "
               "\"config\" or \"permission_store\"",
               list_name.c_str());
      err = E::INVALID_CONFIG;
      return false;
    }

    if (!list_iter->second.isArray()) {
      ld_error("Invalid \"%s\" attribute within "
               "\"security_information\", \"admin_list\" should map to "
               "an array",
               list_name.c_str());
      err = E::INVALID_CONFIG;
      return false;
    }

    for (const auto& admin : list_iter->second) {
      if (!admin.isString()) {
        ld_error(
            "Items within the \"%s\" list must be strings", list_name.c_str());
        err = E::INVALID_CONFIG;
        return false;
      }
      values.insert(admin.asString());
    }
  }
  return true;
}

bool parseShapingConfig(ShapingConfig& sc,
                        const folly::dynamic& shaping_section) {
  if (!shaping_section.isObject()) {
    ld_error("Section must be a map");
    err = E::INVALID_CONFIG;
    return false;
  }

  auto map_it = shaping_section.find("scopes");
  if (map_it == shaping_section.items().end()) {
    // Not a required Field
    return true;
  }

  auto& scopes = map_it->second;
  if (!scopes.isArray()) {
    ld_error("Scopes must be an array");
    err = E::INVALID_CONFIG;
    return false;
  }

  for (auto& scope : scopes) {
    if (!parseShapingScope(sc, scope)) {
      ld_error("Parsing of scope failed!");
      return false;
    }
  }

  return true;
}

bool parseTrafficShaping(const folly::dynamic& map, TrafficShapingConfig& tsc) {
  // 1. Find optional "traffic_shaping" section
  folly::dynamic::const_item_iterator iter = map.find("traffic_shaping");
  if (iter == map.items().end()) {
    return true;
  }

  // 2. Parse common parts of a shaping config
  if (!parseShapingConfig(tsc, iter->second)) {
    ld_error("Parsing of traffic_shaping failed");
    return false;
  }

  // 3. Parse optional fields that are relevant only to "traffic_shaping"
  tsc.default_read_traffic_class = TrafficClass::READ_BACKLOG; // default value
  bool successful = parseEnum(iter->second,
                              "default_read_traffic_class",
                              tsc.default_read_traffic_class,
                              /*defaults*/ nullptr,
                              read_tc_filter);
  if (!(successful || err == E::NOTFOUND)) {
    ld_error("While processing the \"traffic_shaping\" section.");
    err = E::INVALID_CONFIG;
    return false;
  }

  return true;
}

bool parseReadIOThrottling(const folly::dynamic& map, ShapingConfig& rsc) {
  // 1. Find optional "read_throttling" section
  folly::dynamic::const_item_iterator iter = map.find("read_throttling");
  if (iter == map.items().end()) {
    return true;
  }

  // 2. Parse common parts of a shaping config
  if (!parseShapingConfig(rsc, iter->second)) {
    ld_error("Parsing of read_throttling failed");
    return false;
  }
  return true;
}

bool parseShapingScope(ShapingConfig& sc, const folly::dynamic& scope) {
  if (!scope.isObject()) {
    ld_error("Elements of scopes must be objects");
    err = E::INVALID_CONFIG;
    return false;
  }

  NodeLocationScope scope_enum;
  bool successful = parseEnum(scope, "name", scope_enum);
  if (!successful) {
    return false;
  }

  const std::set<NodeLocationScope>& valid_scopes = sc.getValidScopes();
  const std::string& scope_name = NodeLocation::scopeNames()[scope_enum];
  if (std::find(valid_scopes.begin(), valid_scopes.end(), scope_enum) ==
      valid_scopes.end()) {
    ld_error("Invalid scope_name %s found", scope_name.c_str());
    err = E::INVALID_CONFIG;
    return false;
  }

  auto fgp_it = sc.flowGroupPolicies.find(scope_enum);
  if (fgp_it == sc.flowGroupPolicies.end()) {
    auto result = sc.flowGroupPolicies.insert(
        std::pair<NodeLocationScope, FlowGroupPolicy>(
            scope_enum, FlowGroupPolicy()));
    if (!result.second) {
      ld_error("Couldn't insert scope_name:%s", scope_name.c_str());
      return false;
    }
    fgp_it = result.first;
  }
  auto& fgp(fgp_it->second);

  // Not a required field, default value for shaping_enabled is false.
  bool shaping_enabled = false;
  successful = getBoolFromMap(scope, "shaping_enabled", shaping_enabled);
  if (!(successful || err == E::NOTFOUND)) {
    ld_error("Invalid type for scopes[%s].shaping_enabled. Bool expected.",
             scope_name.c_str());
    err = E::INVALID_CONFIG;
    return false;
  }
  fgp.setEnabled(shaping_enabled);

  // Not a required Field
  auto iter = scope.find("meters");
  if (iter != scope.items().end()) {
    auto& meters = iter->second;
    if (!meters.isArray()) {
      ld_error("Invalid type for "
               "\"traffic_shaping.scopes[%s].meters\". Array expected.",
               scope_name.c_str());
      err = E::INVALID_CONFIG;
      return false;
    }

    for (auto& meter : meters) {
      if (!parseShapingScopeMeter(scope_name, meter, fgp)) {
        ld_error("Parsing of scope meter failed!");
        return false;
      }
    }
  }
  fgp.setConfigured(true);

  return true;
}

bool parseShapingScopeMeter(const std::string& scope_name,
                            const folly::dynamic& meter,
                            FlowGroupPolicy& fgp) {
  if (!meter.isObject()) {
    ld_error("Invalid element within "
             "scopes[%s].meters[]. Object expected.",
             scope_name.c_str());
    err = E::INVALID_CONFIG;
    return false;
  }

  std::string meter_name;
  if (!getStringFromMap(meter, "name", meter_name)) {
    if (err == E::NOTFOUND) {
      ld_error("Missing required attribute scopes[%s].meters[??].name.",
               scope_name.c_str());
    } else {
      ld_error("Invalid type for scopes[%s].meters[??].name. "
               "String representing a priority expected.",
               scope_name.c_str());
    }
    err = E::INVALID_CONFIG;
    return false;
  }

  Priority p = Priority::NUM_PRIORITIES;
  if (meter_name != "PRIORITY_QUEUE" && !parseEnum(meter, "name", p)) {
    ld_error(
        "While processing scopes[%s].meters[??].name.", scope_name.c_str());
    return false;
  }

  using AttributeNames = const std::vector<const char*>;
  auto fetch_attribute =
      [&meter, &scope_name, &meter_name](
          AttributeNames attrib_names, int64_t& output, bool required = true) {
        ld_check(!attrib_names.empty());
        if (attrib_names.empty()) {
          return false;
        }

        int64_t value;

        // All but the the first attribute name are considered deprecated.
        const char* preferred_attrib_name = attrib_names.front();
        const char* found_attrib_name = "not found";
        bool rv;
        for (auto attrib_name : attrib_names) {
          rv = getIntFromMap(meter, attrib_name, value);
          if (rv) {
            found_attrib_name = attrib_name;
            break;
          }
        }

        if (!rv) {
          if (err == E::NOTFOUND) {
            if (required) {
              ld_error("Missing required attribute scopes[%s].meters[%s].%s.",
                       scope_name.c_str(),
                       meter_name.c_str(),
                       preferred_attrib_name);
            }
          } else {
            ld_error("Invalid value for scopes[%s].meters[%s].%s. "
                     "Positive integer less than INT64_MAX expected.",
                     scope_name.c_str(),
                     meter_name.c_str(),
                     found_attrib_name);
          }
          err = E::INVALID_CONFIG;
          return false;
        } else if (value < 0) {
          ld_error("Invalid value for scopes[%s].meters[%s].%s. "
                   "Non-negative integer less than INT64_MAX expected.",
                   scope_name.c_str(),
                   meter_name.c_str(),
                   found_attrib_name);
          err = E::INVALID_CONFIG;
          return false;
        }
        output = value;
        return true;
      };

  int64_t max_burst_bytes;
  int64_t guaranteed_bytes_per_second;
  if (!fetch_attribute({"max_burst_bytes"}, max_burst_bytes) ||
      !fetch_attribute({"guaranteed_bytes_per_second", "bytes_per_second"},
                       guaranteed_bytes_per_second)) {
    return false;
  }

  // Optional - No-limit if not specified.
  int64_t max_bytes_per_second = INT64_MAX;
  fetch_attribute({"max_bytes_per_second"},
                  max_bytes_per_second,
                  /*required*/ false);
  guaranteed_bytes_per_second =
      std::min(guaranteed_bytes_per_second, max_bytes_per_second);

  fgp.set(
      p, max_burst_bytes, guaranteed_bytes_per_second, max_bytes_per_second);
  return true;
}

std::pair<std::string, std::string> parseIpPort(const std::string& hostStr) {
  return parse_ip_port(hostStr);
}

template <typename Duration>
bool parseChronoValue(const folly::dynamic& sectionMap,
                      const char* key,
                      const std::string& entity_name,
                      Duration* out,
                      const folly::dynamic* defaults) {
  ld_check(out != nullptr);

  std::string value;
  if (!getStringFromMap(sectionMap, key, value, defaults)) {
    if (err != E::NOTFOUND) {
      ld_error("Invalid value of \"%s\" attribute for %s. Expected a "
               "string.",
               key,
               entity_name.c_str());
      err = E::INVALID_CONFIG;
    }
    return false;
  }

  int rv = parse_chrono_string(value, out);
  if (rv != 0) {
    ld_error("Duration entry \"%s\" for %s is invalid. Got \"%s\", "
             "expected a number followed by one of: us,ms,s,min,h,hr,d,days,"
             "w,weeks.",
             key,
             entity_name.c_str(),
             value.c_str());
    err = E::INVALID_CONFIG;
    return false;
  }

  return true;
}

template <typename Duration>
bool parseChronoValueOptAttribute(
    const folly::dynamic& sectionMap,
    const char* key,
    const std::string& entity_name,
    logsconfig::Attribute<folly::Optional<Duration>>& out) {
  ld_check(sectionMap.isObject());
  auto iter = sectionMap.find(key);
  if (iter == sectionMap.items().end()) {
    // key is not found, we are inheriting.
    return true;
  }
  const folly::dynamic& val = iter->second;
  if (val.isNull()) {
    // we are unsetting the value
    out = folly::Optional<Duration>();
    return true;
  }
  Duration duration;
  if (parseChronoValue(sectionMap, key, entity_name, &duration, nullptr)) {
    out = folly::Optional<Duration>(duration);
    return true;
  }
  // possibly value or parse error
  return false;
}

template bool
parseChronoValue<std::chrono::seconds>(const folly::dynamic& sectionMap,
                                       const char* key,
                                       const std::string& entity_name,
                                       std::chrono::seconds* out,
                                       const folly::dynamic* defaults);

template bool
parseChronoValue<std::chrono::milliseconds>(const folly::dynamic& sectionMap,
                                            const char* key,
                                            const std::string& entity_name,
                                            std::chrono::milliseconds* out,
                                            const folly::dynamic* defaults);

template bool parseChronoValueOptAttribute<std::chrono::seconds>(
    const folly::dynamic& sectionMap,
    const char* key,
    const std::string& entity_name,
    logsconfig::Attribute<folly::Optional<std::chrono::seconds>>& out);

template bool parseChronoValueOptAttribute<std::chrono::milliseconds>(
    const folly::dynamic& sectionMap,
    const char* key,
    const std::string& entity_name,
    logsconfig::Attribute<folly::Optional<std::chrono::milliseconds>>& out);
}}}} // namespace facebook::logdevice::configuration::parser
