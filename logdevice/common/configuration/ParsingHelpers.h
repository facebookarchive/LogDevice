/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/SecurityConfig.h"
#include "logdevice/common/configuration/ShapingConfig.h"
#include "logdevice/common/debug.h"
#include "logdevice/include/Err.h"
#include "logdevice/include/LogAttributes.h"

namespace folly {
struct dynamic;
}

namespace facebook { namespace logdevice { namespace configuration {
namespace parser {

// A helper method to run a provided function f : folly::dynamic -> bool on a
// value from the map, given a key.
template <typename ExtractFuncType>
bool getFromMap(const folly::dynamic& map,
                const char* key,
                const folly::dynamic* defaults,
                ExtractFuncType func) {
  ld_check(map.isObject());
  ld_check(defaults == nullptr || defaults->isObject());
  auto iter = map.find(key);
  if (iter == map.items().end()) {
    if (defaults == nullptr) {
      err = E::NOTFOUND;
      return false;
    }
    iter = defaults->find(key);
    if (iter == defaults->items().end()) {
      err = E::NOTFOUND;
      return false;
    }
  }
  const folly::dynamic& val = iter->second;
  if (val.isNull()) {
    err = E::NOTFOUND;
    return false;
  }
  return func(val) || (err = E::INVALID_CONFIG, false);
}

/**
 * Helper method to extract an int value from a map, given a key.
 */
template <typename OutputIntType>
bool getIntFromMap(const folly::dynamic& map,
                   const char* key,
                   OutputIntType& output,
                   const folly::dynamic* defaults = nullptr) {
  static_assert(sizeof(OutputIntType) <= sizeof(int64_t),
                "OutputIntType can be no bigger than int64_t since that is "
                "what folly::dynamic::asInt() returns");

  auto f = [&](const folly::dynamic& val) {
    if (!val.isInt()) {
      return false;
    }
    output = OutputIntType(val.asInt());
    return true;
  };

  return getFromMap(map, key, defaults, f);
}

template <typename OutputIntType>
bool getIntAttributeFromMap(const folly::dynamic& map,
                            const char* key,
                            logsconfig::Attribute<OutputIntType>& output,
                            const folly::dynamic* defaults = nullptr) {
  static_assert(sizeof(OutputIntType) <= sizeof(int64_t),
                "OutputIntType can be no bigger than int64_t since that is "
                "what folly::dynamic::asInt() returns");

  auto f = [&](const folly::dynamic& val) {
    if (!val.isInt()) {
      return false;
    }
    output = val.asInt();
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
                      const folly::dynamic* defaults = nullptr);

/**
 * Helper method to extract a string value from a map, given a key.
 */
bool getStringFromMap(const folly::dynamic& map,
                      const char* key,
                      std::string& output,
                      const folly::dynamic* defaults = nullptr);

/**
 * Helper method to extract a bool value from a map, given a key.
 *
 * NOTE: return value indicates success, output will contain the extracted bool.
 */
bool getBoolFromMap(const folly::dynamic& map,
                    const char* key,
                    bool& output,
                    const folly::dynamic* defaults = nullptr);

/**
 * Helper method to extract a object from a map, given a key.
 */
bool getObjectFromMap(const folly::dynamic& map,
                      const char* key,
                      folly::dynamic& outputMap,
                      const folly::dynamic* defaults = nullptr);

/**
 * Helper method to extract a object from a map, given a key.
 */
bool getArrayFromMap(const folly::dynamic& map,
                     const char* key,
                     folly::dynamic& outputMap,
                     const folly::dynamic* defaults = nullptr);
/**
 * Helper function that reads a file into a string
 */
std::string readFileIntoString(const char* path);

/**
 * Helper function that parses a string into JSON
 */
folly::dynamic parseJson(const std::string& jsonPiece);

/**
 * Parses the "cluster" entry
 */
bool parseClusterName(const folly::dynamic& clusterMap, std::string& output);

/**
 * Parses the "version" entry
 */
bool parseVersion(const folly::dynamic& clusterMap, config_version_t& output);

template <typename Enum>
using EnumFilter = bool(const Enum&);

/**
 * Parses the "server_settings" or "client_settings" section that can be found
 * at the root of the server config or a "settings" section inside a node
 * section.
 */
bool parseSettings(const folly::dynamic& map,
                   const std::string& element_name,
                   ServerConfig::SettingsConfig& output);

/**
 * Parse a setting.
 */
bool parseOneSetting(std::string key,
                     const folly::dynamic& value,
                     const std::string& element_name,
                     ServerConfig::SettingsConfig& output);

/**
 * Parses the "cluster_creation_time" entry
 */
bool parseClusterCreationTime(
    const folly::dynamic& clusterMap,
    ServerConfig::OptionalTimestamp& clusterCreationTime);

/**
 * Parses any key that contains a string representation
 * of a valid element of Enum.
 *
 * Note: The valid elements exclude entries such as MAX,
 *       NUM_ELEMENTS, or INVALID which may be used in
 *       code but do not make sense in configurations.
 *       The list of valid elements is controlled by the
 *       EnumMap used for string->Enum conversion. See
 *       template instantiations in ParsingHelpers.cpp
 *       for details.
 */
template <typename Enum>
bool parseEnum(const folly::dynamic& map,
               const char* key,
               Enum& value,
               const folly::dynamic* defaults = nullptr,
               EnumFilter<Enum>* = nullptr);

/**
 * Parses a textual representation of duration into the templated type.
 * Only defined for std::chrono::seconds and std::chrono::milliseconds
 * (see .cpp)
 */
template <typename Duration>
bool parseChronoValue(const folly::dynamic& sectionMap,
                      const char* key,
                      const std::string& entity_name,
                      Duration* out,
                      const folly::dynamic* defaults = nullptr);

/**
 * Same as parseChronoValue but fills an
 * Attribute<folly::Optional<Duration>> to indicate whether a value was
 * explicitly set,
 * unset (by setting to null), or left to inheritance.
 */
template <typename Duration>
bool parseChronoValueOptAttribute(
    const folly::dynamic& sectionMap,
    const char* key,
    const std::string& entity_name,
    logsconfig::Attribute<folly::Optional<Duration>>& out);

// alias for parse_ip_port() in util.h
std::pair<std::string, std::string> parseIpPort(const std::string&);

/**
 * @return true iff _name_ is a valid cluster name, that is a non-empty
 *         string of printable characters (spaces ok) no longer than
 *         MAX_CLUSTER_NAME.
 */
bool validClusterName(std::string);

/**
 * Parses the "permissions" entry within the config
 *
 * @param permissions    The dynamic object of the permissions field
 * @param permissionMap  The output of the parsed "permissions" will be placed
 *                       in here
 *
 * @return               true if the parsing was successful, false otherwise
 */

bool parseLogPermissions(
    const folly::dynamic& permissions,
    logsconfig::Attribute<logsconfig::LogAttributes::PermissionsMap>& output);

/**
 * Parses the "acls" entry within the config
 *
 * @param acls           The dynamic object of the acls field
 * @param aclsMap        The output of the parsed "acls" will be place in here
 *
 * @return               true if the parsing was successful, false otherwise
 */

bool parseLogACLs(
    const folly::dynamic& acls,
    const std::string& field,
    logsconfig::Attribute<logsconfig::LogAttributes::ACLList>& output);

/**
 * Parses "shadow" for traffic shadowing
 */
bool parseShadow(
    const folly::dynamic& shadow,
    logsconfig::Attribute<logsconfig::LogAttributes::Shadow>& output);

/**
 * Parses the optional "principals" array within the config
 */
bool parsePrincipals(const folly::dynamic& clusterMap, PrincipalsConfig&);

/**
 * Parses the "security_information" entry within the config
 */
bool parseSecurityInfo(const folly::dynamic& clusterMap, SecurityConfig&);

/**
 * Common code to parse the optional Shaping map within the config,
 */
bool parseShapingConfig(ShapingConfig& sc,
                        const folly::dynamic& shaping_section);

/**
 * Extract optional "traffic_shaping" section of the config
 */
bool parseTrafficShaping(const folly::dynamic& clusterMap,
                         TrafficShapingConfig&);

/**
 * Extract optional "read_throttling" section of the config
 */
bool parseReadIOThrottling(const folly::dynamic& clusterMap,
                           ShapingConfig& rsc);

/**
 * Generic method to parse an element of the "shaping::scopes" array
 */
bool parseShapingScope(ShapingConfig& sc, const folly::dynamic& scope);

/**
 * Parses an element of the "scopes::meters" array.
 */
bool parseShapingScopeMeter(const std::string& scope_name,
                            const folly::dynamic& meter,
                            FlowGroupPolicy& fgp);

/**
 * Parses an optional element `list_name` in the map, to a set of strings
 * contained inside it (`values`).
 * For example: "security_information" section in the config contains lists like
 * "admin_list" : ["logdevice_admins", "logdevice_testing_admins"]
 * "domain_list" : ["domains/hipster/AuthorizationScribeSigned"]
 */
bool parseList(const folly::dynamic& map,
               const std::string& list_name,
               SecurityConfig& securityConfig,
               std::unordered_set<std::string>& values);
}}}} // namespace facebook::logdevice::configuration::parser
