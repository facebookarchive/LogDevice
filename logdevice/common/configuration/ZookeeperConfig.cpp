/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/configuration/ZookeeperConfig.h"

#include "logdevice/common/commandline_util_chrono.h"
#include "logdevice/common/configuration/ParsingHelpers.h"

namespace facebook { namespace logdevice { namespace configuration {

/* static */ constexpr const size_t ZookeeperConfig::MAX_CLUSTER_NAME;
/* static */ constexpr const char* ZookeeperConfig::URI_SCHEME_IP;

std::string
ZookeeperConfig::makeQuorumString(const std::vector<Sockaddr>& quorum) {
  std::string result;
  for (const Sockaddr& addr : quorum) {
    if (!result.empty()) {
      result += ',';
    }
    // Do not include brackets "[a:b:c..]" around IPv6 addresses in Zookeeper
    // quorum string. Zookeeper C client currently only supports
    // a:b:c:..:z:port format of IPv6+port specifiers
    result += addr.toStringNoBrackets();
  }
  return result;
}

static bool parseZookeeperQuorum(const folly::dynamic& quorum_list,
                                 std::vector<Sockaddr>& quorum_out) {
  if (!quorum_list.isArray()) {
    ld_error("\"quorum\" entry in \"zookeeper\" section must be an array");
    err = E::INVALID_CONFIG;
    return false;
  }

  ld_check(quorum_out.empty());
  int itemno = 0;
  for (const folly::dynamic& item : quorum_list) {
    if (!item.isString()) {
      ld_error("Item %d in the zookeeper quorum section "
               "is not a string",
               itemno + 1);
      err = E::INVALID_CONFIG;
      return false;
    }
    std::string ip_port_str = item.asString();

    std::pair<std::string, std::string> ip_port =
        parser::parseIpPort(ip_port_str);
    if (ip_port.first.empty() || ip_port.second.empty()) {
      ld_error("malformed ip:port entry \"%s\" in item %d of the zookeeper "
               "quorum section",
               ip_port_str.c_str(),
               itemno + 1);
      err = E::INVALID_CONFIG;
      return false;
    }

    try {
      quorum_out.emplace_back( // creating a Sockaddr
          ip_port.first,
          ip_port.second);
    } catch (const ConstructorFailed&) {
      ld_error("invalid ip:port entry \"%s\" in item %d of the zookeeper "
               "quorum section",
               ip_port_str.c_str(),
               itemno + 1);
      err = E::INVALID_CONFIG;
      return false;
    }
    itemno++;
  }
  return true;
}

std::unique_ptr<ZookeeperConfig>
ZookeeperConfig::fromJson(const folly::dynamic& parsed) {
  std::vector<Sockaddr> quorum;
  std::chrono::milliseconds session_timeout{0};
  std::string uri_scheme, quorum_string;

  if (!parsed.isObject()) {
    ld_error("\"zookeeper\" cluster is not a JSON object");
    err = E::INVALID_CONFIG;
    return nullptr;
  }

  // quorum is now deprecated but kept for backward compatibility.
  // It is used only if zookeeper_uri is not present.
  auto iter = parsed.find("quorum");
  if (iter != parsed.items().end()) {
    if (!parseZookeeperQuorum(iter->second, quorum)) {
      return nullptr;
    }
  }

  iter = parsed.find("zookeeper_uri");
  if (iter == parsed.items().end()) {
    // construct quroum string from the quorum property
    quorum_string = makeQuorumString(quorum);
    // Since quorum was specified as a list iof ip addresses, pretend that
    // a URI was passed with "ip" scheme.
    uri_scheme = URI_SCHEME_IP;
  } else {
    if (!iter->second.isString()) {
      ld_error("zookeeper_uri property in the zookeeper section "
               "is not a string");
      err = E::INVALID_CONFIG;
      return nullptr;
    }
    std::string uri = iter->second.asString();

    auto delim = uri.find("://");
    if (delim == std::string::npos) {
      ld_error("Invalid zookeeper_uri property. The format must be "
               "<scheme>://<value>");
      err = E::INVALID_CONFIG;
      return nullptr;
    }

    uri_scheme = std::string(uri, 0, delim);
    quorum_string = std::string(uri, delim + 3);
  }

  if (quorum_string.empty() || uri_scheme.empty()) {
    ld_error("Missing or invalid zookeeper_uri property in zookeeper section.");
    err = E::INVALID_CONFIG;
    return nullptr;
  }

  std::string session_timeout_str;
  if (!parser::getStringFromMap(parsed, "timeout", session_timeout_str)) {
    ld_error("\"timeout\" entry in \"zookeeper\" section is missing or is "
             "not a string");
    err = E::INVALID_CONFIG;
    return nullptr;
  }

  static const std::chrono::milliseconds MAX_SESSION_TIMEOUT((1ull << 32) - 1);

  int rv = parse_chrono_string(session_timeout_str, &session_timeout);

  if (rv != 0 || session_timeout <= session_timeout.zero() ||
      session_timeout > MAX_SESSION_TIMEOUT) {
    ld_error("\"timeout\" entry in \"zookeeper\" section is invalid. Expected "
             "a number followed by one of: us,ms,s,min,h,hr,d,days, "
             "representing from 1ms to (2^32)-1 ms. Got \"%s\".",
             session_timeout_str.c_str());
    err = E::INVALID_CONFIG;
    return nullptr;
  }

  // Save properties that are not recognized by the parser, into a map.
  // They can be used by the Zookeeper client factory to initialize clients.
  folly::dynamic props = folly::dynamic::object;
  for (auto& it : parsed.keys()) {
    std::string key = it.asString();
    if (key == "quorum" || key == "timeout" || key == "zookeeper_uri") {
      continue;
    }

    props[it] = parsed[it];
  }

  return std::make_unique<ZookeeperConfig>(std::move(quorum),
                                           session_timeout,
                                           std::move(uri_scheme),
                                           std::move(quorum_string),
                                           std::move(props));
}

folly::dynamic ZookeeperConfig::toFollyDynamic() const {
  folly::dynamic zookeeper = getProperties();
  auto& quorum = getQuorum();
  if (!quorum.empty()) {
    folly::dynamic quorum_out = folly::dynamic::array;
    for (const Sockaddr& addr : quorum) {
      quorum_out.push_back(addr.toString());
    }
    zookeeper["quorum"] = std::move(quorum_out);
  }
  if (quorum.empty() || getUriScheme() != ZookeeperConfig::URI_SCHEME_IP) {
    zookeeper["zookeeper_uri"] = getZookeeperUri();
  }
  std::string timeout_str = std::to_string(getSessionTimeout().count()) + "ms";
  zookeeper["timeout"] = timeout_str;
  return zookeeper;
}

bool ZookeeperConfig::operator==(const ZookeeperConfig& other) const {
  return getQuorum() == other.getQuorum() &&
      getSessionTimeout() == other.getSessionTimeout() &&
      getQuorumString() == other.getQuorumString() &&
      getUriScheme() == other.getUriScheme() &&
      getProperties() == other.getProperties();
}

}}} // namespace facebook::logdevice::configuration
