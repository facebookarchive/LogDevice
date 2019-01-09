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

std::string ZookeeperConfig::getQuorumString() const {
  std::string result;
  for (const Sockaddr& addr : quorum_) {
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

  if (!parsed.isObject()) {
    ld_error("\"zookeeper\" cluster is not a JSON object");
    err = E::INVALID_CONFIG;
    return nullptr;
  }

  auto iter = parsed.find("quorum");
  if (iter == parsed.items().end()) {
    ld_error("\"quorum\" is missing in \"zookeeper\" section");
    err = E::INVALID_CONFIG;
    return nullptr;
  }

  if (!parseZookeeperQuorum(iter->second, quorum)) {
    return nullptr;
  } else if (quorum.empty()) {
    ld_error("zookeeper quorum section is empty");
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

  return std::make_unique<ZookeeperConfig>(std::move(quorum), session_timeout);
}

}}} // namespace facebook::logdevice::configuration
