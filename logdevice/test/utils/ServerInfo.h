/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <algorithm>
#include <iterator>
#include <string>

#include <folly/dynamic.h>
#include <folly/json.h>

namespace facebook { namespace logdevice { namespace test {

struct ServerInfo {
 private:
  static size_t findIndex(const folly::dynamic& headers,
                          const std::string& key) {
    return std::distance(
        headers.begin(), std::find(headers.begin(), headers.end(), key));
  }

 public:
  std::string pid;
  std::string package;
  std::string server_id;
  folly::fbvector<std::string> shards_missing_data;

  static ServerInfo fromJson(const std::string& data) {
    ServerInfo s;
    folly::dynamic info_map =
        folly::parseJson(data.substr(0, data.rfind("END")));
    auto headers = info_map["headers"];

    size_t pid_idx = findIndex(headers, "PID");
    size_t server_id_idx = findIndex(headers, "Server ID");
    size_t pkg_idx = findIndex(headers, "Package");
    size_t shards_missing_data_idx = findIndex(headers, "Shards Missing Data");

    s.pid = info_map["rows"][0][pid_idx].getString();
    s.server_id = info_map["rows"][0][server_id_idx].getString();
    s.package = info_map["rows"][0][pkg_idx].getString();
    folly::fbvector<std::string> shards_missing_data;
    auto shards_missing_data_str =
        info_map["rows"][0][shards_missing_data_idx].getString();
    if (shards_missing_data_str != "none") {
      folly::split(",", shards_missing_data_str, shards_missing_data);
    }
    s.shards_missing_data = shards_missing_data;
    return s;
  }
};
}}} // namespace facebook::logdevice::test
