/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <map>
#include <vector>

#include "../Context.h"
#include "AdminCommandTable.h"

namespace facebook {
  namespace logdevice {
    namespace ldquery {
      namespace tables {

class Info : public AdminCommandTable {
 public:
  explicit Info(std::shared_ptr<Context> ctx) : AdminCommandTable(ctx) {}
  static std::string getName() {
    return "info";
  }
  std::string getDescription() override {
    return "A general information table about the nodes in the cluster, like "
           "server start time, package version etc.";
  }
  TableColumns getFetchableColumns() const override {
    return {
        {"pid", DataType::BIGINT, "Process ID of logdeviced."},
        {"version",
         DataType::TEXT,
         "A string that holds the version and revision, built by who and "
         "when."},
        {"package", DataType::TEXT, "Package name and hash."},
        {"build_user",
         DataType::TEXT,
         "Unixname of user who built this package"},
        {"build_time", DataType::TEXT, "Date and Time of the build."},
        {"start_time",
         DataType::TEXT,
         "Date and Time of when the daemon was started on that node."},
        {"server_id", DataType::TEXT, "Server Generated ID."},
        {"shards_missing_data",
         DataType::TEXT,
         "A list of the shards that are empty and waiting to be rebuilt."},
        {"min_proto", DataType::BIGINT, "Minimum protocol version supported."},
        {"max_proto", DataType::BIGINT, "Maximum protocol version supported."},
        {"is_auth_enabled",
         DataType::BOOL,
         "Whether authentication is enabled for this node."},
        {"auth_type",
         DataType::TEXT,
         "Authentication Type.  Can be null or one of \"self_identification\" "
         "(insecure authentication method where the server trusts the client), "
         "\"ssl\" (the client can provide a TLS certificate by using the "
         "\"ssl-load-client-cert\" and \"ssl-cert-path\" settings).  "
         "See \"AuthenticationType\" in "
         "\"logdevice/common/SecurityInformation.h\" for more information."},
        {"is_unauthenticated_allowed",
         DataType::BOOL,
         "Is anonymous access allowed to this server (set only if "
         "authentication is enabled, ie \"auth_type\" is not null)."},
        {"is_permission_checking_enabled",
         DataType::BOOL,
         "Do we check permissions?"},
        {"permission_checker_type",
         DataType::TEXT,
         "Permission checker type.  Can be null or one of \"config\" (this "
         "method stores the permission data in the config file), "
         "\"permission_store\" (this method stores the ACLs in the config file "
         "while the permissions and users are stored in an external store).  "
         "See \"PermissionCheckerType\" in "
         "\"logdevice/common/SecurityInformation.h\" for more information."},
        {"rocksdb_version", DataType::TEXT, "Version of RocksDB."}};
  }
  std::string getCommandToSend(QueryContext& /*ctx*/) const override {
    return std::string("info --json\n");
  }
};

}}}} // namespace facebook::logdevice::ldquery::tables
