/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/ops/ldquery/tables/LogDirectories.h"

#include <queue>
#include <folly/Conv.h>
#include <folly/json.h>

#include "../Table.h"
#include "../Utils.h"
#include "LogTreeBase.h"

#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/ReplicationProperty.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/debug.h"
#include "logdevice/lib/ClientImpl.h"


namespace facebook {
  namespace logdevice {
    namespace ldquery {
      namespace tables {

TableColumns LogDirectories::getColumns() const {
  return LogTreeBase::getAttributeColumns();
}

std::shared_ptr<TableData> LogDirectories::getData(QueryContext& ctx) {
  auto result = std::make_shared<TableData>();
  auto client = ld_ctx_->getFullClient();

  ld_check(client);

  auto add_row =
      [&](client::Directory* dir, const std::string path) {
    auto dir_attrs = dir->attrs();

    // This should remain the first ColumnValue as expected by the code below.
    result->cols["parent_dir"].push_back(path);
    result->cols["name"].push_back(dir->getFullyQualifiedName());
    result->cols["logid_lo"].push_back(std::string("0"));
    result->cols["logid_hi"].push_back(std::string("0"));

    LogTreeBase::populateAttributeData(*result, dir_attrs);
  };

  // If the query contains a constraint on the directory name, we can efficiently
  // find the directory using getDirectorySync().
  std::string expr;
  if (columnHasEqualityConstraint(10, ctx, expr)) {
      auto parent_dir = client->getDirectorySync(expr);
      if (parent_dir) {
          const client::DirectoryMap& children = parent_dir->children();
          for (auto& dir_pair : children) {
              add_row(dir_pair.second.get(), parent_dir->getFullyQualifiedName());
          }
      }
  } else if (columnHasEqualityConstraint(0, ctx, expr)) {
    auto dir = client->getDirectorySync(expr);
    if (dir) {
      add_row(dir.get(), dir->parent()->getFullyQualifiedName());
    }
  } else if (columnHasLikeConstraint(0, ctx, expr)) {
      std::unique_ptr<client::Directory> root_dir
          = client->getDirectorySync(
                client->getLogNamespaceDelimiter());

      if (match_likexpr(root_dir->getFullyQualifiedName(), expr)) {
          add_row(root_dir.get(), "");
      }
      std::queue<client::Directory*> dir_q;
      dir_q.push(root_dir.get());

      while (!dir_q.empty()) {
          client::Directory* parent_dir = dir_q.front();
          dir_q.pop();
          for (const auto& dir_pair: parent_dir->children()) {
              if (match_likexpr(dir_pair.second->getFullyQualifiedName(), expr)) {
                add_row(dir_pair.second.get(), parent_dir->getFullyQualifiedName());
              }
              dir_q.push(dir_pair.second.get());
          }
      }
  } else {
      std::unique_ptr<client::Directory> root_dir
          = client->getDirectorySync(
              client->getLogNamespaceDelimiter());

      add_row(root_dir.get(), "");
      std::queue<client::Directory*> dir_q;
      dir_q.push(root_dir.get());

      while (!dir_q.empty()) {
          client::Directory* parent_dir = dir_q.front();
          dir_q.pop();
          for (const auto& dir_pair: parent_dir->children()) {
              add_row(dir_pair.second.get(), parent_dir->getFullyQualifiedName());
              dir_q.push(dir_pair.second.get());
          }
      }
  }

  return result;
}

}}}} // namespace facebook::logdevice::ldquery::tables
