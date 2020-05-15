/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/ops/ldquery/tables/LogGroups.h"

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

TableColumns LogGroups::getColumns() const {
  return LogTreeBase::getAttributeColumns();
}

std::shared_ptr<TableData> LogGroups::getData(QueryContext& ctx) {
  auto result = std::make_shared<TableData>();
  auto full_client = ld_ctx_->getFullClient();

  ld_check(full_client);
  ClientImpl* client_impl = static_cast<ClientImpl*>(full_client.get());
  auto config = client_impl->getConfig()->get();

  auto add_row = [&](const std::string name, logid_range_t range) {
    auto log = config->getLogGroupByIDShared(range.first);
    ld_check(log);
    auto log_attrs = log->attrs();

    // This should remain the first ColumnValue as expected by the code below.
    result->cols["name"].push_back(name);

    result->cols["logid_lo"].push_back(s(range.first.val_));
    result->cols["logid_hi"].push_back(s(range.second.val_));
    LogTreeBase::populateAttributeData(*result, log_attrs);

  };

  // If the query contains a constraint on the group name, we can efficiently
  // find the log group using getLogRangeByName().
  std::string expr;
  if (columnHasEqualityConstraint(0, ctx, expr)) {
    logid_range_t range = config->logsConfig()->getLogRangeByName(expr);
    if (range.first != LOGID_INVALID) {
      add_row(expr, range);
    }
  } else {
    auto ranges = config->logsConfig()->getLogRangesByNamespace("");
    for (auto& r : ranges) {
      add_row(s(r.first), r.second);
    }
  }

  return result;
}

}}}} // namespace facebook::logdevice::ldquery::tables
