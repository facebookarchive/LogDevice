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
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/ReplicationProperty.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/debug.h"
#include "logdevice/lib/ClientImpl.h"

using facebook::logdevice::Configuration;

namespace facebook {
  namespace logdevice {
    namespace ldquery {
      namespace tables {

TableColumns LogGroups::getColumns() const {
  return {
      {"name", DataType::TEXT, "Name of the log group."},
      {"logid_lo",
       DataType::LOGID,
       "Defines the lower bound (inclusive) of the range of log ids in this "
       "log group."},
      {"logid_hi",
       DataType::LOGID,
       "Defines the upper bound (inclusive) of the range of log ids in this "
       "log group."},
      {"replication_property",
       DataType::TEXT,
       "Replication property configured for this log group."},
      {"synced_copies",
       DataType::INTEGER,
       "Number of copies that must be acknowledged by storage nodes are synced "
       "to disk before the record is acknowledged to the client as fully "
       "appended."},
      {"max_writes_in_flight",
       DataType::INTEGER,
       "The largest number of records not released for delivery that the "
       "sequencer allows to be outstanding."},
      {"backlog_duration_sec",
       DataType::INTEGER,
       "Time-based retention of records of logs in that log group.  If null or "
       "zero, this log group does not use time-based retention."},
      {"storage_set_size",
       DataType::INTEGER,
       "Size of the storage set for logs in that log group.  The storage set "
       "is the set of shards that may hold data for a log."},
      {"delivery_latency",
       DataType::INTEGER,
       "For logs in that log group, maximum amount of time that we can delay "
       "delivery of newly written records.  This option increases delivery "
       "latency but improves server and client performance."},
      {"scd_enabled",
       DataType::INTEGER,
       "Indicates whether the Single Copy Delivery optimization is enabled for "
       "this log group.  This efficiency optimization allows only one copy of "
       "each record to be served to readers."},
      {"custom_fields",
       DataType::TEXT,
       "Custom text field provided by the user."},
  };
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

    ColumnValue custom;
    if (log_attrs.extras().hasValue() && (*log_attrs.extras()).size() > 0) {
      folly::dynamic extra_attrs = folly::dynamic::object;
      for (const auto& it : log_attrs.extras().value()) {
        extra_attrs[it.first] = it.second;
      }
      custom = folly::toJson(extra_attrs);
    }

    ReplicationProperty rprop =
        ReplicationProperty::fromLogAttributes(log_attrs);
    // This should remain the first ColumnValue as expected by the code below.
    result->cols["name"].push_back(name);

    result->cols["logid_lo"].push_back(s(range.first.val_));
    result->cols["logid_hi"].push_back(s(range.second.val_));
    result->cols["replication_property"].push_back(rprop.toString());
    result->cols["synced_copies"].push_back(s(*log_attrs.syncedCopies()));
    result->cols["max_writes_in_flight"].push_back(
        s(*log_attrs.maxWritesInFlight()));
    result->cols["backlog_duration_sec"].push_back(
        s(*log_attrs.backlogDuration()));
    result->cols["storage_set_size"].push_back(s(*log_attrs.nodeSetSize()));
    result->cols["delivery_latency"].push_back(s(*log_attrs.deliveryLatency()));
    result->cols["scd_enabled"].push_back(s(*log_attrs.scdEnabled()));
    result->cols["custom_fields"].push_back(custom);
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
