/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/ops/ldquery/tables/IsLogEmpty.h"

#include <folly/Conv.h>
#include <folly/json.h>

#include "../Table.h"
#include "../Utils.h"
#include "logdevice/common/Semaphore.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/ReplicationProperty.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/debug.h"
#include "logdevice/lib/ClientImpl.h"
#include "logdevice/lib/ops/IsLogEmptyBatchRunner.h"
#include "logdevice/ops/ldquery/Errors.h"

using facebook::logdevice::Configuration;

namespace facebook {
  namespace logdevice {
    namespace ldquery {
      namespace tables {

TableColumns IsLogEmpty::getColumns() const {
  return {
      {"log_id", DataType::LOGID, "ID of the log."},
      {"status",
       DataType::TEXT,
       "Response status of running isLogEmpty() on this log. Check the "
       "documentation of isLogEmpty() for the list of error codes."},
      {"empty",
       DataType::BOOL,
       "Wether the log is empty, null if \"status\" != OK."},
  };
}

std::shared_ptr<TableData> IsLogEmpty::getData(QueryContext& ctx) {
  auto result = std::make_shared<TableData>();
  auto full_client = ld_ctx_->getFullClient();

  ld_check(full_client);
  ClientImpl* client_impl = static_cast<ClientImpl*>(full_client.get());
  auto config = client_impl->getConfig()->get();

  Semaphore sem;
  auto callback = [&](IsLogEmptyBatchRunner::Results results) {
    for (const auto& r : results) {
      result->cols["log_id"].push_back(s(r.first.val_));
      result->cols["status"].push_back(s(error_name(r.second.status)));
      if (r.second.status != E::OK) {
        result->cols["empty"].push_back(folly::none);
      } else {
        result->cols["empty"].push_back(s(r.second.empty));
      }
    }
    sem.post();
  };

  std::vector<logid_t> logs;

  // If the query contains a constraint on the log id, make sure we only fetch
  // data given the constraint.
  std::string expr;
  if (columnHasEqualityConstraint(0, ctx, expr)) {
    const logid_t logid = logid_t(folly::to<logid_t::raw_type>(expr));
    logs.push_back(logid);
  } else {
    auto logs_config = config->localLogsConfig();
    ld_check(logs_config);
    for (auto it = logs_config->logsBegin(); it != logs_config->logsEnd();
         ++it) {
      logs.push_back(logid_t(it->first));
    }
  }

  IsLogEmptyBatchRunner runner(full_client, logs, callback);
  runner.setMaxInFlight(100);
  runner.start();
  sem.wait();

  return result;
}

}}}} // namespace facebook::logdevice::ldquery::tables
