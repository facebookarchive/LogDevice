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

class StorageTasks : public AdminCommandTable {
 public:
  using AdminCommandTable::AdminCommandTable;
  static std::string getName() {
    return "storage_tasks";
  }
  virtual std::string getDescription() override {
    return "List of storage tasks currently pending on the storage thread "
           "queues. Note that this does not include the task that is currently "
           "executing, nor the tasks that are queueing on the per-worker "
           "storage task queues.  Querying this table prevents the storage "
           "tasks from being popped off the queue while it's executing, so be "
           "careful with it.";
  }
  virtual TableColumns getFetchableColumns() const override {
    return {
        {"shard",
         DataType::BIGINT,
         "Index of the local log store shard to query storage tasks on."},
        {"priority",
         DataType::TEXT,
         "Priority of the storage task. The tasks with a higher priority get "
         "executed before tasks with a lower priority."},
        {"is_write_queue",
         DataType::BOOL,
         "True if this is a task from the write_queue, otherwise this is a "
         "task from the ordinary storage task queue for the thread."},
        {"sequence_no",
         DataType::BIGINT,
         "Sequence No. of the task. For task that are on the same queue with "
         "the same priority, tasks with lower sequence No. will execute "
         "first."},
        {"thread_type",
         DataType::TEXT,
         "The type of the thread the task is queueing for."},
        {"task_type", DataType::TEXT, "Type of the task, if specified."},
        {"enqueue_time",
         DataType::TIME,
         "Time when the storage task has been inserted into the queue."},
        {"durability",
         DataType::TEXT,
         "The durability requirement for this storage task (applies to "
         "writes)."},
        {"log_id",
         DataType::LOGID,
         "Log ID that the storage task will perform writes/reads on."},
        {"lsn",
         DataType::LSN,
         "LSN that the storage task will act on. The specific meaning of this "
         "field varies depending on the task type."},
        {"client_id",
         DataType::TEXT,
         "ClientID of the client that initiated the storage task."},
        {"client_address",
         DataType::TEXT,
         "Address of the client that initiated the storage task."},
        {"extra_info",
         DataType::TEXT,
         "Other information specific to particular task type."}};
  }
  virtual std::string getCommandToSend(QueryContext& ctx) const override {
    std::string expr;
    if (columnHasEqualityConstraint(1, ctx, expr)) {
      return std::string("info storage_tasks ") + expr.c_str() + " --json\n";
    } else {
      return std::string("info storage_tasks --json\n");
    }
  }
};

}}}} // namespace facebook::logdevice::ldquery::tables
