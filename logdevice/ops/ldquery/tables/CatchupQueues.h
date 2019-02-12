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

class CatchupQueues : public AdminCommandTable {
 public:
  explicit CatchupQueues(std::shared_ptr<Context> ctx)
      : AdminCommandTable(ctx) {}
  static std::string getName() {
    return "catchup_queues";
  }
  std::string getDescription() override {
    return "CatchupQueue is a state machine that manages all the read streams "
           "of one client on one socket.  It contains a queue of read streams "
           "for which there are new records to be sent to the client (we say "
           "these read streams are not caught up).  Read streams from that "
           "queue are processed (or \"woken-up\") in a round-robin fashion. "
           "The state machine is implemented in "
           "logdevice/common/CatchupQueue.h.";
  }
  TableColumns getFetchableColumns() const override {
    return {
        {"client", DataType::TEXT, "Id of the client."},
        {"queued_total",
         DataType::BIGINT,
         "Number of read streams queued in that CatchupQueue (ie read streams "
         "that are not caught up)."},
        {"queued_immediate",
         DataType::BIGINT,
         "Number of read streams that are not queued with a delay, see "
         "\"queue_delayed\"."},
        {"queued_delayed",
         DataType::BIGINT,
         "(Number of read streams that are queued with a delay.  When these "
         "read streams are queued, CatchupQueue waits for a configured "
         "amount of time before dequeuing the stream for processing. This "
         "happens if the log is configured with the \"delivery_latency\" "
         " option, which enables better batching of reads when tailing.  "
         "See \"deliveryLatency\" logdevice/include/LogAttributes.h."},
        {"record_bytes_queued",
         DataType::BIGINT,
         "(CatchupQueue also does accounting of how many bytes are enqueued in "
         "the socket's output evbuffer. CatchupQueue wakes up several read "
         "streams until the buffer reaches the limit set by the option "
         "--output-max-records-kb (see logdevice/common/Settings.h)."},
        {"storage_task_in_flight",
         DataType::INTEGER,
         "Each read stream is processed one by one.  When a read stream is "
         "processed, it will first try to read some records from the worker "
         "thread if there are some records that can be read from RocksDB's "
         "block cache. When all records that could be read from the worker "
         "thread were read, and if there are more records that can be read, "
         "the read stream will issue a storage task to read such records in "
         "a slow storage thread.  This flag indicates whether or not there "
         "is such a storage task currently in flight."},
        {"ping_timer_active",
         DataType::INTEGER,
         "Ping timer is a timer that is used to ensure we eventually try to "
         "schedule more reads under certain conditions.  This column indicates "
         "whether the timer is currently active."}};
  }
  std::string getCommandToSend(QueryContext& /*ctx*/) const override {
    return std::string("info catchup_queues --json\n");
  }
};

}}}} // namespace facebook::logdevice::ldquery::tables
