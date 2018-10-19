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

class Readers : public AdminCommandTable {
 public:
  explicit Readers(std::shared_ptr<Context> ctx) : AdminCommandTable(ctx) {}
  static std::string getName() {
    return "readers";
  }
  std::string getDescription() override {
    return "Tracks all ServerReadStreams. A ServerReadStream is a stream of "
           "records sent by a storage node to a client running a "
           "ClientReadStream (see \"client_read_streams\" table).";
  }
  TableColumns getFetchableColumns() const override {
    return {
        {"shard", DataType::INTEGER, "Shard on which the stream is reading."},
        {"client",
         DataType::TEXT,
         "Name of the client (similar to so the \"client\" column of the "
         "\"sockets table\")."},
        {"log_id", DataType::LOGID, "Id of the log being read."},
        {"start_lsn",
         DataType::LSN,
         "LSN from which the ClientReadStream started reading."},
        {"until_lsn",
         DataType::LSN,
         "LSN up to which ClientReadStream is interested in reading."},
        {"read_pointer",
         DataType::LSN,
         "LSN to the next record to be read by the storage node."},
        {"last_delivered",
         DataType::LSN,
         "Last LSN sent to the ClientReadStream either through a data record "
         "or a gap."},
        {"last_record",
         DataType::LSN,
         "LSN of the last record the storage node delivered to the "
         "ClientReadStream. This value is not necessarily equal to "
         "\"last_delivered\" as the storage node does not store all "
         "records of a log."},
        {"window_high",
         DataType::LSN,
         "Current window used by the ClientReadStream. This instructs the "
         "storage node to not send records with LSNs higher than this value. "
         "When the read pointer reaches this value, the stream is caught up "
         "and the storage node will wake up the stream when the window is slid "
         "by ClientReadStream."},
        {"last_released",
         DataType::LSN,
         "Last LSN released for delivery by the sequencer. If this value is "
         "stale, check if purging could be the reason using the \"purges\" "
         "table."},
        {"catching_up",
         DataType::INTEGER,
         "Indicates whether or not the stream is catching up, ie there are "
         "records that can be delivered now to the client, and the stream is "
         "enqueued in a CatchupQueue (see the \"catchup_queues\" table)."},
        {"window_end",
         DataType::INTEGER,
         "Should be true if \"read_pointer\" is past \"window_high\"."},
        {"known_down",
         DataType::TEXT,
         "List of storage nodes that the ClientReadStream is not able to "
         "receive records from. This list is used so that other storage "
         "nodes can send the records that nodes in that list were supposed "
         "to send. If this columns shows \"ALL_SEND_ALL\", this means that "
         "the ClientReadStream is not running in Single-Copy-Delivery mode, "
         "meaning it requires all storage nodes to send every record they "
         "have.  This can be either because SCD is not enabled for the log, "
         "or because data is not correctly replicated and ClientReadStream "
         "is not able to reconstitute a contiguous sequencer of records "
         "from what storage nodes are sending, or because the "
         "ClientReadStream is going through an epoch boundary."},
        {"filter_version",
         DataType::BIGINT,
         "Each time ClientReadStream rewinds the read streams (and possibly "
         "changes the \"known_down\" list), this counter is updated. Rewinding "
         "a read stream means asking the storage node to rewind to a given "
         "LSN."},
        {"last_batch_status",
         DataType::TEXT,
         "Status code that was issued when the last batch of records was read "
         "for this read stream.  See LocalLogStoreReader::read() in "
         "logdevice/common/LocalLogStore.h."},
        {"created",
         DataType::TIME,
         "Timestamp of when this ServerReadStream was created."},
        {"last_enqueue_time",
         DataType::TIME,
         "Timestamp of when this ServerReadStream was last enqueued for "
         "processing."},
        {"last_batch_started_time",
         DataType::TIME,
         "Timestamp of the last time we started reading a batch of records for "
         "this read stream."},
        {"storage_task_in_flight",
         DataType::INTEGER,
         "True if there is currently a storage task running on a slow storage "
         "thread for reading a batch of records."},
    };
  }
  std::string getCommandToSend(QueryContext& ctx) const override {
    logid_t logid;
    if (columnHasEqualityConstraintOnLogid(3, ctx, logid)) {
      return std::string("info readers log ") + std::to_string(logid.val_) +
          " --json\n";
    } else {
      return std::string("info readers all --json\n");
    }
  }
};

}}}} // namespace facebook::logdevice::ldquery::tables
