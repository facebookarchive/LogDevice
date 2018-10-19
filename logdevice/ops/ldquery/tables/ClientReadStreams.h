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

class ClientReadStreams : public AdminCommandTable {
 public:
  explicit ClientReadStreams(std::shared_ptr<Context> ctx)
      : AdminCommandTable(ctx) {}
  static std::string getName() {
    return "client_read_streams";
  }
  std::string getDescription() override {
    return "ClientReadStream is the state machine responsible for reading "
           "records of a log on the client side.  The state machine connects "
           "to all storage nodes that may contain data for a log and request "
           "them to send new records as they are appended to the log.  For "
           "each ClientReadStream there is one ServerReadStream per storage "
           "node the ClientReadStream is talking to.  The \"readers\" "
           "table lists all existing ServerReadStreams.  Because LDQuery "
           "does not fetch any debugging information from clients connected "
           "to the cluster, the only ClientReadStreams that will be shown in "
           "this table are for internal read streams on the "
           "server.";
  }
  TableColumns getFetchableColumns() const override {
    return {
        {"log_id", DataType::LOGID, "Id of the log being read."},
        {"id", DataType::BIGINT, "Internal identifier for the read stream."},
        {"next_lsn_to_deliver",
         DataType::BIGINT,
         "Next LSN that needs to be delivered to the client."},
        {"window_high",
         DataType::LSN,
         "Current window.  This is used for flow control.  ClientReadStream "
         "instructs storage nodes to not send records with LSNs higher than "
         "this value.  ClientReadStream slides the window as it is able to "
         "make progress (see the --client-read-flow-control-threshold "
         "setting)."},
        {"until_lsn",
         DataType::LSN,
         "LSN up to which the ClientReadStream must read."},
        {"read_set_size",
         DataType::BIGINT,
         "Number of storage nodes this ClientReadStream is reading from.  "
         "Usally equal to the size of the log's nodeset but may be smaller if "
         "some nodes from that nodeset are rebuilding."},
        {"gap_end_outside_window",
         DataType::LSN,
         "Sometimes there are no more records to deliver that have a LSN "
         "smaller than \"window_high\".  When a storage node reaches the end "
         "of the window, it sends a GAP message to inform of the next LSN it "
         "will be able to ship past the window.  This value is the smallest "
         "LSN greater than \"window_high\" reported by storage nodes and is "
         "used for determining the right endpoint of a gap interval in such a "
         "situation."},
        {"trim_point",
         DataType::LSN,
         "When a storage node reaches a log's trim point, it informs "
         "ClientReadStream through a gap message.  This is ClientReadStream's "
         "current view of the log's trim point."},
        {"gap_nodes_next_lsn",
         DataType::TEXT,
         "Contains the list of nodes for which we know they don't have a "
         "record with LSN \"next_lsn_to_deliver\".  Alongside the node id is "
         "the LSN of the next record or gap this storage node is expected to "
         "send us."},
        {"unavailable_nodes",
         DataType::TEXT,
         "List of nodes that ClientReadStream knows are unavailable and thus "
         "is not trying to read from."},
        {"connection_health",
         DataType::TEXT,
         "Summary of authoritative status of the read session. An "
         "AUTHORITATIVE session has a least an f-majority of nodes "
         "participating. Reported dataloss indicates all copies of a record "
         "were lost, or, much less likely, the only copies of the data are on "
         "the R-1 nodes that are currently unavailable, and the cluster failed "
         "to detect or remediate the failure that caused some copies to be "
         "lost. A NON_AUTHORITATIVE session has less than an f-majority of "
         "nodes participating, but those not participating have had detected "
         "failures, are not expected to participate, and are being rebuilt. "
         "Most readers will stall in this case. Readers that proceed can see "
         "dataloss gaps for records that are merely currently unavailable, but "
         "will become readable once failed nodes are repaired.  "
         "An UNHEALTHY session has too many storage nodes down but not marked "
         "as failed, to be able to read even non-authoritatively."},
        {"redelivery_inprog",
         DataType::BOOL,
         "True if a retry of delivery to the application is outstanding."},
        {"filter_version",
         DataType::BIGINT,
         "Read session version.  This is bumped every time parameters "
         "(start, until, SCD, etc.) are changed by the client."}};
  }
  std::string getCommandToSend(QueryContext& /*ctx*/) const override {
    return std::string("info client_read_streams --json\n");
  }
};

}}}} // namespace facebook::logdevice::ldquery::tables
