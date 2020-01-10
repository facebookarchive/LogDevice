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

class Sockets : public AdminCommandTable {
 public:
  explicit Sockets(std::shared_ptr<Context> ctx) : AdminCommandTable(ctx) {}
  static std::string getName() {
    return "sockets";
  }
  std::string getDescription() override {
    return "Tracks all Connections on all nodes in the cluster.";
  }
  TableColumns getFetchableColumns() const override {
    return {
        {"state",
         DataType::TEXT,
         "State of the Connection. \n I: The connection is Inactive; \n C: The "
         "connection "
         "is connecting; \n H: The connection is doing the handshake at the LD "
         "protocol level; \n A: The connection is active."},
        {"name",
         DataType::TEXT,
         "Name of the Connection. If the other end is a client, the format is "
         "similar to the column \"client\" of the table \"catchup_queues\" and "
         "the column \"client\" of the table \"readers\". If the other end is "
         "another node in the cluster, describes that's node's id and ip."},
        {"pending_kb",
         DataType::REAL,
         "Number of bytes that are available for writing on the Connection's "
         "output buffer. If this value is high this usually means that the "
         "other end is not able to read messages as fast as we are writing "
         "them."},
        {"available_kb",
         DataType::REAL,
         "Number of bytes that are available for reading on the Connection's "
         "input "
         "buffer.  If this value is high this usually means that the other "
         "end is writing faster than this node is able to read."},
        {"read_mb",
         DataType::REAL,
         "Number of bytes that were read from the Connection."},
        {"write_mb",
         DataType::REAL,
         "Number of bytes that were written to the Connection."},
        {"read_cnt",
         DataType::INTEGER,
         "Number of messages that were read from the Connection."},
        {"write_cnt",
         DataType::INTEGER,
         "Number of messages that were written to the Connection."},
        {"bytes_per_second",
         DataType::REAL,
         "Connection throughput in the last health check period."},
        {"rwnd_limited_pct",
         DataType::REAL,
         "Portion of last health check period, when Connection throughput was "
         "limited by receiver."},
        {"sndbuf_limited_pct",
         DataType::REAL,
         "Portion of last health check peiod, when Connection throughput was "
         "limited by send buffer."},
        {"proto",
         DataType::INTEGER,
         "Protocol that was handshaken. Do not trust this value if the "
         "Connection's state is not active."},
        {"sendbuf",
         DataType::INTEGER,
         "Size of the send buffer of the underlying TCP socket."},
        {"peer_config_version",
         DataType::INTEGER,
         "Last config version that the peer advertised"},
        {"is_ssl",
         DataType::INTEGER,
         "Set to true if this Connection uses SSL."},
        {"fd",
         DataType::INTEGER,
         "The file descriptor of the underlying os socket."},
    };
  }
  std::string getCommandToSend(QueryContext& /*ctx*/) const override {
    return std::string("info sockets --json\n");
  }
};

}}}} // namespace facebook::logdevice::ldquery::tables
