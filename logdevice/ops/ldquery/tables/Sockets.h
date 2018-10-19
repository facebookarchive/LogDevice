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
    return "Tracks all sockets on all nodes in the cluster.";
  }
  TableColumns getFetchableColumns() const override {
    return {
        {"state",
         DataType::TEXT,
         "State of the socket. \n I: The socket is Inactive; \n C: The socket "
         "is connecting; \n H: The socket is doing the handshake at the LD "
         "protocol level; \n A: The socket is active."},
        {"name",
         DataType::TEXT,
         "Name of the socket. If the other end is a client, the format is "
         "similar to the column \"client\" of the table \"catchup_queues\" and "
         "the column \"client\" of the table \"readers\". If the other end is "
         "another node in the cluster, describes that's node's id and ip."},
        {"pending_kb",
         DataType::REAL,
         "Number of bytes that are available for writting on the socket's "
         "output evbuffer. If this value is high this usually means that the "
         "other end is not able to read messages as fast as we are writting "
         "them."},
        {"available_kb",
         DataType::REAL,
         "Number of bytes that are available for reading on the socket's input "
         "evbuffer.  If this value is high this usually means that the other "
         "end is writting faster than this node is able to read."},
        {"read_mb",
         DataType::REAL,
         "Number of bytes that were read from the socket."},
        {"write_mb",
         DataType::REAL,
         "Number of bytes that were written to the socket."},
        {"read_cnt",
         DataType::INTEGER,
         "Number of messages that were read from the socket."},
        {"write_cnt",
         DataType::INTEGER,
         "Number of messages that were written to the socket."},
        {"proto",
         DataType::INTEGER,
         "Protocol that was handshaken. Do not trust this value if the "
         "socket's state is not active."},
        {"sendbuf",
         DataType::INTEGER,
         "Size of the send buffer of the underlying TCP socket."},
        {"peer_config_version",
         DataType::INTEGER,
         "Last config version that the peer advertised"},
        {"is_ssl", DataType::INTEGER, "Set to true if this socket uses SSL."},
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
