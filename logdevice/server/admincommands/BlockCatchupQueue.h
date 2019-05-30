/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <algorithm>

#include <folly/ScopeGuard.h>

#include "logdevice/common/Sender.h"
#include "logdevice/common/Socket.h"
#include "logdevice/common/SocketTypes.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/request_util.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/server/ServerWorker.h"
#include "logdevice/server/admincommands/AdminCommand.h"
#include "logdevice/server/read_path/AllServerReadStreams.h"

namespace facebook { namespace logdevice { namespace commands {

/**
 * This admin command can be used to "block" the catchup queue for a given
 * client host or for all client hosts.
 *
 * This can be used in E2E tests to simulate a bug leading to a storage shard
 * not pushing records to a client and expecting SCD Greylisting to kick in to
 * restore read availability.
 *
 * This can also be used in case of an emergency to stop all read traffic from
 * clients.
 */

class BlockCatchupQueue : public AdminCommand {
  using AdminCommand::AdminCommand;

 private:
  std::string hostname_;
  std::string type_; // "off" or "on"
  bool force_{false};

 public:
  void getOptions(
      boost::program_options::options_description& out_options) override {
    out_options.add_options()(
        "hostname",
        boost::program_options::value<std::string>(&hostname_)->required())(
        "type", boost::program_options::value<std::string>(&type_)->required())(
        "force", boost::program_options::bool_switch(&force_));
  }

  std::string getUsage() override {
    return "block catchup_queue <hostname>|all [off|on]";
  }

  void getPositionalOptions(
      boost::program_options::positional_options_description& out_options)
      override {
    out_options.add("hostname", 1);
    out_options.add("type", 1);
  }

  void run() override {
    // If this is a production build, require passing --force.
    if (!folly::kIsDebug) {
      if (!force_) {
        out_.write("Production build. Command disabled. "
                   "Use --force to proceed anyway.\r\n");
        return;
      }
    }

    if (!server_->getProcessor()->runningOnStorageNode()) {
      out_.printf("Cannot process 'block catchup_queue' command, not a "
                  "storage node\r\n");
      return;
    }

    if (type_ != "on" && type_ != "off") {
      out_.printf("Usage: %s\r\n", getUsage().c_str());
      return;
    }

    folly::IPAddress addr;
    if (hostname_ != "all") {
      try {
        addr = folly::IPAddress(hostname_);
      } catch (const folly::IPAddressFormatException& e) {
        out_.printf("Cannot parse ip address: %s\r\n", e.what());
        return;
      }
    }

    auto fn = [&](Socket& s) {
      auto sockaddr = s.peerSockaddr();
      if (!sockaddr.getSocketAddress().isFamilyInet() ||
          !s.peer_name_.isClientAddress() || s.peer_node_id_.isNodeID()) {
        return;
      }
      if (hostname_ != "all" && sockaddr.getIPAddress() != addr) {
        return;
      }
      auto cid = s.peer_name_.asClientID();
      ServerWorker* w = ServerWorker::onThisThread();
      w->serverReadStreams().blockUnblockClient(cid, type_ == "on");
    };

    std::function<void(Socket&)> fn_ = fn;

    run_on_all_workers(server_->getProcessor(), [&]() {
      auto* worker = Worker::onThisThread();
      worker->sender().forAllClientSockets(fn_);
      return true;
    });

    out_.printf("Done.\r\n");
  }
};

}}} // namespace facebook::logdevice::commands
