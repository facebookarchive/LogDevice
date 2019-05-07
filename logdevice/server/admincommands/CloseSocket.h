/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/Processor.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Socket.h"
#include "logdevice/common/WorkerType.h"
#include "logdevice/common/request_util.h"
#include "logdevice/server/admincommands/AdminCommand.h"

namespace facebook { namespace logdevice { namespace commands {

class CloseSocket : public AdminCommand {
 private:
  WorkerType worker_type_ = WorkerType::MAX;
  worker_id_t worker_id_ = WORKER_ID_INVALID;
  Address address_;
  bool all_clients_{false};

 public:
  using AdminCommand::AdminCommand;

  void getOptions(
      boost::program_options::options_description& out_options) override {
    out_options.add_options()(
        "address",
        boost::program_options::value<std::string>()->notifier(
            [this](const std::string& s) {
              try {
                do { // while (false)
                  std::vector<std::string> tokens;
                  folly::split(':', s, tokens);
                  if (tokens.size() != 1 && tokens.size() != 2) {
                    break;
                  }
                  if (tokens.size() == 2) {
                    if (tokens[0].size() < 3 || tokens[0][0] != 'W') {
                      break;
                    }
                    worker_type_ = workerTypeByChar(tokens[0][1]);
                    if (worker_type_ == WorkerType::MAX) {
                      break;
                    }
                    worker_id_ = worker_id_t(
                        folly::to<worker_id_t::raw_type>(tokens[0].substr(2)));
                    if (worker_id_.val_ < 0) {
                      break;
                    }
                  }
                  std::string& addr = tokens.back();
                  if (addr.empty()) {
                    break;
                  }
                  int32_t id = folly::to<int32_t>(addr.substr(1));
                  if (id < 0) {
                    break;
                  }
                  if (addr[0] == 'N') {
                    address_ = Address(NodeID(id));
                  } else if (addr[0] == 'C') {
                    address_ = Address(ClientID(id));
                  } else {
                    break;
                  }
                  if (!address_.valid()) {
                    break;
                  }
                  return;
                } while (false);
              } catch (folly::ConversionError&) {
              }
              throw boost::program_options::error(
                  "Unexpected address format: \"%s\". Accepted formats: N12, "
                  "C42, "
                  "WG3:N12, WG3:C42.");
            }))("all-clients",
                boost::program_options::value<bool>(&all_clients_)
                    ->default_value(false));
  }
  void getPositionalOptions(
      boost::program_options::positional_options_description& out_options)
      override {
    out_options.add("address", 1);
  }
  std::string getUsage() override {
    return "close_socket [--all-clients] "
           "[W<type><id>:](N<node_id>|C<client_id>)";
  }

  void run() override {
    auto cb = [&] {
      Sender& sender = Worker::onThisThread()->sender();
      int rv;
      if (all_clients_) {
        return sender.closeAllClientSockets(E::PEER_CLOSED);
      }
      if (address_.isClientAddress()) {
        rv = sender.closeClientSocket(address_.asClientID(), E::PEER_CLOSED);
      } else {
        rv = sender.closeServerSocket(address_.asNodeID(), E::PEER_CLOSED);
      }
      if (rv == 0) {
        return 1;
      } else {
        ld_check(err == E::NOTFOUND);
        return 0;
      }
    };

    int count = 0;
    if (worker_type_ == WorkerType::MAX) {
      auto counts = run_on_all_workers(server_->getProcessor(), cb);
      count = std::accumulate(counts.begin(), counts.end(), 0);
    } else {
      count = run_on_worker(
          server_->getProcessor(), worker_id_.val_, worker_type_, cb);
    }

    out_.printf("closed %d sockets\r\n", count);
  }
};

}}} // namespace facebook::logdevice::commands
