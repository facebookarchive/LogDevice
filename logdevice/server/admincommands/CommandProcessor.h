/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>
#include <sstream>
#include <string>

#include <folly/executors/SerialExecutor.h>
#include <folly/futures/Future.h>
#include <folly/io/IOBuf.h>

#include "logdevice/server/ServerSettings.h"
#include "logdevice/server/admincommands/AdminCommandFactory.h"

namespace facebook { namespace logdevice {

class Server;

/**
 * @file A class which executes the admin command.
 */
class CommandProcessor {
 public:
  explicit CommandProcessor(Server* server);

  /**
   * Executes the command.
   *
   * @command_line the command to execute.
   * @address is an address of the machine which sent a request.
   */
  std::unique_ptr<folly::IOBuf>
  processCommand(const char* command_line, const folly::SocketAddress& address);

  /**
   * Async version of processorCommand, that are serialized under the same
   * serial executor.
   */
  folly::SemiFuture<std::unique_ptr<folly::IOBuf>>
  asyncProcessCommand(const std::string& command_line,
                      const folly::SocketAddress& address);

 private:
  Server* server_;
  UpdateableSettings<ServerSettings> server_settings_;
  std::unique_ptr<AdminCommandFactory> command_factory_;

  // Admin commands do all kind of weird blocking stuff. Serialize their runs to
  // make sure they only block a single thread at a time.
  folly::Executor::KeepAlive<folly::SerialExecutor> executor_;
};

}} // namespace facebook::logdevice
