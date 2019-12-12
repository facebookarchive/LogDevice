/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <sstream>

// clang-format off
/**
 * Order matters!
 * Without this include token functions will not compile in boost 1.65 which
 * ubuntu is using now.
 * https://github.com/boostorg/tokenizer/pull/10 (internal task T53218151)
 */
#include <boost/type_traits/is_pointer.hpp>
#include <boost/token_functions.hpp>
// clang-format on

#include "logdevice/common/Processor.h"
#include "logdevice/common/RequestType.h"
#include "logdevice/common/Semaphore.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/include/Err.h"
#include "logdevice/server/Server.h"
#include "logdevice/server/admincommands/AdminCommand.h"
#include "logdevice/server/admincommands/AdminCommandFactory.h"
#include "logdevice/server/read_path/LogStorageState.h"
#include "logdevice/server/storage_tasks/ShardedStorageThreadPool.h"

namespace facebook { namespace logdevice {

namespace {
std::unique_ptr<AdminCommandFactory> createAdminCommandFactory(bool test_mode) {
  if (test_mode) {
    return std::make_unique<TestAdminCommandFactory>();
  } else {
    return std::make_unique<AdminCommandFactory>();
  }
}
} // namespace

CommandProcessor::CommandProcessor(Server* server)
    : server_(server),
      server_settings_(server_->getServerSettings()),
      command_factory_(createAdminCommandFactory(
          /*test_mode=*/server_settings_->test_mode)),
      executor_(folly::SerialExecutor::create()) {}

std::unique_ptr<folly::IOBuf>
CommandProcessor::processCommand(const char* command_line,
                                 const folly::SocketAddress& address) {
  auto start_time = std::chrono::steady_clock::now();
  ld_debug("Processing command: %s", sanitize_string(command_line).c_str());

  auto buffer = std::make_unique<folly::IOBuf>();
  folly::io::Appender output(buffer.get(), 1024);
  std::vector<std::string> args;
  try {
    args = boost::program_options::split_unix(command_line);
  } catch (boost::escaped_list_error& e) {
    output.printf("Failed to split: %s\r\nEND\r\n", e.what());
    RATELIMIT_INFO(std::chrono::seconds(10),
                   2,
                   "Got bad admin command (split failed) from %s: %s",
                   address.describe().c_str(),
                   sanitize_string(command_line).c_str());
    return buffer;
  }

  auto command = command_factory_->get(args, output);

  if (!command) {
    RATELIMIT_INFO(std::chrono::seconds(10),
                   2,
                   "Got bad admin command (unknown command) from %s: %s",
                   address.describe().c_str(),
                   sanitize_string(command_line).c_str());
    return buffer;
  }

  // Enforce restriction level of admin command
  switch (command->getRestrictionLevel()) {
    case AdminCommand::RestrictionLevel::UNRESTRICTED:
      break;
    case AdminCommand::RestrictionLevel::LOCALHOST_ONLY:
      if (!address.isLoopbackAddress()) {
        output.printf(
            "Permission denied: command is localhost-only!\r\nEND\r\n");
        RATELIMIT_INFO(
            std::chrono::seconds(10),
            2,
            "Localhost-only admin command called from non-localhost %s: %s",
            address.describe().c_str(),
            sanitize_string(command_line).c_str());
        return buffer;
      }
      break;
  }

  command->setServer(server_);

  boost::program_options::options_description options;
  boost::program_options::positional_options_description positional;
  namespace style = boost::program_options::command_line_style;

  try {
    command->getOptions(options);
    command->getPositionalOptions(positional);
    boost::program_options::variables_map vm;
    boost::program_options::store(
        boost::program_options::command_line_parser(args)
            .options(options)
            .positional(positional)
            .style(style::unix_style & ~style::allow_guessing)
            .run(),
        vm);
    boost::program_options::notify(vm);
  } catch (boost::program_options::error& e) {
    output.printf("Options error: %s\r\n", e.what());
    std::string usage = command->getUsage();
    if (!usage.empty()) {
      output.printf("USAGE %s\r\n", usage.c_str());
    }
    output.printf("END\r\n");
    RATELIMIT_INFO(std::chrono::seconds(10),
                   2,
                   "Got bad admin command (bad options: %s) from %s: %s",
                   e.what(),
                   address.describe().c_str(),
                   sanitize_string(command_line).c_str());
    return buffer;
  }

  command->run();

  output.printf("END\r\n");

  auto duration = std::chrono::steady_clock::now() - start_time;
  ld_log(duration > std::chrono::milliseconds(50) ? dbg::Level::INFO
                                                  : dbg::Level::DEBUG,
         "Admin command from %s took %.3f seconds to output %lu bytes: %s",
         address.describe().c_str(),
         std::chrono::duration_cast<std::chrono::duration<double>>(duration)
             .count(),
         buffer->computeChainDataLength(),
         sanitize_string(command_line).c_str());
  return buffer;
}

folly::SemiFuture<std::unique_ptr<folly::IOBuf>>
CommandProcessor::asyncProcessCommand(const std::string& command_line,
                                      const folly::SocketAddress& address) {
  return folly::via(executor_.get())
      .thenValue([command_line, address, this](auto&&) {
        return processCommand(command_line.data(), address);
      });
}

}} // namespace facebook::logdevice
