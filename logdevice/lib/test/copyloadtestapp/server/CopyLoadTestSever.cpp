/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <thrift/lib/cpp2/server/ThriftServer.h>

#include <folly/Singleton.h>

#include "common/services/cpp/ServiceFramework.h"
#include "logdevice/lib/test/copyloadtestapp/server/CopyLoadTestServiceHandler.h"

#include "logdevice/common/LibeventTimer.h"
#include "logdevice/common/commandline_util.h"
#include "logdevice/common/commandline_util_chrono.h"

using namespace facebook;
using namespace facebook::logdevice;
using apache::thrift::HeaderClientChannel;
using apache::thrift::async::TAsyncSocket;
using folly::EventBase;
using std::make_unique;
using std::chrono::steady_clock;

namespace ld_copyload = facebook::logdevice::copyloadtestapp;

void parse_command_line(int argc,
                        const char* argv[],
                        boost::program_options::options_description& desc,
                        ld_copyload::CommandLineSettings& command_line_settings,
                        ClientSettingsImpl& client_settings) {
  using boost::program_options::bool_switch;
  using boost::program_options::value;

  // clang-format off

  desc.add_options()

    ("help,h",
     "produce this help message and exit")

    ("verbose,v",
     "also include a description of all LogDevice Client settings in the help "
     "message")

    ("config-path",
     value<std::string>(&command_line_settings.config_path)
     ->required(),
     "path to config file")

    ("port",
     value<uint32_t>(&command_line_settings.port)
     ->default_value(1739),
     "Port for thrift service")

    ("logs-limit",
     value<uint64_t>(&command_line_settings.logs_limit)
     ->default_value(command_line_settings.logs_limit),
     "Limit of the logs number to be copyloaded."
     "No limits by default")

    ("find-time-approximate",
     bool_switch(&command_line_settings.find_time_approximate)
     ->default_value(false),
     "Flag which specify if FINDKEY_Header::APPROXIMATE option is"
     "going to be used during findTime()")

    ("since",
     chrono_value(&command_line_settings.since),
     "Read records younger than a given duration value. Example values:"
     "3min, 1hr, 3hrs, 1d, 2days, 1w, 2 weeks, ...")

    ("parallel-findtime-batch",
     value<size_t>(&command_line_settings.parallel_findtime_batch)
     ->default_value(command_line_settings.parallel_findtime_batch),
     "Specify how many simultaneous findTime() calles "
     "can be executed on cluster")

    ("tasks-queue-size-limit",
     value<size_t>(&command_line_settings.tasks_queue_size_limit)
     ->default_value(command_line_settings.tasks_queue_size_limit),
     "Limit of tasks queue size. If thread generated more than limit size "
     "tasks, it will wait until tasks get out of queue for processing before "
     "adding new tasks")

    ("tasks-in-flight-size-limit",
     value<size_t>(&command_line_settings.tasks_in_flight_size_limit)
     ->default_value(command_line_settings.tasks_in_flight_size_limit),
     "Limit of number of simultaneous tasks that are processed by clients")

    ("chunk-duration",
     chrono_value(&command_line_settings.chunk_duration),
     "Duration value of chunks that are going to be read one by one."
     "Example values: 3min, 1hr, 3hrs, 1d, 2days, 1w, 2 weeks, ...")

    ("no-payload",
     bool_switch(&command_line_settings.no_payload)
     ->default_value(false),
     "request storage servers send records without paylaod")

    ("buffer-size",
     value<size_t>(&command_line_settings.total_buffer_size)
     ->default_value(command_line_settings.total_buffer_size),
     "client read buffer size for all logs")

    ("loglevel",
     value<std::string>()
     ->default_value("info")
     ->notifier(dbg::parseLoglevelOption),
     "One of the following: critical, error, warning, info, debug")

    ("findtime-timeout",
     chrono_value(&command_line_settings.findtime_timeout)
     ->notifier([](std::chrono::milliseconds val) -> void {
       if (val.count() < 0) {
       throw boost::program_options::error("findtime-timeout must be > 0");
       }
       }),
     "Timeout for calls to findTime")
    ;
  // clang-format on

  try {
    auto fallback_fn = [&](int ac, const char* av[]) {
      boost::program_options::variables_map parsed =
          program_options_parse_no_positional(ac, av, desc);

      // Check for --help before calling notify(), so that required options
      // aren't required.
      if (parsed.count("help")) {
        std::cout
            << "Test application that connects to LogDevice and reads logs.\n\n"
            << desc;
        if (parsed.count("verbose")) {
          std::cout << std::endl;
          std::cout << "LogDevice Client settings:" << std::endl << std::endl;
          std::cout << client_settings.getSettingsUpdater()->help(
              SettingFlag::CLIENT);
        }
        exit(0);
      }

      // Surface any errors
      boost::program_options::notify(parsed);
    };
    client_settings.getSettingsUpdater()->parseFromCLI(
        argc, argv, &SettingsUpdater::mustBeClientOption, fallback_fn);
  } catch (const boost::program_options::error& ex) {
    std::cerr << argv[0] << ": " << ex.what() << '\n';
    exit(1);
  }
}

int main(int argc, const char** argv) {
  logdeviceInit();
  std::unique_ptr<ClientSettingsImpl> clientSettings =
      std::make_unique<ClientSettingsImpl>();
  ld_copyload::CommandLineSettings command_line_settings;
  boost::program_options::options_description desc;
  parse_command_line(argc, argv, desc, command_line_settings, *clientSettings);

  EventBase eventBase;

  std::shared_ptr<Client> logdevice_client =
      Client::create("Test cluster",
                     command_line_settings.config_path,
                     "none",
                     command_line_settings.findtime_timeout,
                     std::move(clientSettings));

  // getting all logs renges
  auto logs_map = logdevice_client->getLogRangesByNamespace("");
  for (const auto& kv : logs_map) {
    auto first_log = uint64_t(kv.second.first);
    auto last_log = uint64_t(kv.second.second);
    for (auto i = first_log; i <= last_log; ++i) {
      if (command_line_settings.log_ids.size() >=
          command_line_settings.logs_limit) {
        break;
      }
      command_line_settings.log_ids.push_back(logid_t(i));
    }
    if (command_line_settings.log_ids.size() >=
        command_line_settings.logs_limit) {
      break;
    }
  }

  // Don't use custom command line parameters in in initFacebook()
  // to prevent parsing errors
  argc = 0;
  facebook::initFacebook(&argc, const_cast<char***>(&argv));

  auto handler = std::make_shared<ld_copyload::CopyLoadTestServiceHandler>(
      logdevice_client, command_line_settings);
  auto thriftServer = std::make_shared<apache::thrift::ThriftServer>();
  std::string service_name = "LogDevice Thrift Service";
  facebook::services::ServiceFramework service(service_name.c_str());

  thriftServer->setInterface(handler);
  thriftServer->setPort(command_line_settings.port);
  // Client execution thread can sleep for up to 1 minute before making
  // new call to server. Server will trigger idle exception on client in case if
  // client will send no task request or updates in last
  // 1 minute + execution_confirmation_timeout time + 1 extra minute.
  thriftServer->setIdleTimeout(
      command_line_settings.execution_confirmation_timeout +
      std::chrono::minutes(2));
  service.addThriftService(
      thriftServer, handler.get(), command_line_settings.port);

  LOG(INFO) << "Running " << service_name << " on port "
            << command_line_settings.port << ".";
  service.go();

  LOG(INFO) << service_name << " stopped.";
  return 0;
}
