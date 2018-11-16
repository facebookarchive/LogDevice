/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <cstdlib>
#include <memory>
#include <random>
#include <unistd.h>

#include <folly/Memory.h>
#include <folly/Singleton.h>
#include <folly/io/async/EventBase.h>
#include <thrift/lib/cpp/async/TAsyncSocket.h>
#include <thrift/lib/cpp2/async/HeaderClientChannel.h>

#include "common/init/Init.h"
#include "logdevice/common/Checksum.h"
#include "logdevice/common/EpochMetaData.h"
#include "logdevice/common/ReaderImpl.h"
#include "logdevice/common/Semaphore.h"
#include "logdevice/common/Timer.h"
#include "logdevice/common/commandline_util.h"
#include "logdevice/common/commandline_util_chrono.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/util.h"
#include "logdevice/include/Client.h"
#include "logdevice/include/Err.h"
#include "logdevice/include/Record.h"
#include "logdevice/include/types.h"
#include "logdevice/lib/ClientSettingsImpl.h"
#include "logdevice/lib/test/copyloadtestapp/server/gen-cpp2/CopyLoaderTest.h"

using namespace facebook;
using namespace facebook::logdevice;
using apache::thrift::HeaderClientChannel;
using apache::thrift::async::TAsyncSocket;
using folly::EventBase;
using std::make_unique;
using std::chrono::steady_clock;

struct CommandLineSettings {
  bool no_payload;
  size_t total_buffer_size = 2000000;
  std::chrono::milliseconds findtime_timeout{10000};
  std::chrono::milliseconds confirmation_duration{60 * 1000};
  boost::program_options::options_description desc;
  std::string config_path;
  std::string host_ip;
  uint32_t num_threads;
  uint32_t port;
  uint32_t task_index;
};

void parse_command_line(int argc,
                        const char* argv[],
                        CommandLineSettings& command_line_settings,
                        ClientSettingsImpl& client_settings) {
  using boost::program_options::bool_switch;
  using boost::program_options::value;

  // clang-format off
  command_line_settings.desc.add_options()

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

    ("task-index",
     value<uint32_t>(&command_line_settings.task_index)
     ->default_value(0),
     "tupperware task index used for process identication")

    ("server-address",
     value<std::string>(&command_line_settings.host_ip)
     ->default_value("127.0.0.1"),
     "host server ip address. Default is 127.0.0.1")

    ("threads-number",
     value<uint32_t>(&command_line_settings.num_threads)
     ->default_value(10),
     "number of threads that are going to execute tasks independently")

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
          program_options_parse_no_positional(
              ac, av, command_line_settings.desc);

      // Check for --help before calling notify(), so that required options
      // aren't required.
      if (parsed.count("help")) {
        std::cout
            << "Test application that connects to LogDevice and reads logs.\n\n"
            << command_line_settings.desc;
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

void client_thread(CommandLineSettings command_line_settings,
                   const ClientSettingsImpl& client_settings,
                   int thread_id) {
  EventBase eventBase;

  uint64_t client_id =
      (uint64_t(command_line_settings.task_index) << 32) | uint64_t(thread_id);

  try {
    // Create a client to the service
    auto socket = TAsyncSocket::newSocket(
        &eventBase, command_line_settings.host_ip, command_line_settings.port);
    auto channel = HeaderClientChannel::newChannel(socket);
    auto thrift_client =
        make_unique<thrift::CopyLoaderTestAsyncClient>(std::move(channel));

    auto local_client_settings =
        std::make_unique<ClientSettingsImpl>(client_settings);

    ld_check_eq(0,
                local_client_settings->set(
                    "findkey-timeout",
                    chrono_string(command_line_settings.findtime_timeout)));

    std::shared_ptr<Client> logdevice_client =
        ClientFactory()
            .setClientSettings(std::move(local_client_settings))
            .create(command_line_settings.config_path);

    if (!logdevice_client) {
      ld_error("Could not create client: %s", error_description(err));
      return;
    }

    // Reader will read one log at one range at once.
    // stopReading() will be invoced after end of each reading.
    auto reader = logdevice_client->createReader(1);
    reader->waitOnlyWhenNoData();
    reader->setTimeout(std::chrono::seconds(1));
    unsigned long wait_time = 100;
    std::default_random_engine generator;

    std::string process_info;
    process_info.append("Tupperware task id: ")
        .append(std::to_string(command_line_settings.task_index))
        .append(". Thread id: ")
        .append(std::to_string(thread_id));

    while (1) {
      // Get a tasks
      std::vector<thrift::Task> tasks;
      while (true) {
        thrift_client->sync_getTask(tasks, client_id, process_info);
        if (tasks.size() != 0) {
          wait_time = 1000;
          break;
        } else {
          // radnomize wait time
          std::uniform_int_distribution<int> distribution(
              wait_time / 2 * 3, wait_time * 3);
          wait_time = std::min(distribution(generator), 60 * 1000);
          LOG(INFO) << "Thread " << thread_id << " is about to sleep "
                    << wait_time << "ms.";
          std::this_thread::sleep_for(std::chrono::milliseconds(wait_time));
        }
      }

      for (int i = 0; i < tasks.size(); ++i) {
        lsn_t lsn_start = compose_lsn(epoch_t(tasks[i].start_lsn.lsn_hi),
                                      esn_t(tasks[i].start_lsn.lsn_lo));
        lsn_t lsn_end = compose_lsn(
            epoch_t(tasks[i].end_lsn.lsn_hi), esn_t(tasks[i].end_lsn.lsn_lo));

        LOG(INFO) << "Task id: " << tasks[i].id << " thread id: " << thread_id
                  << " lsn: " << lsn_start << " - " << lsn_end
                  << "start procesing.";

        auto ret =
            reader->startReading((logid_t)tasks[i].log_id, lsn_start, lsn_end);
        ld_check(ret == 0);

        auto time_begin = std::chrono::steady_clock::now();
        auto last_time_confirmed = time_begin;
        std::vector<std::unique_ptr<DataRecord>> records;
        GapRecord gap;
        size_t all_records_size = 0;
        size_t all_records_count = 0;
        bool is_confirmed = true;
        while (reader->isReading((logid_t)tasks[i].log_id)) {
          records.clear();
          int nread = reader->read(100, &records, &gap);
          all_records_size += std::accumulate(
              std::begin(records),
              std::end(records),
              std::size_t(0),
              [](size_t& sum, std::unique_ptr<DataRecord>& record) {
                return sum + record->payload.size();
              });
          all_records_count += records.size();
          auto cur_time = std::chrono::steady_clock::now();
          auto time_elapsed =
              std::chrono::duration_cast<std::chrono::milliseconds>(
                  cur_time - last_time_confirmed);
          if (time_elapsed > command_line_settings.confirmation_duration) {
            bool rv = thrift_client->sync_confirmOngoingTaskExecution(
                tasks[i].id, client_id);
            if (!rv) {
              is_confirmed = false;
              break;
            }
            last_time_confirmed = cur_time;
          }
        }
        if (!is_confirmed) {
          continue;
        }
        auto time_all = std::chrono::steady_clock::now() - time_begin;
        double time_elapsed_sec =
            double(
                std::chrono::duration_cast<std::chrono::milliseconds>(time_all)
                    .count()) /
            1000;
        char buf[128];
        snprintf(buf, sizeof buf, "%.3f", time_elapsed_sec);
        std::string logging_info;
        logging_info.append(process_info)
            .append(". Records size read: ")
            .append(std::to_string(all_records_count))
            .append(". Data loaded: ")
            .append(std::to_string(all_records_size))
            .append(". Time spent to read in seconds: ")
            .append(buf);
        thrift_client->sync_informTaskFinished(
            tasks[i].id, client_id, logging_info, all_records_size);
      }
    }
  } catch (apache::thrift::transport::TTransportException& ex) {
    LOG(ERROR) << "Request failed " << ex.what();
  }
}

int main(int argc, const char* argv[]) {
  logdeviceInit();
  ClientSettingsImpl client_settings;
  CommandLineSettings command_line_settings;
  parse_command_line(argc, argv, command_line_settings, client_settings);

  std::vector<std::thread> client_threads;
  for (int i = 0; i < command_line_settings.num_threads; ++i) {
    client_threads.emplace_back(
        client_thread, command_line_settings, std::ref(client_settings), i);
  }

  for (auto& client_thread : client_threads) {
    client_thread.join();
  }

  // Don't use custom command line parameters in in initFacebook()
  // to prevent parsing errors
  argc = 0;
  initFacebook(&argc, const_cast<char***>(&argv));

  return 0;
}
