/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <cstdlib>
#include <iostream>
#include <memory>
#include <random>
#include <unistd.h>

#include <folly/Memory.h>
#include <folly/Random.h>
#include <folly/Singleton.h>

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

using namespace facebook;
using namespace facebook::logdevice;
using std::make_unique;
using std::chrono::steady_clock;

namespace {
struct CommandLineSettings {
  size_t parallel_findtime_batch = 2000;
  std::chrono::milliseconds findtime_timeout{10000};
  std::chrono::milliseconds since{24 * 60 * 60 * 1000};
  std::string config_path;
  std::vector<logid_t> log_ids;
  uint64_t logs_limit = std::numeric_limits<uint64_t>::max();
  boost::program_options::options_description desc;
};
} // namespace

static void parse_command_line(int argc,
                               const char* argv[],
                               CommandLineSettings& command_line_settings,
                               ClientSettingsImpl* client_settings) {
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

    ("logs-limit",
     value<uint64_t>(&command_line_settings.logs_limit)
     ->default_value(command_line_settings.logs_limit),
     "Limit of the logs number to be copyloaded."
     "No limits by default")

    ("since",
     chrono_value(&command_line_settings.since),
     "Read records younger than a given duration value. Example values:"
     "3min, 1hr, 3hrs, 1d, 2days, 1w, 2 weeks, ...")

    ("parallel-findtime-batch",
     value<size_t>(&command_line_settings.parallel_findtime_batch)
     ->default_value(command_line_settings.parallel_findtime_batch),
     "Specify how many simultaneous findTime() calles "
     "can be executed on cluster")

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
          std::cout << client_settings->getSettingsUpdater()->help(
              SettingFlag::CLIENT);
        }
        exit(0);
      }

      // Surface any errors
      boost::program_options::notify(parsed);
    };
    client_settings->getSettingsUpdater()->parseFromCLI(
        argc, argv, &SettingsUpdater::mustBeClientOption, fallback_fn);
  } catch (const boost::program_options::error& ex) {
    std::cerr << argv[0] << ": " << ex.what() << '\n';
    exit(1);
  }
}

template <typename T>
std::string getStatisticString(std::vector<T> input) {
  std::string results;
  if (input.size() == 0) {
    return results;
  }
  std::sort(input.begin(), input.end());
  T avg_time = std::accumulate(input.begin(), input.end(), T(0)) / input.size();
  int p50 = input.size() / 2;
  int p99 = input.size() * 99 / 100;
  ld_check(p50 >= 0 && p50 < input.size());
  ld_check(p99 >= 0 && p99 < input.size());

  results.append(" avg: ")
      .append(std::to_string(avg_time))
      .append(" p50: ")
      .append(std::to_string(input[p50]))
      .append(" p99: ")
      .append(std::to_string(input[p99]))
      .append("\n");
  return results;
}

namespace {
struct FindTimeResult {
  Status status;
  int execution_time_ms;
  lsn_t result_lsn;
};
} // namespace

std::string resultsToString(std::map<logid_t, FindTimeResult>& results) {
  std::map<Status, size_t> status_map;
  std::vector<int> execution_times;
  for (auto& kv : results) {
    ++status_map[kv.second.status];
    if (kv.second.status == E::OK) {
      execution_times.push_back(kv.second.execution_time_ms);
    }
  }
  std::string timing_results;
  if (execution_times.size() != 0) {
    std::sort(execution_times.begin(), execution_times.end());
    timing_results.append("Among successful results time (in ms) was:")
        .append(getStatisticString(execution_times));
  }
  std::string result;
  result.append("All results count: ")
      .append(std::to_string(results.size()))
      .append(". Succeed: ")
      .append(std::to_string(status_map[E::OK]))
      .append(". Partial: ")
      .append(std::to_string(status_map[E::PARTIAL]))
      .append(". Failed: ")
      .append(std::to_string(status_map[E::FAILED]))
      .append("\n")
      .append(timing_results);
  return result;
}

std::string
getComparisonStatistics(const CommandLineSettings& settings,
                        const std::shared_ptr<Client> client,
                        std::map<logid_t, FindTimeResult> results_strict,
                        std::map<logid_t, FindTimeResult> results_approximate) {
  auto reader = client->createReader(1);
  reader->waitOnlyWhenNoData();

  int failed_res = 0, partial_res = 0, strict_res_is_less = 0, equal_res = 0,
      approx_less_results = 0;
  std::vector<int> records_sizes, records_counts, records_time_diff;
  for (int i = 0; i < settings.log_ids.size(); ++i) {
    auto strict_res = results_strict[settings.log_ids[i]];
    auto approx_res = results_approximate[settings.log_ids[i]];

    if (strict_res.result_lsn == LSN_INVALID ||
        approx_res.result_lsn == LSN_INVALID ||
        strict_res.status == E::FAILED || approx_res.status == E::FAILED) {
      failed_res++;
      continue;
    } else if (strict_res.status == E::PARTIAL ||
               approx_res.status == E::PARTIAL) {
      partial_res++;
      continue;
    } else if (strict_res.result_lsn < approx_res.result_lsn) {
      strict_res_is_less++;
      continue;
    } else if (strict_res.result_lsn == approx_res.result_lsn) {
      equal_res++;
      continue;
    }
    approx_less_results++;
    int rv = reader->startReading(
        settings.log_ids[i], approx_res.result_lsn, strict_res.result_lsn);

    size_t all_records_size = 0;
    size_t all_records_count = 0;
    std::chrono::milliseconds min_timestamp = std::chrono::milliseconds::max();
    std::chrono::milliseconds max_timestamp = std::chrono::milliseconds::min();
    std::vector<std::unique_ptr<DataRecord>> records;
    GapRecord gap;
    while (reader->isReading(settings.log_ids[i])) {
      records.clear();
      int nread = reader->read(100, &records, &gap);
      if (records.size() != 0) {
        min_timestamp = std::min(min_timestamp, records[0]->attrs.timestamp);
        max_timestamp =
            std::max(max_timestamp, records.back()->attrs.timestamp);
      }
      all_records_size +=
          std::accumulate(std::begin(records),
                          std::end(records),
                          std::size_t(0),
                          [](size_t& sum, std::unique_ptr<DataRecord>& record) {
                            return sum + record->payload.size();
                          });
      all_records_count += records.size();
    }
    records_sizes.push_back(all_records_size);
    records_counts.push_back(all_records_count);
    records_time_diff.push_back((max_timestamp - min_timestamp).count());
  }

  std::string result;
  result.append("Results where at least one FindTime failed: ")
      .append(std::to_string(failed_res))
      .append("\n")
      .append("Results where at least one FindTime was partial: ")
      .append(std::to_string(partial_res))
      .append("\n")
      .append("Results where at strict result is earlier then approximate: ")
      .append(std::to_string(strict_res_is_less))
      .append("\n")
      .append("Results where at strict lsn result is equal to approximate: ")
      .append(std::to_string(equal_res))
      .append("\n");
  if (approx_less_results > 0) {
    result.append("Results where Approximate version gave less lsn number: ")
        .append(std::to_string(approx_less_results))
        .append("\n")
        .append("Approximate version was behind strict one by records size: ")
        .append(getStatisticString(records_sizes))
        .append("\n")
        .append("Approximate version was behind strict one by records count: ")
        .append(getStatisticString(records_counts))
        .append("\n")
        .append("Approximate version was behind strict one by time(ms) : ")
        .append(getStatisticString(records_time_diff))
        .append("\n");
  }
  return result;
}

void oneWaveFindTimeStatistics(const CommandLineSettings& settings,
                               std::shared_ptr<Client> client,
                               std::chrono::milliseconds timestamp,
                               bool is_approximate,
                               std::map<logid_t, FindTimeResult>& out_results) {
  Semaphore sem(settings.parallel_findtime_batch);
  std::queue<logid_t> logs_to_query;
  for (int i = 0; i < settings.log_ids.size(); ++i) {
    logs_to_query.push(settings.log_ids[i]);
  }

  std::set<logid_t> invalid_logs;
  int invalid_results = 0;
  // Multy thread mutex to make find time callbacks thread safe
  std::mutex mutex;
  while (1) {
    if (logs_to_query.empty()) {
      if (sem.value() == settings.parallel_findtime_batch) {
        return;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      continue;
    }
    sem.wait();
    logid_t log_id = logs_to_query.front();
    logs_to_query.pop();
    auto time_begin = std::chrono::steady_clock::now();
    auto cb = [log_id, &sem, &out_results, &logs_to_query, &mutex, &time_begin](
                  Status st, lsn_t result) {
      std::lock_guard<std::mutex> guard(mutex);

      auto time_elapsed = std::chrono::steady_clock::now() - time_begin;
      int exec_time =
          std::chrono::duration_cast<std::chrono::milliseconds>(time_elapsed)
              .count();
      FindTimeResult ft_result = {st, exec_time, result};
      out_results[log_id] = ft_result;
      sem.post();
    };
    client->findTime(log_id,
                     timestamp,
                     cb,
                     is_approximate ? FindKeyAccuracy::APPROXIMATE
                                    : FindKeyAccuracy::STRICT);
  }
}

int main(int argc, const char* argv[]) {
  logdeviceInit();

  CommandLineSettings command_line_settings;
  std::unique_ptr<ClientSettingsImpl> clientSettings =
      std::make_unique<ClientSettingsImpl>();
  parse_command_line(argc, argv, command_line_settings, clientSettings.get());
  ld_check(clientSettings->set(
               "findkey-timeout",
               chrono_string(command_line_settings.findtime_timeout)) == 0);

  std::shared_ptr<Client> logdevice_client =
      ClientFactory()
          .setClientSettings(std::move(clientSettings))
          .create(command_line_settings.config_path);

  if (!logdevice_client) {
    ld_error("Could not create client: %s", error_description(err));
    return -1;
  }

  std::vector<logid_t> all_logs;
  auto logs_map = logdevice_client->getLogRangesByNamespace("");
  for (const auto& kv : logs_map) {
    auto first_log = uint64_t(kv.second.first);
    auto last_log = uint64_t(kv.second.second);
    for (auto i = first_log; i <= last_log; ++i) {
      all_logs.push_back(logid_t(i));
    }
  }

  size_t left = all_logs.size();
  int num_selected = 0;
  int max_selected_size =
      std::min(command_line_settings.logs_limit, all_logs.size());
  while (num_selected < max_selected_size) {
    int next = num_selected + (folly::Random::rand32() % left);
    std::iter_swap(all_logs.begin() + num_selected, all_logs.begin() + next);
    ++num_selected;
    --left;
  }

  std::copy(all_logs.begin(),
            all_logs.begin() + max_selected_size,
            std::back_inserter(command_line_settings.log_ids));

  std::map<logid_t, FindTimeResult> approx_results, strict_results;
  auto cur_timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::high_resolution_clock::now().time_since_epoch());
  auto timestamp = std::max(cur_timestamp - command_line_settings.since,
                            std::chrono::milliseconds{0});

  auto time_begin = std::chrono::steady_clock::now();
  oneWaveFindTimeStatistics(command_line_settings,
                            logdevice_client,
                            timestamp,
                            false,
                            strict_results);
  auto time_elapsed_strict = std::chrono::steady_clock::now() - time_begin;

  time_begin = std::chrono::steady_clock::now();
  oneWaveFindTimeStatistics(
      command_line_settings, logdevice_client, timestamp, true, approx_results);
  auto time_elapsed_apporox = std::chrono::steady_clock::now() - time_begin;

  std::string comparison_stat = getComparisonStatistics(
      command_line_settings, logdevice_client, strict_results, approx_results);

  ld_info(
      "\n"
      "Approximate findTime finished in %ld ms.\n"
      "Strict findTime finished in %ld ms.\n\n"
      "Apporoximate FindTime Statistics: \n%s\n"
      "Strict FindTime Statistics: \n%s\n"
      "Comparison Statistics: \n%s\n",
      std::chrono::duration_cast<std::chrono::milliseconds>(
          time_elapsed_apporox)
          .count(),
      std::chrono::duration_cast<std::chrono::milliseconds>(time_elapsed_strict)
          .count(),
      resultsToString(approx_results).c_str(),
      resultsToString(strict_results).c_str(),
      comparison_stat.c_str());

  return 0;
}
