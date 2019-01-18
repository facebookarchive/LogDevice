/**
 * Copyright (c) 2019-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <mutex>
#include <string>

#include <boost/program_options.hpp>
#include <folly/Singleton.h>

#include "logdevice/examples/parse_target_log.h"
#include "logdevice/include/BufferedWriter.h"
#include "logdevice/include/debug.h"
/**
 * @file buffered_writer.cpp
 * This sample code demonstrates how to use the BufferedWriter append API.
 */

static const char* USAGE =
    R"DOC(Usage: buffered_writer [options...] CONFIG LOG

This sample code demonstrates how to write records to LogDevice using
the BufferedWriter append API.

The CONFIG argument points the tool to the cluster.  The LOG argument says
which log to write to.  See below for formats.

)DOC";

using namespace facebook::logdevice;

namespace {
struct {
  std::string config_path;
  std::string target_log;
} command_line_options;

// This class is called when the cluster reports the status of an append.
class BufferedWriterCallback : public BufferedWriter::AppendCallback {
 public:
  void onSuccess(logid_t /* unused */,
                 ContextSet contexts,
                 const DataRecordAttributes& /* unused */) override {
    std::lock_guard<std::mutex> guard(mutex);
    confirmedRecords += contexts.size();
    std::cout << "BufferedWriterCallback: " << confirmedRecords
              << " records successfully written (so far)." << '\n';
    appendsAckd += contexts.size();
    cv.notify_all();
  }

  /* Called when a batch of records for the same log failed to be appended,
   * and BufferedWriter exhausted all retries it was configured to do.
   */
  void onFailure(logid_t log_id, ContextSet contexts, Status status) override {
    std::lock_guard<std::mutex> guard(mutex);
    std::cerr << "BufferedWriterCallback: " << contexts.size()
              << " records could not be written to log: " << log_id.val_
              << ". Status: " << error_name(status) << '\n';

    // Handle the error if needed. For example, collect the
    // failed payloads to retry them later.
    for (auto& ctx : contexts) {
      payloadsFailed.push_back(std::move(ctx.second));
    }
    appendsAckd += contexts.size();
    cv.notify_all();
  }
  // If a retry is needed, LogDevice calls this before the retry.
  RetryDecision onRetry(logid_t log_id,
                        const ContextSet& /* contexts */,
                        Status /* status */) override {
    std::cerr << "Retrying append to LogDevice log: " << log_id.val_ << '\n';
    return RetryDecision::ALLOW;
  }

  std::mutex mutex;
  int appendsAckd = 0;
  std::condition_variable cv;
  // Number of successful writes that BufferedWriter has told us about.
  size_t confirmedRecords = 0;
  // Collects payloads for all failed writes.
  std::vector<std::string> payloadsFailed;
};
} // namespace

static void parse_command_line(int argc, const char** argv);

int main(int argc, const char* argv[]) {
  folly::SingletonVault::singleton()->registrationComplete();
  facebook::logdevice::dbg::currentLevel =
      facebook::logdevice::dbg::Level::ERROR;

  parse_command_line(argc, argv);

  std::shared_ptr<facebook::logdevice::Client> client =
      facebook::logdevice::ClientFactory().create(
          command_line_options.config_path);

  if (!client) {
    std::cerr << "logdevice::ClientFactory().create() failed. Check the config "
                 "path.\n";
    exit(1);
  }

  logid_t log = parse_target_log(command_line_options.target_log, *client);
  if (log == facebook::logdevice::LOGID_INVALID) {
    exit(1);
  }

  facebook::logdevice::BufferedWriter::Options options;
  // Writes for this log are flushed once the oldest has been
  // buffered for 10 ms (default is 1000 ms).
  options.time_trigger = std::chrono::milliseconds(10);
  // Highest throughput, but can cause writes to get reordered.
  options.mode = BufferedWriter::Options::Mode::INDEPENDENT;
  // How many times to try to append a batch of writes.
  options.retry_count = 4;

  BufferedWriterCallback cb;
  std::unique_ptr<facebook::logdevice::BufferedWriter> buffered_writer;

  // Get an instance of BufferedWriter - usually, one per application.
  buffered_writer =
      facebook::logdevice::BufferedWriter::create(client, &cb, options);

  int buffered = 0;
  for (int record_idx = 0; record_idx < 15000; ++record_idx) {
    int rv = buffered_writer->append(
        log,
        std::string("payload " + std::to_string(record_idx)),
        /* context */ nullptr);
    if (rv) {
      // Insert error handling. For example, try again at least once.
      // Payload remains in std::string.
      std::cerr << "BufferedWriter->append() failed. "
                << facebook::logdevice::error_description(
                       facebook::logdevice::err)
                << '\n';
    } else {
      ++buffered;
    }
  }

  {
    std::unique_lock<std::mutex> lock(cb.mutex);
    cb.cv.wait(lock, [&] { return cb.appendsAckd == buffered; });
  }
  std::cout << "BufferedWriter has returned the status for all appends.\n";
  return 0;
}

void parse_command_line(int argc, const char** argv) {
  using boost::program_options::bool_switch;
  using boost::program_options::value;
  namespace style = boost::program_options::command_line_style;
  try {
    boost::program_options::options_description desc("Options");
    // clang-format off
    desc.add_options()

    ("help,h",
     "print help and exit")

    ("config,c",
     value<std::string>(&command_line_options.config_path)
       ->required(),
     "location of the cluster config. It can be a file, or more generally, "
     "[scheme:]<path-to-config>")

    ("log,l",
     value<std::string>(&command_line_options.target_log)
       ->required(),
     "log to append records to. It can be a numeric log ID, a log name from the "
     "config, or log group name with an offset (e.g., \"my_log_group[0]\")")

     ;
    // clang-format on
    boost::program_options::positional_options_description positional;
    positional.add("config", /* max_count */ 1);
    positional.add("log", /* max_count */ 1);

    boost::program_options::command_line_parser parser(argc, argv);
    boost::program_options::variables_map parsed;
    boost::program_options::store(
        parser.options(desc)
            .positional(positional)
            .style(style::unix_style & ~style::allow_guessing)
            .run(),
        parsed);
    if (parsed.count("help")) {
      std::cout << USAGE << '\n' << desc;
      exit(0);
    }
    boost::program_options::notify(parsed);
  } catch (const boost::program_options::error& ex) {
    std::cerr << argv[0] << ": " << ex.what() << '\n';
    exit(1);
  }
}
