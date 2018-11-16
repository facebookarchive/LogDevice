/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <semaphore.h>
#include <string>

#include <boost/program_options.hpp>
#include <folly/Singleton.h>

#include "logdevice/examples/parse_target_log.h"
#include "logdevice/include/Client.h"
#include "logdevice/include/debug.h"
#include "logdevice/include/types.h"

/**
 * @file Simple tool for writing to LogDevice, demonstrating usage of the
 * append API.
 */

static const char* USAGE =
    R"DOC(Usage: write [options...] CONFIG LOG

Write records to LogDevice.  By default, store each line from stdin as a
separate record.  This is not binary-safe (the newline character cannot be
written); the --whole-stdin option can be used for binary-safe writing.

The CONFIG argument points the tool to the cluster.  The LOG argument says
which log to write to.  See below for formats.

)DOC";

static constexpr size_t BUFSIZE = 8192;
static constexpr unsigned MAX_INFLIGHT_APPENDS = 100;
struct {
  std::string config_path;
  std::string target_log;
  bool whole_stdin = false;
} command_line_options;

using facebook::logdevice::logid_t;

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
    fprintf(stderr,
            "logdevice::ClientFactory::create() failed.  Is the config path "
            "correct?\n");
    exit(1);
  }

  logid_t log = parse_target_log(command_line_options.target_log, *client);
  if (log == facebook::logdevice::LOGID_INVALID) {
    exit(1);
  }

  // We'll use the nonblocking append() API for better throughput, but with
  // limited concurrency to avoid overloading the sequencer
  sem_t sem;
  sem_init(&sem, 0, MAX_INFLIGHT_APPENDS);

  std::atomic<int> inflight(0), failures(0), critical(0);
  std::string payload_builder;
  facebook::logdevice::append_callback_t append_cb =
      [&](facebook::logdevice::Status st,
          const facebook::logdevice::DataRecord&) {
        if (st != facebook::logdevice::E::OK) {
          fprintf(stderr,
                  "error: append failed: %s\n",
                  facebook::logdevice::error_description(st));
          ++failures;
          if (st == facebook::logdevice::E::NOTFOUND) {
            ++critical;
          }
        }
        --inflight;
        sem_post(&sem);
      };

  // This will get called every time we encounter the newline delimiter and/or
  // at EOF
  auto flush = [&] {
    // If we encountered a critical failure (e.g. log ID does not exist), do
    // not send out new appends
    if (critical.load() > 0) {
      return;
    }

    // Wait for an inflight slot to free up
    sem_wait(&sem);

    int rv = client->append(log, std::move(payload_builder), append_cb);
    if (rv == 0) {
      ++inflight;
    } else {
      fprintf(stderr,
              "error: failed to post append: %s\n",
              facebook::logdevice::error_description(facebook::logdevice::err));
      ++failures;
    }
    payload_builder.clear();
  };

  // Read stdin BUFSIZE chars at a time
  static char buf[BUFSIZE];
  while (!feof(stdin) && critical.load() == 0) {
    size_t nread = fread(buf, 1, sizeof buf, stdin);
    if (command_line_options.whole_stdin) {
      payload_builder.append(buf, buf + nread);
    } else {
      for (size_t i = 0; i < nread; ++i) {
        if (buf[i] == '\n') {
          flush();
        } else {
          payload_builder += buf[i];
        }
      }
    }
  }
  if (command_line_options.whole_stdin || !payload_builder.empty()) {
    flush();
  }
  // Wait for the inflight appends to come back
  while (inflight.load() > 0) {
    sem_wait(&sem);
  }
  return failures.load() == 0 ? 0 : 1;
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
     "location of the cluster config to use; can be a file or more generally "
     "[scheme:]<path-to-config>")

    ("log,l",
     value<std::string>(&command_line_options.target_log)
       ->required(),
     "log to append records to; can be a numeric log ID, a log name from the "
     "config, or log group name with an offset (e.g. \"my_log_group[0]\")")

    ("whole-stdin",
     bool_switch(&command_line_options.whole_stdin),
     "instead of one payload per stdin line, write all of stdin as one payload")

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
      std::cout << USAGE << "\n" << desc;
      exit(0);
    }
    boost::program_options::notify(parsed);
  } catch (const boost::program_options::error& ex) {
    std::cerr << argv[0] << ": " << ex.what() << '\n';
    exit(1);
  }
}
