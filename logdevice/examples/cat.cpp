/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <string>

#include <boost/program_options.hpp>
#include <folly/Singleton.h>

#include "logdevice/examples/parse_target_log.h"
#include "logdevice/include/Client.h"
#include "logdevice/include/debug.h"
#include "logdevice/include/types.h"

/**
 * @file Simple tool for reading from LogDevice, demonstrating usage of the
 * read API.
 */

static const char* USAGE =
    R"DOC(Usage: cat [options...] CONFIG LOG

Read a LogDevice log.  Records are printed to stdout, one per line.

The CONFIG argument points the tool to the cluster.  The LOG argument says
which log to write to.  See below for formats.

)DOC";

struct {
  std::string config_path;
  std::string target_log;
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

  // Query the LSN of the last written record; that's how far we will read.
  const facebook::logdevice::lsn_t until_lsn = client->getTailLSNSync(log);
  if (until_lsn == facebook::logdevice::LSN_INVALID) {
    fprintf(stderr, "error: could not query tail LSN\n");
    exit(1);
  }

  if (facebook::logdevice::LSN_OLDEST > until_lsn) {
    // Nothing to read.
    return 0;
  }

  std::unique_ptr<facebook::logdevice::Reader> reader = client->createReader(1);
  int rv __attribute__((__unused__)) =
      reader->startReading(log, facebook::logdevice::LSN_OLDEST, until_lsn);
  assert(rv == 0);

  int exit_code = 0;
  bool done = false;
  std::vector<std::unique_ptr<facebook::logdevice::DataRecord>> data;
  do {
    data.clear();
    facebook::logdevice::GapRecord gap;
    ssize_t nread = reader->read(100, &data, &gap);
    if (nread >= 0) {
      // Got some data, print to stdout
      for (auto& record_ptr : data) {
        const facebook::logdevice::Payload& payload = record_ptr->payload;
        ::fwrite(payload.data(), 1, payload.size(), stdout);
        ::putchar('\n');
        if (record_ptr->attrs.lsn == until_lsn) {
          done = true;
        }
      }
    } else {
      // A gap in the numbering sequence.  Warn about data loss but ignore
      // other types of gaps.
      if (gap.type == facebook::logdevice::GapType::DATALOSS) {
        fprintf(stderr,
                "warning: DATALOSS gaps for LSN range [%ld, %ld]\n",
                gap.lo,
                gap.hi);
        exit_code = 1;
      }
      if (gap.hi == until_lsn) {
        done = true;
      }
    }
  } while (!done);
  return exit_code;
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
