/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstdio>
#include <sstream>
#include <string>

#include <boost/program_options.hpp>
#include <folly/Singleton.h>

#include "logdevice/examples/parse_target_log.h"
#include "logdevice/include/Client.h"
#include "logdevice/include/debug.h"
#include "logdevice/include/types.h"

/**
 * @file Simple tool for trimming from LogDevice, demonstrating usage of the
 * trim API.
 */

static const char* USAGE =
    R"DOC(Usage: trim [options...] CONFIG LOG

Trim a LogDevice log.

The CONFIG argument points the tool to the cluster.  The LOG argument says
which log to write to.  See below for formats.

)DOC";

using facebook::logdevice::logid_range_t;
using facebook::logdevice::logid_t;
using facebook::logdevice::lsn_t;

namespace {
struct {
  std::string config_path;
  std::string target_log;
  lsn_t through_lsn{facebook::logdevice::LSN_INVALID};
} command_line_options;
} // namespace

static void parse_command_line(int argc, const char** argv);

void trim_log(logid_t log, facebook::logdevice::Client& client) {
  lsn_t last_lsn = client.getTailLSNSync(log);
  if (last_lsn == facebook::logdevice::LSN_INVALID) {
    fprintf(stderr, "error: could not query tail LSN\n");
    exit(1);
  }

  if (command_line_options.through_lsn != facebook::logdevice::LSN_INVALID &&
      command_line_options.through_lsn < last_lsn) {
    last_lsn = command_line_options.through_lsn;
  }

  printf("Trimming log %ld to LSN %ld\n", log.val(), last_lsn);
  int rv = client.trimSync(log, last_lsn);
  if (rv != 0) {
    fprintf(stderr,
            "error: failed to trim: %s\n",
            facebook::logdevice::error_description(facebook::logdevice::err));
    exit(1);
  }
}

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

  logid_range_t range =
      parse_target_log_range(command_line_options.target_log, *client);

  if (range.first == facebook::logdevice::LOGID_INVALID) {
    exit(1);
  } else {
    if (range.second > range.first &&
        command_line_options.through_lsn != facebook::logdevice::LSN_INVALID) {
      fprintf(stderr,
              "error: if through-lsn is specified, the log argument "
              "must specify a single log.\n");
      exit(1);
    }
    for (logid_t log = range.first; log <= range.second;
         log = (logid_t)(log.val() + 1)) {
      trim_log(log, *client);
    }
  }
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

    ("through-lsn,t",
     value<lsn_t>(&command_line_options.through_lsn),
     "If specified, then the log argument must identify a single log. The log "
     "will be trimmed up to and including this LSN. If omitted, then all "
     "all logs identified by the log argument will be trimmed through their "
     "respective tail LSNs.")

      ;
    // clang-format on

    boost::program_options::positional_options_description positional;
    positional.add("config", /* max_count */ 1);
    positional.add("log", /* max_count */ 1);
    positional.add("through-lsn", /* max_count */ 1);

    boost::program_options::command_line_parser parser(argc, argv);
    boost::program_options::variables_map parsed;
    boost::program_options::store(
        parser.options(desc)
            .positional(positional)
            .style(style::unix_style & ~style::allow_guessing)
            .run(),
        parsed);
    if (parsed.count("help")) {
      std::stringstream desc_string;
      desc_string << desc;
      printf("%s\n%s", USAGE, desc_string.str().c_str());
      exit(0);
    }
    boost::program_options::notify(parsed);
  } catch (const boost::program_options::error& ex) {
    fprintf(stderr, "%s: %s\n", argv[0], ex.what());
    exit(1);
  }
}
