/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <string>

#include <boost/program_options.hpp>
#include <folly/Singleton.h>

#include "logdevice/examples/parse_target_log.h"
#include "logdevice/examples/tail_flags.h"
#include "logdevice/include/Client.h"
#include "logdevice/include/debug.h"
#include "logdevice/include/types.h"

/**
 * @file Simple tool for reading from LogDevice, demonstrating usage of the
 * read API.
 */

using facebook::logdevice::logid_t;

int main(int argc, const char* argv[]) {
  folly::SingletonVault::singleton()->registrationComplete();
  facebook::logdevice::dbg::currentLevel =
      facebook::logdevice::dbg::Level::ERROR;

  Config command_line_options;
  parse_command_line(argc, argv, command_line_options);

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

  facebook::logdevice::lsn_t start_lsn, until_lsn;

  // Use the findTime() API to figure out where to read from
  using namespace std::chrono;
  const auto start_time = system_clock::now() - command_line_options.time;
  facebook::logdevice::Status status;
  start_lsn = client->findTimeSync(
      log, duration_cast<milliseconds>(start_time.time_since_epoch()), &status);
  if (status != facebook::logdevice::E::OK) {
    fprintf(stderr, "error: could not query start LSN\n");
    exit(1);
  }

  // Find how far to read, depending on whether --follow was passed on the
  // command line
  if (command_line_options.follow) {
    until_lsn = facebook::logdevice::LSN_MAX;
  } else {
    // Query the LSN of the last written record; that's how far we will read.
    until_lsn = client->getTailLSNSync(log);
    if (until_lsn == facebook::logdevice::LSN_INVALID) {
      fprintf(stderr, "error: could not query tail LSN\n");
      exit(1);
    }
  }

  if (start_lsn > until_lsn) {
    // Nothing to read.
    return 0;
  }

  std::unique_ptr<facebook::logdevice::Reader> reader = client->createReader(1);
  int rv __attribute__((__unused__)) =
      reader->startReading(log, start_lsn, until_lsn);
  assert(rv == 0);
  // In follow mode, ask Reader to give us records as they come in instead of
  // waiting for full batches to form.
  if (command_line_options.follow) {
    reader->waitOnlyWhenNoData();
  }

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
