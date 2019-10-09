/**
 * Copyright (c) 2019-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <chrono>
#include <cstdio>
#include <iostream>

#include <folly/Singleton.h>
#include <folly/synchronization/Baton.h>

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

static bool
recordCallback(std::unique_ptr<facebook::logdevice::DataRecord>& record);

static bool gapCallback(const facebook::logdevice::GapRecord& gap);

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

  std::unique_ptr<facebook::logdevice::AsyncReader> reader =
      client->createAsyncReader();

  folly::Baton<> stop_baton;
  reader->setRecordCallback(recordCallback);
  reader->setGapCallback(gapCallback);
  reader->setDoneCallback([&](logid_t log) {
    fprintf(stderr, "AsyncReader: Done reading log %lu", log.val_);
    stop_baton.post();
  });

  int rv = reader->startReading(log, start_lsn, until_lsn);
  ld_check(!rv);

  stop_baton.wait();
  return 0;
}

bool recordCallback(std::unique_ptr<facebook::logdevice::DataRecord>& record) {
  std::cout << "Received record for log " << record->logid.val() << ": LSN "
            << facebook::logdevice::lsn_to_string(record->attrs.lsn)
            << ", payload \"" << record->payload.toString() << "\""
            << std::endl;
  return true;
}

bool gapCallback(const facebook::logdevice::GapRecord& gap) {
  using facebook::logdevice::GapType;
  switch (gap.type) {
    case GapType::BRIDGE:
    case GapType::HOLE:
    case GapType::TRIM:
    case GapType::FILTERED_OUT:
      // benign gaps in LSN numbering sequence
      break;
    case GapType::DATALOSS:
      std::cout << "Error! Data has been lost for log " << gap.logid.val()
                << " from LSN " << facebook::logdevice::lsn_to_string(gap.lo)
                << " to LSN " << facebook::logdevice::lsn_to_string(gap.hi)
                << std::endl;
      break;
    case GapType::ACCESS:
      std::cout << "Error! Access denied to log " << gap.logid.val()
                << std::endl;
      break;
    case GapType::NOTINCONFIG:
      std::cout << "Error! Log " << gap.logid.val() << " not found!"
                << std::endl;
      break;
    default:
      std::cout << "Unrecognized gap of type "
                << facebook::logdevice::gapTypeToString(gap.type) << " for log "
                << gap.logid.val() << " from LSN "
                << facebook::logdevice::lsn_to_string(gap.lo) << " to LSN "
                << facebook::logdevice::lsn_to_string(gap.hi) << std::endl;
      break;
  }
  return true;
}
