/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/EpochMetaData.h"
#include "logdevice/common/FailureDomainNodeSet.h"
#include "logdevice/common/MetaDataLogReader.h"
#include "logdevice/common/Request.h"
#include "logdevice/common/ShardAuthoritativeStatusMap.h"
#include "logdevice/common/Worker.h"

#include "logdevice/include/types.h"
#include "logdevice/include/Err.h"

#include "SafetyAPI.h"

namespace facebook { namespace logdevice {

class Processor;

class SafetyChecker {
 public:
  /**
   * @param processor                            Logdevice processor
   * (client or server) to use;
   * @param logs_in_flight                      How many logs to check in
   *                                            parallel;
   * @param abort_on_error                      If some errors are found,
   *                                            abort as soon as possible
   * @param timeout                             How long before we give up. In
   *                                            this case `err` will be set to
   *                                            `E::TIMEDOUT`
   * @param error_sample_size                   How many samples of errors to
   *                                            collect and describe in the
   *                                            human readable error returned
   * @param read_epoch_metadata_from_sequencer  Assume the server is recent
   *                                            enough to be able to send
   *                                            EpochMetaData through the
   *                                            sequencer.
   */
  SafetyChecker(Processor* processor,
                size_t logs_in_flight,
                bool abort_on_error = true,
                std::chrono::milliseconds timeout = std::chrono::minutes(2),
                size_t error_sample_size = 10,
                bool read_epoch_metadata_from_sequencer = false);

  ~SafetyChecker() {}

  /*
   * Find out what would be impact on 'logids_to_check'
   * (if 'logids_to_check' is empty then on all logs in the cluster)
   * if 'operations' are applied on specified shards
   */
  Impact checkImpact(const ShardAuthoritativeStatusMap& status_map,
                     const ShardSet& shards,
                     int operations,
                     SafetyMargin safety_margin = SafetyMargin(),
                     std::vector<logid_t> logids_to_check = {});

 private:
  Processor* processor_;
  std::chrono::milliseconds timeout_;
  size_t logs_in_flight_;
  bool abort_on_error_;
  size_t error_sample_size_;
  // TODO(T28386689): remove once all production tiers are on 2.35.
  bool read_epoch_metadata_from_sequencer_;
};

}} // namespace facebook::logdevice
