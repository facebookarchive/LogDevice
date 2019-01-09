/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_set>
#include <utility>

#include <folly/Random.h>
#include <folly/SharedMutex.h>

#include "logdevice/common/toString.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/Client.h"
#include "logdevice/include/types.h"
#include "logdevice/lib/verifier/VerificationDataStructures.h"

namespace facebook { namespace logdevice {

class GenVerifyData {
 public:
  struct WriterVerificationData {
    vsn_t next_seq_num;
    // TODO: make this a map of sequence numbers to the ack lists of their
    // corresponding records.
    std::set<vsn_t> in_flight;
    // TODO: use struct
    std::set<std::pair<vsn_t, lsn_t>> to_ack_list;
  };

  GenVerifyData(uint64_t wid = folly::Random::rand64());

  VerificationData generateVerificationData(logid_t log_id,
                                            const std::string& payload);

  // Public because we need to frequently modify log_states_ anyway by
  // inserting into in_flight whenever an append is called.
  std::map<logid_t, WriterVerificationData> log_states_;

  uint64_t getWriterID();

  void appendUpdateOnCallback(
      logid_t log_id,
      vsn_t appended_seq_num,
      lsn_t appended_lsn,
      const std::vector<std::pair<vsn_t, lsn_t>>& appended_ack_list);

 private:
  uint64_t writer_id_;
};
}} // namespace facebook::logdevice
