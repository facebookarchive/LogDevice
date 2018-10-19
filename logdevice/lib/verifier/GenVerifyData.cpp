/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/lib/verifier/GenVerifyData.h"

#include <chrono>
#include <exception>
#include <stdlib.h>
#include <string.h>
#include <string>
#include <unordered_map>
#include <utility>

#include <boost/algorithm/string.hpp>
#include <folly/Memory.h>
#include <folly/hash/Hash.h>

#include "logdevice/common/protocol/ProtocolWriter.h"
#include "logdevice/include/Record.h"
#include "logdevice/lib/verifier/VerificationDataStructures.h"

namespace facebook { namespace logdevice {

GenVerifyData::GenVerifyData(uint64_t wid) : writer_id_(wid) {}

// Generates a VerificationData instance to be serialized.
VerificationData
GenVerifyData::generateVerificationData(logid_t log_id,
                                        const std::string& payload) {
  auto& log_state = log_states_[log_id];
  VerificationData vd;
  vd.vdh.magic_number = vflag_;
  vd.vdh.writer_id = writer_id_;
  vd.vdh.sequence_num = log_state.next_seq_num++;
  vd.vdh.oldest_in_flight = *(log_state.in_flight.begin());
  vd.vdh.payload_checksum = folly::hash::fnv32(payload);
  vd.vdh.payload_size = payload.size();
  vd.vdh.ack_list_length = log_state.to_ack_list.size();
  vd.ack_list.assign(
      log_state.to_ack_list.begin(), log_state.to_ack_list.end());

  // Dry run which will not take time or memory, just want to get the size.
  vd.vdh.payload_pos =
      VerificationDataStructures::serializeVerificationData(vd, nullptr);
  return vd;
}

void GenVerifyData::appendUpdateOnCallback(
    logid_t log_id,
    vsn_t appended_seq_num,
    lsn_t appended_lsn,
    const std::vector<std::pair<vsn_t, lsn_t>>& appended_ack_list) {
  // Append completed, so move the sequence number from in flight to the to-ack
  // list.
  WriterVerificationData& log_state = log_states_[log_id];

  std::set<vsn_t>::iterator it = log_state.in_flight.find(appended_seq_num);
  if (it == log_state.in_flight.end()) {
    ld_check(false);
  }
  log_state.in_flight.erase(it);
  log_state.to_ack_list.insert(std::make_pair(appended_seq_num, appended_lsn));

  // If append was successful, subtract the record's ack list from the to-ack
  // list for that log.
  if (appended_lsn != LSN_INVALID) {
    for (std::pair<vsn_t, lsn_t> p : appended_ack_list) {
      std::set<std::pair<vsn_t, lsn_t>>::iterator it =
          log_state.to_ack_list.find(p);
      if (it != log_state.to_ack_list.end()) {
        log_state.to_ack_list.erase(it);
      }
    }
  }
}

uint64_t GenVerifyData::getWriterID() {
  return writer_id_;
}
}} // namespace facebook::logdevice
