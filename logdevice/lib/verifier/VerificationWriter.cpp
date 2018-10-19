/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/lib/verifier/VerificationWriter.h"

#include <algorithm>
#include <chrono>
#include <memory>
#include <unordered_map>

#include <boost/algorithm/string.hpp>
#include <folly/Memory.h>
#include <folly/Random.h>

#include "logdevice/include/Record.h"
#include "logdevice/lib/verifier/VerificationDataStructures.h"

namespace facebook { namespace logdevice {

VerificationWriter::VerificationWriter(
    std::unique_ptr<DataSourceWriter> mydatasource,
    std::unique_ptr<GenVerifyData> mygvd)
    : ds_(std::move(mydatasource)), gvd_(std::move(mygvd)) {}

int VerificationWriter::append(logid_t logid,
                               std::string payload,
                               append_callback_t cb,
                               AppendAttributes attrs) {
  VerificationData vd = gvd_->generateVerificationData(logid, payload);
  std::string new_payload_buffer;
  VerificationDataStructures::serializeVerificationData(
      vd, &new_payload_buffer);

  // Extract specific elements for capture.
  vsn_t appended_seq_num = vd.vdh.sequence_num;
  uint32_t payload_pos = vd.vdh.payload_pos;
  uint32_t payload_size = vd.vdh.payload_size;
  std::vector<std::pair<vsn_t, lsn_t>> curr_ack_list = std::move(vd.ack_list);

  auto mod_cb = [=, curr_ack_list = std::move(curr_ack_list)](
                    Status st, const DataRecord& r) {
    gvd_->appendUpdateOnCallback(
        logid, appended_seq_num, r.attrs.lsn, curr_ack_list);

    // Execute the customer's callback on a DataRecord with the Payload
    // field pointing to the actual string payload, not to the beginning of
    // the prepended verification data.
    const char* customer_payload_ptr =
        static_cast<const char*>(r.payload.data()) + payload_pos;
    cb(st,
       DataRecord(
           logid, Payload(customer_payload_ptr, payload_size), r.attrs.lsn));
  };

  // Insert the just-appended sequence number into the in-flight list.
  gvd_->log_states_[logid].in_flight.insert(appended_seq_num);
  new_payload_buffer += payload;
  return ds_->append(logid, std::move(new_payload_buffer), mod_cb, attrs);
}

}} // namespace facebook::logdevice
