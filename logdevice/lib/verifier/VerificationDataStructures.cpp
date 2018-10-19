/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

/**
 * Extracts verification data.
 * Note: obviously, the verification data parameters are only guaranteed to be
 * extracted and non-null if the payload string is actually using the
 * verification framework as marked by the presence of a verification flag.
 * Otherwise, no verification data parameters are filled in.
 */

#include "logdevice/lib/verifier/VerificationDataStructures.h"

#include "logdevice/common/protocol/ProtocolReader.h"
#include "logdevice/common/protocol/ProtocolWriter.h"
#include "logdevice/common/toString.h"

namespace facebook { namespace logdevice {

size_t VerificationDataStructures::serializeVerificationData(
    const VerificationData& vd,
    std::string* buffer_ptr) {
  if (buffer_ptr != nullptr) {
    size_t buffer_size = vd.vdh.payload_pos;
    buffer_ptr->resize(buffer_size);

    ProtocolWriter w({const_cast<char*>(buffer_ptr->data()), buffer_size},
                     "SerializeVerificationData",
                     0);

    w.write(vd.vdh);
    w.writeVector(vd.ack_list);

    return buffer_size;
  } else {
    ProtocolWriter w({nullptr, 0}, "SerializeVerificationData_dry_run", 0);
    w.write(vd.vdh);
    w.writeVector(vd.ack_list);
    return w.result();
  }
}

VerExtractionResult
VerificationDataStructures::extractVerificationData(const Payload& p) {
  VerExtractionResult ver;
  ver.user_payload = Payload(p.data(), p.size());

  // Using verification if and only if the size >= 8 bytes and begins with the
  // magic number.
  if (p.size() < sizeof(vflag_)) {
    // Not using the verification framework.
    ver.ves = VerExtractionStatus::NO_VERIFICATION;
    return ver;
  }
  ProtocolReader r(Slice(p.data(), p.size()), "ExtractVerificationData", 0);
  r.read(&ver.vd.vdh.magic_number, sizeof(uint64_t));
  if (ver.vd.vdh.magic_number != vflag_) {
    // Not using the verification framework.
    ver.ves = VerExtractionStatus::NO_VERIFICATION;
    return ver;
  }

  if (p.size() < sizeof(VerificationDataHeader)) {
    // We are using the verification framework, yet the size is smaller than
    // the VerificationHeader. Hence, malformed verification data.
    ver.ves = VerExtractionStatus::MALFORMED_VER_DATA;
    return ver;
  }
  r.read(reinterpret_cast<char*>(&ver.vd.vdh) + sizeof(uint64_t),
         sizeof(VerificationDataHeader) - sizeof(uint64_t));

  // Check that ack_list_length is reasonable.
  if (ver.vd.vdh.ack_list_length * sizeof(std::pair<vsn_t, lsn_t>) >
      r.bytesRemaining()) {
    ver.ves = VerExtractionStatus::MALFORMED_VER_DATA;
    return ver;
  }
  r.readVector(&ver.vd.ack_list, ver.vd.vdh.ack_list_length);

  if (ver.vd.vdh.payload_pos != r.bytesRead() ||
      ver.vd.vdh.payload_size != p.size() - r.bytesRead()) {
    ver.ves = VerExtractionStatus::MALFORMED_VER_DATA;
    return ver;
  }
  // Define the new payload to point to the position at which the user's
  // payload string begins.
  ver.user_payload =
      Payload(static_cast<const char*>(p.data()) + ver.vd.vdh.payload_pos,
              ver.vd.vdh.payload_size);
  ver.ves = VerExtractionStatus::OK;
  return ver;
}

}} // namespace facebook::logdevice
