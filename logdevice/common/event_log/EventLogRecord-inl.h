/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/Traits.h>

namespace facebook { namespace logdevice {

template <class Header,
          EventType Type,
          event_log_record_version_t FormatVersion>
int FixedEventLogRecord<Header, Type, FormatVersion>::fromPayload(
    event_log_record_version_t version,
    Payload payload,
    std::unique_ptr<EventLogRecord>& out) {
  const size_t header_size =
      sizeof(event_log_record_version_t) + sizeof(EventType);

  const size_t min_expected_size = sizeof(Header);
  if (payload.size() < header_size + min_expected_size) {
    ld_error("Invalid record in event log: expecting at least %zu bytes "
             "after the header",
             min_expected_size);
    err = E::BADMSG;
    return -1;
  }

  const uint8_t* ptr = reinterpret_cast<const uint8_t*>(payload.data());
  ptr += header_size;
  Header header;
  memcpy(&header, ptr, sizeof(header));

  out = std::make_unique<FixedEventLogRecord<Header, Type, FormatVersion>>(
      header, version);

  return 0;
}

template <class Header,
          EventType Type,
          event_log_record_version_t FormatVersion>
int FixedEventLogRecord<Header, Type, FormatVersion>::toPayload(
    void* payload,
    size_t size) const {
  const int header_size = writeHeader(payload, size);
  if (header_size < 0) {
    return -1;
  }

  const size_t actual_size = header_size + sizeof(Header);

  if (payload) {
    if (actual_size > size) {
      err = E::NOBUFS;
      return -1;
    }

    uint8_t* ptr = reinterpret_cast<uint8_t*>(payload);
    ptr += header_size;
    memcpy(ptr, &header, sizeof(Header));
  }

  return actual_size;
}

template <class Header,
          EventType Type,
          event_log_record_version_t FormatVersion>
bool FixedEventLogRecord<Header, Type, FormatVersion>::operator==(
    const FixedEventLogRecord<Header, Type, FormatVersion>& other) const {
  static_assert(std::is_trivially_copyable<Header>::value,
                "Header must be trivially copyable");
  return memcmp(&header, &other.header, sizeof(Header)) == 0;
}

}} // namespace facebook::logdevice
