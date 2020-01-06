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
          event_log_record_version_t FormatVersion,
          size_t MinAllowedSize>
int FixedEventLogRecord<Header, Type, FormatVersion, MinAllowedSize>::
    fromPayload(event_log_record_version_t version,
                Payload payload,
                std::unique_ptr<EventLogRecord>& out) {
  const size_t header_size =
      sizeof(event_log_record_version_t) + sizeof(EventType);

  if (payload.size() < header_size + MinAllowedSize) {
    ld_error("Invalid record in event log: expecting at least %zu bytes "
             "after the header",
             MinAllowedSize);
    err = E::BADMSG;
    return -1;
  }

  if (payload.size() - header_size > sizeof(Header)) {
    RATELIMIT_INFO(
        std::chrono::seconds(10),
        10,
        "Unexpectedly long event log record of type %s: %lu > %lu. Truncating "
        "and proceeding with deserialization, assuming that the record was "
        "written by a future version of the code, and that the author of that "
        "code has thought forward compatibility through.",
        toString(Type).c_str(),
        payload.size() - header_size,
        sizeof(Header));
  }

  const uint8_t* ptr = reinterpret_cast<const uint8_t*>(payload.data());
  ptr += header_size;
  Header header{};
  memcpy(&header, ptr, std::min(sizeof(header), payload.size() - header_size));

  out = std::make_unique<
      FixedEventLogRecord<Header, Type, FormatVersion, MinAllowedSize>>(
      header, version);

  return 0;
}

template <class Header,
          EventType Type,
          event_log_record_version_t FormatVersion,
          size_t MinAllowedSize>
int FixedEventLogRecord<Header, Type, FormatVersion, MinAllowedSize>::toPayload(
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
          event_log_record_version_t FormatVersion,
          size_t MinAllowedSize>
bool FixedEventLogRecord<Header, Type, FormatVersion, MinAllowedSize>::
operator==(
    const FixedEventLogRecord<Header, Type, FormatVersion, MinAllowedSize>&
        other) const {
  static_assert(std::is_trivially_copyable<Header>::value,
                "Header must be trivially copyable");
  return memcmp(&header, &other.header, sizeof(Header)) == 0;
}

}} // namespace facebook::logdevice
