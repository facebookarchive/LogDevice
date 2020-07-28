/**
 * Copyright (c) 2020-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/PayloadGroupCodec.h"

#include "logdevice/common/ThriftCodec.h"
#include "logdevice/common/if/gen-cpp2/payload_types.h"
#include "thrift/lib/cpp2/protocol/Serializer.h"

namespace facebook { namespace logdevice {

namespace {

using ThriftSerializer = apache::thrift::BinarySerializer;

/**
 * Converts single PayloadGroup to its uncompressed Thrift representation.
 */
thrift::CompressedPayloadGroups convert(const PayloadGroup& payload_group) {
  thrift::CompressedPayloadGroups compressed_payload_groups;
  auto& compressed_payloads_map =
      compressed_payload_groups.payloads_ref().emplace();
  if (payload_group.empty()) {
    // this a special case: there are no keys in payload, but one descriptor to
    // indicate that there's one payload group, so just add key 0 with one empty
    // descriptor
    thrift::CompressedPayloads& compressed_payloads =
        compressed_payloads_map[0];
    compressed_payloads.compression_ref() =
        static_cast<int8_t>(Compression::NONE);
    compressed_payloads.descriptors_ref().ensure().resize(1);
    return compressed_payload_groups;
  }

  for (const auto& [key, payload] : payload_group) {
    thrift::CompressedPayloads& compressed_payloads =
        compressed_payloads_map[key];

    auto& descriptors = compressed_payloads.descriptors_ref().ensure();
    descriptors.reserve(1);
    thrift::PayloadDescriptor& payload_descriptor =
        descriptors.emplace_back().descriptor_ref().emplace();
    payload_descriptor.uncompressed_size_ref() =
        payload.computeChainDataLength();

    compressed_payloads.compression_ref() =
        static_cast<int8_t>(Compression::NONE);
    compressed_payloads.payload_ref() = payload;
  }
  return compressed_payload_groups;
}

/**
 * Converts uncompressed Thrift representation to a vector of PayloadGroups.
 * Returns 0 if successful.
 */
FOLLY_NODISCARD
int convert(thrift::CompressedPayloadGroups&& compressed_payload_groups,
            std::vector<PayloadGroup>& payload_groups_out) {
  auto& group_payloads = *compressed_payload_groups.payloads_ref();
  if (group_payloads.empty()) {
    // nothing to convert
    return 0;
  }

  // all descriptors should have same size, so use first one to reserve capacity
  const size_t size = group_payloads.cbegin()->second.descriptors_ref()->size();

  std::vector<PayloadGroup> decoded_groups{size};
  for (auto& [key, compressed_payloads] : group_payloads) {
    // this function must not be called for compresssed payloads
    ld_check(*compressed_payloads.compression_ref() ==
             static_cast<int>(Compression::NONE));

    if (compressed_payloads.descriptors_ref()->size() != size) {
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      1,
                      "Descriptors size %zu of key %d doesn't match "
                      "descriptors size %zu of key %d",
                      size,
                      group_payloads.cbegin()->first,
                      compressed_payloads.descriptors_ref()->size(),
                      key);
      err = E::BADMSG;
      return -1;
    }

    // split compressed_payload.payload into pieces according to the
    // uncompressed_size specified in descriptors
    size_t index = 0;
    folly::io::Cursor cursor{&compressed_payloads.payload_ref().value()};
    for (const auto& opt_descriptor : *compressed_payloads.descriptors_ref()) {
      if (auto descriptor = opt_descriptor.descriptor_ref()) {
        const auto uncompressed_size = *descriptor->uncompressed_size_ref();
        if (uncompressed_size < 0) {
          RATELIMIT_ERROR(std::chrono::seconds(1),
                          1,
                          "Negative payload size (%d) for key %d",
                          uncompressed_size,
                          key);
          err = E::BADMSG;
          return -1;
        }
        folly::IOBuf iobuf;
        if (cursor.cloneAtMost(iobuf, uncompressed_size) != uncompressed_size) {
          RATELIMIT_ERROR(std::chrono::seconds(1),
                          1,
                          "Underflow while splitting payload for key %d: "
                          "%d bytes expected, but only %lu bytes remaining",
                          key,
                          uncompressed_size,
                          iobuf.computeChainDataLength());
          err = E::BADMSG;
          return -1;
        }
        decoded_groups[index][key] = std::move(iobuf);
      }
      ++index;
    }
    if (!cursor.isAtEnd()) {
      // all payload should be consumed at this point
      // if more data is remaining then something is wrong with descriptors of
      // payload
      RATELIMIT_ERROR(
          std::chrono::seconds(1),
          1,
          "Extra data in payload for key %d: consumed %zu, total %zu",
          key,
          cursor.getCurrentPosition(),
          compressed_payloads.payload_ref()->computeChainDataLength());
      err = E::BADMSG;
      return -1;
    }
  }

  // move decoded groups to output
  payload_groups_out.reserve(payload_groups_out.size() + decoded_groups.size());
  for (auto& decoded_group : decoded_groups) {
    payload_groups_out.emplace_back(std::move(decoded_group));
  }

  return 0;
}

/**
 * Uncompresses all payloads in place.
 */
FOLLY_NODISCARD
int uncompress(thrift::CompressedPayloadGroups& compressed_payload_groups) {
  for (auto& [key, compressed_payloads] :
       *compressed_payload_groups.payloads_ref()) {
    // TODO compression/uncompression is not currently supported
    if (compressed_payloads.compression_ref() !=
        static_cast<int8_t>(Compression::NONE)) {
      err = E::BADMSG;
      return -1;
    }
  }
  return 0;
}

} // namespace

std::string PayloadGroupCodec::encode(const PayloadGroup& payload_group) {
  thrift::CompressedPayloadGroups compressed_payload_groups =
      convert(payload_group);
  // TODO replace with serialization to IOBuf
  return ThriftCodec::serialize<ThriftSerializer>(compressed_payload_groups);
}

size_t PayloadGroupCodec::decode(Slice binary,
                                 PayloadGroup& payload_group_out) {
  std::vector<PayloadGroup> payload_groups;
  const size_t deserialized_size = decode(binary, payload_groups);
  if (deserialized_size == 0) {
    return 0;
  }
  if (payload_groups.size() != 1) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    1,
                    "Expected one payload group, got %lu",
                    payload_groups.size());

    err = E::BADMSG;
    return 0;
  }
  payload_group_out = std::move(payload_groups[0]);
  return deserialized_size;
}

size_t
PayloadGroupCodec::decode(Slice binary,
                          std::vector<PayloadGroup>& payload_groups_out) {
  thrift::CompressedPayloadGroups compressed_payload_groups;
  const size_t deserialized_size = ThriftCodec::deserialize<ThriftSerializer>(
      binary, compressed_payload_groups);
  if (deserialized_size == 0) {
    return 0;
  }
  int rv = uncompress(compressed_payload_groups);
  if (rv != 0) {
    return 0;
  }
  rv = convert(std::move(compressed_payload_groups), payload_groups_out);
  if (rv != 0) {
    return 0;
  }
  return deserialized_size;
}

}} // namespace facebook::logdevice
