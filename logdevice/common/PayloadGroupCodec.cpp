/**
 * Copyright (c) 2020-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/PayloadGroupCodec.h"

#include <folly/Likely.h>
#include <folly/io/IOBufQueue.h>

#include "logdevice/common/ThriftCodec.h"
#include "thrift/lib/cpp2/protocol/Serializer.h"

namespace facebook { namespace logdevice {

namespace {

using ThriftSerializer = apache::thrift::BinarySerializer;

// Calculate some constants used by Estimator

const size_t empty_groups_encoded_bytes = [] {
  thrift::CompressedPayloadGroups payload_groups;
  return ThriftCodec::serialize<ThriftSerializer>(payload_groups).size();
}();

const size_t new_payloads_encoded_bytes = [] {
  // When new payload key is added, the cost of adding it includes storing the
  // key and value in payloads map. Calculate it by serializing a group with
  // only one key added and subtract empty groups size.
  thrift::CompressedPayloadGroups payload_groups;
  payload_groups.payloads_ref().ensure()[0] = {};
  return ThriftCodec::serialize<ThriftSerializer>(payload_groups).size() -
      empty_groups_encoded_bytes;
}();

const size_t payload_descriptor_encoded_bytes = [] {
  thrift::OptionalPayloadDescriptor payload_descriptor;
  payload_descriptor.descriptor_ref().emplace();
  return ThriftCodec::serialize<ThriftSerializer>(payload_descriptor).size();
}();

const size_t empty_payload_descriptor_encoded_bytes = [] {
  thrift::OptionalPayloadDescriptor payload_descriptor;
  return ThriftCodec::serialize<ThriftSerializer>(payload_descriptor).size();
}();

} // namespace

PayloadGroupCodec::Encoder::Encoder(size_t expected_appends_count)
    : expected_appends_count_(expected_appends_count) {
  encoded_payload_groups_.payloads_ref().ensure();
}

void PayloadGroupCodec::Encoder::append(folly::IOBuf&& payload) {
  PayloadGroup payload_group{{0, std::move(payload)}};
  append(payload_group);
}

void PayloadGroupCodec::Encoder::update(PayloadKey key,
                                        const folly::IOBuf* iobuf) {
  auto [it, inserted] =
      encoded_payload_groups_.payloads_ref()->try_emplace(key);
  auto& encoded_payloads = it->second;
  auto& descriptors = encoded_payloads.descriptors_ref().ensure();
  if (inserted) {
    encoded_payloads.compression_ref() = static_cast<int8_t>(Compression::NONE);
    encoded_payloads.payload_ref().ensure();
    descriptors.reserve(expected_appends_count_);
    descriptors.resize(appends_count_);
  }
  if (iobuf) {
    // For the existing keys, update should be called after reserving empty
    // descriptors for these keys. For the new keys, descriptors are allocated
    // in the code above.
    ld_check(!descriptors.empty());
    auto& desc = descriptors.back().descriptor_ref().emplace();
    desc.uncompressed_size_ref() = iobuf->computeChainDataLength();
    encoded_payloads.payload_ref()->prependChain(iobuf->clone());
  }
}

void PayloadGroupCodec::Encoder::append(const PayloadGroup& payload_group) {
  appends_count_++;

  // Add empty descriptor for each known key.
  for (auto& [key, encoded_payloads] :
       *encoded_payload_groups_.payloads_ref()) {
    encoded_payloads.descriptors_ref()->emplace_back();
  }

  // Now update payloads and descriptors for the appended group.
  constexpr PayloadKey tmp_empty_key = 0;
  if (UNLIKELY(payload_group.empty())) {
    // When encoding empty group, we need to create empty descriptors for it.
    // In case there are non-empty groups we can just reuse any of the existing
    // keys to do the update. However, if there are no keys to reuse, we need
    // to use a temporary key to encode payload and change this key once first
    // non-empty payload is appended.
    PayloadKey key;
    if (encoded_payload_groups_.payloads_ref()->empty()) {
      // use temporary key
      key = tmp_empty_key;
      contains_only_empty_groups_ = true;
    } else {
      // reuse existing key (note: it can be from the previous empty payload)
      key = encoded_payload_groups_.payloads_ref()->begin()->first;
    }
    update(key, nullptr);
  } else {
    if (UNLIKELY(contains_only_empty_groups_)) {
      // update temporary key used to encode empty payloads
      const PayloadKey key = payload_group.begin()->first;
      if (key != tmp_empty_key) {
        auto node =
            encoded_payload_groups_.payloads_ref()->extract(tmp_empty_key);
        node.key() = key;
        encoded_payload_groups_.payloads_ref()->insert(std::move(node));
      }
      contains_only_empty_groups_ = false;
    }
    for (const auto& [key, iobuf] : payload_group) {
      update(key, &iobuf);
    }
  }
}

void PayloadGroupCodec::Encoder::encode(folly::IOBufQueue& out) {
  ThriftCodec::serialize<ThriftSerializer>(
      encoded_payload_groups_,
      &out,
      apache::thrift::ExternalBufferSharing::SHARE_EXTERNAL_BUFFER);
}

PayloadGroupCodec::Estimator::Estimator()
    : encoded_bytes_(empty_groups_encoded_bytes) {}

void PayloadGroupCodec::Estimator::append(const folly::IOBuf& payload) {
  PayloadGroup payload_group{{0, payload}};
  append(payload_group);
}

void PayloadGroupCodec::Estimator::update(PayloadKey key,
                                          const folly::IOBuf* iobuf) {
  auto [it, inserted] = payload_keys_.insert(key);
  if (inserted) {
    // account for the newly added key
    encoded_bytes_ += new_payloads_encoded_bytes;
    // account for empty descriptors added for this key each of already appended
    // groups
    encoded_bytes_ += appends_count_ * empty_payload_descriptor_encoded_bytes;
  }
  if (iobuf) {
    // change empty descriptor to non-empty
    encoded_bytes_ += payload_descriptor_encoded_bytes -
        empty_payload_descriptor_encoded_bytes;
    // account for bytes in payload
    encoded_bytes_ += iobuf->computeChainDataLength();
  }
}

void PayloadGroupCodec::Estimator::append(const PayloadGroup& payload_group) {
  appends_count_++;

  // Add empty descriptor for each existing key.
  encoded_bytes_ +=
      payload_keys_.size() * empty_payload_descriptor_encoded_bytes;

  constexpr PayloadKey tmp_empty_key = 0;
  if (UNLIKELY(payload_group.empty())) {
    // When encoding empty group, we need to create empty descriptors for it.
    // In case there are non-empty groups we can just reuse any of the existing
    // keys to do the update. However, if there are no keys to reuse, we need
    // to use a temporary key to encode payload and change this key once first
    // non-empty payload is appended.
    PayloadKey key;
    if (payload_keys_.empty()) {
      // use temporary key
      key = tmp_empty_key;
      contains_only_empty_groups_ = true;
    } else {
      // reuse existing key (note: it can be from the previous empty payload)
      key = *payload_keys_.begin();
    }
    update(key, nullptr);
  } else {
    if (UNLIKELY(contains_only_empty_groups_)) {
      // update temporary key used to encode empty payloads
      // this should be the only key in payload_keys_
      const PayloadKey key = payload_group.begin()->first;
      if (key != tmp_empty_key) {
        payload_keys_ = {key};
      }
      contains_only_empty_groups_ = false;
    }
    for (const auto& [key, iobuf] : payload_group) {
      update(key, &iobuf);
    }
  }
}

namespace {

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

void PayloadGroupCodec::encode(const PayloadGroup& payload_group,
                               folly::IOBufQueue& out) {
  Encoder encoder(1);
  encoder.append(payload_group);
  encoder.encode(out);
}

void PayloadGroupCodec::encode(const std::vector<PayloadGroup>& payload_groups,
                               folly::IOBufQueue& out) {
  Encoder encoder(payload_groups.size());
  for (const auto& payload_group : payload_groups) {
    encoder.append(payload_group);
  }
  encoder.encode(out);
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
