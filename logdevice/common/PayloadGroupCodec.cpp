/**
 * Copyright (c) 2020-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/PayloadGroupCodec.h"

#include <folly/Likely.h>
#include <folly/compression/Compression.h>
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
  // key, value and metadata in payloads map. Calculate it by serializing a
  // group with only one key added and subtract empty groups size.
  thrift::CompressedPayloadGroups payload_groups;
  thrift::CompressedPayloadsMetadata metadata;
  payload_groups.payloads_ref().ensure()[0] = {};
  return ThriftCodec::serialize<ThriftSerializer>(payload_groups).size() +
      ThriftCodec::serialize<ThriftSerializer>(metadata).size() -
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

/** Creates codec for compression/uncompression. */
std::unique_ptr<folly::io::Codec>
createCompressionCodec(Compression compression,
                       int level = folly::io::COMPRESSION_LEVEL_DEFAULT) {
  switch (compression) {
    case Compression::NONE:
      return folly::io::getCodec(folly::io::CodecType::NO_COMPRESSION);
    case Compression::LZ4:
      return folly::io::getCodec(
          folly::io::CodecType::LZ4, folly::io::COMPRESSION_LEVEL_DEFAULT);
    case Compression::LZ4_HC:
      return folly::io::getCodec(
          folly::io::CodecType::LZ4, folly::io::COMPRESSION_LEVEL_BEST);
    case Compression::ZSTD:
      return folly::io::getCodec(folly::io::CodecType::ZSTD, level);
  }
  return nullptr;
}

/**
 * Compresses payload in place and returns compression method used. It can
 * decide to skip compression if compression does not reduce size of payload.
 */
Compression compress(Compression compression,
                     int zstd_level,
                     folly::IOBuf& payload) {
  if (compression == Compression::NONE) {
    return compression;
  }
  auto codec = createCompressionCodec(compression, zstd_level);
  ld_check(codec);
  auto compressed = codec->compress(&payload);
  if (compressed->computeChainDataLength() < payload.computeChainDataLength()) {
    payload = std::move(*compressed);
    return compression;
  }
  return Compression::NONE;
}

folly::Optional<folly::IOBuf> uncompress(Compression compression,
                                         size_t uncompressed_size,
                                         const folly::IOBuf& payload) {
  auto codec = createCompressionCodec(compression);
  if (codec == nullptr) {
    return folly::none;
  }
  try {
    return *codec->uncompress(&payload, uncompressed_size);
  } catch (const std::exception& e) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    1,
                    "Failed to uncompress payload: %s",
                    e.what());
    return folly::none;
  }
}

} // namespace

PayloadGroupCodec::Encoder::Encoder(size_t expected_appends_count)
    : expected_appends_count_(expected_appends_count) {}

void PayloadGroupCodec::Encoder::append(folly::IOBuf&& payload) {
  PayloadGroup payload_group{{0, std::move(payload)}};
  append(payload_group);
}

void PayloadGroupCodec::Encoder::update(PayloadKey key,
                                        const folly::IOBuf* iobuf) {
  auto [it, inserted] = encoded_payload_groups_.try_emplace(key);
  auto& encoded_payloads = it->second;
  auto& descriptors = encoded_payloads.metadata.descriptors_ref().ensure();
  if (inserted) {
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
    encoded_payloads.payloads.prependChain(iobuf->clone());
  }
}

void PayloadGroupCodec::Encoder::append(const PayloadGroup& payload_group) {
  appends_count_++;

  // Add empty descriptor for each known key.
  for (auto& [key, encoded_payloads] : encoded_payload_groups_) {
    encoded_payloads.metadata.descriptors_ref()->emplace_back();
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
    if (encoded_payload_groups_.empty()) {
      // use temporary key
      key = tmp_empty_key;
      contains_only_empty_groups_ = true;
    } else {
      // reuse existing key (note: it can be from the previous empty payload)
      key = encoded_payload_groups_.begin()->first;
    }
    update(key, nullptr);
  } else {
    if (UNLIKELY(contains_only_empty_groups_)) {
      // update temporary key used to encode empty payloads
      const PayloadKey key = payload_group.begin()->first;
      if (key != tmp_empty_key) {
        auto node = encoded_payload_groups_.extract(tmp_empty_key);
        node.key() = key;
        encoded_payload_groups_.insert(std::move(node));
      }
      contains_only_empty_groups_ = false;
    }
    for (const auto& [key, iobuf] : payload_group) {
      update(key, &iobuf);
    }
  }
}

void PayloadGroupCodec::Encoder::encode(folly::IOBufQueue& out,
                                        Compression compression,
                                        int zstd_level) {
  thrift::CompressedPayloadGroups thrift;
  thrift.payloads_ref().ensure();
  for (auto& [key, encoded_payloads] : encoded_payload_groups_) {
    auto& compressed_payloads = thrift.payloads_ref()[key];

    // Serialize and compress metadata.
    folly::IOBufQueue queue;
    ThriftCodec::serialize<ThriftSerializer>(encoded_payloads.metadata, &queue);
    compressed_payloads.compressed_metadata_ref() = queue.moveAsValue();
    compressed_payloads.metadata_uncompressed_size_ref() =
        compressed_payloads.compressed_metadata_ref()->computeChainDataLength();
    compressed_payloads.metadata_compression_ref() = static_cast<int8_t>(
        compress(compression,
                 zstd_level,
                 *compressed_payloads.compressed_metadata_ref()));

    // Compress payloads.
    compressed_payloads.compressed_payloads_ref() =
        std::move(encoded_payloads.payloads);
    compressed_payloads.payloads_compression_ref() = static_cast<int8_t>(
        compress(compression,
                 zstd_level,
                 *compressed_payloads.compressed_payloads_ref()));
  }
  ThriftCodec::serialize<ThriftSerializer>(
      thrift,
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

void PayloadGroupCodec::encode(const PayloadGroup& payload_group,
                               folly::IOBufQueue& out) {
  Encoder encoder(1);
  encoder.append(payload_group);
  encoder.encode(out, Compression::NONE);
}

void PayloadGroupCodec::encode(const std::vector<PayloadGroup>& payload_groups,
                               folly::IOBufQueue& out) {
  Encoder encoder(payload_groups.size());
  for (const auto& payload_group : payload_groups) {
    encoder.append(payload_group);
  }
  encoder.encode(out, Compression::NONE);
}

size_t PayloadGroupCodec::decode(Slice binary,
                                 PayloadGroup& payload_group_out,
                                 bool allow_buffer_sharing) {
  std::vector<PayloadGroup> payload_groups;
  const size_t deserialized_size =
      decode(binary, payload_groups, allow_buffer_sharing);
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

namespace {
/**
 * Decodes and validates metadata.
 * Returns 0 if successful.
 */
FOLLY_NODISCARD
int decodeMetadata(PayloadKey key,
                   const thrift::CompressedPayloads& compressed_payloads,
                   thrift::CompressedPayloadsMetadata& metadata_out) {
  auto uncompressed_metadata = uncompress(
      static_cast<Compression>(*compressed_payloads.metadata_compression_ref()),
      *compressed_payloads.metadata_uncompressed_size_ref(),
      *compressed_payloads.compressed_metadata_ref());
  if (!uncompressed_metadata) {
    RATELIMIT_ERROR(
        std::chrono::seconds(1),
        1,
        "Failed to uncompress metadata for key %d with compression %d, "
        "uncompressed size %d",
        key,
        *compressed_payloads.metadata_compression_ref(),
        *compressed_payloads.metadata_uncompressed_size_ref());
    return -1;
  }
  const size_t deserialized_size = ThriftCodec::deserialize<ThriftSerializer>(
      &uncompressed_metadata.value(), metadata_out);
  if (deserialized_size != uncompressed_metadata->computeChainDataLength()) {
    RATELIMIT_ERROR(
        std::chrono::seconds(1),
        1,
        "Decoded metadata size mismatch for key %d: actual %zu, expected %d",
        key,
        deserialized_size,
        *compressed_payloads.metadata_uncompressed_size_ref());
    return -1;
  }

  // Descriptors must not contain negative values.
  for (const auto& opt_descriptor : *metadata_out.descriptors_ref()) {
    if (auto descriptor = opt_descriptor.descriptor_ref()) {
      const auto uncompressed_size = *descriptor->uncompressed_size_ref();
      if (uncompressed_size < 0) {
        RATELIMIT_ERROR(std::chrono::seconds(1),
                        1,
                        "Negative payload size (%d) for key %d",
                        uncompressed_size,
                        key);
        return -1;
      }
    }
  }
  return 0;
}
} // namespace

size_t PayloadGroupCodec::decode(Slice binary,
                                 std::vector<PayloadGroup>& payload_groups_out,
                                 bool allow_buffer_sharing) {
  thrift::CompressedPayloadGroups compressed_payload_groups;
  const size_t deserialized_size = ThriftCodec::deserialize<ThriftSerializer>(
      binary,
      compressed_payload_groups,
      apache::thrift::ExternalBufferSharing::SHARE_EXTERNAL_BUFFER);
  if (deserialized_size == 0) {
    err = E::BADMSG;
    return 0;
  }

  folly::Optional<size_t> batch_size;
  std::vector<PayloadGroup> decoded_groups;
  for (auto& [key, compressed_payloads] :
       *compressed_payload_groups.payloads_ref()) {
    thrift::CompressedPayloadsMetadata metadata;
    int rv = decodeMetadata(key, compressed_payloads, metadata);
    if (rv != 0) {
      err = E::BADMSG;
      return 0;
    }

    size_t payloads_uncompressed_size = 0;
    for (const auto& opt_descriptor : *metadata.descriptors_ref()) {
      if (auto descriptor = opt_descriptor.descriptor_ref()) {
        payloads_uncompressed_size += *descriptor->uncompressed_size_ref();
      }
    }
    auto uncompressed_payloads =
        uncompress(static_cast<Compression>(
                       *compressed_payloads.payloads_compression_ref()),
                   payloads_uncompressed_size,
                   *compressed_payloads.compressed_payloads_ref());
    if (!uncompressed_payloads) {
      err = E::BADMSG;
      return 0;
    }
    if (!allow_buffer_sharing) {
      // Payloads can share buffer with binary, if no compression was used,
      // so make a copy if needed.
      uncompressed_payloads->makeManaged();
    }

    // All descriptors must have same size. Preallocate decoded_groups when
    // first group is decoded and verify equality for the remaining groups.
    if (!batch_size) {
      batch_size = metadata.descriptors_ref()->size();
      decoded_groups.resize(*batch_size);
    } else {
      if (metadata.descriptors_ref()->size() != *batch_size) {
        RATELIMIT_ERROR(std::chrono::seconds(1),
                        1,
                        "Descriptors sizes mismatch for key %d: %zu vs %zu",
                        key,
                        *batch_size,
                        metadata.descriptors_ref()->size());
        err = E::BADMSG;
        return 0;
      }
    }

    // Split uncompressed_payloads into pieces according to the
    // uncompressed_size specified in descriptors.
    size_t index = 0;
    folly::io::Cursor cursor{&uncompressed_payloads.value()};
    for (const auto& opt_descriptor : *metadata.descriptors_ref()) {
      if (auto descriptor = opt_descriptor.descriptor_ref()) {
        const auto uncompressed_size = *descriptor->uncompressed_size_ref();
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
          return 0;
        }
        decoded_groups[index][key] = std::move(iobuf);
      }
      ++index;
    }
  }

  payload_groups_out.insert(payload_groups_out.end(),
                            std::make_move_iterator(decoded_groups.begin()),
                            std::make_move_iterator(decoded_groups.end()));
  return deserialized_size;
}

namespace {
std::vector<folly::Optional<PayloadDescriptor>>
convert(const std::vector<thrift::OptionalPayloadDescriptor>& thrift) {
  std::vector<folly::Optional<PayloadDescriptor>> result;
  result.reserve(thrift.size());
  for (const auto& thrift_opt_descriptor : thrift) {
    auto& descriptor = result.emplace_back();
    if (const auto thrift_descriptor = thrift_opt_descriptor.descriptor_ref()) {
      descriptor.emplace().uncompressed_size =
          *thrift_descriptor->uncompressed_size_ref();
    }
  }
  return result;
}
} // namespace

size_t PayloadGroupCodec::decode(
    const folly::IOBuf& binary,
    CompressedPayloadGroups& compressed_payload_groups_out,
    bool allow_buffer_sharing) {
  thrift::CompressedPayloadGroups compressed_payload_groups;
  const size_t deserialized_size = ThriftCodec::deserialize<ThriftSerializer>(
      &binary,
      compressed_payload_groups,
      apache::thrift::ExternalBufferSharing::SHARE_EXTERNAL_BUFFER);
  if (deserialized_size == 0) {
    err = E::BADMSG;
    return 0;
  }

  folly::Optional<size_t> batch_size;
  CompressedPayloadGroups result;
  for (auto& [key, compressed_payloads] :
       *compressed_payload_groups.payloads_ref()) {
    thrift::CompressedPayloadsMetadata metadata;
    int rv = decodeMetadata(key, compressed_payloads, metadata);
    if (rv != 0) {
      err = E::BADMSG;
      return 0;
    }
    if (!batch_size) {
      batch_size = metadata.descriptors_ref()->size();
    } else {
      if (metadata.descriptors_ref()->size() != *batch_size) {
        RATELIMIT_ERROR(std::chrono::seconds(1),
                        1,
                        "Descriptors sizes mismatch for key %d: %zu vs %zu",
                        key,
                        *batch_size,
                        metadata.descriptors_ref()->size());
        err = E::BADMSG;
        return 0;
      }
    }
    auto& result_payloads = result[key];
    result_payloads.payload = *compressed_payloads.compressed_payloads_ref();
    result_payloads.compression = static_cast<Compression>(
        *compressed_payloads.payloads_compression_ref());
    result_payloads.descriptors = convert(*metadata.descriptors_ref());
  }

  compressed_payload_groups_out = result;
  return deserialized_size;
}

}} // namespace facebook::logdevice
