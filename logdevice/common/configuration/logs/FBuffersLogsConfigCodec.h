/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>
#include <memory>
#include <zstd.h>

#include <flatbuffers/flatbuffers.h>
#include <folly/Memory.h>

#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/logs/LogsConfigCodec.h"
#include "logdevice/common/configuration/logs/LogsConfigDeltaTypes.h"
#include "logdevice/common/configuration/logs/LogsConfigStructures_generated.h"
#include "logdevice/common/configuration/logs/LogsConfigTree.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/include/LogAttributes.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice { namespace logsconfig {
/**
 * An actual serializer that is built for CodecType::FLATBUFFERS
 * the internal methods for this codec are prefixed with fb_ which stands for
 * flatbuffers
 */
template <>
class LogsConfigCodec<CodecType::FLATBUFFERS> {
 public:
  /*
   * This version is written in the serialized payloads to allow for
   * forward/backward compatibility
   */
  // NOTE update this number due to adding shadow attributes?
  constexpr static logsconfig_codec_version_t CODEC_VERSION = 1;

  template <typename In>
  class TypeMapping {};

  /**
   * takes an object and serialize it to payload
   * the codec_type picks the correct encoding algorithm at compile-time
   *
   * This may fail if the delta log is corrupted or we have very old payloads
   * that were added to the log before a (protocol-breaking) change was done to
   * logdeviced's code. In general this should not happen but *IF* it happens,
   * we will increase the stats counter for
   * `logsconfig_manager_serialization_errors`
   * @param flatten will make sure that we don't skip inherited attributes in
   *                LogAttributes.
   */
  template <typename In>
  static facebook::logdevice::PayloadHolder serialize(const In& in,
                                                      bool flatten) {
    flatbuffers::FlatBufferBuilder builder;
    auto buffer = fbuffers_serialize<const In&, typename TypeMapping<In>::to>(
        builder, in, flatten);
    builder.Finish(buffer);
    size_t original_size = builder.GetSize();
    // flatbuffers::unique_ptr_t fb_ptr = builder.ReleaseBufferPointer();
    // We are copying the buffer out of flatbuffers to hold complete control
    // overy the part the we need in the potentially bigger buffer allocated by
    // flatbuffers during building. This also allow us to pass PayloadHolder
    // instead of flatbuffers::unique_ptr_t around.
    void* source = builder.GetBufferPointer();
    /*
     * The decision on whether we should compress this payload or not is
     * defined per-type in `flatbuffers_type_mapping.inc`. This is designed so
     * that we can change this easily in the future, we can enable/disable
     * compression for specific types while maintaining backward compatibility
     * since the deserialization process will read (isCompressed) from the
     * buffer to decide whether it needs to decompress or not.
     */
    bool shouldCompress = TypeMapping<In>::shouldBeCompressed;
    const size_t compressed_data_bound =
        shouldCompress ? ZSTD_compressBound(original_size) : original_size;
    // The output buffer layout is like this
    // (CODEC_VERSION)(isCompressed)(buffer)
    size_t header_size =
        sizeof(logsconfig_codec_version_t) /* 1 byte for CODEC_VERSION */
        + sizeof(uint8_t);                 /* 1 byte for is_compressed */
    size_t out_size = header_size + compressed_data_bound /* buffer */;

    // The output buffer
    uint8_t* out = (uint8_t*)malloc(out_size);
    uint8_t* const beginning = out;
    uint8_t* const end = out + out_size;

    *out++ = CODEC_VERSION;
    *out++ = (uint8_t)shouldCompress;

    size_t compressed_size = original_size;
    if (shouldCompress) {
      /*
       * We do compression with zstd, we pick ZSTD_LEVEL=5 to offer good
       * compression with low memory overhead. In the future we can experiment
       * with different levels if needed.
       */
      const int ZSTD_LEVEL = 5;
      compressed_size = ZSTD_compress(out,           // dst
                                      end - out,     // dstCapacity
                                      source,        // src
                                      original_size, // srcSize
                                      ZSTD_LEVEL);   // level

      if (ZSTD_isError(compressed_size)) {
        ld_error(
            "ZSTD_compress() failed: %s", ZSTD_getErrorName(compressed_size));
        ld_check(false);
        free(beginning);
        return PayloadHolder(); // contains nullptr
      }
      ld_spew("original size is %zu, compressed size %zu",
              original_size,
              compressed_size);
    } else {
      // In case of no compression, we just copy the buffer from original
      memcpy(out, source, original_size);
    }
    // recalculating the actual out size after we did the encoding and
    // the compression.
    out_size = header_size + compressed_size;
    return PayloadHolder(beginning, out_size, true /* ignore_size_limit */);
  }

  /**
   * Deserialize a Payload (without owning its memory life-cycle) into an object
   */
  template <typename Out>
  static std::unique_ptr<Out>
  deserialize(const facebook::logdevice::Payload& payload,
              const std::string& delimiter) {
    const uint8_t* ptr = static_cast<const uint8_t*>(payload.data());
    const uint8_t* end = ptr + payload.size();
    if (payload.size() == 0 || ptr + 1 >= end) {
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      1,
                      "Invalid payload size, cannot deserialize!");
      ld_check(false);
      STAT_INCR(Worker::stats(), logsconfig_manager_serialization_errors);
      return nullptr;
    }
    logsconfig_codec_version_t codec_version =
        *ptr++; // Read the first uint8_t of the buffer to know the version

    if (codec_version != CODEC_VERSION) {
      ld_critical("Unknown Codec Version (%u) in the serialized payload",
                  codec_version);
      return nullptr;
    }

    bool shouldDecompress = (*ptr++ == 1) ? true : false;
    std::unique_ptr<uint8_t[]> buf;
    if (shouldDecompress) {
      // Try to Decompress
      size_t uncompressed_size = ZSTD_getDecompressedSize(ptr, end - ptr);
      if (uncompressed_size == 0) {
        RATELIMIT_ERROR(
            std::chrono::seconds(1), 1, "ZSTD_getDecompressedSize() failed!");
        ld_check(false);
        STAT_INCR(Worker::stats(), logsconfig_manager_serialization_errors);
        return nullptr;
      }
      buf = std::make_unique<uint8_t[]>(uncompressed_size);
      size_t rv = ZSTD_decompress(buf.get(),         // dst
                                  uncompressed_size, // dstCapacity
                                  ptr,               // src
                                  end - ptr);        // compressedSize
      if (ZSTD_isError(rv)) {
        RATELIMIT_ERROR(std::chrono::seconds(1),
                        1,
                        "ZSTD_decompress() failed: %s",
                        ZSTD_getErrorName(rv));
        ld_check(false);
        STAT_INCR(Worker::stats(), logsconfig_manager_serialization_errors);
        return nullptr;
      }
      if (rv != uncompressed_size) {
        RATELIMIT_ERROR(std::chrono::seconds(1),
                        1,
                        "Zstd decompression length %zu does not match %lu found"
                        "in header",
                        rv,
                        uncompressed_size);
        ld_check(false);
        STAT_INCR(Worker::stats(), logsconfig_manager_serialization_errors);
        return nullptr;
      }
      // Use the decompressed buffer.
      ptr = buf.get();
      end = ptr + uncompressed_size;
    }
    // verify that this a valid buffer for this type.
    auto verification_start_time = std::chrono::high_resolution_clock::now();
    auto verifier = flatbuffers::Verifier(
        ptr,
        end - ptr, /* buffer size */
        128,       /* max verification depth */
        10000000 /* max number of tables to be verified */);

    bool is_valid_buffer =
        verifier.VerifyBuffer<typename TypeMapping<Out>::to>(nullptr);
    if (!is_valid_buffer) {
      err = E::BADPAYLOAD;
      ld_error("Buffer verification failed while deserializing type %s.",
               typeid(typename TypeMapping<Out>::to).name());

      STAT_INCR(Worker::stats(), logsconfig_manager_serialization_errors);
      return nullptr;
    }
    const int64_t verification_latency_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::high_resolution_clock::now() - verification_start_time)
            .count();

    ld_debug("Payload verification took %zums.", verification_latency_ms);
    auto ret = fbuffers_deserialize<Out>(
        flatbuffers::GetRoot<typename TypeMapping<Out>::to>(ptr), delimiter);

    if (!ret) {
      STAT_INCR(Worker::stats(), logsconfig_manager_serialization_errors);
    }

    return ret;
  }

  /* Internal Intemediate Serializers */

  /*
   * Serializes an object (In) into a flatbuffers Offset<Out>, for every type in
   * the LogsConfigTree structure we have a serialize method, this is a
   * recursive serializer that tries to find serializers to the sub-types as it
   * witness them in the tree.
   *
   * For every type in our tree, there is a corresponding type defined in
   * LogsConfigStructures.fbs and there should be a specialization of
   * serialize() for that type conversion.
   *
   *
   * e.g., fbuffers_serialize<const LogAttributes&, fbuffers::LogAttrs>(...)
   */
  template <typename In, typename Out>
  static flatbuffers::Offset<Out>
  fbuffers_serialize(flatbuffers::FlatBufferBuilder& builder,
                     In in,
                     bool flatten);

  /*
   * This is mainly used to be used as the top-level deserialize method for
   * DirectoryNode.
   *
   */
  template <typename Out, typename In>
  static std::unique_ptr<Out> fbuffers_deserialize(const In* in,
                                                   DirectoryNode* parent,
                                                   const std::string& delimiter,
                                                   bool apply_defaults);
  // helper used in deserializing deltas
  template <typename Out, typename In>
  static std::unique_ptr<Out>
  fbuffers_deserialize(const In* in,
                       const DeltaHeader& header,
                       const std::string& delimiter);
  /*
   * Deserialize have different flavors, it's a recursive deserializer that
   * tries to deserialize the flatbuffers structures to our LogsConfigTree
   * structures. Specializations of these methods will cover all of the possible
   * types defined in LogsConfigStructures.fbs
   *
   * An example is to deserialize (In) LogAttrs into
   * LogAttributes (Out)
   *
   * Or if you want to deserialize LogAttr (defines a single attribute in
   * LogAttrs), you need to pass LogAttr (In) to get Attribute<Type> where Type
   * is the type you *expect* for that attribute. We know the Type for each
   * attribute by its name, hence the DESERIALIZE_ATTRS macro. It takes the name
   * of the attribute and the type to get us the correct deserializer for the
   * Attribute<type> for that particular attribute. (e.g, replicationFactor is
   * int32_t). So when we read the name of the attribute from LogAttr we decide
   * that we want (Out) to be Attribute<int32_t>.
   *
   * e.g., fbuffers_deserialize<LogAttributes>(...) // <In> will be inferred.
   *
   *
   * e.g, std::unique_ptr<LogsConfigTree> deserialize(LogsConfig, delimiter)
   *
   */
  template <typename Out, typename In>
  static std::unique_ptr<Out>
  fbuffers_deserialize(const In* in, const std::string& delimiter);

  template <typename Out, typename In>
  static std::unique_ptr<Out> fbuffers_deserialize(const In* in,
                                                   const std::string& delimiter,
                                                   bool apply_defaults);

  template <typename Out, typename In>
  static std::unique_ptr<Out>
  fbuffers_deserialize(const In* in,
                       const std::string& delimiter,
                       const LogAttributes& parent_attributes);
  /*
   * A deserializer that doesn't need the delimiter.
   * , e.g., Attribute<uint32_t> fbuffers_deserialize<Attribute<uint32_t>>(...)
   *
   */
  template <typename Out, typename In>
  static Out fbuffers_deserialize(const In* in);

  template <typename Out, typename In>
  static Out fbuffers_deserialize(const In* in, const Out& parent);

  /*
   * Deserializer for Optional attributes
   */
  template <typename Out>
  static Attribute<folly::Optional<Out>> fbuffers_deserializeOptionalAttr(
      const fbuffers::LogAttr* in,
      const Attribute<folly::Optional<Out>>& parent);
};

using FBuffersLogsConfigCodec = LogsConfigCodec<CodecType::FLATBUFFERS>;

// @nolint
#include "logdevice/common/configuration/logs/flatbuffers_type_mapping.inc"
// @nolint
#include "logdevice/common/configuration/logs/FBuffersLogsConfigDecoder-inl.h"
// @nolint
#include "logdevice/common/configuration/logs/FBuffersLogsConfigEncoder-inl.h"
}}} // namespace facebook::logdevice::logsconfig
