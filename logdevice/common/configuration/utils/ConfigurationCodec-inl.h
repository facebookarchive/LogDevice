/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#ifndef LOGDEVICE_CONFIGURATION_CODEC_H_
#error "This should only be included by ConfigurationCodec.h"
#endif

namespace facebook { namespace logdevice { namespace configuration {

using apache::thrift::BinarySerializer;

/*static*/
template <typename ConfigurationType,
          typename ConfigurationVersionType,
          typename ThriftConverter,
          uint32_t CURRENT_PROTO>
void ConfigurationCodec<ConfigurationType,
                        ConfigurationVersionType,
                        ThriftConverter,
                        CURRENT_PROTO>::serialize(const ConfigurationType&
                                                      config,
                                                  ProtocolWriter& writer,
                                                  SerializeOptions options) {
  std::string thrift_str = ThriftCodec::serialize<BinarySerializer>(
      ThriftConverter::toThrift(config));
  auto data_blob = Slice::fromString(thrift_str);

  std::unique_ptr<uint8_t[]> buffer;
  if (options.compression) {
    size_t compressed_size_upperbound = ZSTD_compressBound(data_blob.size);
    buffer = std::make_unique<uint8_t[]>(compressed_size_upperbound);
    size_t compressed_size =
        ZSTD_compress(buffer.get(),               // dst
                      compressed_size_upperbound, // dstCapacity
                      data_blob.data,             // src
                      data_blob.size,             // srcSize
                      /*compressionLevel=*/5);    // level

    if (ZSTD_isError(compressed_size)) {
      ld_error(
          "ZSTD_compress() failed: %s", ZSTD_getErrorName(compressed_size));
      writer.setError(E::INVALID_PARAM);
      return;
    }
    ld_debug("original size is %zu, compressed size %zu",
             data_blob.size,
             compressed_size);
    ld_check(compressed_size <= compressed_size_upperbound);
    // revise the data_blob to point to the
    // compressed blob instead
    data_blob = Slice{buffer.get(), compressed_size};
  }

  thrift::ConfigurationCodecHeader wrapper_header{};
  wrapper_header.set_proto_version(CURRENT_PROTO_VERSION);
  wrapper_header.set_config_version(config.getVersion().val());
  wrapper_header.set_is_compressed(options.compression);

  thrift::ConfigurationCodecWrapper wrapper{};
  wrapper.set_header(std::move(wrapper_header));
  // TODO get rid of this copy
  wrapper.set_serialized_config(std::string(data_blob.ptr(), data_blob.size));

  writer.writeVector(ThriftCodec::serialize<BinarySerializer>(wrapper));
}

/* static */
template <typename ConfigurationType,
          typename ConfigurationVersionType,
          typename ThriftConverter,
          uint32_t CURRENT_PROTO>
std::string ConfigurationCodec<
    ConfigurationType,
    ConfigurationVersionType,
    ThriftConverter,
    CURRENT_PROTO>::debugJsonString(const ConfigurationType& config) {
  return ThriftCodec::serialize<apache::thrift::SimpleJSONSerializer>(
      ThriftConverter::toThrift(config));
}

/*static*/
template <typename ConfigurationType,
          typename ConfigurationVersionType,
          typename ThriftConverter,
          uint32_t CURRENT_PROTO>
std::shared_ptr<const ConfigurationType>
ConfigurationCodec<ConfigurationType,
                   ConfigurationVersionType,
                   ThriftConverter,
                   CURRENT_PROTO>::deserialize(Slice wrapper_blob) {
  auto wrapper_ptr =
      ThriftCodec::deserialize<BinarySerializer,
                               thrift::ConfigurationCodecWrapper>(wrapper_blob);
  if (wrapper_ptr == nullptr) {
    err = E::BADMSG;
    return nullptr;
  }

  if (wrapper_ptr->header.proto_version > CURRENT_PROTO_VERSION) {
    RATELIMIT_ERROR(
        std::chrono::seconds(10),
        5,
        "Received codec protocol version %u is larger than current "
        "codec protocol version %u. There might be incompatible data, "
        "aborting deserialization",
        wrapper_ptr->header.proto_version,
        CURRENT_PROTO_VERSION);
    err = E::NOTSUPPORTED;
    return nullptr;
  }

  std::unique_ptr<uint8_t[]> buffer;
  const auto& serialized_config = wrapper_ptr->serialized_config;
  auto data_blob = Slice::fromString(serialized_config);

  if (wrapper_ptr->header.is_compressed) {
    size_t uncompressed_size =
        ZSTD_getDecompressedSize(data_blob.data, data_blob.size);
    if (uncompressed_size == 0) {
      RATELIMIT_ERROR(
          std::chrono::seconds(5), 1, "ZSTD_getDecompressedSize() failed!");
      err = E::BADMSG;
      return nullptr;
    }
    buffer = std::make_unique<uint8_t[]>(uncompressed_size);
    uncompressed_size = ZSTD_decompress(buffer.get(),      // dst
                                        uncompressed_size, // dstCapacity
                                        data_blob.data,    // src
                                        data_blob.size);   // compressedSize
    if (ZSTD_isError(uncompressed_size)) {
      RATELIMIT_ERROR(std::chrono::seconds(5),
                      1,
                      "ZSTD_decompress() failed: %s",
                      ZSTD_getErrorName(uncompressed_size));
      err = E::BADMSG;
      return nullptr;
    }
    // revise the data_blob to point to the uncompressed data
    data_blob = Slice{buffer.get(), uncompressed_size};
  }

  auto config_ptr =
      ThriftCodec::deserialize<BinarySerializer,
                               typename ThriftConverter::ThriftConfigType>(
          data_blob);
  if (config_ptr == nullptr) {
    err = E::BADMSG;
    return nullptr;
  }
  return ThriftConverter::fromThrift(*config_ptr);
}

/*static*/
template <typename ConfigurationType,
          typename ConfigurationVersionType,
          typename ThriftConverter,
          uint32_t CURRENT_PROTO>
std::string
ConfigurationCodec<ConfigurationType,
                   ConfigurationVersionType,
                   ThriftConverter,
                   CURRENT_PROTO>::serialize(const ConfigurationType& config,
                                             SerializeOptions options) {
  std::string result;
  ProtocolWriter w(&result, "ConfiguratonCodec", 0);
  serialize(config, w, options);
  if (w.error()) {
    err = w.status();
    return "";
  }
  return result;
}

/*static*/
template <typename ConfigurationType,
          typename ConfigurationVersionType,
          typename ThriftConverter,
          uint32_t CURRENT_PROTO>
std::shared_ptr<const ConfigurationType>
ConfigurationCodec<ConfigurationType,
                   ConfigurationVersionType,
                   ThriftConverter,
                   CURRENT_PROTO>::deserialize(folly::StringPiece buf) {
  return deserialize(Slice(buf.data(), buf.size()));
}

/*static*/
template <typename ConfigurationType,
          typename ConfigurationVersionType,
          typename ThriftConverter,
          uint32_t CURRENT_PROTO>
folly::Optional<ConfigurationVersionType> ConfigurationCodec<
    ConfigurationType,
    ConfigurationVersionType,
    ThriftConverter,
    CURRENT_PROTO>::extractConfigVersion(folly::StringPiece serialized_data) {
  if (serialized_data.empty()) {
    return folly::none;
  }
  // TODO consider using thrift frozen for this wrapper to avoid deserializing
  // the whole struct to get the version.
  auto wrapper_ptr =
      ThriftCodec::deserialize<BinarySerializer,
                               thrift::ConfigurationCodecWrapper>(
          Slice{serialized_data.data(), serialized_data.size()});
  if (wrapper_ptr == nullptr) {
    RATELIMIT_ERROR(
        std::chrono::seconds(5), 1, "Failed to extract configuration version");
    return folly::none;
  }

  return ConfigurationVersionType(wrapper_ptr->header.config_version);
}

}}} // namespace facebook::logdevice::configuration
