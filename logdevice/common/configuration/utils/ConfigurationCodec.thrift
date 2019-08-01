/**
 * Copyright (c) 2018-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

namespace cpp2 facebook.logdevice.configuration.thrift

typedef byte (cpp2.type = "std::uint8_t") u8
typedef i16 (cpp2.type = "std::uint16_t") u16
typedef i32 (cpp2.type = "std::uint32_t") u32
typedef i64 (cpp2.type = "std::uint64_t") u64

struct ConfigurationCodecHeader {
  1: u32 proto_version;
  2: u64 config_version;
  3: bool is_compressed;
}

struct ConfigurationCodecWrapper {
  1: ConfigurationCodecHeader header;
  2: binary serialized_config;
}
