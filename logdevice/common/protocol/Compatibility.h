/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <cstdint>

namespace facebook { namespace logdevice { namespace Compatibility {

// When adding a new protocol version, add it above PROTOCOL_VERSION_UPPER_BOUND
// at the end of the enum and add a static_assert verifying that its value is
// what you'd expect after any rebases and merges too

enum ProtocolVersion : uint16_t {
  // NOTE: do not add anything above PROTOCOL_VERSION_LOWER_BOUND
  //
  // Minimum version number of the protocol this version of LogDevice is
  // backward compatible with - 1
  PROTOCOL_VERSION_LOWER_BOUND = 91,

  // GOSSIP_Message includes node_list_ to deliver hashmap
  HASHMAP_SUPPORT_IN_GOSSIP, // = 92

  // Adds an optional request ID to CONFIG_FETCH & CONFIG_CHANGED messages
  RID_IN_CONFIG_MESSAGES, // = 93

  // Include recovery wave (not to be confused with recovery epoch) in
  // MUTATED_Message.
  WAVE_IN_MUTATED, // = 94

  // SEALED message will include trim point for the given shard of the log
  TRIM_POINT_IN_SEALED, // == 95

  // Increase size of flags in GET_SEQ_STATE_Message
  GSS_32BIT_FLAG, // = 96

  IS_LOG_EMPTY_IN_GSS_REPLY, // = 97

  STREAM_WRITER_SUPPORT, // = 98

  // NOTE: insert new protocol versions here

  // Maximum version number of the protocol this version of LogDevice
  // implements + 1.
  //
  // NOTE: Most production code should not refer to this constant directly but
  // to Settings::max_protocol which supports clamping the max version via
  // configuration overrides.
  PROTOCOL_VERSION_UPPER_BOUND

  // NOTE: do not add anything below PROTOCOL_VERSION_UPPER_BOUND
};

static_assert(HASHMAP_SUPPORT_IN_GOSSIP == 92, "");
static_assert(RID_IN_CONFIG_MESSAGES == 93, "");
static_assert(WAVE_IN_MUTATED == 94, "");
static_assert(TRIM_POINT_IN_SEALED == 95, "");
static_assert(GSS_32BIT_FLAG == 96, "");
static_assert(IS_LOG_EMPTY_IN_GSS_REPLY == 97, "");
static_assert(STREAM_WRITER_SUPPORT == 98, "");

constexpr uint16_t MIN_PROTOCOL_SUPPORTED = PROTOCOL_VERSION_LOWER_BOUND + 1;
constexpr uint16_t MAX_PROTOCOL_SUPPORTED = PROTOCOL_VERSION_UPPER_BOUND - 1;

}}} // namespace facebook::logdevice::Compatibility
