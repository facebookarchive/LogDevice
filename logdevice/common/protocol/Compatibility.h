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
  PROTOCOL_VERSION_LOWER_BOUND = 56,

  // START_Message can specify key(s) which server can uses
  // to filter out records.
  SERVER_CAN_FILTER_RECORD, // == 57;

  // Client can provide custom counter in APPEND_message which get aggregated
  // on server.
  SERVER_CUSTOM_COUNTER_SUPPORT, // == 58

  // clients can send NODE_STATS_Message and nodes can send
  // NODE_STATS_REPLY_Messages in response
  CLIENT_SEND_NODE_STATS, // == 59

  // All optional keys including FINDKEY and filterable for now
  APPEND_WITH_OPTIONAL_KEYS, // == 60;

  SHARD_ID_IN_CLEANED_PROTOCOL, // == 61;

  // A rebuilding store message will include the durability of the
  // rebuilding.
  REBUILDING_WITHOUT_WAL_2, // ==  62;

  SHARD_ID_IN_STORE_MSG, // = 63;

  SHARD_ID_IN_DELETE_MSG, // = 64;

  // NODE_STATS_AGGREGATE_Message for controller nodes to aggregate information
  // from all other nodes
  NODE_STATS_AGGREGATE, // == 65

  BOYCOTT_IN_CLUSTER, // == 66

  // GOSSIP_Message now includes boycott information
  GOSSIP_WITH_BOYCOTT, // == 67

  WORST_CLIENT_FOR_BOYCOTT, // == 68

  // Change START to support encoding of the known down size along with the
  // vector and ignore the num_filtered_out member of the header
  SUPPORT_LARGER_FILTERED_OUT_LIST, // 69

  // SEALED message will include tail record for the epoch range
  TAIL_RECORD_IN_SEALED, // == 70

  SHARD_ID_IN_REBUILD_METADATA, // == 71

  SHARD_ID_IN_RELEASE_MSG, // == 72

  SHARD_ID_IN_CHECK_SEAL_MSG, // == 73

  RECORD_TIMESTAMP_IN_APPENDED_MSG, // == 74

  HISTORICAL_METADATA_IN_GSS_REPLY, // == 75

  // GET_EPOCH_RECOVERY_METADATA/REPLY will support a range of epoch
  GET_EPOCH_RECOVERY_RANGE_SUPPORT, // = 76;

  LOGS_CONFIG_API_SUBSCRIPTIONS, // = 77

  GET_TRIM_POINT_SUPPORT, // = 78

  // START_Message can specify a hash of the client session id,
  // which server can use to parameterize single copy delivery.
  SERVER_CAN_PROCESS_CSID, // == 79;

  // When e2e tracing is on, tracing information should be included
  APPEND_E2E_TRACING_SUPPORT, // == 80

  // Support for checksumming of any message in the Protocol layer
  CHECKSUM_SUPPORT, // = 81

  // include tail record in Get Sequencer State message replies
  TAIL_RECORD_IN_GSS_REPLY, // = 82

  // When e2e tracing is on, store message should also have tracing context
  STORE_E2E_TRACING_SUPPORT, // = 83

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

static_assert(SERVER_CAN_FILTER_RECORD == 57, "");
static_assert(SERVER_CUSTOM_COUNTER_SUPPORT == 58, "");
static_assert(CLIENT_SEND_NODE_STATS == 59, "");
static_assert(APPEND_WITH_OPTIONAL_KEYS == 60, "");
static_assert(SHARD_ID_IN_CLEANED_PROTOCOL == 61, "");
static_assert(REBUILDING_WITHOUT_WAL_2 == 62, "");
static_assert(SHARD_ID_IN_STORE_MSG == 63, "");
static_assert(SHARD_ID_IN_DELETE_MSG == 64, "");
static_assert(NODE_STATS_AGGREGATE == 65, "");
static_assert(BOYCOTT_IN_CLUSTER == 66, "");
static_assert(GOSSIP_WITH_BOYCOTT == 67, "");
static_assert(WORST_CLIENT_FOR_BOYCOTT == 68, "");
static_assert(SUPPORT_LARGER_FILTERED_OUT_LIST == 69, "");
static_assert(TAIL_RECORD_IN_SEALED == 70, "");
static_assert(SHARD_ID_IN_REBUILD_METADATA == 71, "");
static_assert(SHARD_ID_IN_RELEASE_MSG == 72, "");
static_assert(SHARD_ID_IN_CHECK_SEAL_MSG == 73, "");
static_assert(RECORD_TIMESTAMP_IN_APPENDED_MSG == 74, "");
static_assert(HISTORICAL_METADATA_IN_GSS_REPLY == 75, "");
static_assert(GET_EPOCH_RECOVERY_RANGE_SUPPORT == 76, "");
static_assert(LOGS_CONFIG_API_SUBSCRIPTIONS == 77, "");
static_assert(GET_TRIM_POINT_SUPPORT == 78, "");
static_assert(SERVER_CAN_PROCESS_CSID == 79, "");
static_assert(APPEND_E2E_TRACING_SUPPORT == 80, "");
static_assert(CHECKSUM_SUPPORT == 81, "");
static_assert(TAIL_RECORD_IN_GSS_REPLY == 82, "");
static_assert(STORE_E2E_TRACING_SUPPORT == 83, "");

constexpr uint16_t MIN_PROTOCOL_SUPPORTED = PROTOCOL_VERSION_LOWER_BOUND + 1;
constexpr uint16_t MAX_PROTOCOL_SUPPORTED = PROTOCOL_VERSION_UPPER_BOUND - 1;

}}} // namespace facebook::logdevice::Compatibility
