/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "MessageDeserializers.h"
#include "ACK_Message.h"
#include "APPENDED_Message.h"
#include "APPEND_Message.h"
#include "APPEND_PROBE_Message.h"
#include "APPEND_PROBE_REPLY_Message.h"
#include "CHECK_NODE_HEALTH_Message.h"
#include "CHECK_NODE_HEALTH_REPLY_Message.h"
#include "CHECK_SEAL_Message.h"
#include "CHECK_SEAL_REPLY_Message.h"
#include "CLEAN_Message.h"
#include "CLEANED_Message.h"
#include "CONFIG_ADVISORY_Message.h"
#include "CONFIG_CHANGED_Message.h"
#include "CONFIG_FETCH_Message.h"
#include "DATA_SIZE_Message.h"
#include "DATA_SIZE_REPLY_Message.h"
#include "DELETE_Message.h"
#include "DELETE_LOG_METADATA_Message.h"
#include "DELETE_LOG_METADATA_REPLY_Message.h"
#include "FINDKEY_Message.h"
#include "FINDKEY_REPLY_Message.h"
#include "GAP_Message.h"
#include "GET_EPOCH_RECOVERY_METADATA_Message.h"
#include "GET_EPOCH_RECOVERY_METADATA_REPLY_Message.h"
#include "GET_HEAD_ATTRIBUTES_Message.h"
#include "GET_HEAD_ATTRIBUTES_REPLY_Message.h"
#include "GET_LOG_INFO_Message.h"
#include "GET_LOG_INFO_REPLY_Message.h"
#include "GET_SEQ_STATE_Message.h"
#include "GET_SEQ_STATE_REPLY_Message.h"
#include "GET_CLUSTER_STATE_Message.h"
#include "GET_CLUSTER_STATE_REPLY_Message.h"
#include "GET_TRIM_POINT_Message.h"
#include "GET_TRIM_POINT_REPLY_Message.h"
#include "GOSSIP_Message.h"
#include "HELLO_Message.h"
#include "IS_LOG_EMPTY_Message.h"
#include "IS_LOG_EMPTY_REPLY_Message.h"
#include "LOGS_CONFIG_API_Message.h"
#include "LOGS_CONFIG_API_REPLY_Message.h"
#include "MEMTABLE_FLUSHED_Message.h"
#include "MUTATED_Message.h"
#include "NODE_STATS_Message.h"
#include "NODE_STATS_REPLY_Message.h"
#include "NODE_STATS_AGGREGATE_Message.h"
#include "NODE_STATS_AGGREGATE_REPLY_Message.h"
#include "RECORD_Message.h"
#include "RELEASE_Message.h"
#include "SEAL_Message.h"
#include "SEALED_Message.h"
#include "SHUTDOWN_Message.h"
#include "SHARD_STATUS_UPDATE_Message.h"
#include "START_Message.h"
#include "STARTED_Message.h"
#include "STOP_Message.h"
#include "STORED_Message.h"
#include "STORE_Message.h"
#include "TEST_Message.h"
#include "TRIM_Message.h"
#include "TRIMMED_Message.h"
#include "WINDOW_Message.h"

namespace facebook { namespace logdevice {

EnumMap<MessageType, Message::deserializer_t*> messageDeserializers;

template <>
/* static */
Message::deserializer_t* const&
EnumMap<MessageType, Message::deserializer_t*>::invalidValue() {
  static Message::deserializer_t* const res(nullptr);
  return res;
}

template <>
void EnumMap<MessageType, Message::deserializer_t*>::setValues() {
#define MESSAGE_TYPE(sym, num)                                             \
  set(MessageType::sym,                                                    \
      static_cast<Message::deserializer_t*>(&sym##_Message::deserialize)); \
  static_assert(num > 0, #sym);                                            \
  static_assert(num < static_cast<int>(MessageType::MAX), #sym);
#include "logdevice/common/message_types.inc"
}

}} // namespace facebook::logdevice
