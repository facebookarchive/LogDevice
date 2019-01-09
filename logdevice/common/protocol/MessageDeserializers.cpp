/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/protocol/MessageDeserializers.h"

#include "logdevice/common/protocol/ACK_Message.h"
#include "logdevice/common/protocol/APPENDED_Message.h"
#include "logdevice/common/protocol/APPEND_Message.h"
#include "logdevice/common/protocol/APPEND_PROBE_Message.h"
#include "logdevice/common/protocol/APPEND_PROBE_REPLY_Message.h"
#include "logdevice/common/protocol/CHECK_NODE_HEALTH_Message.h"
#include "logdevice/common/protocol/CHECK_NODE_HEALTH_REPLY_Message.h"
#include "logdevice/common/protocol/CHECK_SEAL_Message.h"
#include "logdevice/common/protocol/CHECK_SEAL_REPLY_Message.h"
#include "logdevice/common/protocol/CLEANED_Message.h"
#include "logdevice/common/protocol/CLEAN_Message.h"
#include "logdevice/common/protocol/CONFIG_ADVISORY_Message.h"
#include "logdevice/common/protocol/CONFIG_CHANGED_Message.h"
#include "logdevice/common/protocol/CONFIG_FETCH_Message.h"
#include "logdevice/common/protocol/DATA_SIZE_Message.h"
#include "logdevice/common/protocol/DATA_SIZE_REPLY_Message.h"
#include "logdevice/common/protocol/DELETE_LOG_METADATA_Message.h"
#include "logdevice/common/protocol/DELETE_LOG_METADATA_REPLY_Message.h"
#include "logdevice/common/protocol/DELETE_Message.h"
#include "logdevice/common/protocol/FINDKEY_Message.h"
#include "logdevice/common/protocol/FINDKEY_REPLY_Message.h"
#include "logdevice/common/protocol/GAP_Message.h"
#include "logdevice/common/protocol/GET_CLUSTER_STATE_Message.h"
#include "logdevice/common/protocol/GET_CLUSTER_STATE_REPLY_Message.h"
#include "logdevice/common/protocol/GET_EPOCH_RECOVERY_METADATA_Message.h"
#include "logdevice/common/protocol/GET_EPOCH_RECOVERY_METADATA_REPLY_Message.h"
#include "logdevice/common/protocol/GET_HEAD_ATTRIBUTES_Message.h"
#include "logdevice/common/protocol/GET_HEAD_ATTRIBUTES_REPLY_Message.h"
#include "logdevice/common/protocol/GET_LOG_INFO_Message.h"
#include "logdevice/common/protocol/GET_LOG_INFO_REPLY_Message.h"
#include "logdevice/common/protocol/GET_SEQ_STATE_Message.h"
#include "logdevice/common/protocol/GET_SEQ_STATE_REPLY_Message.h"
#include "logdevice/common/protocol/GET_TRIM_POINT_Message.h"
#include "logdevice/common/protocol/GET_TRIM_POINT_REPLY_Message.h"
#include "logdevice/common/protocol/GOSSIP_Message.h"
#include "logdevice/common/protocol/HELLO_Message.h"
#include "logdevice/common/protocol/IS_LOG_EMPTY_Message.h"
#include "logdevice/common/protocol/IS_LOG_EMPTY_REPLY_Message.h"
#include "logdevice/common/protocol/LOGS_CONFIG_API_Message.h"
#include "logdevice/common/protocol/LOGS_CONFIG_API_REPLY_Message.h"
#include "logdevice/common/protocol/MEMTABLE_FLUSHED_Message.h"
#include "logdevice/common/protocol/MUTATED_Message.h"
#include "logdevice/common/protocol/NODE_STATS_AGGREGATE_Message.h"
#include "logdevice/common/protocol/NODE_STATS_AGGREGATE_REPLY_Message.h"
#include "logdevice/common/protocol/NODE_STATS_Message.h"
#include "logdevice/common/protocol/NODE_STATS_REPLY_Message.h"
#include "logdevice/common/protocol/RECORD_Message.h"
#include "logdevice/common/protocol/RELEASE_Message.h"
#include "logdevice/common/protocol/SEALED_Message.h"
#include "logdevice/common/protocol/SEAL_Message.h"
#include "logdevice/common/protocol/SHARD_STATUS_UPDATE_Message.h"
#include "logdevice/common/protocol/SHUTDOWN_Message.h"
#include "logdevice/common/protocol/STARTED_Message.h"
#include "logdevice/common/protocol/START_Message.h"
#include "logdevice/common/protocol/STOP_Message.h"
#include "logdevice/common/protocol/STORED_Message.h"
#include "logdevice/common/protocol/STORE_Message.h"
#include "logdevice/common/protocol/TEST_Message.h"
#include "logdevice/common/protocol/TRIMMED_Message.h"
#include "logdevice/common/protocol/TRIM_Message.h"
#include "logdevice/common/protocol/WINDOW_Message.h"

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
