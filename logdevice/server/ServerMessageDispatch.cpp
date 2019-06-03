/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/ServerMessageDispatch.h"

#include "logdevice/common/GetEpochRecoveryMetadataRequest.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/UpdateableSecurityInfo.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/protocol/CLEAN_Message.h"
#include "logdevice/common/protocol/MessageTypeNames.h"
#include "logdevice/common/protocol/RELEASE_Message.h"
#include "logdevice/common/protocol/STOP_Message.h"
#include "logdevice/common/protocol/STORE_Message.h"
#include "logdevice/common/protocol/WINDOW_Message.h"
#include "logdevice/common/util.h"
#include "logdevice/server/CHECK_NODE_HEALTH_onReceived.h"
#include "logdevice/server/CHECK_SEAL_onReceived.h"
#include "logdevice/server/DATA_SIZE_onReceived.h"
#include "logdevice/server/DELETE_LOG_METADATA_onReceived.h"
#include "logdevice/server/DELETE_onReceived.h"
#include "logdevice/server/FINDKEY_onReceived.h"
#include "logdevice/server/GAP_onSent.h"
#include "logdevice/server/GET_EPOCH_RECOVERY_METADATA_REPLY_onReceived.h"
#include "logdevice/server/GET_EPOCH_RECOVERY_METADATA_onReceived.h"
#include "logdevice/server/GET_HEAD_ATTRIBUTES_onReceived.h"
#include "logdevice/server/GET_TRIM_POINT_onReceived.h"
#include "logdevice/server/GOSSIP_onReceived.h"
#include "logdevice/server/GOSSIP_onSent.h"
#include "logdevice/server/IS_LOG_EMPTY_onReceived.h"
#include "logdevice/server/LOGS_CONFIG_API_onReceived.h"
#include "logdevice/server/MEMTABLE_FLUSHED_onReceived.h"
#include "logdevice/server/RECORD_onSent.h"
#include "logdevice/server/SEAL_onReceived.h"
#include "logdevice/server/STARTED_onSent.h"
#include "logdevice/server/START_onReceived.h"
#include "logdevice/server/STOP_onReceived.h"
#include "logdevice/server/STORED_onReceived.h"
#include "logdevice/server/STORE_onSent.h"
#include "logdevice/server/ServerMessagePermission.h"
#include "logdevice/server/ServerWorker.h"
#include "logdevice/server/StoreStateMachine.h"
#include "logdevice/server/TRIM_onReceived.h"
#include "logdevice/server/read_path/AllServerReadStreams.h"
#include "logdevice/server/sequencer_boycotting/NODE_STATS_AGGREGATE_REPLY_onReceived.h"
#include "logdevice/server/sequencer_boycotting/NODE_STATS_AGGREGATE_onReceived.h"
#include "logdevice/server/storage/PurgeCoordinator.h"
#include "logdevice/server/storage/PurgeUncleanEpochs.h"
#include "logdevice/server/storage_tasks/ReadStorageTask.h"

namespace facebook { namespace logdevice {

Message::Disposition
ServerMessageDispatch::onReceivedImpl(Message* msg,
                                      const Address& from,
                                      const PrincipalIdentity& principal) {
  auto params = ServerMessagePermission::computePermissionParams(msg);

  std::shared_ptr<PermissionChecker> permission_checker =
      processor_->security_info_->getPermissionChecker();

  if (permission_checker && params.requiresPermission &&
      processor_->settings()->require_permission_message_types.count(
          msg->type_) == 0) {
    // override permission requirement per configured settings
    RATELIMIT_INFO(std::chrono::seconds(10),
                   1,
                   "Bypassing permission check for message of type %s from "
                   "%s per configured 'require-permission-message-type' "
                   "setting",
                   messageTypeNames()[msg->type_].c_str(),
                   Sender::describeConnection(from).c_str());
    params.requiresPermission = false;
    STAT_INCR(processor_->stats_, server_message_dispatch_bypass_permission);
  }

  if (permission_checker && params.requiresPermission) {
    STAT_INCR(processor_->stats_, server_message_dispatch_check_permission);
    permission_checker->isAllowed(params.action,
                                  principal,
                                  params.log_id,

                                  [=](PermissionCheckStatus permission_status) {
                                    Message::Disposition disp =
                                        onReceivedHandler(
                                            msg, from, permission_status);
                                    switch (disp) {
                                      case Message::Disposition::KEEP:
                                        break;
                                      case Message::Disposition::NORMAL:
                                      case Message::Disposition::ERROR:
                                      default:
                                        delete msg;
                                    }
                                  });
    return Message::Disposition::KEEP;
  } else {
    STAT_INCR(processor_->stats_, server_message_dispatch_skip_permission);
    return onReceivedHandler(msg, from, PermissionCheckStatus::NONE);
  }
}

Message::Disposition ServerMessageDispatch::onReceivedHandler(
    Message* msg,
    const Address& from,
    PermissionCheckStatus permission_status) const {
  switch (msg->type_) {
    case MessageType::CHECK_NODE_HEALTH:
      return CHECK_NODE_HEALTH_onReceived(
          checked_downcast<CHECK_NODE_HEALTH_Message*>(msg), from);

    case MessageType::CHECK_SEAL:
      return CHECK_SEAL_onReceived(
          checked_downcast<CHECK_SEAL_Message*>(msg), from);

    case MessageType::CLEAN:
      return PurgeCoordinator::onReceived(
          checked_downcast<CLEAN_Message*>(msg), from);

    case MessageType::DATA_SIZE:
      return DATA_SIZE_onReceived(
          checked_downcast<DATA_SIZE_Message*>(msg), from);

    case MessageType::DELETE:
      return DELETE_onReceived(checked_downcast<DELETE_Message*>(msg), from);

    case MessageType::DELETE_LOG_METADATA:
      return DELETE_LOG_METADATA_onReceived(
          checked_downcast<DELETE_LOG_METADATA_Message*>(msg), from);

    case MessageType::FINDKEY:
      return FINDKEY_onReceived(
          checked_downcast<FINDKEY_Message*>(msg), from, permission_status);

    case MessageType::GET_EPOCH_RECOVERY_METADATA:
      return GET_EPOCH_RECOVERY_METADATA_onReceived(
          checked_downcast<GET_EPOCH_RECOVERY_METADATA_Message*>(msg), from);

    case MessageType::GET_EPOCH_RECOVERY_METADATA_REPLY:
      return GET_EPOCH_RECOVERY_METADATA_REPLY_onReceived(
          checked_downcast<GET_EPOCH_RECOVERY_METADATA_REPLY_Message*>(msg),
          from);

    case MessageType::GET_HEAD_ATTRIBUTES:
      return GET_HEAD_ATTRIBUTES_onReceived(
          checked_downcast<GET_HEAD_ATTRIBUTES_Message*>(msg), from);

    case MessageType::GET_TRIM_POINT:
      return GET_TRIM_POINT_onReceived(
          checked_downcast<GET_TRIM_POINT_Message*>(msg), from);

    case MessageType::GOSSIP:
      return GOSSIP_onReceived(checked_downcast<GOSSIP_Message*>(msg), from);

    case MessageType::IS_LOG_EMPTY:
      return IS_LOG_EMPTY_onReceived(
          checked_downcast<IS_LOG_EMPTY_Message*>(msg),
          from,
          permission_status);

    case MessageType::MEMTABLE_FLUSHED:
      return MEMTABLE_FLUSHED_onReceived(
          checked_downcast<MEMTABLE_FLUSHED_Message*>(msg), from);

    case MessageType::NODE_STATS_AGGREGATE:
      return NODE_STATS_AGGREGATE_onReceived(
          checked_downcast<NODE_STATS_AGGREGATE_Message*>(msg), from);

    case MessageType::NODE_STATS_AGGREGATE_REPLY:
      return NODE_STATS_AGGREGATE_REPLY_onReceived(
          checked_downcast<NODE_STATS_AGGREGATE_REPLY_Message*>(msg), from);

    case MessageType::RELEASE:
      return PurgeCoordinator::onReceived(
          checked_downcast<RELEASE_Message*>(msg), from);

    case MessageType::SEAL:
      return SEAL_onReceived(checked_downcast<SEAL_Message*>(msg), from);

    case MessageType::START:
      return START_onReceived(
          checked_downcast<START_Message*>(msg), from, permission_status);

    case MessageType::STOP:
      return STOP_onReceived(checked_downcast<STOP_Message*>(msg), from);

    case MessageType::STORE:
      return StoreStateMachine::onReceived(
          checked_downcast<STORE_Message*>(msg), from);

    case MessageType::STORED:
      return STORED_onReceived(checked_downcast<STORED_Message*>(msg), from);

    case MessageType::TRIM:
      return TRIM_onReceived(
          checked_downcast<TRIM_Message*>(msg), from, permission_status);

    case MessageType::WINDOW:
      return AllServerReadStreams::onWindowMessage(
          checked_downcast<WINDOW_Message*>(msg), from);

    case MessageType::LOGS_CONFIG_API:
      return LOGS_CONFIG_API_onReceived(
          checked_downcast<LOGS_CONFIG_API_Message*>(msg),
          from,
          permission_status);

    case MessageType::NODE_STATS_REPLY:
      RATELIMIT_ERROR(
          std::chrono::seconds(60),
          1,
          "ServerMessageDispatch::onReceived() called with %s message"
          "which is supposed to be client-only!",
          messageTypeNames()[msg->type_].c_str());
      err = E::PROTO;
      return Message::Disposition::ERROR;

    default:
      // By default, call the Message's onReceived() implementation (for
      // messages whose handler lives in common/ with the Message subclass)
      return msg->onReceived(from);
  }
}

void ServerMessageDispatch::onSentImpl(const Message& msg,
                                       Status st,
                                       const Address& to,
                                       const SteadyTimestamp enqueue_time) {
  switch (msg.type_) {
    case MessageType::GAP:
      return GAP_onSent(
          checked_downcast<const GAP_Message&>(msg), st, to, enqueue_time);

    case MessageType::GET_EPOCH_RECOVERY_METADATA:
      return GetEpochRecoveryMetadataRequest::onSent(
          checked_downcast<const GET_EPOCH_RECOVERY_METADATA_Message&>(msg),
          st,
          to);

    case MessageType::GOSSIP:
      return GOSSIP_onSent(
          checked_downcast<const GOSSIP_Message&>(msg), st, to, enqueue_time);

    case MessageType::NODE_STATS:
      RATELIMIT_ERROR(std::chrono::seconds(60),
                      1,
                      "ServerMessageDispatch::onSent() called with %s message"
                      "which is supposed to be client-only!",
                      messageTypeNames()[msg.type_].c_str());
      ld_check(false);
      return;

    case MessageType::RECORD:
      return RECORD_onSent(
          checked_downcast<const RECORD_Message&>(msg), st, to, enqueue_time);

    case MessageType::SHARD_STATUS_UPDATE:
      return ServerWorker::onThisThread()
          ->serverReadStreams()
          .onShardStatusUpdateMessageSent(to.asClientID(), st);

    case MessageType::STARTED:
      return STARTED_onSent(
          checked_downcast<const STARTED_Message&>(msg), st, to, enqueue_time);

    case MessageType::STORE:
      return STORE_onSent(
          checked_downcast<const STORE_Message&>(msg), st, to, enqueue_time);

    default:
      // By default, call the Message's onSent() implementation (for messages
      // whose handler lives in common/ with the Message subclass)
      return msg.onSent(st, to);
  }
}
}} // namespace facebook::logdevice
