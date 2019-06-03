/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/ServerMessagePermission.h"

#include "logdevice/common/protocol/FINDKEY_Message.h"
#include "logdevice/common/protocol/IS_LOG_EMPTY_Message.h"
#include "logdevice/common/protocol/LOGS_CONFIG_API_Message.h"
#include "logdevice/common/protocol/START_Message.h"
#include "logdevice/common/protocol/TRIM_Message.h"

namespace facebook { namespace logdevice {

PermissionParams
ServerMessagePermission::computePermissionParams(Message* msg) {
  PermissionParams params;

  switch (msg->type_) {
    case MessageType::START:
      params.requiresPermission = true;
      params.action = ACTION::READ;
      params.log_id = checked_downcast<START_Message*>(msg)->header_.log_id;
      break;
    case MessageType::TRIM:
      params.requiresPermission = true;
      params.action = ACTION::TRIM;
      params.log_id = checked_downcast<TRIM_Message*>(msg)->getHeader().log_id;
      break;
    case MessageType::LOGS_CONFIG_API:
      // Only require permissions to mutate the logs config. Anyone can
      // read/query it.
      params.requiresPermission =
          checked_downcast<LOGS_CONFIG_API_Message*>(msg)
              ->header_.request_type ==
          LOGS_CONFIG_API_Header::Type::MUTATION_REQUEST;
      params.action = ACTION::LOG_MANAGEMENT;
      params.log_id = LOGID_INVALID;
      break;
    case MessageType::FINDKEY:
      params.requiresPermission = true;
      params.action = ACTION::READ;
      params.log_id = checked_downcast<FINDKEY_Message*>(msg)->header_.log_id;
      break;
    case MessageType::IS_LOG_EMPTY:
      params.requiresPermission = true;
      params.action = ACTION::READ;
      params.log_id =
          checked_downcast<IS_LOG_EMPTY_Message*>(msg)->getHeader().log_id;
      break;
    default:
      break;
  }

  return params;
}

}} // namespace facebook::logdevice
