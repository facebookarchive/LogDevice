/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/Request.h"
#include "logdevice/common/protocol/FixedSizeMessage.h"
#include "logdevice/common/protocol/MessageType.h"

namespace facebook { namespace logdevice {

/**
 * @file Message sent by a client to a node of the cluster to get its view of
 *       the cluster state, such as the list of dead nodes.
 */

struct GET_CLUSTER_STATE_Header {
  request_id_t client_rqid;
} __attribute__((__packed__));

using GET_CLUSTER_STATE_Message =
    FixedSizeMessage<GET_CLUSTER_STATE_Header,
                     MessageType::GET_CLUSTER_STATE,
                     TrafficClass::FAILURE_DETECTOR>;
}} // namespace facebook::logdevice
