/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/configuration/TrafficClass.h"
#include "logdevice/common/protocol/MessageType.h"
#include "logdevice/common/protocol/SimpleMessage.h"
#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice {

/**
 * @file  a template class for all messages that consist entirely of a
 *        fixed-sized header. Implemented via SimpleMessage without the
 *        variable size blob.
 *
 * @param Header        header class
 * @param MESSAGE_TYPE  a constant of type logdevice::MessageType for
 *                      the type of message represented by this class
 */

// FixedSizeMessage is a SimpleMessage without the additional blob.
template <class Header, MessageType MESSAGE_TYPE, TrafficClass TRAFFIC_CLASS>
using FixedSizeMessage =
    SimpleMessage<Header, MESSAGE_TYPE, TRAFFIC_CLASS, false>;

}} // namespace facebook::logdevice
