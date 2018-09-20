/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/protocol/MEMTABLE_FLUSHED_Message.h"
#include "logdevice/common/protocol/Message.h"

namespace facebook { namespace logdevice {

struct Address;

Message::Disposition MEMTABLE_FLUSHED_onReceived(MEMTABLE_FLUSHED_Message* msg,
                                                 const Address& from);

}} // namespace facebook::logdevice
