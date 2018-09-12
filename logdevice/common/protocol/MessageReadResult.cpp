/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "MessageReadResult.h"

#include "logdevice/common/protocol/Message.h"

namespace facebook { namespace logdevice {

// Having the constructor and destructor here allows `Message' to be
// forward-declared in the header file
MessageReadResult::MessageReadResult(std::unique_ptr<Message> msg)
    : msg(std::move(msg)) {}
MessageReadResult::MessageReadResult(MessageReadResult&&) noexcept = default;
MessageReadResult& MessageReadResult::operator=(MessageReadResult&&) noexcept =
    default;
MessageReadResult::~MessageReadResult() {}

}} // namespace facebook::logdevice
