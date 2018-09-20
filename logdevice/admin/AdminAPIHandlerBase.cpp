/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/admin/AdminAPIHandlerBase.h"
#include "logdevice/server/Server.h"
#include "logdevice/server/ServerProcessor.h"

namespace facebook { namespace logdevice {
AdminAPIHandlerBase::AdminAPIHandlerBase(Server* server)
    : ld_server_(server), processor_(server->getServerProcessor()){};

}} // namespace facebook::logdevice
