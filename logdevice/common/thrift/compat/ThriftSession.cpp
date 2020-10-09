/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/thrift/compat/ThriftSession.h"

#include <memory>
#include <string>

#include <sys/stat.h>

#include "logdevice/common/NetworkDependencies.h"
#include "logdevice/common/checks.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/if/gen-cpp2/ApiModel_types.h"
#include "logdevice/common/if/gen-cpp2/LogDeviceAPIAsyncClient.h"
#include "logdevice/common/protocol/HELLO_Message.h"
#include "logdevice/common/protocol/MessageType.h"
#include "logdevice/common/settings/Settings.h"

using facebook::logdevice::thrift::LogDeviceAPIAsyncClient;

namespace facebook { namespace logdevice {

ThriftSession::ThriftSession(ConnectionInfo&& info, NetworkDependencies& deps)
    : info_(std::move(info)), deps_(deps), description_(info_.describe()) {}

void ThriftSession::setInfo(ConnectionInfo&& new_info) {
  // Peer name is not allowed to change
  ld_check(info_.peer_name == new_info.peer_name);

  if (*new_info.principal != *info_.principal) {
    // Only incoming connections are authenticated
    ld_check(new_info.peer_name.isClientAddress());
    // No changes to principal apart from one-time upgrade from empty
    ld_check(info_.principal->isEmpty());
  }
  info_ = std::move(new_info);
}

void ThriftSession::fillDebugInfo(InfoSocketsTable&) const {
  // TODO(mmhg): Implement this
  ld_check(false);
}

ServerSession::ServerSession(ConnectionInfo&& info, NetworkDependencies& deps)
    : ThriftSession(std::move(info), deps) {
  ld_check(info_.peer_name.isNodeAddress());
}

ClientSession::ClientSession(ConnectionInfo&& info, NetworkDependencies& deps)
    : ThriftSession(std::move(info), deps) {
  ld_check(info_.peer_name.isClientAddress());
}

}} // namespace facebook::logdevice
