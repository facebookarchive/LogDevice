/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

namespace facebook { namespace logdevice {
/**
 * @file ConnectionKind enum represents the kind of connection is
 *       defined by the server-side port, so all connection coming to
 *       the same port are of the same kind and it is different from kinds
 *       of other ports.
 */

enum class ConnectionKind : uint8_t {
  DATA,
  DATA_LOW_PRIORITY,
  DATA_HIGH_PRIORITY,
  DATA_SSL,
  GOSSIP,
  SERVER_TO_SERVER,
  MAX,
};

}} // namespace facebook::logdevice
