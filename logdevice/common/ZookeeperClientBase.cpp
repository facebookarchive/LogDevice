/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/ZookeeperClientBase.h"

namespace facebook { namespace logdevice {

/* static */ Status ZookeeperClientBase::toStatus(int zk_rc) {
  switch (zk_rc) {
    case ZOK:
      return E::OK;
    case ZNONODE:
      return E::NOTFOUND;
    case ZNODEEXISTS:
      return E::EXISTS;

    case ZBADVERSION:
      return E::VERSION_MISMATCH;

    case ZNOAUTH:
    case ZAUTHFAILED:
      return E::ACCESS;

    case ZCONNECTIONLOSS:
    case ZOPERATIONTIMEOUT:
    case ZSESSIONEXPIRED:
    case ZSESSIONMOVED:
      return E::CONNFAILED;
    case ZCLOSING:
      return E::SHUTDOWN;

    case ZBADARGUMENTS:
      return E::INTERNAL;
    case ZMARSHALLINGERROR:
      return E::SYSLIMIT;

    case ZINVALIDSTATE:
    case ZRUNTIMEINCONSISTENCY:
      return E::FAILED;
  }

  RATELIMIT_ERROR(std::chrono::seconds(10),
                  5,
                  "Unknown / unexpected Zookeeper error code %d",
                  zk_rc);
  return E::UNKNOWN;
}

}} // namespace facebook::logdevice
