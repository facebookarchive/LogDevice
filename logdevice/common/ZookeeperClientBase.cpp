/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/ZookeeperClientBase.h"

using namespace facebook::logdevice::zk::detail;

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
    case ZCLOSING:
      return E::CONNFAILED;

    case ZBADARGUMENTS:
    case ZNOCHILDRENFOREPHEMERALS:
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

/* static */ zk::Op
ZookeeperClientBase::makeCreateOp(std::string path,
                                  std::string data,
                                  int32_t flags,
                                  std::vector<zk::ACL> acl) {
  zk::Op op;
  op.op_ = CreateOp{std::move(path), std::move(data), flags, std::move(acl)};
  return op;
}

/* static */ zk::Op ZookeeperClientBase::makeDeleteOp(std::string path,
                                                      zk::version_t version) {
  zk::Op op;
  op.op_ = DeleteOp{std::move(path), version};
  return op;
}

/* static */ zk::Op ZookeeperClientBase::makeSetOp(std::string path,
                                                   std::string data,
                                                   zk::version_t version) {
  zk::Op op;
  op.op_ = SetOp{std::move(path), std::move(data), version};
  return op;
}

/* static */ zk::Op ZookeeperClientBase::makeCheckOp(std::string path,
                                                     zk::version_t version) {
  zk::Op op;
  op.op_ = CheckOp{std::move(path), version};
  return op;
}

}} // namespace facebook::logdevice
