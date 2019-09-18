/**
 * Copyright (c) 2019-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/lib/checkpointing/CheckpointStoreImpl.h"

#include <folly/Format.h>
#include <folly/Optional.h>
#include <folly/synchronization/Baton.h>

#include "logdevice/common/toString.h"
#include "logdevice/common/util.h"

namespace facebook { namespace logdevice {

CheckpointStoreImpl::CheckpointStoreImpl(
    std::unique_ptr<VersionedConfigStore> vcs)
    : vcs_(std::move(vcs)) {}

Status CheckpointStoreImpl::updateLSNSync(const std::string& customer_id,
                                          logid_t log_id,
                                          lsn_t lsn) {
  folly::Baton<> update_baton;
  Status return_status = Status::OK;
  UpdateCallback cb =
      [&update_baton, &return_status](
          Status status, VersionedConfigStore::version_t, std::string) {
        return_status = status;
        update_baton.post();
      };

  updateLSN(customer_id, log_id, lsn, std::move(cb));
  update_baton.wait();

  return return_status;
}

void CheckpointStoreImpl::updateLSN(const std::string& customer_id,
                                    logid_t log_id,
                                    lsn_t lsn,
                                    UpdateCallback cb) {
  auto mcb = [log_id, lsn](folly::Optional<std::string> value) {
    Checkpoint value_thrift;
    if (value.hasValue()) {
      value_thrift = Serializer::deserialize<Checkpoint>(value.value());
    }
    value_thrift.log_lsn_map[static_cast<uint64_t>(log_id)] =
        static_cast<uint64_t>(lsn);
    auto serialized_thrift = Serializer::serialize<std::string>(value_thrift);
    return std::make_pair(Status::OK, std::move(serialized_thrift));
  };

  vcs_->readModifyWriteConfig(customer_id, std::move(mcb), std::move(cb));
};

}} // namespace facebook::logdevice
