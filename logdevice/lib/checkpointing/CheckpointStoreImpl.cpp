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
#include <thrift/lib/cpp2/protocol/Serializer.h>

#include "logdevice/common/ThriftCodec.h"
#include "logdevice/common/toString.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/common/util.h"
#include "logdevice/lib/checkpointing/if/gen-cpp2/Checkpoint_types.h"

namespace facebook { namespace logdevice {

using apache::thrift::BinarySerializer;
using checkpointing::thrift::Checkpoint;

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
    auto value_thrift = std::make_unique<Checkpoint>();
    if (value.hasValue()) {
      value_thrift = ThriftCodec::deserialize<BinarySerializer, Checkpoint>(
          Slice::fromString(value.value()));
      if (value_thrift == nullptr) {
        return std::make_pair(Status::INVALID_PARAM, std::string());
      }
    }
    value_thrift->log_lsn_map[static_cast<uint64_t>(log_id)] =
        static_cast<uint64_t>(lsn);
    auto serialized_thrift =
        ThriftCodec::serialize<BinarySerializer>(*value_thrift);
    return std::make_pair(Status::OK, std::move(serialized_thrift));
  };

  vcs_->readModifyWriteConfig(customer_id, std::move(mcb), std::move(cb));
};

}} // namespace facebook::logdevice
