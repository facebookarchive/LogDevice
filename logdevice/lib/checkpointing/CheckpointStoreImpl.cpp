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

namespace facebook { namespace logdevice {

using apache::thrift::BinarySerializer;
using checkpointing::thrift::Checkpoint;

CheckpointStoreImpl::CheckpointStoreImpl(
    std::unique_ptr<VersionedConfigStore> vcs)
    : vcs_(std::move(vcs)) {}

void CheckpointStoreImpl::getLSN(const std::string& customer_id,
                                 logid_t log_id,
                                 GetCallback gcb) const {
  // TODO: Handle versions
  auto version = folly::none;
  auto cb = [log_id, gcb = std::move(gcb)](
                Status status, std::string value) mutable {
    if (status != Status::OK) {
      gcb(status, lsn_t());
      return;
    }
    auto value_thrift = ThriftCodec::deserialize<BinarySerializer, Checkpoint>(
        Slice::fromString(value));
    if (value_thrift == nullptr) {
      gcb(Status::BADMSG, LSN_INVALID);
      return;
    }
    if (value_thrift->log_lsn_map.count(log_id.val())) {
      auto lsn = value_thrift->log_lsn_map[log_id.val()];
      gcb(Status::OK, lsn);
    } else {
      gcb(Status::NOTFOUND, lsn_t());
    }
  };
  vcs_->getConfig(customer_id, std::move(cb), version);
}

Status CheckpointStoreImpl::getLSNSync(const std::string& customer_id,
                                       logid_t log_id,
                                       lsn_t* value_out) const {
  folly::Baton<> get_baton;
  Status return_status = Status::OK;
  GetCallback cb = [&get_baton, &return_status, &value_out](
                       Status status, lsn_t lsn) mutable {
    return_status = status;
    set_if_not_null(value_out, lsn);
    get_baton.post();
  };
  getLSN(customer_id, log_id, std::move(cb));
  get_baton.wait();
  return return_status;
};

Status CheckpointStoreImpl::updateLSNSync(const std::string& customer_id,
                                          logid_t log_id,
                                          lsn_t lsn) {
  return updateLSNSync(customer_id, {{log_id, lsn}});
}

void CheckpointStoreImpl::updateLSN(const std::string& customer_id,
                                    logid_t log_id,
                                    lsn_t lsn,
                                    UpdateCallback cb) {
  updateLSN(customer_id, {{log_id, lsn}}, std::move(cb));
};

void CheckpointStoreImpl::updateLSN(const std::string& customer_id,
                                    const std::map<logid_t, lsn_t>& checkpoints,
                                    UpdateCallback cb) {
  auto modify_checkpoint = [&checkpoints](Checkpoint& checkpoint) {
    for (auto [log_id, lsn] : checkpoints) {
      checkpoint.log_lsn_map[log_id.val()] = lsn;
    }
  };
  updateCheckpoints(customer_id, std::move(modify_checkpoint), std::move(cb));
}

Status CheckpointStoreImpl::updateLSNSync(
    const std::string& customer_id,
    const std::map<logid_t, lsn_t>& checkpoints) {
  folly::Baton<> update_baton;
  Status return_status = Status::OK;
  UpdateCallback cb =
      [&update_baton, &return_status](
          Status status, VersionedConfigStore::version_t, std::string) {
        return_status = status;
        update_baton.post();
      };

  updateLSN(customer_id, checkpoints, std::move(cb));
  update_baton.wait();

  return return_status;
}

void CheckpointStoreImpl::removeCheckpoints(
    const std::string& customer_id,
    const std::vector<logid_t>& checkpoints,
    UpdateCallback cb) {
  auto modify_checkpoint = [&checkpoints](Checkpoint& checkpoint) {
    for (auto log_id : checkpoints) {
      checkpoint.log_lsn_map.erase(log_id.val());
    }
  };
  updateCheckpoints(customer_id, std::move(modify_checkpoint), std::move(cb));
}

void CheckpointStoreImpl::removeAllCheckpoints(const std::string& customer_id,
                                               UpdateCallback cb) {
  auto modify_checkpoint = [](Checkpoint& checkpoint) {
    checkpoint.log_lsn_map.clear();
  };
  // TODO: Remove the whole checkpoint from the VCS.
  updateCheckpoints(customer_id, std::move(modify_checkpoint), std::move(cb));
}

void CheckpointStoreImpl::updateCheckpoints(
    const std::string& customer_id,
    folly::Function<void(Checkpoint&)> modify_checkpoint,
    UpdateCallback cb) {
  auto mcb = [modify_checkpoint = std::move(modify_checkpoint)](
                 folly::Optional<std::string> value) mutable {
    auto value_thrift = std::make_unique<Checkpoint>();
    if (value.hasValue()) {
      value_thrift = ThriftCodec::deserialize<BinarySerializer, Checkpoint>(
          Slice::fromString(value.value()));
      if (value_thrift == nullptr) {
        return std::make_pair(Status::BADMSG, std::string());
      }
    }
    modify_checkpoint(*value_thrift);
    auto serialized_thrift =
        ThriftCodec::serialize<BinarySerializer>(*value_thrift);
    return std::make_pair(Status::OK, std::move(serialized_thrift));
  };

  vcs_->readModifyWriteConfig(customer_id, std::move(mcb), std::move(cb));
}

}} // namespace facebook::logdevice
