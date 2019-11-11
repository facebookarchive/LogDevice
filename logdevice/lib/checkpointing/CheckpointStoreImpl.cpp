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
#include <folly/executors/GlobalExecutor.h>
#include <folly/io/async/EventBase.h>
#include <folly/synchronization/Baton.h>
#include <thrift/lib/cpp2/protocol/Serializer.h>

#include "logdevice/common/ThriftCodec.h"
#include "logdevice/common/toString.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/common/util.h"

namespace facebook { namespace logdevice {

using apache::thrift::BinarySerializer;
using checkpointing::thrift::Checkpoint;

namespace {
CheckpointStore::StatusCallback
statusCallbackPostingBaton(Status* return_status, folly::Baton<>& call_baton) {
  CheckpointStore::StatusCallback callback = [&call_baton,
                                              return_status](Status status) {
    *return_status = status;
    call_baton.post();
  };
  return callback;
}
} // namespace

CheckpointStoreImpl::CheckpointStoreImpl(
    std::unique_ptr<VersionedConfigStore> vcs,
    const std::string& prefix)
    : vcs_(std::move(vcs)),
      prefix_(prefix),
      event_base_(folly::getEventBase()),
      timer_(folly::HHWheelTimer::newTimer(event_base_)),
      holder_(this) {}

void CheckpointStoreImpl::getLSN(const std::string& customer_id,
                                 logid_t log_id,
                                 GetCallback gcb) const {
  auto cb =
      [this, ref = holder_.ref(), customer_id, log_id, gcb = std::move(gcb)](
          Status status, std::string value) mutable {
        if (status == Status::AGAIN) {
          // Currently, if the VCS is not ready, we schedule a timer to retry
          // getLSN function in kRetryDuration.
          if (!ref) {
            return;
          }
          auto get_lsn_no_args = [this,
                                  ref = holder_.ref(),
                                  customer_id,
                                  log_id,
                                  gcb = std::move(gcb)]() mutable {
            if (!ref) {
              return;
            }
            getLSN(customer_id, log_id, std::move(gcb));
          };
          event_base_->runInEventBaseThread(
              [this, get_lsn_no_args = std::move(get_lsn_no_args)]() mutable {
                timer_->scheduleTimeoutFn(
                    std::move(get_lsn_no_args), kRetryDuration);
              });
          return;
        }
        if (status != Status::OK) {
          gcb(status, lsn_t());
          return;
        }
        auto value_thrift =
            ThriftCodec::deserialize<BinarySerializer, Checkpoint>(
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
  vcs_->getLatestConfig(createKey(customer_id), std::move(cb));
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
                                    StatusCallback cb) {
  updateLSN(customer_id, {{log_id, lsn}}, std::move(cb));
};

void CheckpointStoreImpl::updateLSN(const std::string& customer_id,
                                    const std::map<logid_t, lsn_t>& checkpoints,
                                    StatusCallback cb) {
  auto modify_checkpoint = [checkpoints](Checkpoint& checkpoint) {
    for (auto [log_id, lsn] : checkpoints) {
      checkpoint.log_lsn_map[log_id.val()] = lsn;
    }
  };
  updateCheckpoints(customer_id, std::move(modify_checkpoint), std::move(cb));
}

Status CheckpointStoreImpl::updateLSNSync(
    const std::string& customer_id,
    const std::map<logid_t, lsn_t>& checkpoints) {
  Status status;
  folly::Baton<> call_baton;
  auto cb = statusCallbackPostingBaton(&status, call_baton);
  updateLSN(customer_id, checkpoints, std::move(cb));
  call_baton.wait();
  return status;
}

void CheckpointStoreImpl::removeCheckpoints(
    const std::string& customer_id,
    const std::vector<logid_t>& checkpoints,
    StatusCallback cb) {
  auto modify_checkpoint = [checkpoints](Checkpoint& checkpoint) {
    for (auto log_id : checkpoints) {
      checkpoint.log_lsn_map.erase(log_id.val());
    }
  };
  updateCheckpoints(customer_id, std::move(modify_checkpoint), std::move(cb));
}

void CheckpointStoreImpl::removeAllCheckpoints(const std::string& customer_id,
                                               StatusCallback cb) {
  auto modify_checkpoint = [](Checkpoint& checkpoint) {
    checkpoint.log_lsn_map.clear();
  };
  // TODO: Remove the whole checkpoint from the VCS.
  updateCheckpoints(customer_id, std::move(modify_checkpoint), std::move(cb));
}

Status CheckpointStoreImpl::removeCheckpointsSync(
    const std::string& customer_id,
    const std::vector<logid_t>& checkpoints) {
  Status status;
  folly::Baton<> call_baton;
  auto cb = statusCallbackPostingBaton(&status, call_baton);
  removeCheckpoints(customer_id, checkpoints, std::move(cb));
  call_baton.wait();
  return status;
}

Status
CheckpointStoreImpl::removeAllCheckpointsSync(const std::string& customer_id) {
  Status status;
  folly::Baton<> call_baton;
  auto cb = statusCallbackPostingBaton(&status, call_baton);
  removeAllCheckpoints(customer_id, std::move(cb));
  call_baton.wait();
  return status;
}

void CheckpointStoreImpl::updateCheckpoints(
    const std::string& customer_id,
    folly::Function<void(Checkpoint&) const> modify_checkpoint,
    StatusCallback cb) {
  auto mcb = [modify_checkpoint = std::move(modify_checkpoint)](
                 folly::Optional<std::string> value) {
    auto value_thrift = std::make_unique<Checkpoint>();
    if (value.hasValue()) {
      value_thrift = ThriftCodec::deserialize<BinarySerializer, Checkpoint>(
          Slice::fromString(value.value()));
      if (value_thrift == nullptr) {
        return std::make_pair(Status::BADMSG, std::string());
      }
    }
    modify_checkpoint(*value_thrift);
    value_thrift->version++;
    auto serialized_thrift =
        ThriftCodec::serialize<BinarySerializer>(*value_thrift);
    return std::make_pair(Status::OK, std::move(serialized_thrift));
  };

  auto ucb = [customer_id, cb = std::move(cb)](
                 Status status, CheckpointStore::Version, std::string) mutable {
    if (status == Status::VERSION_MISMATCH) {
      RATELIMIT_ERROR(
          std::chrono::minutes(1),
          1,
          "Got a VERSION_MISMATCH when writing to the checkpoint store, this "
          "means that there's potentially another reader using the same "
          "customer ID. Customer IDs should be used exclusively by a single "
          "reader instance. Multiple readers with the same customer ID can "
          "leave the checkpoint in an inconsistent state. Customer ID: %s",
          customer_id.c_str());
    }
    cb(status);
  };
  vcs_->readModifyWriteConfig(
      createKey(customer_id), std::move(mcb), std::move(ucb));
}

folly::Optional<CheckpointStore::Version>
CheckpointStoreImpl::extractVersion(folly::StringPiece value) {
  auto value_thrift = ThriftCodec::deserialize<BinarySerializer, Checkpoint>(
      Slice::fromString(value.toString()));
  if (value_thrift == nullptr) {
    return folly::none;
  }
  return CheckpointStore::Version(value_thrift->version);
}

std::string
CheckpointStoreImpl::createKey(const std::string& customer_id) const {
  if (prefix_.empty()) {
    return customer_id;
  }
  return prefix_ + customer_id;
}

}} // namespace facebook::logdevice
