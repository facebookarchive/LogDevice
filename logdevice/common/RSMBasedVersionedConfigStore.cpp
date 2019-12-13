/**
 * Copyright (c) 2018-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE RSM in the root directory of this source tree.
 */

#include "logdevice/common/RSMBasedVersionedConfigStore.h"

#include <thrift/lib/cpp2/protocol/Serializer.h>

#include "logdevice/common/ThriftCodec.h"

namespace facebook { namespace logdevice {

using apache::thrift::BinarySerializer;
using replicated_state_machine::thrift::KeyValueStoreDelta;
using replicated_state_machine::thrift::KeyValueStoreState;
using replicated_state_machine::thrift::UpdateValue;

RSMBasedVersionedConfigStore::RSMBasedVersionedConfigStore(
    logid_t log_id,
    extract_version_fn f,
    Processor* processor,
    std::chrono::milliseconds stop_timeout)
    : VersionedConfigStore(std::move(f)),
      state_machine_(std::make_unique<KeyValueStoreStateMachine>(log_id)),
      processor_(processor),
      stop_timeout_(stop_timeout) {
  auto cb = [this](const KeyValueStoreState& state,
                   const KeyValueStoreDelta*,
                   lsn_t version) {
    {
      // It should probably always override the state.
      auto locked_state = state_.wlock();
      if (version > locked_state->version) {
        locked_state->store = state.store;
        locked_state->version = version;
      }
    }
    ready_.store(true);
  };
  subscription_handle_ = state_machine_->subscribe(std::move(cb));

  std::unique_ptr<Request> rq = std::make_unique<
      StartReplicatedStateMachineRequest<KeyValueStoreStateMachine>>(
      state_machine_.get());
  processor_->postWithRetrying(rq);
}

RSMBasedVersionedConfigStore::~RSMBasedVersionedConfigStore() {
  shutdown();
}

void RSMBasedVersionedConfigStore::shutdown() {
  bool has_shutdown = shutdown_signaled_.exchange(true);
  if (!has_shutdown) {
    std::unique_ptr<Request> rq = std::make_unique<
        StopReplicatedStateMachineRequest<KeyValueStoreStateMachine>>(
        state_machine_.get());
    processor_->postWithRetrying(rq);

    bool success = state_machine_->wait(stop_timeout_);
    if (!success) {
      ld_error("RSMBasedVersionedConfigStore stop timeout expired.");
    }
  }
}

void RSMBasedVersionedConfigStore::getConfig(
    std::string key,
    value_callback_t cb,
    folly::Optional<version_t> base_version) const {
  if (shutdown_signaled_.load()) {
    cb(Status::SHUTDOWN, "");
    return;
  }
  if (!ready_.load()) {
    cb(Status::AGAIN, "");
    return;
  }

  std::string value;
  {
    auto locked_state = state_.rlock();
    auto it = locked_state->store.find(key);
    if (it == locked_state->store.end()) {
      cb(Status::NOTFOUND, "");
      return;
    }
    value = it->second;
  }

  if (base_version.hasValue()) {
    auto current_version_opt = (extract_fn_)(value);
    if (!current_version_opt) {
      RATELIMIT_WARNING(std::chrono::seconds(10),
                        5,
                        "Failed to extract version from value read from "
                        "RMSBasedVersionedConfigurationStore. key: \"%s\"",
                        key.c_str());
      cb(Status::BADMSG, "");
      return;
    }
    if (current_version_opt.value() <= base_version.value()) {
      cb(Status::UPTODATE, "");
      return;
    }
  }
  cb(Status::OK, std::move(value));
}

void RSMBasedVersionedConfigStore::getLatestConfig(std::string key,
                                                   value_callback_t cb) const {
  // TODO: Current implementation may return stale data, fix that.
  getConfig(std::move(key), std::move(cb));
}

void RSMBasedVersionedConfigStore::readModifyWriteConfig(
    std::string key,
    mutation_callback_t mcb,
    write_callback_t cb) {
  if (shutdown_signaled_.load()) {
    cb(Status::SHUTDOWN, version_t{}, "");
    return;
  }
  auto get_cb = [this, key, mcb = std::move(mcb), cb = std::move(cb)](
                    Status status, std::string current_value) mutable {
    if (status != Status::OK && status != Status::NOTFOUND) {
      cb(status, version_t{}, "");
      return;
    }

    folly::Optional<version_t> cur_ver = folly::none;
    if (status == Status::OK) {
      auto curr_version_opt = extract_fn_(current_value);
      if (!curr_version_opt) {
        cb(Status::BADMSG, version_t{}, "");
        return;
      }
      cur_ver = curr_version_opt.value();
    }

    auto status_value =
        (mcb)((status == Status::NOTFOUND)
                  ? folly::none
                  : folly::Optional<std::string>(current_value));
    auto& write_value = status_value.second;
    if (status_value.first != Status::OK) {
      cb(status_value.first, version_t{}, std::move(write_value));
      return;
    }

    auto optional_version = (extract_fn_)(write_value);
    if (!optional_version) {
      cb(Status::INVALID_PARAM, {}, "");
      return;
    }
    version_t new_version = optional_version.value();

    // TODO: Add stricter enforcement of monotonic increment of version.
    if (cur_ver.hasValue() && new_version.val() <= cur_ver.value().val()) {
      RATELIMIT_WARNING(std::chrono::seconds(10),
                        5,
                        "Config value's version is not monitonically increasing"
                        "key: \"%s\". prev version: \"%lu\". version: \"%lu\"",
                        key.c_str(),
                        cur_ver.value().val(),
                        new_version.val());
      cb(Status::VERSION_MISMATCH, cur_ver.value(), std::move(current_value));
      return;
    }

    auto cb_as_std = std::move(cb).asStdFunction();
    auto write_cb = [this, key, write_value, cb = cb_as_std, new_version](
                        Status status, lsn_t version, const std::string&) {
      updateStateEntry(key, write_value, version);
      cb(status, new_version, "");
    };

    UpdateValue update_value;
    update_value.key = key;
    update_value.value = write_value;
    KeyValueStoreDelta delta;
    delta.set_update_value(update_value);

    auto serialized_delta =
        ThriftCodec::serialize<apache::thrift::BinarySerializer>(delta);

    // TODO(T58949318) handle racing writes between the mutation, and the write.
    std::unique_ptr<Request> rq =
        std::make_unique<WriteDeltaRequest<KeyValueStoreStateMachine>>(
            state_machine_.get(),
            std::move(serialized_delta),
            std::move(write_cb));
    int rv = processor_->postWithRetrying(rq);
    if (rv != 0) {
      cb_as_std(Status::NOBUFS, {}, "");
    }
  };
  getConfig(key, std::move(get_cb));
}

void RSMBasedVersionedConfigStore::updateStateEntry(const std::string& key,
                                                    const std::string& value,
                                                    lsn_t version) {
  // It should probably always override the state.
  auto locked_state = state_.wlock();
  if (version > locked_state->version) {
    locked_state->store[key] = value;
    locked_state->version = version;
  }
}
}} // namespace facebook::logdevice
