/**
 * Copyright (c) 2019-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/Synchronized.h>

#include "logdevice/common/Processor.h"
#include "logdevice/common/VersionedConfigStore.h"
#include "logdevice/common/replicated_state_machine/KeyValueStoreStateMachine.h"

namespace facebook { namespace logdevice {

/**
 * RSMBasedVersionedConfigStore implements VersionedConfigStore interface.
 * The config is stored in a log. Each record represents a single update or
 * remove for given key. All the records form a state which is a current config.
 */
class RSMBasedVersionedConfigStore : public VersionedConfigStore {
 public:
  /**
   * @param stop_timeout: timeout for the replicated state machine to stop since
   *   the beginning of shutdown.
   */
  explicit RSMBasedVersionedConfigStore(logid_t log_id,
                                        extract_version_fn f,
                                        Processor* processor,
                                        std::chrono::milliseconds stop_timeout);

  ~RSMBasedVersionedConfigStore() override;

  void getConfig(std::string key,
                 value_callback_t cb,
                 folly::Optional<version_t> base_version = {}) const override;
  void getLatestConfig(std::string key, value_callback_t cb) const override;

  void readModifyWriteConfig(std::string key,
                             mutation_callback_t mcb,
                             write_callback_t cb) override;

  void shutdown() override;

 private:
  void updateStateEntry(const std::string& key,
                        const std::string& value,
                        lsn_t version);

  std::unique_ptr<KeyValueStoreStateMachine> state_machine_;
  std::unique_ptr<ReplicatedStateMachine<
      replicated_state_machine::thrift::KeyValueStoreState,
      replicated_state_machine::thrift::KeyValueStoreDelta>::SubscriptionHandle>
      subscription_handle_;
  folly::Synchronized<replicated_state_machine::thrift::KeyValueStoreState>
      state_;
  /**
   * The processor is used to perform RSM operations on the client side.
   */
  Processor* processor_;
  bool ready_;
  std::atomic<bool> shutdown_signaled_{false};
  std::chrono::milliseconds stop_timeout_;
};

}} // namespace facebook::logdevice
