/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/Synchronized.h>

#include "logdevice/common/configuration/logs/FBuffersLogsConfigCodec.h"
#include "logdevice/common/configuration/logs/LogsConfigDeltaTypes.h"
#include "logdevice/common/configuration/logs/LogsConfigTree.h"
#include "logdevice/common/replicated_state_machine/ReplicatedStateMachine.h"
#include "logdevice/common/replicated_state_machine/TrimRSMRetryHandler.h"
#include "logdevice/include/ConfigSubscriptionHandle.h"

namespace facebook { namespace logdevice {
namespace logsconfig {
class Delta;
class LogsConfigTree;
class DirectoryNode;
} // namespace logsconfig

/**
 * LogsConfigStateMachine is a replicated state machine that maintains the
 * LogsConfig state in memory (LogsConfigTree). for detailed information on how
 * this works, check out the documentation on ReplicatatedStateMachine.h
 */
class LogsConfigStateMachine
    : public ReplicatedStateMachine<logsconfig::LogsConfigTree,
                                    logsconfig::Delta> {
 public:
  using Parent =
      ReplicatedStateMachine<logsconfig::LogsConfigTree, logsconfig::Delta>;
  LogsConfigStateMachine(
      UpdateableSettings<Settings> settings,
      std::shared_ptr<UpdateableServerConfig> updateable_server_config,
      bool is_writable,
      bool allow_snapshotting = true);

  void writeDelta(
      std::string payload,
      std::function<
          void(Status st, lsn_t version, const std::string& failure_reason)> cb,
      WriteMode mode = WriteMode::CONFIRM_APPLIED);
  /**
   * Serialization and Deserialization Helpers
   */
  static std::unique_ptr<logsconfig::Delta>
  deserializeDelta(std::string payload, std::string delimiter);

  static std::string serializeLogGroup(const logsconfig::LogGroupNode& lg);

  static std::string serializeDirectory(const logsconfig::DirectoryNode& dir);

  static std::unique_ptr<logsconfig::LogGroupNode>
  deserializeLogGroup(const std::string& payload, const std::string& delimiter);

  static std::unique_ptr<logsconfig::DirectoryNode>
  deserializeDirectory(const std::string& payload,
                       const std::string& delimiter);

  ~LogsConfigStateMachine() override {}

  /**
   * Currently called by AdminCommand class
   */
  virtual void snapshot(std::function<void(Status st)> cb) override;

 protected:
  virtual bool canSnapshot() const override;

  bool canTrimAndSnapshot() const;

  bool shouldTrim() const;

  bool shouldCreateSnapshot() const;

  void onUpdate(const logsconfig::LogsConfigTree&,
                const logsconfig::Delta*,
                lsn_t);

  // Snapshot creation completion callback. On success, also issue a request to
  // trim the RSM if possible.
  void onSnapshotCreated(Status st, size_t snapshotSize) override;

  void trim();

  std::unique_ptr<logsconfig::LogsConfigTree>
  makeDefaultState(lsn_t version) const override;

  std::unique_ptr<logsconfig::LogsConfigTree>
  deserializeState(Payload payload,
                   lsn_t version,
                   std::chrono::milliseconds timestamp) const override;

  std::unique_ptr<logsconfig::Delta> deserializeDelta(Payload payload) override;

  int applyDelta(const logsconfig::Delta& delta,
                 logsconfig::LogsConfigTree& tree,
                 lsn_t version,
                 std::chrono::milliseconds timestamp,
                 std::string& failure_reason) override;

  int serializeState(const logsconfig::LogsConfigTree& tree,
                     void* buf,
                     size_t size) override;

  StatsHolder* getStats();

  UpdateableSettings<Settings> settings_;
  std::shared_ptr<UpdateableServerConfig> updateable_server_config_;
  folly::Synchronized<std::string> delimiter_;
  ConfigSubscriptionHandle server_config_updates_handle_;
  std::unique_ptr<SubscriptionHandle> self_subscription_;
  std::unique_ptr<TrimRSMRetryHandler> trim_retry_handler_;
  bool is_writable_;
  bool allow_snapshotting_;
};
}} // namespace facebook::logdevice
