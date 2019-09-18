/**
 * Copyright (c) 2019-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <thrift/lib/cpp2/protocol/Serializer.h>

#include "logdevice/common/VersionedConfigStore.h"
#include "logdevice/include/CheckpointStore.h"
#include "logdevice/include/Err.h"
#include "logdevice/lib/checkpointing/if/gen-cpp2/Checkpoint_types.h"

namespace facebook { namespace logdevice {
/*
 * @file CheckpointStoreImpl implements CheckpointStore. It stores LSNs for logs
 *       using VersionedConfigStore.
 */
class CheckpointStoreImpl : public CheckpointStore {
 public:
  using Serializer = apache::thrift::SimpleJSONSerializer;
  using Checkpoint = checkpointing::thrift::Checkpoint;

  explicit CheckpointStoreImpl(std::unique_ptr<VersionedConfigStore> vcs);

  Status updateLSNSync(const std::string& customer_id,
                       logid_t log_id,
                       lsn_t lsn) override;

  void updateLSN(const std::string& customer_id,
                 logid_t log_id,
                 lsn_t lsn,
                 UpdateCallback cb) override;

 private:
  std::unique_ptr<VersionedConfigStore> vcs_;
};

}} // namespace facebook::logdevice
