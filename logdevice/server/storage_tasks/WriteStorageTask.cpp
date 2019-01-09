/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/storage_tasks/WriteStorageTask.h"

#include <algorithm>

#include <folly/small_vector.h>

namespace facebook { namespace logdevice {

void WriteStorageTask::execute() {
  // WriteStorageTask execution actually happens in WriteBatchStorageTask to
  // allow writes to be batched.  We should not ever be here.
  ld_check(false);
}

Durability WriteStorageTask::durability() const {
  size_t num_write_ops = getNumWriteOps();

  if (status_ != E::OK || num_write_ops == 0) {
    return Durability::INVALID;
  }

  folly::small_vector<const WriteOp*, 16> write_ops(num_write_ops);
  size_t write_ops_written = getWriteOps(write_ops.data(), write_ops.size());
  ld_check(num_write_ops == write_ops_written);
  (void)write_ops_written;

  Durability max_durability = Durability::INVALID;
  for (auto& op : write_ops) {
    if (op->durability() != Durability::INVALID &&
        (max_durability == Durability::INVALID ||
         op->durability() > max_durability)) {
      max_durability = op->durability();
    }
  }
  return max_durability;
}

FlushToken WriteStorageTask::syncToken() const {
  size_t num_write_ops = getNumWriteOps();

  if (status_ != E::OK || num_write_ops == 0) {
    return FlushToken_INVALID;
  }

  folly::small_vector<const WriteOp*, 16> write_ops(num_write_ops);
  size_t write_ops_written = getWriteOps(write_ops.data(), write_ops.size());
  ld_check(num_write_ops == write_ops_written);
  (void)write_ops_written;

  return (*std::max_element(write_ops.begin(),
                            write_ops.end(),
                            [](auto lhs, auto rhs) {
                              return lhs->syncToken() < rhs->syncToken();
                            }))
      ->syncToken();
}
}} // namespace facebook::logdevice
