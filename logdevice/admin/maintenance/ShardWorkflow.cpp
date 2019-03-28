/**
 *  Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 *  All rights reserved.
 *
 *  This source code is licensed under the BSD-style license found in the
 *  LICENSE file in the root directory of this source tree.
 **/

#include "logdevice/admin/maintenance/ShardWorkflow.h"

namespace facebook { namespace logdevice { namespace maintenance {

folly::SemiFuture<MaintenanceStatus>
ShardMaintenanceWorkflow::run(membership::StorageState storage_state,
                              AuthoritativeStatus auth_status) {
  folly::Promise<MaintenanceStatus> p;
  return p.getFuture();
}
}}} // namespace facebook::logdevice::maintenance
