/**
 *  Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 *  All rights reserved.
 *
 *  This source code is licensed under the BSD-style license found in the
 *  LICENSE file in the root directory of this source tree.
 **/

#include "logdevice/admin/maintenance/SequencerWorkflow.h"

namespace facebook { namespace logdevice { namespace maintenance {

folly::SemiFuture<MaintenanceStatus>
SequencerMaintenanceWorkflow::run(bool is_sequencer_enabled) {
  folly::Promise<MaintenanceStatus> p;
  return p.getFuture();
}

}}} // namespace facebook::logdevice::maintenance
