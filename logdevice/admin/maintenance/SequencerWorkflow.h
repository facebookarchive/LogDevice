/**
 *  Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 *  All rights reserved.
 *
 *  This source code is licensed under the BSD-style license found in the
 *  LICENSE file in the root directory of this source tree.
 **/
#pragma once

#include <folly/futures/Future.h>

#include "logdevice/admin/maintenance/Workflow.h"
#include "logdevice/common/NodeID.h"

namespace facebook { namespace logdevice { namespace maintenance {
/**
 * A SequencerMaintenanceworkflow is a state machine that tracks state
 * transitions of a Sequencer node.
 */
class SequencerWorkflow : public Workflow {
 public:
  explicit SequencerWorkflow(NodeID node) : node_(node) {}

  folly::SemiFuture<MaintenanceStatus> run(bool is_sequencing_enabled);

  // Returns the target_op_state_
  SequencingState getTargetOpState() const;

 private:
  SequencingState target_op_state_;
  // The shard this workflow is for
  NodeID node_;
};

}}} // namespace facebook::logdevice::maintenance
