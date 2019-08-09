// Copyright 2004-present Facebook. All Rights Reserved.

#include "logdevice/common/FlowGroupDependencies.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Worker.h"

namespace facebook { namespace logdevice {

NwShapingFlowGroupDeps::NwShapingFlowGroupDeps(StatsHolder* stats,
                                               Sender* sender)
    : stats_(stats), sender_(sender) {}

bool NwShapingFlowGroupDeps::onCorrectInstance() {
  // sender_ is null in unit tests.
  return (sender_ == nullptr || &Worker::onThisThread()->sender() == sender_);
}
}} // namespace facebook::logdevice
