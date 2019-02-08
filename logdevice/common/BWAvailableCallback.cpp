/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/BWAvailableCallback.h"

#include "logdevice/common/FlowGroup.h"

namespace facebook { namespace logdevice {

void BWAvailableCallback::deactivate() {
  if (flow_group_links_.is_linked()) {
    // To ensure priority state is accurate, we have to let
    // the FlowGroup's priorityq do the unlink for us.
    ld_check(flow_group_ != nullptr);
    flow_group_->erase(*this);
  }
  links_.unlink();
}

void BWAvailableCallback::setAffiliation(FlowGroup* fg, Priority p) {
  ld_check(!active());
  flow_group_ = fg;
  priority_ = p;
}

}} // namespace facebook::logdevice
