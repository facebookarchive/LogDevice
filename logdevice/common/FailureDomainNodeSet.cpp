/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/FailureDomainNodeSet.h"

namespace facebook { namespace logdevice {

const char* fmajorityResultString(FmajorityResult result) {
  switch (result) {
    case FmajorityResult::NONE:
      return "NONE";
    case FmajorityResult::AUTHORITATIVE_COMPLETE:
      return "AUTHORITATIVE_COMPLETE";
    case FmajorityResult::AUTHORITATIVE_INCOMPLETE:
      return "AUTHORITATIVE_INCOMPLETE";
    case FmajorityResult::NON_AUTHORITATIVE:
      return "NON_AUTHORITATIVE";
    default:
      ld_check(false);
      return "invalid";
  }
}

}} // namespace facebook::logdevice
