/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>

#include "logdevice/common/AllSequencers.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/SequencerPlacement.h"
#include "logdevice/common/debug.h"

namespace facebook { namespace logdevice {

/**
 * @file  Implementation of SequencerPlacement that activates Sequencers for
 *        all logs in the configuration.
 */

class StaticSequencerPlacement : public SequencerPlacement {
 public:
  explicit StaticSequencerPlacement(Processor* processor) {
    ld_check(processor != nullptr);
    auto start_time = std::chrono::steady_clock::now();
    int rv = processor->allSequencers().activateAllSequencers(
        std::chrono::seconds(10), "static sequencer placement");
    if (rv != 0) {
      ld_error("Failed to activate sequencers for all logs: err = %s",
               error_name(err));
      throw ConstructorFailed();
    } else {
      auto end_time = std::chrono::steady_clock::now();
      ld_info("Activated sequencers for all logs in %.3fs.",
              std::chrono::duration_cast<std::chrono::duration<double>>(
                  end_time - start_time)
                  .count());
    }
  }
};

}} // namespace facebook::logdevice
