/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <string>

#include "logdevice/common/ExponentialBackoffAdaptiveVariable.h"
#include "logdevice/common/Timer.h"

namespace facebook { namespace logdevice {

class ClientReadStream;
class TimeoutMap;
class Timer;

/**
 * RewindScheduler allows scheduling a rewind. It uses
 * ChronoExponentialBackoffAdaptiveVariable to protect against too many rewinds
 * using an adaptive delay between each rewind.
 */
class RewindScheduler {
 public:
  RewindScheduler(ClientReadStream* owner);

  /**
   * Schedule a rewind.
   * @map    TimeoutMap to use for the underlying timer;
   * @reason Reason for this rewind. If another rewind was already scheduled,
   *         this value is appended to the existing reason.
   */
  void schedule(TimeoutMap* map, std::string reason);

  /**
   * Cancel any scheduled rewind.
   */
  void cancel();

  /**
   * @returns True if a rewind is scheduled.
   */
  bool isScheduled() const;

 private:
  ClientReadStream* owner_;

  // Reason for the rewind.
  std::string reason_;

  // When a rewind is scheduled, how long to wait until it is actually done.
  // This delay is adaptive. It increases multiplicatively and decreases
  // linearly over time. This is a protection against too many rewinds which
  // hurts read amplification.
  ChronoExponentialBackoffAdaptiveVariable<std::chrono::milliseconds> delay_{
      /*min=*/std::chrono::milliseconds{1},
      /*initial=*/std::chrono::milliseconds{1},
      /*max=*/std::chrono::seconds{10},
      /*multiplier=*/2,
      /*decrease_rate=*/100,
      /*fuzz_factor=*/0};

  std::unique_ptr<Timer> timer_;

  void rewind();
};

}} // namespace facebook::logdevice
