/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/AppendProbeController.h"

#include "logdevice/common/debug.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/util.h"

namespace facebook { namespace logdevice {

bool AppendProbeController::shouldProbe(NodeID node_id, logid_t) {
  constexpr bool PROBE = true;
  constexpr bool NO_PROBE = false;

  folly::SharedMutex::ReadHolder guard(node_state_map_mutex_);
  NodeStateMap::const_iterator it = node_state_map_.find(node_id);
  if (it == node_state_map_.end()) {
    // Implicitly HEALTHY, do not probe
    return NO_PROBE;
  }

  const State& state = it->second;
  switch (state.e.load()) {
    case State::Enum::HEALTHY:
      return NO_PROBE;
    case State::Enum::LAST_APPEND_FAILED:
      return PROBE;
    case State::Enum::RECOVERING:
      return now() < state.healthy_at.load() ? PROBE : NO_PROBE;
    default:
      std::abort();
  }
}

void AppendProbeController::onSuccess(NodeID node_id, logid_t) {
  folly::SharedMutex::ReadHolder guard(node_state_map_mutex_);
  NodeStateMap::iterator it = node_state_map_.find(node_id);
  if (it == node_state_map_.end()) {
    // Implicitly already HEALTHY, nothing to do
    return;
  }

  State* state = &it->second;
  State::Enum prev = state->e.load();
  // We don't need to be super-tolerant to races here (no CAS loop).  In the
  // presence of concurrent onFailure() calls, whether we stay in
  // LAST_APPEND_FAILED or go to RECOVERING is a coin flip anyway.  In the
  // presence of concurrent onSuccess() calls, it is enough for one thread to
  // make the state transition.
  switch (prev) {
    case State::Enum::HEALTHY:
      break;
    case State::Enum::LAST_APPEND_FAILED:
      // We had a failure but now we succeeded, transition to RECOVERING
      if (state->e.compare_exchange_weak(prev, State::Enum::RECOVERING)) {
        ld_debug("Transitioned %s to RECOVERING", node_id.toString().c_str());
        state->healthy_at.store(now() + recovery_interval_);
      }
      break;
    case State::Enum::RECOVERING:
      // Success after success.  If the recovery interval has elapsed,
      // return to HEALTHY.
      if (now() >= state->healthy_at.load()) {
        if (state->e.compare_exchange_weak(prev, State::Enum::HEALTHY)) {
          ld_debug("Transitioned %s to HEALTHY", node_id.toString().c_str());
          guard.unlock();
          garbageCollectIfHealthy(node_id);
          // `it' and `state' likely to be invalid at this point
          state = nullptr;
        }
      }
      break;
  }
}

void AppendProbeController::onFailure(NodeID node_id, logid_t, Status status) {
  if (status == E::PREEMPTED || status == E::ACCESS ||
      status == E::NOTINSERVERCONFIG) {
    // These errors do not indicate something wrong with the node so ignore
    // them for probe calculations.
    return;
  }

  folly::SharedMutex::ReadHolder read_guard(node_state_map_mutex_);
  folly::SharedMutex::WriteHolder write_guard(nullptr);
  NodeStateMap::iterator it = node_state_map_.find(node_id);
  if (it == node_state_map_.end()) {
    // Not using proper UpgradeHolder here since in most cases we don't need to
    // upgrade and we don't need to exclude other writers during the upgrade
    read_guard.unlock();
    write_guard = folly::SharedMutex::WriteHolder(node_state_map_mutex_);
    it = node_state_map_
             .emplace(std::piecewise_construct,
                      std::forward_as_tuple(node_id),
                      std::forward_as_tuple(State::Enum::LAST_APPEND_FAILED))
             .first;
    // We're not guaranteed to have inserted (someone might have beaten us
    // to upgrading the lock) so fall through.
  }
  // State machine transitions in case of a failure are simple: move/stay in
  // the LAST_APPEND_FAILED state.
  State::Enum prev = it->second.e.exchange(State::Enum::LAST_APPEND_FAILED);
  if (prev != State::Enum::LAST_APPEND_FAILED) {
    ld_debug(
        "Transitioned %s to LAST_APPEND_FAILED", node_id.toString().c_str());
  }
}

void AppendProbeController::garbageCollectIfHealthy(NodeID node_id) {
  folly::SharedMutex::WriteHolder guard(node_state_map_mutex_);
  NodeStateMap::iterator it = node_state_map_.find(node_id);
  if (it != node_state_map_.end() &&
      it->second.e.load() == State::Enum::HEALTHY) {
    // Because we are holding the writer lock, we are guaranteed that all
    // readers are excluded; nobody else is doing anything with the map so it
    // is safe to GC the entry.
    node_state_map_.erase(it);
  }
}

}} // namespace facebook::logdevice
