/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/PeriodicReleases.h"

#include <utility>

#include "logdevice/common/AllSequencers.h"
#include "logdevice/common/MetaDataLogWriter.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Sequencer.h"
#include "logdevice/common/Timer.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/protocol/RELEASE_Message.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/util.h"

namespace facebook { namespace logdevice {

PeriodicReleases::PerShardState::PerShardState()
    : last_released_lsn(0), lng(0) {}

PeriodicReleases::PerShardState::PerShardState(const PerShardState& rhs)
    : last_released_lsn(rhs.last_released_lsn.load()), lng(rhs.lng.load()) {}

void PeriodicReleases::PerShardState::operator=(const PerShardState& rhs) {
  if (this != &rhs) {
    last_released_lsn = rhs.last_released_lsn.load();
    lng = rhs.lng.load();
  }
}

PeriodicReleases::PeriodicReleases(Sequencer* sequencer)
    : sequencer_(sequencer),
      node_release_map_(std::make_shared<NodeReleaseMap>()) {}

void PeriodicReleases::startBroadcasting() {
  bool already_broadcasting = broadcasting_.exchange(true);
  if (!already_broadcasting) {
    auto req =
        std::make_unique<StartBroadcastingReleaseRequest>(weak_from_this());
    postBroadcastingRequest(std::move(req));
  }
}

int PeriodicReleases::sendReleases(Type type) {
  if (isShuttingDown()) {
    return 0;
  }

  ld_spew("Sending Releases for log %lu, Type %d.", getLogID().val_, (int)type);

  // Load lng first to avoid the case where lng and last_released are
  // concurrently updated to the same lsn, but we could see last_released < lng
  // if last_released was loaded first. It is okay to see last_released > lng.
  // In this case, we can safely assume there has been a concurrent update, and
  // we will want to send a single, global release message.
  lsn_t lng = getLastKnownGood();
  lsn_t last_released = getLastReleased();

  Sequencer::SendReleasesPred pred = nullptr;
  switch (type) {
    case PeriodicReleases::Type::BROADCAST: {
      // for broadcast, always send to all shards
      pred = [](lsn_t /* unused */,
                ReleaseType /* unused */,
                ShardID /* unused */) -> bool { return true; };
    } break;
    case PeriodicReleases::Type::RETRY: {
      // for retry, construct the predicate based on per-shard progress
      std::shared_ptr<const NodeReleaseMap> node_map = node_release_map_.get();
      // a map should always be populated throughout the lifetime of this class
      ld_check(node_map != nullptr);

      // Predicate function for use with Sequencer::sendReleases() below. Avoids
      // sending RELEASE messages to shards we tracked and know already have
      // successfully sent a RELEASE with larger LSN.
      pred = [node_map](
                 lsn_t lsn, ReleaseType release_type, ShardID shard) -> bool {
        const PerShardState* shard_state =
            findPerShardState(node_map.get(), shard);
        if (shard_state == nullptr) {
          // Since updating the tracked shard is done outside of sequencer's
          // state mutex (see Sequencer::onMetaDataMapUpdate()). There is a
          // chance that the shard here is currently not tracked in periodic
          // release, we should always send in such case.
          return true;
        }
        switch (release_type) {
          case ReleaseType::GLOBAL:
            return shard_state->last_released_lsn.load() < lsn;
          case ReleaseType::PER_EPOCH:
            return shard_state->lng.load() < lsn;
          case ReleaseType::INVALID:
            break;
        }
        ld_check(false);
        return false;
      };
    } break;
  };

  ld_check(pred != nullptr);

  // First send global release message to shards that haven't caught up,
  // then possibly send per-epoch release message to shards that haven't
  // caught up with per-epoch release. No need to send per-epoch release
  // message if ESN is invalid (no released records in epoch yet, or feature
  // disabled), or global last_released is caught up to per-epoch lng.

  bool send_failed = false;
  if (last_released != LSN_INVALID) {
    send_failed = (sendReleaseToStorageSets(
                       last_released, ReleaseType::GLOBAL, pred) != 0);
  }
  if (lsn_to_esn(lng) != ESN_INVALID && lng > last_released) {
    send_failed |=
        sendReleaseToStorageSets(lng, ReleaseType::PER_EPOCH, pred) != 0;
  }

  return send_failed ? -1 : 0;
}

void PeriodicReleases::schedule() {
  if (isShuttingDown()) {
    return;
  }

  State prev = state_.load();
  State next;

  do {
    if (prev == State::NEEDS_REACTIVATION) {
      // NEEDS_REACTIVATION -(schedule)-> NEEDS_REACTIVATION
      return;
    }
    next = State(int(prev) + 1);
  } while (!state_.compare_exchange_weak(prev, next));

  if (prev != State::INACTIVE) {
    // ACTIVE -(schedule)-> NEEDS_REACTIVATION
    return;
  }
  // INACTIVE -(schedule)-> ACTIVE
  startPeriodicReleaseTimer(Type::RETRY);
}

void PeriodicReleases::timerCallback(ExponentialBackoffTimerNode* timer_node,
                                     Type type) {
  if (isShuttingDown()) {
    return;
  }

  // for broadcast timer, just send all releases as a best effort attempt.
  if (type == PeriodicReleases::Type::BROADCAST) {
    if (shouldSendBroadcast()) {
      sendReleases(type);
    }
    // always schedule the next batch of broadcast regardless of the release
    // status
    activatePeriodicReleaseTimer(timer_node, PeriodicReleases::Type::BROADCAST);
    return;
  }

  ld_check(type == PeriodicReleases::Type::RETRY);

  int rv = sendReleases(type);
  if (rv != 0) {
    // failure to send any of the releases requires us to reactivate a timer
    // and is treated the same as being in the NEEDS_REACTIVATION state
    State prev = state_.exchange(State::NEEDS_REACTIVATION);
    ld_check(prev == State::ACTIVE || prev == State::NEEDS_REACTIVATION);
  }

  State prev = state_.load();
  State next;

  do {
    // If state_ == INACTIVE, either a timer has not been created yet, or
    // we've just moved to it from ACTIVE and are about to destroy it.
    ld_check(prev != State::INACTIVE && "State::INACTIVE in timerCallback");

    next = State(int(prev) - 1);
  } while (!state_.compare_exchange_weak(prev, next));

  switch (prev) {
    case State::NEEDS_REACTIVATION:
      // NEEDS_REACTIVATION -(timer fire)-> ACTIVE
      ld_check(next == State::ACTIVE);
      activatePeriodicReleaseTimer(timer_node, PeriodicReleases::Type::RETRY);
      break;
    case State::ACTIVE:
      // ACTIVE -(timer fire)-> INACTIVE
      ld_check(next == State::INACTIVE);
      cancelPeriodicReleaseTimer(timer_node, PeriodicReleases::Type::RETRY);
      break;
    case State::INACTIVE:
      // timer shouldn't fire in INACTIVE state
      RATELIMIT_CRITICAL(std::chrono::seconds(10),
                         10,
                         "Timer callback fired in INACTIVE state for log %lu!",
                         getLogID().val_);
      ld_check(false);
      break;
  }
}

void PeriodicReleases::noteReleaseSuccessful(ShardID shard,
                                             lsn_t released_lsn,
                                             ReleaseType release_type) {
  if (isShuttingDown()) {
    return;
  }

  ld_spew("Successful %s RELEASE of %s log %lu on shard %s",
          release_type_to_string(release_type).c_str(),
          lsn_to_string(released_lsn).c_str(),
          getLogID().val_,
          shard.toString().c_str());

  std::shared_ptr<NodeReleaseMap> node_map = node_release_map_.get();
  ld_check(node_map != nullptr);

  PerShardState* shard_state = findPerShardState(node_map.get(), shard);
  if (shard_state == nullptr) {
    // the shard is currently not tracked. Ignore and do not update its
    // state for now. this should be rare since we update the shard map to
    // include a new storage set as soon as the sequencer updates its
    // historical storage sets.
    RATELIMIT_INFO(std::chrono::seconds(10),
                   10,
                   "%s Release of %s successfully sent to shard %s "
                   "for log %lu but the shard is not tracked.",
                   release_type_to_string(release_type).c_str(),
                   lsn_to_string(released_lsn).c_str(),
                   shard.toString().c_str(),
                   getLogID().val_);
    return;
  }

  auto update_lsn = [](std::atomic<lsn_t>& atom, lsn_t new_lsn) {
    // Set atom to new_lsn if it's higher. This is racy on purpose. Doing a
    // load followed by a store (without any memory barriers) is faster than
    // cmpxchg. In the worst case (if a smaller LSN overwrites a larger one)
    // we'll just send an extra RELEASE.
    if (new_lsn > atom.load(std::memory_order_acquire)) {
      atom.store(new_lsn, std::memory_order_release);
    }
  };

  switch (release_type) {
    case ReleaseType::GLOBAL:
      update_lsn(shard_state->last_released_lsn, released_lsn);
      break;
    case ReleaseType::PER_EPOCH:
      update_lsn(shard_state->lng, released_lsn);
      break;
    default:
      ld_check(false);
      break;
  }
}

void PeriodicReleases::onMetaDataMapUpdate(
    const std::shared_ptr<const EpochMetaDataMap>& historical_metadata) {
  if (isShuttingDown()) {
    return;
  }

  ld_check(historical_metadata != nullptr);
  auto all_shards = historical_metadata->getUnionStorageSet();
  ld_check(all_shards != nullptr);

  {
    folly::SharedMutex::UpgradeHolder map_lock(map_mutex_);
    std::shared_ptr<const NodeReleaseMap> node_map = node_release_map_.get();
    ld_check(node_map != nullptr);
    std::shared_ptr<NodeReleaseMap> new_map;

    for (ShardID shard : *all_shards) {
      const PerShardState* shard_state =
          findPerShardState(node_map.get(), shard);
      if (shard_state == nullptr) {
        // new shard found
        if (new_map == nullptr) {
          // copy-on-write: make a private copy of the existing map
          new_map = std::make_shared<NodeReleaseMap>(*node_map);
        }
        // new shard is tracked with all releases set to 0
        (*new_map)[shard.node()][shard.shard()];
      }
    }

    if (new_map != nullptr) {
      folly::SharedMutex::WriteHolder map_write_lock(std::move(map_lock));
      // update needed for the map since new shards are found
      node_release_map_.update(new_map);
    }
  }
}

void PeriodicReleases::invalidateLastLsnsOfNode(node_index_t node_idx) {
  if (isShuttingDown()) {
    return;
  }

  // the lock is needed here since we are invalidating release status and do not
  // want an update (based on stale value) happen after the invalidation
  folly::SharedMutex::ReadHolder map_lock(map_mutex_);
  std::shared_ptr<NodeReleaseMap> node_map = node_release_map_.get();
  ld_check(node_map != nullptr);

  auto nit = node_map->find(node_idx);
  if (nit == node_map->cend()) {
    return;
  }

  for (auto& kv : nit->second) {
    // invalidate both releases
    kv.second.last_released_lsn.store(LSN_INVALID);
    kv.second.lng.store(LSN_INVALID);
  }
}

logid_t PeriodicReleases::getLogID() const {
  return sequencer_->getLogID();
}

lsn_t PeriodicReleases::getLastKnownGood() const {
  return sequencer_->getLastKnownGood();
}

lsn_t PeriodicReleases::getLastReleased() const {
  return sequencer_->getLastReleased();
}

bool PeriodicReleases::shouldSendBroadcast() const {
  return sequencer_->getState() == Sequencer::State::ACTIVE;
}

int PeriodicReleases::sendReleaseToStorageSets(
    lsn_t lsn,
    ReleaseType release_type,
    const Sequencer::SendReleasesPred& pred) {
  return sequencer_->sendReleases(lsn, release_type, pred);
}

const Settings& PeriodicReleases::getSettings() const {
  return Worker::settings();
}

bool PeriodicReleases::isShuttingDown() const {
  Worker* w = Worker::onThisThread(false);
  return w && !w->isAcceptingWork();
}

void PeriodicReleases::startPeriodicReleaseTimer(Type type) {
  chrono_expbackoff_t<ExponentialBackoffTimer::Duration> interval;
  switch (type) {
    case PeriodicReleases::Type::RETRY:
      interval = Worker::settings().release_retry_interval;
      break;
    case PeriodicReleases::Type::BROADCAST: {
      const bool internal = configuration::InternalLogs::isInternal(getLogID());
      interval =
          (internal
               ? Worker::settings().release_broadcast_interval_internal_logs
               : Worker::settings().release_broadcast_interval);
    }
  };

  // timer previosly didn't exist: create it and bind it to this Worker
  ExponentialBackoffTimerNode* timer_node =
      Worker::onThisThread()->registerTimer(
          ReleaseTimerCallback(weak_from_this(), type), interval);
  ld_check(timer_node != nullptr);

  timer_node->timer->activate();
}

void PeriodicReleases::cancelPeriodicReleaseTimer(
    ExponentialBackoffTimerNode* timer_node,
    Type) {
  // Timer needs to be destroyed and unlinked from this Worker's timer_
  // list.
  ld_check(timer_node->list_hook.is_linked());
  delete timer_node;
}

void PeriodicReleases::activatePeriodicReleaseTimer(
    ExponentialBackoffTimerNode* timer_node,
    Type) {
  timer_node->timer->activate();
}

PeriodicReleases::ReleaseTimerCallback::ReleaseTimerCallback(
    std::weak_ptr<PeriodicReleases> owner,
    PeriodicReleases::Type type)
    : periodic_releases_(std::move(owner)), type_(type) {}

void PeriodicReleases::ReleaseTimerCallback::
operator()(ExponentialBackoffTimerNode* node) {
  auto pr = periodic_releases_.lock();
  if (!pr) {
    // PeriodicReleases object no longer exists -- delete the timer

    ld_check(node->list_hook.is_linked());
    delete node; // also unlinks the node
    return;
  }
  pr->timerCallback(node, type_);
}

PeriodicReleases::StartBroadcastingReleaseRequest::
    StartBroadcastingReleaseRequest(
        std::weak_ptr<PeriodicReleases> periodic_releases)
    : Request(RequestType::START_BROADCASTING_RELEASE),
      periodic_releases_(std::move(periodic_releases)) {}

Request::Execution
PeriodicReleases::StartBroadcastingReleaseRequest::execute() {
  auto pr = periodic_releases_.lock();
  if (!pr) {
    // PeriodicReleases object no longer exists, nothing to do
    return Execution::COMPLETE;
  }

  // create and activate the periodic timer for broadcasting
  pr->startPeriodicReleaseTimer(PeriodicReleases::Type::BROADCAST);
  ld_info("Start broadcasting releases for log %lu.", pr->getLogID().val_);
  return Execution::COMPLETE;
}

void PeriodicReleases::postBroadcastingRequest(
    std::unique_ptr<StartBroadcastingReleaseRequest> req) {
  std::unique_ptr<Request> request{std::move(req)};
  Worker::onThisThread()->processor_->postWithRetrying(request);
}

/* static */
PeriodicReleases::PerShardState*
PeriodicReleases::findPerShardState(NodeReleaseMap* map, ShardID shard) {
  ld_check(map != nullptr);
  auto nit = map->find(shard.node());
  if (nit == map->end()) {
    return nullptr;
  }

  auto sit = nit->second.find(shard.shard());
  if (sit == nit->second.cend()) {
    return nullptr;
  }

  return &sit->second;
}

/* static */
const PeriodicReleases::PerShardState*
PeriodicReleases::findPerShardState(const NodeReleaseMap* map, ShardID shard) {
  return findPerShardState(const_cast<NodeReleaseMap*>(map), shard);
}

}} // namespace facebook::logdevice
