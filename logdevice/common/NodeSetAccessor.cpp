/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/NodeSetAccessor.h"

#include <folly/Hash.h>
#include <folly/Memory.h>
#include <folly/Random.h>

#include "logdevice/common/CopySet.h"
#include "logdevice/common/EpochMetaData.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Timer.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/protocol/STORE_Message.h"

namespace facebook { namespace logdevice {

// Appends the given string to trace_.
// If trace_ is already too long, doesn't append and doesn't evaluate x.
#define ADD_TO_TRACE(x)                        \
  do {                                         \
    if (trace_.size() < trace_size_limit_) {   \
      trace_.append(x);                        \
      if (trace_.size() > trace_size_limit_) { \
        trace_.resize(trace_size_limit_);      \
      }                                        \
    }                                          \
  } while (false)

StorageSetAccessor::StorageSetAccessor(
    logid_t log_id,
    StorageSet shards,
    std::shared_ptr<const configuration::nodes::NodesConfiguration>
        nodes_configuration,
    ReplicationProperty replication,
    ShardAccessFunc shard_access,
    CompletionFunc completion,
    Property property,
    std::chrono::milliseconds timeout)
    : StorageSetAccessor(log_id,
                         EpochMetaData(shards, std::move(replication)),
                         nodes_configuration,
                         shard_access,
                         completion,
                         property,
                         timeout) {}

StorageSetAccessor::StorageSetAccessor(
    logid_t log_id,
    EpochMetaData epoch_metadata,
    std::shared_ptr<const configuration::nodes::NodesConfiguration>
        nodes_configuration,
    ShardAccessFunc shard_access,
    CompletionFunc completion,
    Property property,
    std::chrono::milliseconds timeout)
    : log_id_(log_id),
      property_(property),
      timeout_(timeout),
      nodes_configuration_(nodes_configuration),
      epoch_metadata_(std::move(epoch_metadata)),
      nodeset_state_(
          std::make_shared<NodeSetState>(epoch_metadata_.shards,
                                         log_id,
                                         NodeSetState::HealthCheck::DISABLED)),
      shard_func_(std::move(shard_access)),
      completion_func_(std::move(completion)),
      failure_domain_(epoch_metadata_.shards,
                      *nodes_configuration_,
                      epoch_metadata_.replication) {
  ld_check(nodes_configuration_ != nullptr);
  ld_check(epoch_metadata_.isValid());
}

void StorageSetAccessor::successIfAllShardsAccessed() {
  ld_check(!started_);
  allow_success_if_all_accessed_ = true;
}

void StorageSetAccessor::requireStrictWaves() {
  ld_check(!started_);
  require_strict_waves_ = true;
}

void StorageSetAccessor::noEarlyAbort() {
  ld_check(!started_);
  no_early_abort_ = true;
}

void StorageSetAccessor::setRequiredShards(const StorageSet& required_shards) {
  ld_check(!started_);
  required_shards_.clear();
  for (ShardID shard : required_shards) {
    if (failure_domain_.containsShard(shard)) {
      required_shards_.insert(shard);
    }
  }

  if (!required_shards_.empty()) {
    ADD_TO_TRACE("required:");
    ADD_TO_TRACE(toString(required_shards_));
    ADD_TO_TRACE(";");
  }
}

void StorageSetAccessor::start() {
  ld_check(!started_);
  ld_check(!finished_);

  started_ = true;

  // initially failure domain, set all nodes to state NOT_SENT
  for (auto shard : epoch_metadata_.shards) {
    setShardState(shard, ShardState::NOT_SENT, Status::UNKNOWN);
  }

  if (timeout_ > std::chrono::milliseconds::zero()) {
    job_timer_ = createJobTimer([this] { onJobTimedout(); });
    activateJobTimer();
  }

  if (property_ != Property::FMAJORITY) {
    // copyset selector is not needed for FMAJORITY property
    copyset_selector_ = createCopySetSelector(
        log_id_, epoch_metadata_, nodeset_state_, nodes_configuration_);
  }

  // Apply shard authoritative statuses.
  // checkIfDone() is going to be executed and it will check if it is possible
  // to complete the task at all regarding the failure domain property, and fail
  // immediately if not.
  applyShardStatus();
  if (checkIfDone()) {
    return;
  }

  // send the first wave
  sendWave();
}

void StorageSetAccessor::complete(Status status, const char* trace) {
  ld_check(started_);
  ld_check(!finished_);
  ld_check_in(status, ({E::OK, E::TIMEDOUT, E::ABORTED, E::FAILED}));
  // destroy all timers
  wave_timer_.reset();
  job_timer_.reset();
  grace_period_timer_.reset();
  finished_ = true;

  ADD_TO_TRACE("done:");
  ADD_TO_TRACE(trace);
  ADD_TO_TRACE(":");
  ADD_TO_TRACE(error_name(status));
  ADD_TO_TRACE(";");

  ld_debug("StorageSetAccessor for log %lu completed with status %s",
           log_id_.val_,
           error_description(status));
  completion_func_(status);
}

void StorageSetAccessor::setGracePeriod(std::chrono::milliseconds grace_period,
                                        CompletionCondition completion_cond) {
  ld_check(!started_);
  // Set up grace period timer
  grace_period_ = grace_period;
  grace_period_timer_ =
      createGracePeriodTimer([this] { this->onGracePeriodTimedout(); });
  completion_cond_ = folly::Optional<CompletionCondition>(completion_cond);
}

void StorageSetAccessor::onGracePeriodTimedout() {
  ld_check(grace_period_.hasValue());
  complete(E::OK, "grace_period");
}

void StorageSetAccessor::onJobTimedout() {
  ld_check(timeout_ > std::chrono::milliseconds::zero());
  complete(grace_period_timer_ && grace_period_timer_->isActive() ? E::OK
                                                                  : E::TIMEDOUT,
           "timeout");
}

StorageSet StorageSetAccessor::pickWaveFromCopySet() {
  ld_check(copyset_selector_ != nullptr);
  ld_check(property_ == Property::REPLICATION || property_ == Property::ANY);
  copyset_size_t extras_this_wave = extras_;

  // predicate for nodes that we want to be in the copyset: required nodes
  // and nodes that already succeeded.
  auto access_target_nodes = [](ShardStatus st) {
    return st.state == ShardState::SUCCESS || st.required;
  };

  StorageSet wave_shards = getShardsInStatus(access_target_nodes);
  size_t wave_size_before_copyset = wave_shards.size();
  wave_shards.resize(wave_shards.size() +
                     copyset_selector_->getReplicationFactor());

  // Pick a copyset that contains all required nodes and nodes that have already
  // been successfully accessed. Note that nodes in PERMANENT_ERROR state will
  // not be picked as they are permanently blacklisted in the NodeSet object.
  // If wave_shards is already a valid copyset, augment() won't pick any more
  // nodes.
  copyset_size_t wave_size_after_copyset;
  auto result = copyset_selector_->augment(wave_shards.data(),
                                           wave_size_before_copyset,
                                           &wave_size_after_copyset,
                                           *rng_);
  copyset_selection_failed_ = result != CopySetSelector::Result::SUCCESS;

  if (result != CopySetSelector::Result::SUCCESS) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    1,
                    "Failed to pick a copyset for log %lu, given "
                    "exisiting nodes %s.",
                    log_id_.val_,
                    toString(getShardsInStatus(access_target_nodes)).c_str());

    ADD_TO_TRACE("F;");

    // We are unable to pick a copyset from copyset selector, picking some
    // extra nodes to make progress:
    // 1) if strict wave is required, to preserve the copyset consistency,
    //    pick all nodes that are not finished and still have a chance to
    //    succeed.
    // 2) otherwise, pick extra nodes based on the extras settings with
    //    minimal one node to be accessed
    if (require_strict_waves_) {
      wave_shards = getShardsInStatus([](ShardStatus st) {
        return (st.state != ShardState::SUCCESS &&
                st.state != ShardState::PERMANENT_ERROR);
      });
      // already picked all available nodes
      extras_this_wave = 0;
    } else {
      // Contents of wave_shards are undefined after augment() fails.
      wave_shards = getShardsInStatus([](ShardStatus st) {
        bool res = st.required && st.state != ShardState::SUCCESS;
        ld_check(!res || st.state != ShardState::PERMANENT_ERROR);
        return res;
      });
      extras_this_wave = std::max(extras_this_wave, (copyset_size_t)1);
    }
  } else {
    // Truncate wave_shards to wave_size_after_copyset. Also remove the nodes
    // that were already successfully accessed - no need to access them again.
    // Note that if require_strict_waves_ is true, nothing is removed because
    // all node states are reset to NOT_SENT before each wave.
    wave_shards.erase(
        std::remove_if(wave_shards.begin(),
                       wave_shards.begin() + wave_size_after_copyset,
                       [&](ShardID index) {
                         ShardStatus status;
                         return failure_domain_.getShardAttribute(
                                    index, &status) == 0 &&
                             status.state == ShardState::SUCCESS;
                       }),
        wave_shards.end());
  }

  if (extras_this_wave > 0) {
    copyset_custsz_t<4> extras_nodes;

    // helper function that try to randomly pick @param num_extras nodes in
    // state of @param st, and insert them into extra_nodes
    auto pick_extras_in_state = [&](ShardState st, copyset_size_t num_extras) {
      StorageSet extras = getShardsInState(st);
      // remove nodes already selected in this wave
      extras.erase(
          std::remove_if(extras.begin(),
                         extras.end(),
                         [&](ShardID index) {
                           return (std::find(wave_shards.begin(),
                                             wave_shards.end(),
                                             index) != wave_shards.end());
                         }),
          extras.end());
      std::shuffle(extras.begin(), extras.end(), folly::ThreadLocalPRNG());
      if (extras.size() > num_extras) {
        extras.resize(num_extras);
      }
      extras_nodes.insert(extras_nodes.end(), extras.begin(), extras.end());
    };

    // consider nodes in NOT_SENT first, then TRANSIENT_ERROR and
    // finally INPROGRESS
    for (auto state : {ShardState::NOT_SENT,
                       ShardState::TRANSIENT_ERROR,
                       ShardState::INPROGRESS}) {
      if (extras_nodes.size() >= extras_this_wave) {
        break;
      }
      pick_extras_in_state(state, extras_this_wave - extras_nodes.size());
    }

    ld_debug("Picked %lu extras in wave %u for log %lu.",
             extras_nodes.size(),
             wave_,
             log_id_.val_);

    wave_shards.insert(
        wave_shards.end(), extras_nodes.begin(), extras_nodes.end());
  }

  return wave_shards;
}

void StorageSetAccessor::sendWave() {
  ld_check(!finished_);
  // store nodes to be contacted in this wave
  WaveInfo wave_info;
  wave_info.wave = ++wave_;
  wave_info.offset = 0;
  StorageSet& wave_shards = wave_info.wave_shards;

  if (require_strict_waves_) {
    // if strict wave is required, clear success status from all nodes
    // in previous waves
    StorageSet prev_success_nodes = getShardsInState(ShardState::SUCCESS);
    for (ShardID n : prev_success_nodes) {
      setShardState(n, ShardState::NOT_SENT, Status::UNKNOWN);
    }
  }

  if (wave_preflight_ != nullptr) {
    // call the user provided callback before picking nodes for each wave
    wave_preflight_();
  }

  size_t nodes_sent = 0;

  if (property_ == Property::REPLICATION || property_ == Property::ANY) {
    wave_shards = pickWaveFromCopySet();
  } else {
    // send to all nodes that are not finished and still have a chance to
    // succeed
    wave_shards = getShardsInStatus([](ShardStatus st) {
      return (st.state != ShardState::SUCCESS &&
              st.state != ShardState::PERMANENT_ERROR);
    });
  }
  wave_shards_ = wave_shards;
  wave_start_time_ = SteadyTimestamp::now();

  if (folly::kIsDebug) {
    // wave_shards should not contain duplicates
    StorageSet w(wave_shards);
    std::sort(w.begin(), w.end());
    ld_assert(std::unique(w.begin(), w.end()) == w.end() &&
              "duplicate nodes in a wave");
  }

  ADD_TO_TRACE("W");
  ADD_TO_TRACE(toString(wave_info.wave));
  ADD_TO_TRACE(toString(wave_shards));
  ADD_TO_TRACE(";");

  for (auto index : wave_shards) {
    auto result_status = accessShard(index, wave_info);
    auto result = result_status.result;
    switch (result) {
      case Result::SUCCESS:
        nodes_sent++;
        break;
      case Result::TRANSIENT_ERROR:
        break;
      case Result::PERMANENT_ERROR:
        if (checkIfDone()) {
          return;
        }
        break;
      case Result::ABORT:
        complete(E::ABORTED, "aborted");
        return;
    }
    ++wave_info.offset;
  }

  ld_debug("Successfully send to %lu shards in wave %u for log %lu. "
           "Wave shards: [%s].",
           nodes_sent,
           wave_info.wave,
           log_id_.val_,
           toString(wave_info.wave_shards).c_str());

  // start wave timer for the next wave
  if (wave_timer_ == nullptr) {
    wave_timer_ = createWaveTimer([this]() { sendWave(); });
  }

  wave_timer_->activate();
}

StorageSetAccessor::AccessResult
StorageSetAccessor::accessShard(ShardID shard, const WaveInfo& wave_info) {
  ShardStatus st;
  int rv = failure_domain_.getShardAttribute(shard, &st);
  // shard must have a state
  ld_check(rv == 0);
  // should never send to shard in success or perm_error state
  if (st.state == ShardState::SUCCESS ||
      st.state == ShardState::PERMANENT_ERROR) {
    ld_error("Sent to shard in state %s\n", stateString(st.state));
  }
  ld_check(st.state != ShardState::SUCCESS &&
           st.state != ShardState::PERMANENT_ERROR);

  ld_check(!finished_);
  AccessResult access_result = shard_func_(shard, wave_info);
  auto result = access_result.result;
  auto status = access_result.status;

  ADD_TO_TRACE("X:");
  ADD_TO_TRACE(shard.toString());
  ADD_TO_TRACE(":");
  ADD_TO_TRACE(resultShortString(result));
  if (result != Result::SUCCESS || status != E::OK) {
    ADD_TO_TRACE(":");
    ADD_TO_TRACE(error_name(status));
  }
  ADD_TO_TRACE(";");

  switch (result) {
    case Result::SUCCESS:
      setShardState(shard, ShardState::INPROGRESS, status);
      return access_result;
    case Result::TRANSIENT_ERROR:
      setShardState(shard, ShardState::TRANSIENT_ERROR, status);
      return access_result;
    case Result::PERMANENT_ERROR:
    case Result::ABORT:
      disableShard(shard);
      setShardState(shard, ShardState::PERMANENT_ERROR, status);
      return access_result;
  }

  ld_error(
      "INTERNAL ERROR: Got unrecognized enum value %d", to_integral(result));
  ld_check(false);
  setShardState(shard, ShardState::NOT_SENT, status);
  return access_result;
}

bool StorageSetAccessor::checkIfDone() {
  // check required nodes first, if any
  for (ShardID required : required_shards_) {
    ShardStatus st;
    int rv = failure_domain_.getShardAttribute(required, &st);
    if (rv != 0) {
      RATELIMIT_CRITICAL(std::chrono::seconds(10),
                         10,
                         "Required shard %s is not in the nodeset of log %lu.",
                         required.toString().c_str(),
                         log_id_.val_);
      // we already filtered required nodes, so this should't happen
      ld_check(false);
      continue;
    }
    if (st.state == ShardState::PERMANENT_ERROR) {
      // one of the required node permanently failed, no chance of success
      RATELIMIT_WARNING(std::chrono::seconds(10),
                        2,
                        "Abort StorageSetAccessor for log %lu because "
                        "required shard %s has a permanent error. "
                        "Node states: %s.",
                        log_id_.val_,
                        required.toString().c_str(),
                        describeState().c_str());
      complete(E::FAILED, "required_has_permanent_error");
      return true;
    }

    if (st.state != ShardState::SUCCESS) {
      // do not consider StorageSetAccessor successfully finished if there is
      // one required node that is not successfully accessed.
      return false;
    }
  }

  auto success_nodes_pred = [](ShardStatus st) {
    return st.state == ShardState::SUCCESS;
  };
  auto failed_nodes_pred = [](ShardStatus st) {
    return st.state == ShardState::PERMANENT_ERROR;
  };
  auto potential_nodes_pred = [](ShardStatus st) {
    return st.state != ShardState::PERMANENT_ERROR;
  };
  auto inprogress_nodes_pred = [](ShardStatus st) {
    return st.state == ShardState::INPROGRESS ||
        st.state == ShardState::TRANSIENT_ERROR;
  };

  if (allow_success_if_all_accessed_ &&
      failure_domain_.countShards(success_nodes_pred) ==
          failure_domain_.numShards()) {
    // if allow_success_if_all_accessed_ is set, complete with E::OK if all
    // nodes report success
    complete(E::OK, "all");
    return true;
  }

  // if successIfAllShardsAccessed() is called and there is no node in
  // permanent error, do not abort early
  bool no_abort = allow_success_if_all_accessed_ &&
      failure_domain_.countShards(failed_nodes_pred) == 0;

  switch (property_) {
    case Property::ANY: {
      if (failure_domain_.countShards(success_nodes_pred) > 0) {
        complete(E::OK, "any");
        return true;
      }

      if (failure_domain_.countShards(failed_nodes_pred) ==
          failure_domain_.numShards()) {
        complete(E::FAILED, "all_permanent_error");
        return true;
      }
      return false;
    } break;

    case Property::FMAJORITY: {
      auto fmajority_result = failure_domain_.isFmajority(success_nodes_pred);

      if (fmajority_result == FmajorityResult::NON_AUTHORITATIVE) {
        RATELIMIT_INFO(
            std::chrono::seconds(5),
            1,
            "Complete StorageSetAccessor for log %lu with "
            "non-authoritative f-majority since %lu nodes are "
            "rebuilding",
            log_id_.val_,
            failure_domain_.numShards(AuthoritativeStatus::UNDERREPLICATION));
      }

      if (fmajority_result != FmajorityResult::NONE) {
        if (completion_cond_.hasValue() && !completion_cond_.value()()) {
          RATELIMIT_INFO(std::chrono::seconds(5),
                         1,
                         "StorageSetAccessor has f-majority but completion "
                         "condition is not yet satisfied for log %lu",
                         log_id_.val());
          // Start the grace period timer here
          if (grace_period_.hasValue()) {
            activateGracePeriodTimer();
          }
          // Keep going
          return false;
        }
        ld_debug("Property satisfied with %lu nodes in SUCCESS state",
                 failure_domain_.countShards(success_nodes_pred));
        // conclude once we have f-majority
        complete(E::OK, fmajorityResultString(fmajority_result));
        return true;
      }

      if (!no_abort &&
          failure_domain_.isFmajority(potential_nodes_pred) ==
              FmajorityResult::NONE) {
        RATELIMIT_WARNING(std::chrono::seconds(1),
                          3,
                          "Abort StorageSetAccessor for log %lu because the "
                          "f-majority property cannot be satisfied with "
                          "remaining eligible nodes. Node states: %s.",
                          log_id_.val_,
                          describeState().c_str());
        if (!no_early_abort_ ||
            failure_domain_.countShards(inprogress_nodes_pred) == 0) {
          complete(E::FAILED, "too_many_permanent_errors");
          return true;
        }
      }
      return false;
    } break;

    case Property::REPLICATION: {
      if (failure_domain_.canReplicate(success_nodes_pred)) {
        // subset of nodes that were successfully accessed already satisfy
        // the failure domain property, complete with SUCCESS status
        complete(E::OK, "can_replicate");
        return true;
      }

      if (!no_abort && !failure_domain_.canReplicate(potential_nodes_pred)) {
        RATELIMIT_WARNING(std::chrono::seconds(1),
                          3,
                          "Abort StorageSetAccessor for log %lu because the "
                          "replication property cannot be satisfied with "
                          "remaining eligible nodes. Node states: %s.",
                          log_id_.val_,
                          describeState().c_str());
        complete(E::FAILED, "too_many_permanent_errors");
        return true;
      }

      return false;
    } break;
  }

  return false;
}

void StorageSetAccessor::onShardAccessed(ShardID shard,
                                         AccessResult result,
                                         uint32_t wave) {
  ADD_TO_TRACE("Y");
  ADD_TO_TRACE(toString(wave));
  ADD_TO_TRACE(":");
  ADD_TO_TRACE(shard.toString());
  ADD_TO_TRACE(":");
  ADD_TO_TRACE(resultShortString(result.result));
  if (result.result != Result::SUCCESS || result.status != E::OK) {
    ADD_TO_TRACE(":");
    ADD_TO_TRACE(error_name(result.status));
  }
  ADD_TO_TRACE(";");

  if (finished_) {
    // ignore calls if StorageSetAccessor is finished
    RATELIMIT_WARNING(std::chrono::seconds(1),
                      2,
                      "called when StorageSetAccessor for log %lu is finished.",
                      log_id_.val());
    return;
  }

  if (require_strict_waves_ && wave != WAVE_INVALID && wave != wave_) {
    RATELIMIT_INFO(std::chrono::seconds(5),
                   1,
                   "Received reply from shard %s with result %s for log %lu "
                   "but the reply belongs to a previous wave %u, current "
                   "wave %u. Ignoring.",
                   shard.toString().c_str(),
                   resultString(result.result),
                   log_id_.val_,
                   wave,
                   wave_);
    return;
  }

  ShardStatus st;
  int rv = failure_domain_.getShardAttribute(shard, &st);
  if (rv != 0) {
    RATELIMIT_CRITICAL(std::chrono::seconds(1),
                       10,
                       "Accessed shard %s with result %s but the shard is not "
                       "in the storage set of log %lu.",
                       shard.toString().c_str(),
                       resultString(result.result),
                       log_id_.val_);
    // shard must have a state if it is in the given storage set, which should
    // be ensured by the caller
    ld_check(false);
    return;
  }

  if (st.state == ShardState::SUCCESS) {
    RATELIMIT_INFO(std::chrono::seconds(1),
                   1,
                   "Accessed shard %s with result %s for log %lu but the shard "
                   "has already been successfully accessed.",
                   shard.toString().c_str(),
                   resultString(result.result),
                   log_id_.val_);
    return;
  }

  if (st.state != ShardState::INPROGRESS) {
    RATELIMIT_INFO(std::chrono::seconds(1),
                   1,
                   "Accessed shard %s with result %s for log %lu but the "
                   "shard is in state %s rather than INPROGRESS.",
                   shard.toString().c_str(),
                   resultString(result.result),
                   log_id_.val_,
                   stateString(st.state));
    // still proceeds
  }

  switch (result.result) {
    case Result::SUCCESS:
      setShardState(shard, ShardState::SUCCESS, result.status);
      break;
    case Result::TRANSIENT_ERROR:
      setShardState(shard, ShardState::TRANSIENT_ERROR, result.status);
      break;
    case Result::ABORT:
    case Result::PERMANENT_ERROR:
      // blacklist shard in the storage set state, this shard will never be
      // picked again
      setShardState(shard, ShardState::PERMANENT_ERROR, result.status);
      disableShard(shard);
      break;
  }

  checkIfDone();
}

FailedShardsMap StorageSetAccessor::getFailedShards(
    std::function<bool(Status)> fail_predicate) const {
  FailedShardsMap result;
  for (ShardID shard : epoch_metadata_.shards) {
    ShardStatus status;
    if (failure_domain_.getShardAttribute(shard, &status) == 0) {
      if (!fail_predicate(status.status)) {
        continue;
      }

      auto shards_it = result.find(status.status);
      auto& shards = shards_it == result.end()
          ? result
                .emplace(std::make_pair(status.status, std::vector<ShardID>{}))
                .first->second
          : shards_it->second;
      shards.push_back(shard);
    }
  }

  return result;
}

bool StorageSetAccessor::setShardAuthoritativeStatusImpl(
    ShardID shard,
    AuthoritativeStatus st) {
  if (finished_) {
    // ignore calls if StorageSetAccessor is finished
    RATELIMIT_WARNING(std::chrono::seconds(1),
                      2,
                      "called when StorageSetAccessor for log %lu is finished.",
                      log_id_.val());
    return false;
  }
  if (!failure_domain_.containsShard(shard)) {
    // This node isn't a storage node in config; or at least wasn't at the time
    // of failure_domain_ creation.
    return false;
  }

  auto prev_status = failure_domain_.getShardAuthoritativeStatus(shard);
  if (st == prev_status) {
    return false;
  }

  ADD_TO_TRACE("A:");
  ADD_TO_TRACE(shard.toString());
  ADD_TO_TRACE(":");
  ADD_TO_TRACE(toShortString(st));
  ADD_TO_TRACE(";");

  failure_domain_.setShardAuthoritativeStatus(shard, st);
  // On authoritative status changes, always reset the node to NOT_SENT, and
  // remove the blacklisting (if any), the node can be picked again in the
  // next wave
  setShardState(shard, ShardState::NOT_SENT, Status::UNKNOWN);
  enableShard(shard);
  return true;
}

void StorageSetAccessor::setShardAuthoritativeStatus(ShardID shard,
                                                     AuthoritativeStatus st) {
  if (!setShardAuthoritativeStatusImpl(shard, st)) {
    return;
  }

  // setting authoritative status may make certain property to be satisfied,
  // check it now
  checkIfDone();
}

StorageSet StorageSetAccessor::getShardsInStatus(
    std::function<bool(StorageSetAccessor::ShardStatus)> pred) const {
  StorageSet result;

  for (ShardID shard : epoch_metadata_.shards) {
    ShardStatus status;
    if (failure_domain_.getShardAttribute(shard, &status) == 0) {
      if (pred(status)) {
        result.push_back(shard);
      }
    }
  }
  return result;
}

StorageSet StorageSetAccessor::getShardsInState(ShardState state) const {
  return getShardsInStatus(
      [state](ShardStatus st) { return st.state == state; });
}

std::unique_ptr<Timer>
StorageSetAccessor::createJobTimer(std::function<void()> callback) {
  return std::make_unique<Timer>(callback);
}

std::unique_ptr<Timer>
StorageSetAccessor::createGracePeriodTimer(std::function<void()> callback) {
  return std::make_unique<Timer>(callback);
}

std::unique_ptr<BackoffTimer>
StorageSetAccessor::createWaveTimer(std::function<void()> callback) {
  auto timer = std::make_unique<ExponentialBackoffTimer>(
      callback, wave_timeout_min_, wave_timeout_max_);
  return std::move(timer);
}

void StorageSetAccessor::activateJobTimer() {
  ld_check(job_timer_ != nullptr);
  job_timer_->activate(timeout_);
}

void StorageSetAccessor::activateGracePeriodTimer() {
  ld_check(grace_period_timer_);
  if (!grace_period_timer_->isActive()) {
    ld_check(grace_period_.hasValue());
    ld_check(grace_period_timer_ != nullptr);
    grace_period_timer_->activate(grace_period_.value());
  }
}

void StorageSetAccessor::cancelJobTimer() {
  ld_check(job_timer_ != nullptr);
  job_timer_->cancel();
}

void StorageSetAccessor::cancelGracePeriodTimer() {
  ld_check(grace_period_timer_ != nullptr);
  grace_period_timer_->cancel();
}

std::unique_ptr<CopySetSelector> StorageSetAccessor::createCopySetSelector(
    logid_t log_id,
    const EpochMetaData& epoch_metadata,
    std::shared_ptr<NodeSetState> nodeset_state,
    const std::shared_ptr<const configuration::nodes::NodesConfiguration>&
        nodes_configuration) {
  return CopySetSelectorFactory::create(
      log_id,
      epoch_metadata,
      nodeset_state,
      nodes_configuration,
      Worker::onThisThread()->processor_->getOptionalMyNodeID(),
      /* log_attrs */ nullptr,
      Worker::settings(),
      *rng_);
}

void StorageSetAccessor::disableShard(ShardID shard) {
  nodeset_state_->setNotAvailableUntil(
      shard,
      std::chrono::steady_clock::time_point::max(),
      NodeSetState::NodeSetState::NotAvailableReason::STORE_DISABLED);
}

void StorageSetAccessor::enableShard(ShardID shard) {
  nodeset_state_->clearNotAvailableUntil(shard);
}

bool StorageSetAccessor::isRequired(ShardID node) const {
  return required_shards_.count(node) > 0;
}

void StorageSetAccessor::setShardState(ShardID shard,
                                       ShardState state,
                                       Status status) {
  failure_domain_.setShardAttribute(
      shard, ShardStatus{state, isRequired(shard), status});
}

void StorageSetAccessor::applyShardStatus() {
  const auto& shard_status_map = getShardAuthoritativeStatusMap();

  for (const ShardID shard : epoch_metadata_.shards) {
    const auto st =
        shard_status_map.getShardStatus(shard.node(), shard.shard());
    setShardAuthoritativeStatusImpl(shard, st);
  }
}

std::string StorageSetAccessor::describeState(bool all_shards) const {
  std::stringstream ss;
  ss.precision(3);
  ss.setf(std::ios::fixed, std::ios::floatfield);
  // Describe current wave. Don't do it for FMAJORITY because the waves don't
  // matter in that case.
  if (property_ == Property::REPLICATION || property_ == Property::ANY) {
    ss << "wave " << wave_ << ": " << toString(wave_shards_)
       << ", wave started "
       << (SteadyTimestamp::now().toMilliseconds() -
           wave_start_time_.toMilliseconds())
                .count() /
            1000.
       << "s ago; ";
  }

  // Figure out which shard state is "uninteresting" in current situation.
  ShardState omit_state = ShardState::Count;
  if (!all_shards) {
    if (property_ == Property::FMAJORITY) {
      // If accessing f-majority, only print shards that are not completed yet.
      omit_state = ShardState::SUCCESS;
    } else if (!wave_shards_.empty() && !copyset_selection_failed_) {
      // If accessing a copyset or a single node, and current wave's list of
      // shards is good, only print shards we tried to access. (And we printed
      // current wave's list of shards above.)
      omit_state = ShardState::NOT_SENT;
    }
  }

  // Print the list of shards.
  ss << "{";
  bool first = true;
  size_t num_omitted = 0;
  for (ShardID shard : epoch_metadata_.shards) {
    ShardStatus s;
    if (failure_domain_.getShardAttribute(shard, &s) != 0) {
      continue;
    }
    if (s.state == omit_state && !s.required) {
      ++num_omitted;
      continue;
    }

    if (!first) {
      ss << ", ";
    }
    first = false;

    ss << shard.toString();

    if (s.required) {
      ss << "(r)";
    }
    auto auth_status = failure_domain_.getShardAuthoritativeStatus(shard);
    if (auth_status != AuthoritativeStatus::FULLY_AUTHORITATIVE) {
      ss << "(" << toShortString(auth_status) << ")";
    }

    ss << ": " << getShardState(s.state);

    // Print status unless it's obvious (UNKNOWN for NOT_SENT, OK for INPROGRESS
    // or SUCCESS).
    folly::Optional<Status> normal_status;
    if (s.state == ShardState::NOT_SENT) {
      normal_status = E::UNKNOWN;
    } else if (s.state == ShardState::INPROGRESS ||
               s.state == ShardState::SUCCESS) {
      normal_status = E::OK;
    }
    if (!normal_status.hasValue() || s.status != normal_status.value()) {
      ss << ' ' << error_name(s.status);
    }
  }
  ss << "}";

  // Report number of omitted shards.
  if (num_omitted != 0) {
    ss << "; " << num_omitted << " shards in state "
       << getShardState(omit_state);
  }

  return ss.str();
}

bool StorageSetAccessor::onShardStatusChanged() {
  if (finished_) {
    // ignore calls if StorageSetAccessor is finished
    RATELIMIT_WARNING(std::chrono::seconds(1),
                      2,
                      "called when StorageSetAccessor for log %lu is finished.",
                      log_id_.val());
    return true;
  }

  applyShardStatus();
  return checkIfDone();
}

ShardAuthoritativeStatusMap&
StorageSetAccessor::getShardAuthoritativeStatusMap() {
  return Worker::onThisThread()
      ->shardStatusManager()
      .getShardAuthoritativeStatusMap();
}

const char* StorageSetAccessor::resultString(Result result) {
  switch (result) {
    case Result::SUCCESS:
      return "SUCCESS";
    case Result::TRANSIENT_ERROR:
      return "TRANSIENT_ERROR";
    case Result::PERMANENT_ERROR:
      return "PERMANENT_ERROR";
    case Result::ABORT:
      return "ABORT";
  }
  ld_check(false);
  return "INVALID";
}

const char* StorageSetAccessor::resultShortString(Result result) {
  switch (result) {
    case Result::SUCCESS:
      return "K";
    case Result::TRANSIENT_ERROR:
      return "T";
    case Result::PERMANENT_ERROR:
      return "P";
    case Result::ABORT:
      return "A";
  }
  ld_check(false);
  return "INVALID";
}

const char* StorageSetAccessor::stateString(ShardState state) {
  switch (state) {
    case ShardState::NOT_SENT:
      return "NOT_SENT";
    case ShardState::INPROGRESS:
      return "INPROGRESS";
    case ShardState::SUCCESS:
      return "SUCCESS";
    case ShardState::TRANSIENT_ERROR:
      return "TRANSIENT_ERROR";
    case ShardState::PERMANENT_ERROR:
      return "PERMANENT_ERROR";
    default:
      static_assert((int)ShardState::Count == 5, "");
  }
  ld_check(false);
  return "INVALID";
}

}} // namespace facebook::logdevice
