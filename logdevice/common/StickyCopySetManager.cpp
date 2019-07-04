/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/StickyCopySetManager.h"

#include <shared_mutex>

#include <folly/Memory.h>

#include "logdevice/common/debug.h"

namespace facebook { namespace logdevice {

using NodeStatus = NodeAvailabilityChecker::NodeStatus;

CopySetSelector::Result
StickyCopySetManager::getCopySet(copyset_size_t extras,
                                 StoreChainLink copyset_out[],
                                 copyset_size_t* copyset_size_out,
                                 bool* chain_out,
                                 const AppendContext& append_ctx,
                                 folly::Optional<lsn_t>& block_starting_lsn_out,
                                 CopySetManager::State& csm_state) {
  ld_check(copyset_out != nullptr);
  ld_check(copyset_size_out != nullptr);

  State& state = checked_downcast<State&>(csm_state);

  std::shared_lock<folly::SharedMutex> lock(mutex_);
  size_t invalid_block_seq_no = 0;

  // returns true if we have to change the copyset
  auto block_invalid = [&, this]() {
    return (invalid_block_seq_no == block_seq_no_) ||
        shouldStartNewBlock(state, extras);
  };

  do {
    // For out-of-block records, we use the current block's copyset anyway. We
    // will store these as single records though. It's OK for old records to
    // cause us to start new blocks, as this will only happen if the current
    // copyset is gone bad.

    if (block_invalid()) {
      lock.unlock();
      std::unique_lock<folly::SharedMutex> u_lock(mutex_);
      // Re-checking the state after relock
      if (block_invalid()) {
        // trying to start a new block
        if (!startNewBlock(state, append_ctx, extras, chain_out)) {
          return CopySetSelector::Result::FAILED;
        }
      }
      u_lock.unlock();
      lock.lock();
    }
    ld_check(copyset_.size() > 0);

    // We assume that the underlying copyset selector checks node availability
    // on the copyset too, before issuing it to use with a positive result.
    // Thus, the following check is only here to detect changes between calls
    // to the copyset selector and should not turn this into an endless loop.
    if (!checkAndOutputNodes(copyset_out, copyset_size_out, chain_out)) {
      invalid_block_seq_no = block_seq_no_;
    }
  } while (invalid_block_seq_no == block_seq_no_);

  if (send_block_records_ && append_ctx.lsn >= current_block_starting_lsn_) {
    // This record is part of a block with this starting LSN
    block_starting_lsn_out.assign(current_block_starting_lsn_);
  } else {
    // This is a single (out-of-block) record
    block_starting_lsn_out.assign(LSN_INVALID);
  }

  onCopySetAssigned(state, append_ctx);
  ld_check(current_block_css_result_ == CopySetSelector::Result::PARTIAL ||
           current_block_css_result_ == CopySetSelector::Result::SUCCESS);
  return current_block_css_result_;
}

CopySetSelector::Result StickyCopySetManager::getCopysetUsingUnderlyingSelector(
    logid_t log_id,
    copyset_size_t extras,
    StoreChainLink copyset_out[],
    copyset_size_t* copyset_size_out) {
  ld_check(copyset_out != nullptr);
  ld_check(copyset_size_out != nullptr);

  std::shared_lock<folly::SharedMutex> lock(mutex_);
  auto result =
      underlying_selector_->select(extras, copyset_out, copyset_size_out);
  lock.unlock();

  if (result == CopySetSelector::Result::FAILED) {
    RATELIMIT_ERROR(
        std::chrono::seconds(1),
        1,
        "Underlying copyset selector failed to pick a copyset for log:%lu",
        log_id.val_);
    return result;
  }

  ld_check(*copyset_size_out > 0);
  return result;
}

bool StickyCopySetManager::checkAndOutputNodes(StoreChainLink copyset_out[],
                                               copyset_size_t* copyset_size_out,
                                               bool* chain_out) const {
  bool local_chain_out = (chain_out ? *chain_out : false);
  copyset_size_t offset = 0;
  for (auto shard : copyset_) {
    StoreChainLink destination;
    auto node_status = deps_->getNodeAvailability()->checkNode(
        nodeset_state_.get(), shard, &destination);
    switch (node_status) {
      case NodeStatus::AVAILABLE_NOCHAIN:
        local_chain_out = false;
        break;
      case NodeStatus::AVAILABLE:
        break;
      case NodeStatus::NOT_AVAILABLE:
        return false;
    }
    // we might write an incomplete copyset here, but that's okay, as we won't
    // set copyset_size_out if we fail
    copyset_out[offset++] = destination;
  }

  *copyset_size_out = offset;
  if (chain_out) {
    *chain_out = local_chain_out;
  }
  return true;
};

bool StickyCopySetManager::shouldStartNewBlock(const State& state,
                                               copyset_size_t extras) const {
  if (current_block_starting_lsn_ == LSN_INVALID) {
    // we should start a new block if we don't have a copyset
    return true;
  }

  // We have to regenerate the copysets if the size of the copyset changes
  if (extras != extras_) {
    return true;
  }

  // In a single-copyset scenario we are changing the copyset with every
  // block.
  // TODO: consider not doing that for a multiple-copyset scenario?
  if (current_block_bytes_written_ >= block_size_threshold_) {
    return true;
  }

  // Start a new block if the current one has expired
  if (current_block_expiration_ < std::chrono::steady_clock::now()) {
    return true;
  }

  if (state.last_tried_block_seq_no == block_seq_no_) {
    // We tried this copyset already, it failed.
    // TODO: should we retry to any of the copysets we already tried before
    // breaking the block?
    return true;
  }
  return false;
}

bool StickyCopySetManager::startNewBlock(State& csm_state,
                                         const AppendContext& /*append_ctx*/,
                                         copyset_size_t extras,
                                         bool* chain_out) {
  StoreChainLink copyset[COPYSET_SIZE_MAX];
  copyset_size_t size = 0;

  auto result = underlying_selector_->select(
      extras, copyset, &size, chain_out, csm_state.css_state.get());
  if (result == CopySetSelector::Result::FAILED) {
    return false;
  }

  shuffleCopySet(copyset, size, chain_out ? *chain_out : false);

  copyset_.clear();

  for (size_t i = 0; i < size; ++i) {
    copyset_.push_back(copyset[i].destination);
  }

  extras_ = extras;
  ++block_seq_no_;

  lsn_t max_lsn = max_lsn_seen_.load(); // Since this is happening under a lock,
                                        // no one will modify this while we are
                                        // here.
  current_block_starting_lsn_ = max_lsn + 1;
  current_block_bytes_written_ = 0;
  current_block_expiration_ =
      std::chrono::steady_clock::now() + block_time_threshold_;
  current_block_css_result_ = result;
  return true;
}

void StickyCopySetManager::onCopySetAssigned(State& state,
                                             const AppendContext& append_ctx) {
  // Lots of records being written in parallel might lead to the block
  // extending past the threshold, but we are not too concerned about that.
  current_block_bytes_written_ += append_ctx.payload_size;

  // storing the current LSN as the max if it's larger than what we've seen
  // already
  atomic_fetch_max(max_lsn_seen_, append_ctx.lsn);

  // Updating the state so we know whether to change to a new block if this
  // wave fails
  state.last_tried_block_seq_no = block_seq_no_;
}

// see docblock in CopySetSelector::createState()
std::unique_ptr<CopySetManager::State>
StickyCopySetManager::createState() const {
  return std::make_unique<StickyCopySetManager::State>(
      underlying_selector_->createState());
}

StickyCopySetManager::StickyCopySetManager(
    std::unique_ptr<CopySetSelector> selector,
    std::shared_ptr<NodeSetState> nodeset_state,
    size_t sticky_copysets_block_size,
    std::chrono::milliseconds sticky_copysets_block_max_time,
    const CopySetSelectorDependencies* deps)
    : CopySetManager(std::move(selector), nodeset_state),
      block_size_threshold_(sticky_copysets_block_size),
      block_time_threshold_(sticky_copysets_block_max_time),
      deps_(deps) {}

}} // namespace facebook::logdevice
