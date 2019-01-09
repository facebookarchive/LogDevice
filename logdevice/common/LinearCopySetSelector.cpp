/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/LinearCopySetSelector.h"

#include "logdevice/common/Worker.h"
#include "logdevice/common/debug.h"

namespace facebook { namespace logdevice {

using NodeStatus = NodeAvailabilityChecker::NodeStatus;

const size_t LinearCopySetSelector::MAX_FIRST_NODE_RETRIES = 3;

LinearCopySetSelector::Iterator::Iterator(const LinearCopySetSelector* selector,
                                          RNG& rng,
                                          const StorageSet* nodeset)
    : nodeset_(nodeset), base_it_(rng, nodeset) {}

ShardID LinearCopySetSelector::Iterator::next() {
  ShardID ret;
  if (base_it_.next(&ret) != 0) {
    return ShardID();
  }
  return ret;
}

void LinearCopySetSelector::Iterator::reset(const StorageSet* nodeset) {
  if (nodeset != nullptr) {
    nodeset_ = nodeset;
  }
  if (nodeset) {
    base_it_.setContainer(nodeset);
  }
  base_it_.reset();
}

std::string LinearCopySetSelector::getName() const {
  return "LinearCopySetSelector";
}

CopySetSelector::Result
LinearCopySetSelector::select(copyset_size_t extras,
                              StoreChainLink copyset_out[],
                              copyset_size_t* copyset_size_out,
                              bool* chain_out,
                              CopySetSelector::State* selector_state,
                              RNG& rng,
                              bool retry) const {
  ld_check(copyset_out);
  ld_check(copyset_size_out);
  ld_check(extras <= COPYSET_SIZE_MAX - replication_factor_);
  copyset_size_t ndest = selectImpl(replication_factor_ + extras,
                                    copyset_out,
                                    chain_out,
                                    selector_state,
                                    rng,
                                    nullptr);
  if (ndest < replication_factor_) {
    nodeset_state_->resetGrayList(
        NodeSetState::GrayListResetReason::CANT_PICK_COPYSET);
    auto worker = Worker::onThisThread(false);
    if (worker) {
      worker->resetGraylist();
    }
    if (retry) {
      return select(extras,
                    copyset_out,
                    copyset_size_out,
                    chain_out,
                    selector_state,
                    rng,
                    false /* retry */);
    }
    return CopySetSelector::Result::FAILED;
  }

  *copyset_size_out = ndest;
  return ndest < replication_factor_ + extras
      ? CopySetSelector::Result::PARTIAL
      : CopySetSelector::Result::SUCCESS;
}

CopySetSelector::Result
LinearCopySetSelector::augment(StoreChainLink inout_copyset[],
                               copyset_size_t existing_copyset_size,
                               copyset_size_t* out_full_size,
                               bool fill_client_id,
                               bool* chain_out,
                               RNG& rng,
                               bool retry) const {
  ld_check(inout_copyset != nullptr);

  if (out_full_size) {
    *out_full_size = std::max(existing_copyset_size, replication_factor_);
  }

  if (existing_copyset_size >= replication_factor_) {
    // Existing copyset is already big enough.
    return Result::SUCCESS;
  }

  // Select replication - existing_copy_size more nodes that are not in
  // existing copyset.
  auto pred = [=](ShardID idx) {
    auto it = std::find_if(
        inout_copyset,
        inout_copyset + existing_copyset_size,
        [idx](StoreChainLink const& c) { return c.destination == idx; });
    return it == inout_copyset + existing_copyset_size;
  };

  copyset_size_t ndest = selectImpl(replication_factor_ - existing_copyset_size,
                                    inout_copyset + existing_copyset_size,
                                    chain_out,
                                    nullptr,
                                    rng,
                                    pred);

  if (ndest < replication_factor_ - existing_copyset_size) {
    nodeset_state_->resetGrayList(
        NodeSetState::GrayListResetReason::CANT_PICK_COPYSET);

    auto worker = Worker::onThisThread(false);
    if (worker) {
      worker->resetGraylist();
    }
    if (retry) {
      return augment(inout_copyset,
                     existing_copyset_size,
                     out_full_size,
                     fill_client_id,
                     chain_out,
                     rng,
                     false /* retry */);
    }
    return CopySetSelector::Result::FAILED;
  }

  ld_check(ndest == replication_factor_ - existing_copyset_size);
  return CopySetSelector::Result::SUCCESS;
}

CopySetSelector::Result
LinearCopySetSelector::augment(ShardID inout_copyset[],
                               copyset_size_t existing_copyset_size,
                               copyset_size_t* out_full_size,
                               RNG& rng,
                               bool retry) const {
  ld_check(inout_copyset != nullptr);

  if (existing_copyset_size >= replication_factor_) {
    // Existing copyset is already big enough.
    return Result::SUCCESS;
  }

  StoreChainLink new_copyset[COPYSET_SIZE_MAX];
  copyset_size_t new_out_full_size;
  std::transform(inout_copyset,
                 inout_copyset + existing_copyset_size,
                 new_copyset,
                 [](const ShardID& idx) {
                   return StoreChainLink{idx, ClientID::INVALID};
                 });
  auto result = augment(new_copyset,
                        existing_copyset_size,
                        &new_out_full_size,
                        /* fill_client_id = */ false,
                        /* chain_out = */ nullptr,
                        rng,
                        retry);

  if (result != CopySetSelector::Result::SUCCESS) {
    return result;
  }

  std::transform(new_copyset + existing_copyset_size,
                 new_copyset + new_out_full_size,
                 inout_copyset + existing_copyset_size,
                 [](const StoreChainLink& c) { return c.destination; });

  if (out_full_size) {
    *out_full_size = new_out_full_size;
  }

  return CopySetSelector::Result::SUCCESS;
}

copyset_size_t
LinearCopySetSelector::selectImpl(copyset_size_t ncopies,
                                  StoreChainLink copyset_out[],
                                  bool* chain_out,
                                  CopySetSelector::State* selector_state,
                                  RNG& rng,
                                  pred_func_t pred) const {
  ld_check(copyset_out != nullptr);

  copyset_size_t ndest = 0;
  Iterator it(this, rng);
  Iterator* candidates;

  if (selector_state != nullptr) {
    // use the existing iterator inside selector_state to start the selection
    ld_assert(dynamic_cast<State*>(selector_state) != nullptr);
    candidates = &static_cast<State*>(selector_state)->it_;
    if (!candidates->checkNodeSet(&nodeset_)) {
      // if the nodeset of the selector_state is empty or does not match
      // the current nodeset given, reset the iterator in selector_state.

      // Note: appenders now refresh its copyset manager (selector) on
      // each wave so it is possible that nodeset is changed as some
      // node changing their weight from -1 to non-negative, or some
      // previously invalid node_id in the nodeset reappear in the config.
      resetCandidates(nodeset_, candidates);
    }
  } else {
    // use the newly created iterator
    resetCandidates(nodeset_, &it);
    candidates = &it;
  }

  ld_check(candidates != nullptr);

  size_t first_node_retries = 0;
  while (ndest < ncopies) {
    ShardID dest_idx = getNextCandidate(candidates);
    if (!dest_idx.isValid()) {
      // All nodes in the nodeset have been looked at.
      // Return a short count
      break;
    }

    StoreChainLink destination;
    auto node_status = deps_->getNodeAvailability()->checkNode(
        nodeset_state_.get(), dest_idx, &destination);
    switch (node_status) {
      case NodeStatus::AVAILABLE_NOCHAIN:
        if (chain_out) {
          *chain_out = false;
        }
        break;
      case NodeStatus::AVAILABLE:
        break;
      case NodeStatus::NOT_AVAILABLE:

        // If we are selecting the first node of the nodeset, instead of
        // continuing trying the immediate next node, randomly pick another
        // node as the first node until an available node is selected or
        // the maximum number of retry is reached. This helps to prevent
        // healthy nodes that are immediately after unavailable nodes from
        // having a higher probability to be selected and cause load imbalance
        if (ndest == 0 && first_node_retries++ < MAX_FIRST_NODE_RETRIES) {
          resetCandidates(nodeset_, candidates);
        }
        // skip the node
        continue;
    }

    if (pred != nullptr && !pred(dest_idx)) {
      // filtered out by the pred function
      continue;
    }

    copyset_out[ndest++] = destination;
  }

  ld_check(ndest <= ncopies);
  return ndest;
}

std::unique_ptr<CopySetSelector::State>
LinearCopySetSelector::createState(RNG& rng) const {
  std::unique_ptr<CopySetSelector::State> state(new State(this, rng));
  return state;
}

ShardID LinearCopySetSelector::getNextCandidate(Iterator* it) const {
  return it->next();
}

void LinearCopySetSelector::resetCandidates(const StorageSet& nodeset,
                                            Iterator* it) const {
  it->reset(&nodeset);
}

}} // namespace facebook::logdevice
