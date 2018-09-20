/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/CopySetManager.h"

namespace facebook { namespace logdevice {

/**
 * @file A passthrough copyset manager simply forwards all the requests for
 * copysets to the underlying copyset selector. The only thing it does is
 * shuffle the copyset before returning it (optionally; base class allows this
 * to be bypassed).
 */

class PassThroughCopySetManager : public CopySetManager {
 public:
  /**
   * State only contains the underlying copyset selector's state
   */
  class State : public CopySetManager::State {
   public:
    explicit State(std::unique_ptr<CopySetSelector::State> csss)
        : css_state(std::move(csss)){};

    void reset() override {
      if (css_state) {
        css_state->reset();
      }
    }
    // The underlying copyset selector's state
    std::unique_ptr<CopySetSelector::State> css_state;
  };

  explicit PassThroughCopySetManager(
      std::unique_ptr<CopySetSelector> css,
      std::shared_ptr<NodeSetState> nodeset_state)
      : CopySetManager(std::move(css), nodeset_state) {}

  CopySetSelector::Result
  getCopySet(copyset_size_t extras,
             StoreChainLink copyset_out[],
             copyset_size_t* copyset_size_out,
             bool* chain_out,
             const AppendContext& /*append_ctx*/,
             folly::Optional<lsn_t>& block_starting_lsn_out,
             CopySetManager::State& csm_state) override {
    State& state = checked_downcast<State&>(csm_state);
    CopySetSelector::Result res =
        underlying_selector_->select(extras,
                                     copyset_out,
                                     copyset_size_out,
                                     chain_out,
                                     state.css_state.get());

    // see docblock for CopySetManager::shuffleCopySet
    shuffleCopySet(
        copyset_out, *copyset_size_out, chain_out ? *chain_out : false);
    block_starting_lsn_out.clear();
    return res;
  }

  CopySetSelector::Result
  getCopysetUsingUnderlyingSelector(logid_t /*log_id*/,
                                    copyset_size_t extras,
                                    StoreChainLink copyset_out[],
                                    copyset_size_t* copyset_size_out) override {
    return underlying_selector_->select(extras, copyset_out, copyset_size_out);
  }

  CopySetSelector::Result augmentCopySet(StoreChainLink inout_copyset[],
                                         copyset_size_t existing_copyset_size,
                                         copyset_size_t* out_full_size,
                                         bool fill_client_id = false,
                                         bool* chain_out = nullptr) override {
    return underlying_selector_->augment(inout_copyset,
                                         existing_copyset_size,
                                         out_full_size,
                                         fill_client_id,
                                         chain_out,
                                         DefaultRNG::get(),
                                         /* retry = */ true);
  }

  std::unique_ptr<CopySetManager::State> createState() const override {
    return std::unique_ptr<CopySetManager::State>(
        new State(underlying_selector_->createState()));
  }
};

}} // namespace facebook::logdevice
