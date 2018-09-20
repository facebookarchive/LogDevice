/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>

#include <folly/Optional.h>
#include <folly/SharedMutex.h>

#include "logdevice/common/CopySetManager.h"
#include "logdevice/common/CopySetSelectorDependencies.h"
#include "logdevice/common/ShardID.h"
#include "logdevice/common/types_internal.h"

namespace facebook { namespace logdevice {

/**
 * @file A sticky copyset manager maintains the same copyset for blocks of
 * records - a number of records being stored consecutively. It will start a
 * new block by generating a new copyset whenever a threshold for the total
 * size of processed appends is hit, or the block's maximum lifespan expires.
 * Currently this is an implementation of a single-copyset selector.  When we
 * implement distributed appenders, we will want to maintain several copysets
 * and assign them based on which location scope the appender is in.
 */

class StickyCopySetManager : public CopySetManager {
 public:
  /**
   * State is used to track retries, so a new block could be started when
   * stores fail.
   */
  struct State : public CopySetManager::State {
    explicit State(std::unique_ptr<CopySetSelector::State> csss)
        : css_state(std::move(csss)) {}
    void reset() override {
      // Resets the copyset selector state (not the ptr)
      if (css_state) {
        css_state->reset();
      }
    }
    // The sequence number of the copyset we have tried previously. Used to
    // detect whether we should be changing the copyset when retrying or not
    lsn_t last_tried_block_seq_no{0};
    std::unique_ptr<CopySetSelector::State> css_state;
  };

  StickyCopySetManager(std::unique_ptr<CopySetSelector> selector,
                       std::shared_ptr<NodeSetState> nodeset_state,
                       size_t sticky_copysets_block_size,
                       std::chrono::milliseconds sticky_copysets_block_max_time,
                       const CopySetSelectorDependencies* deps =
                           CopySetSelectorDependencies::instance());

  // see docblock in CopySetManager::getCopySet()
  CopySetSelector::Result
  getCopySet(copyset_size_t extras,
             StoreChainLink copyset_out[],
             copyset_size_t* copyset_size_out,
             bool* chain_out,
             const AppendContext& append_ctx,
             folly::Optional<lsn_t>& block_starting_lsn_out,
             CopySetManager::State& state) override;

  CopySetSelector::Result augmentCopySet(StoreChainLink inout_copyset[],
                                         copyset_size_t existing_copyset_size,
                                         copyset_size_t* out_full_size,
                                         bool fill_client_id = false,
                                         bool* chain_out = nullptr) override {
    throw std::runtime_error("unimplemented");
  }

  // see docblock in CopySetManager::GetCopysetUsingUnderlyingSelector()
  CopySetSelector::Result
  getCopysetUsingUnderlyingSelector(logid_t log_id,
                                    copyset_size_t extras,
                                    StoreChainLink copyset_out[],
                                    copyset_size_t* copyset_size_out) override;

  // see docblock in CopySetSelector::createState()
  std::unique_ptr<CopySetManager::State> createState() const override;

 private:
  // returns true if a new block should be started - either because the current
  // copyset was generated with outdated inputs, or because block size
  // threshold has been exceeded
  virtual bool shouldStartNewBlock(const State& state,
                                   copyset_size_t extras) const;

  // Checks if nodes are up. Returns true and writes the copyset to the args if
  // all nodes are available. Otherwise returns false.
  bool checkAndOutputNodes(StoreChainLink copyset_out[],
                           copyset_size_t* copyset_size_out,
                           bool* chain_out) const;

  // gets a new copyset from the underlying copyset selector and sets copyset_
  // to it. Then starts a new block - resets the block starting LSN and the
  // counters that keep track of block thresholds. Returns true if getting
  // a copyset for the new block was successful
  bool startNewBlock(State& state,
                     const AppendContext& append_ctx,
                     copyset_size_t extras,
                     bool* chain_out);

  // This is called when we assign a copyset to a particular record. Used to
  // update counters, etc.
  virtual void
  onCopySetAssigned(State& state,
                    const CopySetManager::AppendContext& append_ctx);
  folly::SharedMutex mutex_;

  // When the number of bytes appended in a block exceeds this value, we start
  // a new one.
  const size_t block_size_threshold_;

  // When the age of a block exceeds this value, we start a new one.
  const std::chrono::milliseconds block_time_threshold_;

  // Number of extras. Used to start a new block if this changes.
  copyset_size_t extras_{0};

  // Starting LSN of the current block
  lsn_t current_block_starting_lsn_{LSN_INVALID};

  // The largest LSN we've issued a copyset for. Used to track whether to issue
  // a record as part of a block, or to have it as an out-of-block record
  // instead
  std::atomic<lsn_t> max_lsn_seen_{LSN_INVALID};

  // The counter of bytes written so far in a block, to know when we have
  // exceeded the block size threshold
  std::atomic<size_t> current_block_bytes_written_{0};

  // The time when this block should expire
  std::chrono::time_point<std::chrono::steady_clock> current_block_expiration_;

  // The return value of `CopySetSelector::select()` that we got from the
  // underlying copyset selector for the current block. This is what
  // `getCopySet()` will return. Should never be
  // `CopySetSelector::Result::FAILED`
  CopySetSelector::Result current_block_css_result_;

  // The copyset of the current block
  std::vector<ShardID> copyset_;

  // This counter is being bumped every time we change the copyset, so we can
  // keep track of which copyset the appender used.
  size_t block_seq_no_{1};

  // TODO (t9002309): implement block records. Currently this will only send
  // single records
  bool send_block_records_{false};

  const CopySetSelectorDependencies* deps_;
};

}} // namespace facebook::logdevice
