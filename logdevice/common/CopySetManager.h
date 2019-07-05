/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <cstddef>
#include <memory>

#include <folly/Optional.h>

#include "logdevice/common/CopySetSelector.h"
#include "logdevice/common/NodeSetState.h"
#include "logdevice/common/types_internal.h"

namespace facebook { namespace logdevice {

/**
 * @file A copyset manager acts as a wrapper around the copyset selector.
 * Owns a NodeSetState.
 * Unlike the copyset selector, it can maintain internal state, and thus can
 * choose to return copysets on its own rather than request them from the
 * copyset selector. The primary use of this is the StickyCopySetManager
 */

struct StoreChainLink;

class CopySetManager {
 public:
  /**
   * This struct contains some contextual information about the append that
   * could be used when selecting a copyset. Currently only contains the size
   * (in bytes) of the payload and the lsn. In the future will likely contain
   * the distributed appender location.
   */
  struct AppendContext {
    size_t payload_size;
    lsn_t lsn;
  };

  /**
   * Each Appender state machine maintains an instance of CopySetManager::State
   * or a derived class.
   */
  struct State {
    virtual void reset() = 0;
    virtual ~State() {}
  };

  CopySetManager(std::unique_ptr<CopySetSelector> css,
                 std::shared_ptr<NodeSetState> nodeset_state);

  virtual ~CopySetManager();

  // Gets a copyset for a record. See docblock for CopySetSelector::select()
  // for an explanation of most of these parameters. Others include:
  // @param append_ctx
  //    information on the append that the copyset is being requested for, used
  //    to keep track of block boundaries
  // @param block_starting_lsn_out
  //    If called on a block based copyset manager (e.g StickyCopySetManager),
  //    this will be set to the LSN that started the current block if a block
  //    record should be written, or to LSN_INVALID if this is a single
  //    out-of-block entry. If the manager is not block based, the value will
  //    not be assigned.
  // @param state
  //    state of the manager for a particular appender.
  virtual CopySetSelector::Result
  getCopySet(copyset_size_t extras,
             StoreChainLink copyset_out[],
             copyset_size_t* copyset_size_out,
             bool* chain_out,
             const AppendContext& append_ctx,
             folly::Optional<lsn_t>& block_starting_lsn_out,
             State& state) = 0;

  /**
   * Similar to getCopySet() above except that a new copyset
   * is picked by directly calling underlying selector.
   *
   * GET_SEQ_STATE_Message is the only consumer currently.
   */
  virtual CopySetSelector::Result
  getCopysetUsingUnderlyingSelector(logid_t log_id,
                                    copyset_size_t extras,
                                    StoreChainLink copyset_out[],
                                    copyset_size_t* copyset_size_out) = 0;

  /**
   * Augments a copyset for a record. See docblock for
   * CopySetSelector::augment() for an explanation of these parameters.
   */
  virtual CopySetSelector::Result
  augmentCopySet(StoreChainLink inout_copyset[],
                 copyset_size_t existing_copyset_size,
                 copyset_size_t* out_full_size,
                 bool fill_client_id = false,
                 bool* chain_out = nullptr) = 0;

  // see docblock in CopySetSelector::createState()
  virtual std::unique_ptr<CopySetManager::State> createState() const = 0;

  // returns the copyset selector
  CopySetSelector* getCopySetSelector() {
    return underlying_selector_.get();
  }

  std::shared_ptr<NodeSetState> getNodeSetState() {
    return nodeset_state_;
  }

  // Disables copyset shuffling before returning the result in getCopySet().
  // Used in tests
  void disableCopySetShuffling();

  // Returns false if this CopySetManager needs to be replaced with a new one
  // to reflect config changes. More precisely, if the set of writeable storage
  // shards in nodeset has changed after prepareConfigMatchCheck() was called.
  bool matchesConfig(
      const configuration::nodes::NodesConfiguration& nodes_configuration);

  // Call this after constructing CopySetManager. Only needed if you're going to
  // use matchesConfig(); matchesConfig() will check whether cfg given to
  // prepareConfigMatchCheck() and to matchesConfig() are significantly
  // different.
  void prepareConfigMatchCheck(
      StorageSet nodeset,
      const configuration::nodes::NodesConfiguration& nodes_configuration);

 protected:
  // Randomly permute the copyset. The leftmost node in the copyset is the
  // Leader node if SCD is enabled. When at steady state (no failures), only
  // this node should send its copy of this record to the readers, so shuffling
  // the copyset helps distribute read load more evenly across the cluster
  void shuffleCopySet(StoreChainLink* copyset, int size, bool chain);

  std::unique_ptr<CopySetSelector> underlying_selector_;
  std::shared_ptr<NodeSetState> nodeset_state_;

  bool shuffle_copysets_{true}; // when false, copysets will not be shuffled

  // Used for handling config updates: if the nodes config has changed in such
  // a way that effective_nodeset_ is not correct anymore, we need to create
  // a new CopySetManager.
  StorageSet full_nodeset_;
  // Positive-weight nodes in the nodeset.
  StorageSet effective_nodeset_;
};

}} // namespace facebook::logdevice
