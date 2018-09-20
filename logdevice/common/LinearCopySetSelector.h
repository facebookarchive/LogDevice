/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/CopySetSelector.h"
#include "logdevice/common/CopySetSelectorDependencies.h"
#include "logdevice/common/RandomLinearIteratorBase.h"

namespace facebook { namespace logdevice {

/**
 * @file LinearCopySetSelector is an implementation of CopySetSelector that
 *       always attempt to select storage nodes consecutively in the node set.
 *       The starting position is either randomly picked within the nodeset
 *       or specified by the start_hint. The copyset selector then tries to
 *       pick a copyset consecutively begin with the starting position. During
 *       the selection, if a node is not available to store a copy, the selector
 *       skips the node and tries the node next to the unavailable one.
 */

class LinearCopySetSelector : public CopySetSelector {
 public:
  // An iterator in a list of nodes that decides which nodes to pick
  // and tracks the selection state so far
  class Iterator {
   public:
    explicit Iterator(const LinearCopySetSelector* selector,
                      RNG& rng,
                      const StorageSet* nodeset = nullptr);

    /**
     * @return on success returns the index of next node in the underlying
     *         NodeSet. If the iterator object is not associated with a NodeSet,
     *         or it is at the end of a node index sequence, returns -1.
     */
    ShardID next();

    /**
     * Resets the iterator. If nodeset is non-NULL, the iterator is associated
     * with that NodeSet object. Otherwise the iterator remains on the same
     * NodeSet object as before (or none). start_ is placed at a randomly
     * selected location of nodeset_.
     */
    void reset(const StorageSet* nodeset = nullptr);

    /**
     * Check if the iterator is attached to a nodeset object
     */
    bool isValid() const {
      return nodeset_ != nullptr;
    }

    /**
     * Check if the iterator is attached to the given nodeset object
     */
    bool checkNodeSet(const StorageSet* nodeset) const {
      return nodeset_ == nodeset;
    }

   private:
    const StorageSet* nodeset_; // this is what we iterate over
    RandomLinearIteratorBase<ShardID, std::vector> base_it_;
  };

  // the internal state is just a wrapper around an Iterator
  class State : public CopySetSelector::State {
   public:
    explicit State(const LinearCopySetSelector* selector, RNG& rng)
        : selector_(selector), it_(selector_, rng) {
      ld_check(selector_ != nullptr);
    }

    void reset() override {
      it_.reset();
    };

   private:
    const LinearCopySetSelector* const selector_;
    Iterator it_;

    friend class LinearCopySetSelector;
  };

  explicit LinearCopySetSelector(copyset_size_t replication_factor,
                                 StorageSet nodeset,
                                 std::shared_ptr<NodeSetState> nodeset_state,
                                 const CopySetSelectorDependencies* deps =
                                     CopySetSelectorDependencies::instance())
      : deps_(deps),
        replication_factor_(replication_factor),
        nodeset_(std::move(nodeset)),
        nodeset_state_(nodeset_state) {
    ld_check(replication_factor_ > 0 &&
             replication_factor_ <= COPYSET_SIZE_MAX);
  }

  std::string getName() const override;

  // see docblock in CopySetSelector::select()
  CopySetSelector::Result
  select(copyset_size_t extras,
         StoreChainLink copyset_out[],
         copyset_size_t* copyset_size_out,
         bool* chain_out = nullptr,
         CopySetSelector::State* selector_state = nullptr,
         RNG& rng = DefaultRNG::get(),
         bool retry = true) const override;

  // see docblock in CopySetSelector::augment()
  CopySetSelector::Result augment(ShardID inout_copyset[],
                                  copyset_size_t existing_copyset_size,
                                  copyset_size_t* out_full_size,
                                  RNG& rng = DefaultRNG::get(),
                                  bool retry = true) const override;

  CopySetSelector::Result augment(StoreChainLink inout_copyset[],
                                  copyset_size_t existing_copyset_size,
                                  copyset_size_t* out_full_size,
                                  bool fill_client_id = false,
                                  bool* chain_out = nullptr,
                                  RNG& rng = DefaultRNG::get(),
                                  bool retry = true) const override;

  copyset_size_t getReplicationFactor() const override {
    return replication_factor_;
  }

  // see docblock in CopySetSelector::createState()
  std::unique_ptr<CopySetSelector::State>
  createState(RNG& rng = DefaultRNG::get()) const override;

  // maximum number of retries for picking an available node as
  // the first node of the copyset
  static const size_t MAX_FIRST_NODE_RETRIES;

 private:
  // predicate function on whether a node should be selected
  using pred_func_t = std::function<bool(ShardID)>;

  // Implementation function for select(), caller can provide a predicate
  // function through @param pred to filter out certain nodes
  copyset_size_t selectImpl(copyset_size_t ncopies,
                            StoreChainLink copyset_out[],
                            bool* chain_out,
                            CopySetSelector::State* selector_state,
                            RNG& rng,
                            pred_func_t pred) const;

  // functions to manipulate the iterator of a nodeset, override in tests
  virtual ShardID getNextCandidate(Iterator* it) const;
  virtual void resetCandidates(const StorageSet& nodeset, Iterator* it) const;

  const CopySetSelectorDependencies* deps_;

  const copyset_size_t replication_factor_;
  const StorageSet nodeset_;
  const std::shared_ptr<NodeSetState> nodeset_state_;
};

}} // namespace facebook::logdevice
