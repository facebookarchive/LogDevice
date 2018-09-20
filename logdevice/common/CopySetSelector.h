/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>

#include "logdevice/common/Random.h"
#include "logdevice/common/ShardID.h"
#include "logdevice/common/types_internal.h"

namespace facebook { namespace logdevice {

/**
 * @file  Interface for selecting a copyset from a node set to store
 *        replicas of records.
 */

struct StoreChainLink;

class CopySetSelector {
 public:
  // see the @return doc block of the select() function
  enum class Result { SUCCESS, PARTIAL, FAILED };

  /**
   * an abstraction that encapsulates the internal state of the copyset selector
   */
  class State {
   public:
    // reset the internal state
    virtual void reset() = 0;
    virtual ~State() {}
  };

  /**
   * Name of the copyset selector type. Used for logging.
   */
  virtual std::string getName() const {
    return "(unknown)";
  }

  /**
   * Selects a copyset. Note that the select function does
   * not modify the state of the CopySetSelector object and is thread-safe.
   * Things like replication property and nodeset are given to CopySetSelector
   * at construction time.
   *
   * @param extras        extra copies of the log record. extra storage nodes
   *                      are selected for latency optimization but are not
   *                      strictly required in the copyset.
   * @param copyset_out   an array of size of at least
   *                      (getReplicationFactor() + extras)
   *                      to store the result copyset. Elements of the array
   *                      is of type StoreChainLink, which consists of the
   *                      ShardID and ClientID (used for the destination node to
   *                      reply)
   *
   * @param copyset_size_out  to store the size of the result copyset. the value
   *                          may still be modified if the selection fails
   *
   * @param chain_out       if non-null, and one of the destinations is not
   *                        eligible for chain-sending, the function sets
   *                        *chain_out to false. Otherwise it is left
   *                        unchanged.
   *
   * @param selector_state  an optional in-and-out parameter that encapsulates
   *                        the internal state of the selector, the selector
   *                        may leverage the given state to perform stateful
   *                        copyset selections. The selector may also update
   *                        the state object to represent its new internal
   *                        state after a selection is made. Note that the
   *                        selector_state object is not required and may
   *                        not be used in some implementations.
   *
   * @param rng             Random number generator to use. Used only from
   *                        inside this select()/augment() call.
   *
   * @param retry           Used internally by CopySetSelector implementations.
   *                        Always pass `true` (the default) when calling from
   *                        outside.
   *                        In cases where first try didn't succeed (e.g. if
   *                        some nodes were unavailable), the implementation
   *                        resets temporarily unavailable states (e.g. SLOW)
   *                        and tries one more time.
   *
   * @return                CopySetSelect::Result type indicating the outcome
   *                        of selection. Could be one of:
   *                        SUCCESS    a copyset of (_replication_+_extras_)
   *                                   nodes is successfully selected from
   *                                   the nodeset
   *                        PARTIAL    a copyset of at least _replication_ nodes
   *                                   is selected, but there are not enough
   *                                   nodes to store all _extras_
   *                        FAILED     failed to select a copyset of at least
   *                                   _replication_ nodes because there
   *                                   were not enough storage nodes available
   */
  virtual Result select(copyset_size_t extras,
                        StoreChainLink copyset_out[],
                        copyset_size_t* copyset_size_out,
                        bool* chain_out = nullptr,
                        State* selector_state = nullptr,
                        RNG& rng = DefaultRNG::get(),
                        bool retry = true) const = 0;

  /**
   * Given an existing partially selected copyset, add a minimal number of nodes
   * to it to form a valid copyset. Also reorder the resulting copyset so that
   * the first getReplicationFactor() elements form a valid copyset.
   *
   * In particular:
   *  - if the given copyset is already a valid copyset of size
   *    getReplicationFactor(), augment() will leave it unchanged,
   *  - if the given copyset is a valid copyset bigger than
   *    getReplicationFactor(), augment() will reorder it so that the first
   *    getReplicationFactor() nodes form a valid copyset,
   *  - if the given copyset is a subset of a valid copyset of size
   *    getReplicationFactor() (which is almost always the case for rebuilding),
   *    the output will be a superset of the input.
   *
   * Example. Input copyset: [N0, N1, N10]; N0 and N1 are from the same rack,
   * N10 is from another rack, N20 is from yet another rack;
   * replication property: {rack: 3}. Possible output: [N0, N10, N20, N1],
   * *out_full_size = 4.
   *
   * The caller must make sure that `inout_copyset` is allocated to at least
   * `existing_copyset_size` + getReplicationFactor() elements, to accommodate
   * the worst case output.
   *
   * Must be thread safe. Note that unlike the normal select(), currently this
   * procedure does not support stateful selection or chaining.
   *
   * @param  inout_copyset
   *             An array allocated to a length of at least
   *             `existing_copyset_size` + getReplicationFactor(). Used as both
   *             input and output of the method:
   *              - Before the call, the first `existing_copyset_size` elements
   *                contain nodes that are assumed to already have a copy of
   *                the record; cannot contain duplicates.
   *              - After augment() returns SUCCESS, the first `*out_full_size`
   *                elements contain a copyset with the following properties:
   *                  1. it's a superset of the input array, without duplicates,
   *                  2. it's a valid copyset; moreover, the first
   *                     getReplicationFactor() elements form a valid copyset.
   *              - After augment() returns FAILED, the contents are undefined.
   * @param  existing_copyset_size
   *             Size of the existing list of nodes that needs to be augmented
   *             to a valid copyset. Can be 0, e.g. if node is relocating its
   *             own data. Can be greater than getReplicationFactor(), e.g. when
   *             called from Mutator for an already-overreplicated record.
   * @param  out_full_size
   *             The size of the output set will be stored here.
   *
   * @param retry In cases where first try didn't succeed(e.g. if some
   *              nodes were unavailable), the implementation resets
   *              temporarily unavailable states(e.g. SLOW) and tries
   *              one more time.
   * @return      CopySetSelector::Result type indicating the outcome of
   *              selection. Could be one of:
   *                SUCCESS   The copyset is successfully selected.
   *                FAILED    It's impossible to get a valid copyset by adding
   *                          nodes to the given set. Probably because too many
   *                          nodes are unavailable.
   *
   * TODO(T16599789): this augment() overload is being deprecated in favor of
   * the one below.
   */
  virtual Result augment(ShardID inout_copyset[],
                         copyset_size_t existing_copyset_size,
                         copyset_size_t* out_full_size,
                         RNG& rng = DefaultRNG::get(),
                         bool retry = true) const = 0;

  /*
   * Similar to but a superset of the augment API above. Note that inout_copyset
   * is StoreChainLink[] to potentially support chaining. Additional parameters:
   *
   * @param fill_client_id when false, the ClientIDs in inout_copyset might be
   * the default value ClientID::INVALID; when true, the CopySetSelector will
   * try to obtain ClientIDs for the _augmented_ destinations.
   *
   * @param chain_out same as chain_out in select()
   *
   * For context, see T16599789.
   */
  virtual Result augment(StoreChainLink inout_copyset[],
                         copyset_size_t existing_copyset_size,
                         copyset_size_t* out_full_size,
                         bool fill_client_id = false,
                         bool* chain_out = nullptr,
                         RNG& rng = DefaultRNG::get(),
                         bool retry = true) const = 0;

  /**
   * This CopySetSelector selects copysets of size
   * getReplicationFactor() + `extras`.
   */
  virtual copyset_size_t getReplicationFactor() const = 0;

  /**
   * Create a CopySetSelector::State object. Users of this function should
   * ensure that State object returned cannot outlive the CopySetSelector.
   * Can return nullptr if the copyset selector doesn't need any state.
   *
   * The returned state may reference the provided `rng`, so the RNG needs to
   * outlive the State. If select() is called with a different RNG than the
   * RNG State was created with, select() may use either or both of the RNGs.
   */
  virtual std::unique_ptr<State>
  createState(RNG& rng = DefaultRNG::get()) const {
    return nullptr;
  }

  virtual ~CopySetSelector() {}
};

}} // namespace facebook::logdevice
