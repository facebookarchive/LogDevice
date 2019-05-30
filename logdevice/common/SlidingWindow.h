/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <memory>
#include <vector>

#include "logdevice/common/ConstructorFailed.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice {

/**
 * @file SlidingWindow is a templatized lock-free data structure that operates
 * as a sliding window of pointers to objects. We use it to calculate the
 * "last known good" ESN for the epoch, enforce the sequential release
 * rule, and enforce the limit on the maximum number of outstanding appends
 * per log (more precisely, it is a limit on the oldest outstanding append).
 *
 * Objects (Elements) can only be inserted at the leading edge of the
 * window. Insertion fails if the window size (the distance between leading
 * and trailing edges) has reached the limit. Insertion may also fail if
 * the 32-bit sequence number space for the current epoch is almost exhausted.
 *
 * Objects in the window can be retired in arbitrary order. The trailing edge
 * of window moves only when the object at that edge is retired. It moves to
 * the lowest-numbered object not yet retired, or to the leading edge if no
 * such objects are present in the window.
 *
 * The window is implemented as a circular array A of N entries, where N is
 * max-in-flight for the log. Each entry is a 4-aligned pointer with bits 0
 * and 1 used as flags. Entry values and their meaning:
 *
 *  0 : entry is free
 *  non-zero, bits 0 and 1 are off: entry is in use (U)
 *  Bit 0 set: the object in this entry is retired (R)
 *  Bit 1 set: tail is at this entry (T)
 *
 * The entry state transitions as follows: 0 -> U -> U|R -> 0. T can be added at
 * any stage and is only erased with the transition to 0.
 *
 * The array is initialized to {0, T, 0, 0, 0...}  (tail is at entry MIN_ESN)
 *
 * A separate shared atomic counter n is used to limit the total number of
 * outstanding objects to at most N.
 *
 * Every object in the window is identified by a 32-bit ESN (epoch-relative
 * part of LSN) assigned to the record that it attempts to store. That ESN e
 * uniquely identifies the slot in the array that may contain that record
 * (e % N).
 */

static_assert(sizeof(lsn_t) == 8,
              "class SlidingWindow requires a 64-bit lsn_t");
// if we ever move to >32-bit epoch numbers, lsn_t in SlidingWindow
// will have to be replaced with lsn64_t -- the low 64 bits of a
// bigger lsn_t.

const unsigned SLIDING_WINDOW_MIN_CAPACITY = 2;

template <class Element, class Deleter>
class SlidingWindow {
 public:
  static_assert(alignof(Element) % 4 == 0,
                "SlidingWindow Element must be at least 4-byte aligned");

  /**
   * Creates a new SlidingWindow.
   *
   * @param capacity   Window size limit. Must be at least MIN_CAPACITY.
   * @param esn_max    largest ESN to issue in the epoch (typically ESN_MAX
   *                   but can be configured smaller)
   *
   * @throws ConstructorFailed and sets err to INVALID_PARAM if @param capacity
   *         is smaller than MIN_CAPACITY.  Debug builds assert on failure.
   */
  explicit SlidingWindow(int capacity,
                         esn_t esn_max = ESN_MAX,
                         lsn_t start = compose_lsn(EPOCH_INVALID, ESN_MIN))
      : size_(0),
        esn_max_(std::min(esn_max.val_, ESN_MAX.val_ - 1)),
        // This is a bit tricky.  We need all slots in the window to map to
        // valid ESNs (at most `esn_max_') otherwise epoch rollover in
        // advanceEpoch() can break.
        capacity_(std::min<uint64_t>(capacity, esn_max_.val_)),
        right_(start) {
    if (capacity_ < SlidingWindow::MIN_CAPACITY ||
        lsn_to_esn(right_.load()) < ESN_MIN) {
      ld_check(false);
      err = E::INVALID_PARAM;
      throw ConstructorFailed();
    }

    if (lsn_to_esn(right_.load()) > esn_max_) {
      ld_check(false);
      err = E::TOOBIG;
      throw ConstructorFailed();
    }

    state_.reset(new std::atomic<uintptr_t>[capacity_]);
    std::fill(state_.get(), state_.get() + capacity_, (uintptr_t)0);
    slot(right_.load()) = SW_TAIL;
  }

  /**
   * @return the current epoch of the right edge of the window. This
   * can be EPOCH_INVALID if advanceEpoch() has not been called on the
   * window.
   */
  epoch_t epoch() const {
    return lsn_to_epoch(right_.load());
  }

  /**
   * Atomically replaces right_ with an LSN value that has its epoch
   * component equal to @param epoch and its ESN component set to the lowest
   * possible value such that this new LSN is valid and maps to the same slot
   * in the circular buffer as the LSN it replaces.
   *
   * @epoch  epoch number to assign; must be strictly higher than
   *         the current epoch component of right_
   *
   * @return new right_ LSN on success, LSN_INVALID if epoch is not greater
   *         than the current epoch number of this SlidingWindow, sets err
   *         to INVALID_PARAM.
   */
  lsn_t advanceEpoch(epoch_t epoch) {
    lsn_t right_now = right_.load(); // current value of right_
    lsn_t right_next;                // new value of right_

    do {
      if (epoch <= lsn_to_epoch(right_now)) {
        err = E::INVALID_PARAM;
        return LSN_INVALID;
      }
      esn_t::raw_type right_next_esn = index(right_now);
      if (right_next_esn == 0) {
        // 0 is not a valid ESN, pick the next larger ESN that mods to
        // the same slot offset
        right_next_esn = capacity();
      }
      // The window capacity must have been properly set in the constructor so
      // that all slots map to valid ESNs.
      ld_check(right_next_esn <= esn_max_.val_);
      right_next = compose_lsn(epoch, esn_t(right_next_esn));
    } while (!right_.compare_exchange_strong(right_now, right_next));

    return right_next;
  }

  /**
   * Attempts to grow the window by 1, inserting pointer e at right edge.
   *
   * @param   e   pointer to entry to insert. Must not be nullptr.
   *
   * @return  on success a valid lsn at new right edge of window. On failure
   *          LSN_INVALID is returned and err is set to
   *
   *            INVALID_PARAM   p is nullptr or is not 4-byte aligned
   *                            (debug build asserts)
   *            NOBUFS    maximum window size has been reached
   *            TOOBIG    right edge is approaching the end of esn_t range
   */
  lsn_t grow(Element* e) {
    uintptr_t p = reinterpret_cast<uintptr_t>(e);

    if (!p || (p & SW_FLAGS)) {
      // Element is guaranteed to be 4b-aligned, but we still do this run-time
      // check in case the pointer passed to us has been reinterpret_cast.
      ld_check(false);
      err = E::INVALID_PARAM;
      return LSN_INVALID;
    }

    size_t token = size_.fetch_add(1);
    ld_check((ssize_t)token >= 0);

    if (token >= capacity_) {
      size_.fetch_sub(1);
      err = E::NOBUFS;
      return LSN_INVALID;
    }

    // We have reserved 1 slot in the window for a new esn. Now find the
    // actual esn to use.  To do this, increase `right_' atomically using a
    // CAS loop, taking care not to run out of ESNs in the epoch.

    lsn_t r = right_.load();
    do {
      if (lsn_to_esn(r) > esn_max_) {
        // Out of ESNs in this epoch.
        size_.fetch_sub(1);
        err = E::TOOBIG;
        return LSN_INVALID;
      }
      // We don't issue ESN_MAX to avoid overflowing `right_' into the next
      // epoch.  Constructor ought to have esnured this when setting
      // `esn_max_'.
      ld_check(lsn_to_esn(r) < ESN_MAX);
    } while (!right_.compare_exchange_weak(r, r + 1));

    // now atomically |= p into state_[r % capacity_]
    // We do it with an explicit cas loop to check invariants.

    uintptr_t cur; // value of slot [r % N] before we mark it INUSE

    do {
      cur = slot(r).load();

      ld_check((cur & ~SW_TAIL) == 0);
      // cur cannot contain a pointer because we hold 1 space in the
      // window of size capacity_ (=N). The only LSNs that can
      // occupy the same entry as r in the state_[] vector are in the
      // set {r + kN} for arbitrary integers k.  The
      // (size_.fetch_add(1) > capacity_) check at the beginning
      // of this function guarantees that r+kN is not yet issued for
      // all positive k's.  The cleanup code in retire() guarantees
      // that (r + kN) has already been retired for all negative k's
      // by the time we get here.  cur can have TAIL bit set if
      // window is empty
    } while (!slot(r).compare_exchange_strong(cur, cur | p));

    return r;
  }

  /**
   * Retire the entry in a slot previously allocated by grow(). If the slot is
   * at the left edge of the window, shrink the window (move left edge) as far
   * right as possible, until an entry that is not yet retired is reached.
   * Call a deleter for every entry that falls off the left edge of the window.
   * This is called "reaping" the entry.
   *  MUST NOT be called more than once for a given lsn.
   *
   * @param at       a valid LSN previously returned by grow(). It identifies
   *                 the window entry to retire.
   * @param deleter  a deleter functor to call. deleter is only called if
   *                 the left edge of the window moves as a result of this
   *                 retire() call. If the slot being retired is not at the
   *                 left edge at the time of call, deleter will not be called.
   *
   * @return the number of reaped entries, or 0 if _at_ was not at the left
   *         edge of the window
   */
  size_t retire(lsn_t at, Deleter& deleter) {
    unsigned idx = index(at);
    uintptr_t entry = state_[idx].load();
    size_t n_reaped = 0;

    ld_check(lsn_to_esn(at) >= ESN_MIN);
    ld_check(lsn_to_esn(at).val_ <= esn_max_.val_);

    ld_check(!(entry & SW_RETIRED));

    for (;;) {
      // do work for the (idx, entry) pair. If we are at tail, this loop
      // will reap all contiguous retired entries following index(at) in the
      // state_[] circular buffer, moving (idx, entry) forward at every
      // iteration. The loop stops when it hits a non-retired entry or shrinks
      // the window down to 0.

      ld_check(entry & ~SW_FLAGS); // slot must be in use

      if (!(entry & SW_TAIL)) {
        // esn is not at tail. Atomically |= RETIRED into the slot
        if (state_[idx].compare_exchange_strong(entry, entry | SW_RETIRED)) {
          // We won the cas. This means we are still not at tail. Bail
          // and let the entity responsible for the esn at tail reap our
          // esn later.
          break;
        }
        // here we lost the RETIRED cas. The only allowed change to our slot is
        // TAIL moving in from the left. The thread that won the cas
        // doesn't know that our slot is RETIRED. It has given up. Fall
        // through and take over reaping.
      }

      // here esn is at tail

      ld_check(entry & SW_TAIL);

      // Reap Element at idx.
      Element* e = reinterpret_cast<Element*>(entry & ~SW_FLAGS);
      deleter(e);
      n_reaped++;

      // give up our slot
      if (folly::kIsDebug) {
        ld_assert(state_[idx].compare_exchange_strong(entry, 0));
      } else {
        state_[idx].store(0);
      }

      // give up our token
      size_t prev_size = size_.fetch_sub(1);
      ld_check(prev_size > 0);

      // There will be no slot with TAIL set until set_tail() is done.

      unsigned next_idx = next_index(idx);
      bool next_slot_is_retired = set_tail(next_idx);

      if (!next_slot_is_retired) {
        // next entry is not retired. It may not even be in use.
        // Since set_tail() won its cas we are guaranteed that
        // RETIRED did not get set in next entry concurrently, and the
        // present or future owner of next entry will become aware of
        // TAIL in its entry, and will continue moving the window
        // forward. We have passed the baton. Bail.
        break;
      }

      // set_tail() found that next slot was retired. The former
      // owner of next slot was gone before we set TAIL on that slot.
      // Reap that slot on behalf of its former owner in the next
      // iteration.
      idx = next_idx;
      entry = state_[next_idx].load();
    }

    return n_reaped;
  }

  /**
   * @return   current window size
   */
  size_t size() const {
    return std::min(size_.load(), capacity_);
  }

  /**
   * @return   maximum allowed window size
   */
  size_t capacity() const {
    return capacity_;
  }

  /**
   * @return  A sequence number that will be assigned to the next element
   *          passed to a successful call to grow(). This is the leading edge of
   *          the window.
   */
  lsn_t next() const {
    return right_.load();
  }

  /**
   * @return  true if there are enough ESNs available to grow the buffer;
   *          returns false if grow() is likely to fail with TOOBIG
   */
  bool can_grow() const {
    return lsn_to_esn(right_.load()) <= esn_max_;
  }

  // smallest allowed value of capacity in constructor
  static const unsigned MIN_CAPACITY = SLIDING_WINDOW_MIN_CAPACITY;

 private:
  /**
   * @return index of the slot in state_[] array that can hold an
   *         entry for LSN @param lsn
   */
  unsigned index(lsn_t lsn) const {
    return lsn_to_esn(lsn).val_ % capacity();
  }

  unsigned next_index(unsigned idx) const {
    return (idx < capacity() - 1) ? (idx + 1) : 0;
  }

  std::atomic<uintptr_t>& slot(lsn_t lsn) {
    return state_[index(lsn)];
  }

  /**
   * Atomically set TAIL flag in the slot at _idx_ and test whether RETIRED
   * is also on in that slot after we set TAIL. The slot must not yet have
   * TAIL flag set.
   *
   * @return true if RETIRED flag is on in the slot that has just
   *         become new tail slot, otherwise false.
   */
  bool set_tail(const unsigned idx) {
    for (;;) {
      const uintptr_t t = state_[idx].load();
      uintptr_t t_after_cas = t;

      ld_check(!(t & SW_TAIL));

      if (state_[idx].compare_exchange_strong(t_after_cas, t | SW_TAIL)) {
        // we won the TAIL cas. TAIL is now at slot with offset idx.
        return (t & SW_RETIRED);
      } else {
        // we lost the TAIL cas. The only valid state
        // transitions for t slot are 0 => INUSE and INUSE
        // =>INUSE|RETIRED. Try move the tail again.
        ld_check(t == 0 || (t & ~SW_FLAGS) == (t_after_cas & ~SW_FLAGS));
      }
    }
  }

  // This is a token dispenser that approximates the current window
  // size. It is guaranteed to be 0 when the window is empty and no
  // calls to grow() are in progress. It is incremented atomically
  // as tokens are dispensed. A grow() fails unless it can get a
  // token in the range [0..capacity_). Tokens are put back when
  // retire() shrinks the window and when grow() gets a token that's
  // too large. The value may temporarily exceed capacity_.
  std::atomic<size_t> size_;

  // Max ESN to issue, inclusive (typically ESN_MAX - 1)
  const esn_t esn_max_;

  // Maximum window size
  const size_t capacity_;

  // right edge of the window (max LSN in window plus one). Next successful
  // call to grow() will return this LSN.
  std::atomic<lsn_t> right_;

  // circular array of Element pointers and flags defined below. Its size is
  // fixed at construction and equals the capacity.
  std::unique_ptr<std::atomic<uintptr_t>[]> state_;

  // Element state flags:

  // entry is retired and can be passed to a Deleter
  static const uintptr_t SW_RETIRED = 1;

  // this element is at tail (leftmost entry) of the window
  static const uintptr_t SW_TAIL = 2;

  // must cover all SW_ flags above
  static const uintptr_t SW_FLAGS = SW_RETIRED | SW_TAIL;
};

}} // namespace facebook::logdevice
