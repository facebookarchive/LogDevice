/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>
#include <random>

#include "logdevice/common/Random.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/types_internal.h"

namespace facebook { namespace logdevice {

/**
 * @file  RandomLinearIteratorBase is the base class for a type of iterator that
 *        iterates elements in a random accessible container. The iterator,
 *        once attaches to the container, picks a random starting position
 *        in the container to begin iteration. After that, the iterator just
 *        linearly iterates through elements in the container from the starting
 *        position everytime next() is called. It wraps around when end of the
 *        container is reached and is considered at end when it reaches the
 *        starting position after wrapping.
 *        After calls to blacklist*AndReset() functions, the blacklisted
 *        elements are guaranteed not to show up in calls to next(). More
 *        importantly, the probability of picking each remaining element as the
 *        starting element of the sequence is uniform. Internally, the iterator
 *        uses a blacklist or a whitelist depending on the amount of nodes
 *        blacklisted, to ensure optimal performance.
 *        TODO (t10242458): When the iterator switches to using a whitelist, it
 *        might return elements of the container out of order.
 */

template <typename T,
          template <typename, typename = std::allocator<T>> class Container,
          bool enable_blacklisting = true>
class RandomLinearIteratorBase {
 public:
  explicit RandomLinearIteratorBase(RNG& rng,
                                    const Container<T>* container = nullptr)
      : container_(container), start_(-1), rng_(rng) {}

  /**
   * @return  0 in case the iterator advances to the next element, and the
   *          element is return in @param out.
   *          -1 if container_ is nullptr, empty, or the iterator reached the
   *          end.
   */
  int next(T* out) {
    ld_check(out != nullptr);

    if (using_whitelist_) {
      // we read offsets from the whitelist
      size_t offset;
      if (whitelist_iterator_->next(&offset) == -1) {
        return -1;
      }
      cur_ = offset;
      if (start_ == -1) {
        start_ = cur_;
      }
      ld_check(start_ >= 0);
      ld_check(start_ < container_->size());
      ld_check(cur_ >= 0);
      ld_check(cur_ < container_->size());

      *out = (*container_)[cur_];
      return 0;
    }

    if (start_ == -1) { // not yet initialized
      reset();
    }

    if (cur_ == start_) {
      // container_ is nullptr, empty, or reached the end
      return -1;
    }

    ld_check(start_ >= 0);

    if (cur_ == -1) {
      cur_ = start_;
    }

    // get to the next element that is not blacklisted
    bool blacklisted;
    do {
      if (++cur_ == container_->size()) {
        cur_ = 0;
      }
      blacklisted = blacklist_.contains(cur_);
    } while (blacklisted && (cur_ != start_));

    if (blacklisted) {
      // all elements that were found were in the blacklist
      ld_check(cur_ == start_);
      return -1;
    }

    ld_check(cur_ >= 0);
    ld_check(cur_ < container_->size());

    *out = (*container_)[cur_];
    return 0;
  }

  /**
   * Attaches the iterator to the specified container.
   */
  void setContainer(const Container<T>* container) {
    ld_check(container);
    container_ = container;
    clearBlacklist();
  }

  /**
   * Attaches the iterator to the specified container and calls reset().
   */
  void setContainerAndReset(const Container<T>* container) {
    ld_check(container);
    setContainer(container);
    reset();
  }

  /**
   *  Resets the iterator to a random starting position in the container.
   *  NB: does not clear the blacklist
   */
  void reset() {
    if (!container_ || (using_whitelist_ && whitelist_.empty()) ||
        (!using_whitelist_ && blacklist_.size() == container_->size())) {
      start_ = -1;
      cur_ = -1;
      return;
    }

    if (using_whitelist_) {
      // reset the whitelist iterator
      start_ = -1;
      whitelist_iterator_->reset();
    } else {
      // select using blacklist - we choose among (N - B) elements, where N
      // is the total number of elements, and B is the blacklist size. Then
      // we iterate through the blacklist to find a mapping of the result to an
      // offset in the actual vector
      size_t num_to_select = 0;
      ld_check(blacklist_.size() < container_->size());
      num_to_select = container_->size() - blacklist_.size();

      start_ = rng_() % num_to_select;
      start_ = blacklist_.findTrueOffset(start_);
      ld_check(start_ >= 0);
      ld_check(start_ < container_->size());
    }

    cur_ = -1;
  }

  /**
   * Seeks to an element with the given value. If there are multiple elements
   * with this value in the container, it will seek to the first one. Note that
   * the element should exist in the container and should not be blacklisted.
   * If the iterator has not been reset before, resets it to start from the
   * position that it was seeked to.
   */
  bool seekToValue(const T& val) {
    ld_check(container_);
    auto it = std::find(container_->begin(), container_->end(), val);
    if (it == container_->end()) {
      ld_check(false);
      return false;
    }
    size_t offset = std::distance(container_->begin(), it);
    if (folly::kIsDebug) {
      if (using_whitelist_) {
        ld_assert(whitelist_iterator_);
        bool whitelist_seek = whitelist_iterator_->seekToValue(offset);
        ld_assert(whitelist_seek);
      } else {
        ld_assert(!blacklist_.contains(offset));
      }
    }
    cur_ = offset;
    if (start_ == -1) {
      start_ = cur_;
    }
    return true;
  }

  /**
   * Blacklists the current element calls reset()
   */
  bool blacklistCurrentAndReset() {
    if (!enable_blacklisting) {
      ld_check(false);
      return false;
    }
    ld_check(cur_ != -1);
    ld_check(container_);
    if (blacklist_.size() >= max_blacklist_size_ && !using_whitelist_) {
      convertBlackListToWhiteList(cur_);
    } else if (using_whitelist_) {
      removeCurrentFromWhiteList();
    } else {
      // assert that this value was unique
      bool res = blacklist_.addValue(cur_);
      ld_check(res);
    }
    reset();
    return true;
  }

  /**
   * Clears the blacklist (and the whitelist as well)
   */
  void clearBlacklist() {
    if (!enable_blacklisting) {
      ld_check(false);
      return;
    }
    using_whitelist_ = false;
    whitelist_iterator_.reset();
    whitelist_.clear();
    blacklist_.clear();
  }

  /**
   * returns true if the iterator is pointing to an element
   */
  bool valid() {
    return start_ != -1 && cur_ != -1;
  }

  /**
   * returns the index of the current element
   */
  size_t getIndex() {
    return cur_;
  }

 private:
  /**
   * Removes the current entry from the whitelist
   */
  void removeCurrentFromWhiteList() {
    // removing current value from the whitelist
    if (!enable_blacklisting) {
      ld_check(false);
      return;
    }
    size_t whitelist_idx = whitelist_iterator_->getIndex();
    ld_check(whitelist_idx >= 0);
    ld_check(whitelist_idx < whitelist_.size());
    erase_from_vector(whitelist_, whitelist_idx);
  }

  /**
   * Converts the blacklist to a whitelist
   *
   * new_entry_idx: index of the new entry that was being added to the
   * blacklist. It's not in the blacklist (as it reached the size limit), but we
   * have to exclude it from the new whitelist.
   */
  void convertBlackListToWhiteList(size_t new_entry_idx) {
    if (!enable_blacklisting) {
      ld_check(false);
      return;
    }
    // generate whitelist - all offsets except for those that are in the
    // blacklist and the one that was given here as an arg
    ld_check(blacklist_.size() < container_->size());
    whitelist_.reserve(container_->size() - blacklist_.size() - 1);
    auto it = blacklist_.begin();
    for (size_t i = 0; i < container_->size(); ++i) {
      if (it == blacklist_.end() || *it != i) {
        if (i != new_entry_idx) {
          whitelist_.push_back(i);
        }
      } else {
        ++it;
      }
    }
    ld_check(whitelist_.size() == container_->size() - blacklist_.size() - 1);
    using_whitelist_ = true;
    whitelist_iterator_.reset(
        new RandomLinearIteratorBase<size_t, std::vector, false>(
            rng_, &whitelist_));
  }

  const Container<T>* container_; // this is what we iterate over

  // offset in the container identifying the item
  // in this container at which iteration started
  size_t start_;

  // offset in the container identifying the element
  // index that was returned by last call to next(), or -1 if next()
  // has not been called since construction or last reset()
  size_t cur_;

  // random engine to perform random selection
  RNG& rng_;

  // By default, we will iterate over all nodes that are not in the blacklist.
  // However, maintaining the blacklist is expensive if it gets large, so if
  // it exceeds max_blacklist_size_, we switch to a whitelist instead.
  bool using_whitelist_{false};

  // The blacklist is a small_vector of upto 4 offsets in the container we are
  // iterating over.
  class BlackList : public folly::small_vector<size_t, 4> {
   public:
    // When we do element selection from the container, we end up with an offset
    // that refers to the offset of the element in the container
    // `container - blacklist`, so we have to map this offset back to the
    // original container. This is what this method does. For example:
    // `container` == [0 1 2 3 4]
    // `blacklist` == [0 2]
    // `container - blacklist` == [1 3 4]
    // offset in `container - blacklist` == 1
    // true offset in `container` that this method should return == 3
    size_t findTrueOffset(size_t adjusted_offset) {
      size_t res = adjusted_offset;
      for (auto it = begin(); it != end(); ++it) {
        if (*it > res) {
          break;
        }
        ++res;
      }
      return res;
    }

    // inserts into sorted small_vector and checks for duplicates
    bool addValue(size_t idx) {
      auto pos = std::lower_bound(begin(), end(), idx);
      if (pos != end() && *pos == idx) {
        return false;
      }
      insert(pos, idx);
      return true;
    }

    // Returns true if the small_vector contains the specified offset, false
    // otherwise.
    bool contains(size_t idx) {
      auto pos = std::find(begin(), end(), idx);
      return pos != end();
    }
  };

  // contains all element offsets that are blacklisted in sorted order.
  BlackList blacklist_;

  // contains all element offsets that are whitelisted in arbitrary order.
  std::vector<size_t> whitelist_;

  // iterator on the whitelist
  std::unique_ptr<RandomLinearIteratorBase<size_t, std::vector, false>>
      whitelist_iterator_;

  // Size of the blacklist which, if exceeded, will make the iterator switch to
  // using the whitelist instead.
  static constexpr size_t max_blacklist_size_ = 4;
};

}} // namespace facebook::logdevice
