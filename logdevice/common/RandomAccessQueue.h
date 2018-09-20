/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <atomic>
#include <vector>

#include "logdevice/common/UpdateableSharedPtr.h"

namespace facebook { namespace logdevice {

/**
 * A queue that provides random access to its elements.
 * Lock-free for reading. Locks a mutex for writing.
 *
 * Each element gets a unique ID by which it can be accessed.
 * IDs are assigned sequentially.
 *
 * push() and pop() take O(N) time and just replace the whole array.
 */

template <typename T>
class RandomAccessQueue {
 public:
  using id_t = uint64_t;

  // A non-thread-safe implementation of RandomAccessQueue's interface.
  // Only const methods are thread safe.
  // RandomAccessQueue holds an UpdateableSharedPtr to an immutable instance
  // of Version. On each mutation it copies, mutates and replaces Version.
  class Version {
   public:
    explicit Version(id_t base_id) : base_id_(base_id) {}
    Version(const Version& rhs) = default;

    bool empty() const {
      return items_.empty();
    }
    id_t firstID() const {
      return base_id_;
    }
    id_t nextID() const {
      return base_id_ + items_.size();
    }

    size_t size() const {
      return items_.size();
    }

    T get(id_t id) const {
      if (id >= base_id_ && id < nextID()) {
        return items_[id - base_id_];
      }
      return T();
    }

    T front() const {
      return items_.front();
    }
    T back() const {
      return items_.back();
    }

    typename std::vector<T>::iterator begin() {
      return items_.begin();
    }
    typename std::vector<T>::iterator end() {
      return items_.end();
    }
    typename std::vector<T>::const_iterator begin() const {
      return items_.cbegin();
    }
    typename std::vector<T>::const_iterator end() const {
      return items_.cend();
    }

    T pop() {
      ld_check(!items_.empty());
      auto res = std::move(items_[0]);
      ++base_id_;
      // Erasing in O(N) doesn't make much difference here since we're copying
      // the entire Version on each operation.
      items_.erase(items_.begin());
      return res;
    }

    T pop_back() {
      ld_check(!items_.empty());
      auto res = std::move(items_.back());
      items_.pop_back();
      return res;
    }

    std::vector<T> popUpTo(id_t first_to_keep) {
      ld_check(first_to_keep <= nextID());
      if (first_to_keep <= base_id_) {
        return std::vector<T>();
      }
      size_t count = first_to_keep - base_id_;
      std::vector<T> res(count);
      for (size_t i = 0; i < count; ++i) {
        res[i] = std::move(items_[i]);
      }
      items_.erase(items_.begin(), items_.begin() + count);
      base_id_ += count;
      return res;
    }

    id_t push(T value) {
      items_.push_back(std::move(value));
      return nextID() - 1;
    }

    void push(id_t id, T value) {
      ld_check(id >= nextID());
      while (id > nextID()) {
        push(T());
      }
      id_t new_id = push(std::move(value));
      ld_check(new_id == id);
    }

    T put(id_t id, T value) {
      ld_check(!items_.empty());
      ld_check(id >= base_id_ && id < nextID());
      items_[id - base_id_] = std::move(value);
      return items_[id - base_id_];
    }

    // Add to the end.
    void append(std::vector<T> values) {
      items_.insert(items_.end(),
                    std::make_move_iterator(values.begin()),
                    std::make_move_iterator(values.end()));
    }

    // Add to the beginning.
    void prepend(std::vector<T> values) {
      ld_check(values.size() <= base_id_);
      base_id_ -= values.size();
      items_.insert(items_.begin(),
                    std::make_move_iterator(values.begin()),
                    std::make_move_iterator(values.end()));
    }

   private:
    std::vector<T> items_;
    id_t base_id_; // ID of element items_[0].
  };

  using VersionPtr = std::shared_ptr<const Version>;

  RandomAccessQueue() {}

  // Initialize queue to have the given base_id
  explicit RandomAccessQueue(id_t base_id) {
    std::shared_ptr<Version> new_data = std::make_shared<Version>(base_id);
    data_.update(new_data);
  }

  RandomAccessQueue(const RandomAccessQueue& rhs) = delete;
  RandomAccessQueue& operator=(const RandomAccessQueue& rhs) = delete;

  // push() and pop() are not thread safe.
  // Should be called from one thread or with external synchronisation.

  // Push value so that it gets ID `id'. Pad with T()s if needed.
  // `id' must be greater than any used ID.
  void push(id_t id, T value) {
    std::lock_guard<std::mutex> lock(mutex_);
    std::shared_ptr<Version> new_data;
    auto data = data_.get();
    if (data) {
      new_data = std::make_shared<Version>(*data);
    } else {
      new_data = std::make_shared<Version>(id);
    }
    new_data->push(id, std::move(value));
    data_.update(new_data);
  }

  // Reassign an existing element, `id' must be within the range of
  // ids stored in the queue.
  // @return the updated value. complexity: O(N)
  T put(id_t id, T value) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto data = data_.get();
    ld_check(data != nullptr);
    auto new_data = std::make_shared<Version>(*data);
    auto res = new_data->put(id, std::move(value));
    data_.update(new_data);
    return res;
  }

  // Requires the queue to be empty.
  void setBaseID(id_t id) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto data = data_.get();
    ld_check(!data || data->empty());
    data_.update(std::make_shared<Version>(id));
  }

  // Adds the values to the end of the queue.
  void append(std::vector<T> values) {
    std::lock_guard<std::mutex> lock(mutex_);
    std::shared_ptr<Version> new_data;
    auto data = data_.get();
    ld_check(data != nullptr);
    new_data = std::make_shared<Version>(*data);
    new_data->append(std::move(values));
    data_.update(new_data);
  }

  // Adds the values to the beginning of the queue.
  // Side note: Technically this makes this structure a deque, not a queue.
  //            Feel free to rename it if it bothers you :)
  void prepend(std::vector<T> values) {
    std::lock_guard<std::mutex> lock(mutex_);
    std::shared_ptr<Version> new_data;
    auto data = data_.get();
    ld_check(data != nullptr);
    new_data = std::make_shared<Version>(*data);
    new_data->prepend(std::move(values));
    data_.update(new_data);
  }

  // Removes an element from the front of the queue.
  // @return The removed element.
  T pop() {
    std::lock_guard<std::mutex> lock(mutex_);
    auto data = data_.get();
    ld_check(data != nullptr);
    auto new_data = std::make_shared<Version>(*data);
    auto res = new_data->pop();
    data_.update(new_data);
    return res;
  }

  // Removes an element from the back of the queue.
  // @return The removed element.
  T pop_back() {
    std::lock_guard<std::mutex> lock(mutex_);
    auto data = data_.get();
    ld_check(data != nullptr);
    auto new_data = std::make_shared<Version>(*data);
    auto res = new_data->pop_back();
    data_.update(new_data);
    return res;
  }

  // Removes all elements with id < first_to_keep.
  // first_to_keep must not be greater than last assigned id + 1.
  // Returning the removed elements is an optimisation: if T is a
  // shared_ptr<some class with slow destructor>, the destructor won't be
  // called under locked mutex_.
  // @return The removed elements.
  std::vector<T> popUpTo(id_t first_to_keep) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto data = data_.get();
    ld_check(data != nullptr);
    auto new_data = std::make_shared<Version>(*data);
    auto res = new_data->popUpTo(first_to_keep);
    data_.update(new_data);
    return res;
  }

  // Gets element by ID.
  // Returns T() if there's no element with such id or if its value is
  // T() (no way is provided to distinguish between these cases).
  T get(id_t id) const {
    auto data = data_.get();
    if (data == nullptr) {
      return T();
    }
    return data->get(id);
  }

  T front() const {
    auto data = data_.get();
    ld_check(data);
    return data->front();
  }
  T back() const {
    auto data = data_.get();
    ld_check(data);
    return data->back();
  }

  bool empty() const {
    auto data = data_.get();
    return !data || data->empty();
  }

  size_t size() const {
    auto data = data_.get();
    return data ? data->size() : 0ul;
  }

  id_t firstID() const {
    auto data = data_.get();
    ld_check(data);
    return data->firstID();
  }

  id_t nextID() const {
    auto data = data_.get();
    ld_check(data);
    return data->nextID();
  }

  // Returns a pinned snapshot of the structure. nullptr if push() was never
  // called (there's no meaningful value for Version::firstID() in this case).
  VersionPtr getVersion() const {
    return data_.get();
  }

 private:
  std::mutex mutex_;

  // Not using FastUpdateableSharedPtr because the primary use case is partition
  // list of PartitionedRocksDBStore, and we want dropped partition objects to
  // be destroyed quickly to reclaim disk space.
  struct DataPtrTag {};
  UpdateableSharedPtr<const Version, DataPtrTag> data_;
};

}} // namespace facebook::logdevice
