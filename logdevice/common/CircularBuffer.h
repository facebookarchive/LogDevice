/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <cstddef>

#include "logdevice/common/checks.h"

namespace facebook { namespace logdevice {

/**
 * A simple fixed-size circular buffer that provides random access to elements
 * and efficient rotation.  The type is required to be default-initializable.
 */
template <typename T>
class CircularBuffer {
 public:
  explicit CircularBuffer(size_t n) {
    init(n);
  }

  ~CircularBuffer() {
    delete[] raw_begin_;
  }

  size_t size() const {
    return size_;
  }

  /*
   * Deletes the existing buffer and creates a new
   * internal buffer with given size
   */
  void assign(size_t n) {
    ld_check(raw_begin_ != nullptr);
    delete[] raw_begin_;
    raw_begin_ = nullptr;
    init(n);
  }

  /**
   * Rotate the buffer n elements to the left.
   *
   * with n = 1:
   * Before: a b c d
   * After: b c d a
   */
  void rotate(size_t n = 1) {
    ld_check(front_ >= raw_begin_ && front_ < raw_end_);

    if (n >= size()) {
      n %= size();
    }

    ptrdiff_t d = raw_end_ - front_;
    if (n < d) {
      front_ += n;
    } else {
      front_ = raw_begin_ + (n - d);
    }
  }

  T& front() {
    return *front_;
  }
  const T& front() const {
    return *front_;
  }
  T& operator[](int idx) {
    return *ptr<T*>(idx);
  }
  const T& operator[](int idx) const {
    return *ptr<const T*>(idx);
  }

  // Not copyable or movable
  CircularBuffer(const CircularBuffer& other) = delete;
  CircularBuffer& operator=(const CircularBuffer& other) = delete;
  CircularBuffer(CircularBuffer&& other) = delete;
  CircularBuffer& operator=(CircularBuffer&& other) = delete;

 private:
  template <typename Pointer>
  Pointer ptr(size_t idx) const {
    ld_check(idx < size_);
    return front_ < raw_end_ - idx ? front_ + idx : front_ - (size_ - idx);
  }

  void init(size_t n) {
    size_ = n;
    raw_begin_ = new T[n];
    raw_end_ = raw_begin_ + n;
    front_ = raw_begin_;
  }

  size_t size_;

  /**
   * The range of memory containing elements.  begin is inclusive, end is
   * exclusive.
   */
  T *raw_begin_ = nullptr, *raw_end_ = nullptr;

  /**
   * Pointer to the current front of the queue (element 0).
   */
  T* front_ = nullptr;
};

}} // namespace facebook::logdevice
