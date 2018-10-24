/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <functional>
#include <tuple>
#include <utility>

#include <folly/Portability.h>
#include <google/dense_hash_map>

#include "logdevice/common/checks.h"

namespace facebook { namespace logdevice {

/**
 * This is an LRU cache: you can insert key/value pairs, and look up by key.
 * You can also get the element that was least recently added / looked up.
 *
 * Unlike std::unordered_map or folly::EvictingCacheMap, this does not
 * allocate on each insertion, only on resize.
 *
 * It's used by real time reads to keep track of logids, to know which ones to
 * evict when we hit our memory budget.  It's not intended to be general.  For
 * example, it works best when keys are small and fast to hash, since we use
 * those in liu of pointers for the LRU linked list.
 */

template <typename Key,
          typename Value,
          typename HashFcn = std::hash<Key>,
          typename Equal = std::equal_to<Key>>
class UnorderedMapWithLRU {
  struct LinkedListNode {
    Key next;
    Key prev;
  };

  struct ValueAndNode : public std::tuple<Value> {
    LinkedListNode node_;

    template <typename... Params>
    explicit ValueAndNode(Params&&... params)
        : std::tuple<Value>(std::forward<Params>(params)...) {}

    Value& value() {
      return std::get<0>(*this);
    }

    const Value& value() const {
      return std::get<0>(*this);
    }
  };

  using Index = google::dense_hash_map<Key, ValueAndNode, HashFcn, Equal>;

 public:
  using iterator = typename Index::iterator;

  UnorderedMapWithLRU(const Key& empty, const Key& erase)
      : sentinel_{empty, empty}, empty_(empty), erase_{erase} {
    index_.set_empty_key(empty);
    index_.set_deleted_key(erase);

    verifyIntegrity(0);
  }

  const Key& emptyKey() const {
    return empty_;
  }

  const Key& deletedKey() const {
    return erase_;
  }

  iterator end() {
    return index_.end();
  }

  void add(const Key& key, const Value& value) {
    ld_check(key != empty_);
    ld_check(key != erase_);
    verifyIntegrity(-1);

    auto iter = index_.find(key);
    if (iter == index_.end()) {
      iter = index_.insert(std::make_pair(key, ValueAndNode{value})).first;
    } else {
      iter->second.value() = value;
      // Remove it from the linked list, but leave in hash table.
      remove(iter->second.node_);
    }
    insertAtHead(iter->second.node_, key);

    verifyIntegrity(-1);
  }

  Value& getOrAddWithoutPromotion(const Key& key) {
    ld_check(key != empty_);
    ld_check(key != erase_);

    iterator iter;
    bool inserted;
    std::tie(iter, inserted) =
        index_.insert(std::make_pair(key, ValueAndNode()));

    if (inserted) {
      insertAtHead(iter->second.node_, iter->first);
    }

    return iter->second.value();
  }

  // Brings element to the head of the eviction list.
  // Therefore, not const.
  std::pair<Value*, iterator> get(const Key& key) {
    ld_check(key != empty_);
    ld_check(key != erase_);

    auto iter = index_.find(key);
    if (iter == index_.end()) {
      return std::make_pair(nullptr, iter);
    }

    // Bring it to the front.
    remove(iter->second.node_);
    insertAtHead(iter->second.node_, key);

    return std::make_pair(&iter->second.value(), iter);
  }

  std::pair<Value*, iterator> getWithoutPromotion(const Key& key) {
    ld_check(key != empty_);
    ld_check(key != erase_);

    auto iter = index_.find(key);
    return std::make_pair(
        iter == index_.end() ? nullptr : &iter->second.value(), iter);
  }

  Key getLRU() const {
    return sentinel_.prev;
  }

  Key removeLRU() {
    verifyIntegrity(-1);

    if (sentinel_.prev == empty_) {
      return empty_;
    }

    auto iter = index_.find(sentinel_.prev);
    Key key = iter->first;

    erase(iter);
    // iter is now invalid!

    verifyIntegrity(-1);
    return key;
  }

  void erase(iterator it) {
    remove(it->second.node_);
    index_.erase(it);
  }

  bool empty() const {
    if (index_.empty()) {
      ld_check(sentinel_.next == empty_);
      ld_check(sentinel_.prev == empty_);
      return true;
    }
    ld_check(sentinel_.next != empty_);
    ld_check(sentinel_.prev != empty_);
    return false;
  }

  // For debugging.
  void verifyIntegrity(ssize_t expected_size) const {
    if (!folly::kIsDebug) {
      return;
    }

    if (expected_size >= 0) {
      ld_check((size_t)expected_size == index_.size());
    }

    size_t size{0};
    Key expected_prev = empty_;
    for (Key key = sentinel_.next; key != empty_;) {
      auto iter = index_.find(key);
      ld_check(iter != index_.end());
      ld_check(iter->second.node_.prev == expected_prev);

      size++;
      ld_check(size <= index_.size());

      expected_prev = key;
      key = iter->second.node_.next;
    }
    ld_check(sentinel_.prev == expected_prev);
    ld_check(size == index_.size());
  }

 private:
  LinkedListNode& find(const Key& element) {
    if (element == empty_) {
      return sentinel_;
    } else {
      return index_.find(element)->second.node_;
    }
  }

  void insertAtHead(LinkedListNode& node, const Key& key) {
    node.prev = empty_;
    node.next = sentinel_.next;
    find(sentinel_.next).prev = key;
    sentinel_.next = key;
  }

  void remove(const LinkedListNode node) {
    find(node.prev).next = node.next;
    find(node.next).prev = node.prev;
  }

  // This doesn't malloc() / free() on insert, only on resize.
  Index index_;
  LinkedListNode sentinel_;
  const Key empty_;
  const Key erase_;
};

template <typename Key,
          typename Value,
          typename HashFcn = std::hash<Key>,
          typename Equal = std::equal_to<Key>>
class UnorderedSetWithLRU {
  class Empty {};
  class UnorderedMapWithLRU<Key, Empty, HashFcn, Equal> map_;

 public:
  using iterator =
      typename UnorderedMapWithLRU<Key, Empty, HashFcn, Equal>::iterator;

  UnorderedSetWithLRU(const Key& empty, const Key& erase)
      : map_(empty, erase) {}

  const Key& emptyKey() const {
    return map_.emptyKey();
  }

  const Key& deletedKey() const {
    return map_.deletedKey();
  }

  iterator end() {
    return map_.end();
  }

  void add(const Key& key) {
    map_.add(key, Empty{});
  }

  void getOrAddWithoutPromotion(const Key& key) {
    map_.getOrAddWithoutPromotion(key);
  }

  // Brings element to the head of the eviction list.
  // Therefore, not const.
  iterator get(const Key& key) {
    return map_.get(key).second;
  }

  iterator getWithoutPromotion(const Key& key) {
    return map_.getWithoutPromotion(key).second;
  }

  Key getLRU() const {
    return map_.getLRU();
  }

  void erase(iterator it) {
    map_.erase(it);
  }

  bool empty() const {
    return map_.empty();
  }

  void verifyIntegrity(ssize_t expected_size = -1) const {
    map_.verifyIntegrity(expected_size);
  }
};

}} // namespace facebook::logdevice
