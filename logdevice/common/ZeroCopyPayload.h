/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once
#include <memory>

#include <folly/AtomicIntrusiveLinkedList.h>

struct evbuffer;
namespace facebook { namespace logdevice {

class EventLoop;

/**
 * ZeroCopyPayload is a wrapper for struct evbuffer which allows a buffer to be
 * moved across threads and makes sure it gets deleted on the eventloop where it
 * was created.
 */
class ZeroCopyPayload {
 public:
  ~ZeroCopyPayload();

  struct evbuffer* get() {
    return payload_;
  }

  size_t length() {
    return length_;
  }

  static std::shared_ptr<ZeroCopyPayload> create(EventLoop* ev_loop,
                                                 struct evbuffer* payload);

  // atomic list hook for disposal purpose
  folly::AtomicIntrusiveLinkedListHook<ZeroCopyPayload> hook;

 private:
  explicit ZeroCopyPayload(struct evbuffer* payload);
  struct evbuffer* payload_;
  size_t length_;
};

/**
 * Deleter used to delete ZeroCopyPayload on specific EventLoop.
 */
class ZeroCopyPayloadDisposer {
 public:
  explicit ZeroCopyPayloadDisposer(EventLoop* ev_loop) : ev_loop_(ev_loop) {}

  void operator()(ZeroCopyPayload* record);

 private:
  EventLoop* ev_loop_;
};
}} // namespace facebook::logdevice
