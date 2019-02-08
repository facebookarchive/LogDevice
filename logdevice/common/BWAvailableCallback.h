/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <mutex>

#include <folly/IntrusiveList.h>

#include "logdevice/common/Priority.h"
#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice {
class FlowGroup;

/**
 * @file BWAvailableCallback is a pure virtual parent of all callback classes
 *       used to queue up for bandwidth becoming available on a Socket. A
 *       callback object is an element of an intrusive priority queue of
 *       callbacks maintained by a FlowGroup. A BWAvailableCallback instance
 *       can be on not more than one such queue at a time. An attempt to insert
 *       a BWAvailableCallback that's already on a FlowGroup's priority queue
 *       will trigger an assert.
 */

class BWAvailableCallback {
 public:
  explicit BWAvailableCallback() : priority_(Priority::INVALID) {}

  Priority priority() const {
    return priority_;
  }

  /**
   * Unused, but required so that BWAvailableCallbacks can be held
   * in a PriorityQueue.
   */
  size_t cost() const {
    return 0;
  }

  /**
   * Called by a FlowGroup when at least one message at the callback's
   * priority level can be transmitted without exceeding bandwidth limits.
   */
  virtual void operator()(FlowGroup&, std::mutex& flow_meters_mutex) = 0;

  /**
   * Called by a Socket when the callback must be cancelled due to an
   * error. Currently only called when the socket is closed before the
   * callback is invoked.
   */
  virtual void cancelled(Status) {}

  /**
   * If the object is linked into a callback list at the time of destruction,
   * the destructor will unlink it.
   */
  virtual ~BWAvailableCallback() {
    deactivate();
  }

  void swap(BWAvailableCallback& other) noexcept {
    std::swap(flow_group_, other.flow_group_);
    std::swap(priority_, other.priority_);
    flow_group_links_.swap_nodes(other.flow_group_links_);
    links_.swap_nodes(other.links_);
  }

  /**
   * @return true iff this SocketCallback is currently on a callback list
   *              of some Socket and will be called when the event of interest
   *              occurs on the Socket (such as the Socket is closed).
   */
  bool active() const {
    return flow_group_links_.is_linked();
  }

  /**
   * If this SocketCallback is on a callback list of a Socket, removes
   * it from the list (makes inactive). Otherwise does nothing. A
   * callback can only be registered with a Socket if it is inactive.
   */
  void deactivate();

 private:
  friend class FlowGroup;
  friend class Socket;
  friend class SocketImpl;
  friend class RecordRebuildingMockSocket;

  /**
   * The flow group and priority are set when the callback is queued based
   * on the destination scope and priority of the message that had to be
   * deferred.
   */
  void setAffiliation(FlowGroup* fg, Priority p);

  // For priority queuing in the FlowGroup until bandwidth becomes available.
  FlowGroup* flow_group_ = nullptr;
  folly::IntrusiveListHook flow_group_links_;

  // The callback is also tracked by the Socket so that it can be cleaned
  // up when a socket is closed.
  folly::IntrusiveListHook links_;

  Priority priority_;
};

}} // namespace facebook::logdevice
