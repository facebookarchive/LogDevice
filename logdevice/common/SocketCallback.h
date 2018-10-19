/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/IntrusiveList.h>

#include "logdevice/common/Address.h"
#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice {

class Socket;

/**
 * @file SocketCallback is a pure virtual parent of all callback classes
 *       instances of which can be registered with Sockets. A callback object
 *       is an element of an intrusive list of callbacks maintained by a
 *       Socket. A SocketCallback instance can be on not more than one such
 *       list at a time. An attempt to insert a SocketCallback that's already
 *       on some callback list triggers an assert.
 */

class SocketCallback {
 public:
  /**
   * Called by Socket when an event for which this callback is registered
   * occurs. Currently callbacks can be only registered to be called when
   * the socket is closed.
   *
   * @param st    status code associated with the state transition (e.g.,
   *              why the Socket was closed)
   * @param name  LD-level identifier of Socket object
   */
  virtual void operator()(Status st, const Address& name) = 0;

  /**
   * If the object is linked into a callback list at the time of destruction,
   * the destructor will unlink it.
   */
  virtual ~SocketCallback() {}

  /**
   * @return true iff this SocketCallback is currently on a callback list
   *              of some Socket and will be called when the event of interest
   *              occurs on the Socket (such as the Socket is closed).
   */
  bool active() const {
    return listHook_.is_linked();
  }

  /**
   * If this SocketCallback is on a callback list of a Socket, removes
   * it from the list (makes inactive). Otherwise does nothing. A
   * callback can only be registered with a Socket if it is inactive.
   */
  void deactivate() {
    if (listHook_.is_linked())
      listHook_.unlink();
  }

  void swap(SocketCallback& other) {
    listHook_.swap_nodes(other.listHook_);
  }

  folly::IntrusiveListHook listHook_; // links this SocketCallback in a
                                      // list of callbacks off a Socket
                                      // This is an auto-unlink hook.
};

}} // namespace facebook::logdevice
