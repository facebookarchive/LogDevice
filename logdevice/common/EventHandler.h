/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "event2/buffer.h"
#include "event2/bufferevent.h"
#include "logdevice/common/EventLoop.h"
#include "logdevice/common/ThreadID.h"

namespace facebook { namespace logdevice {

/**
 * @file Provide convenience wrapper for LogDevice functions that are registered
 *       as libevent callback functions. Supports three different kinds of
 *       events used in LogDevice, including event, buffer event and evbuffer
 *       events.
 *
 *       For each event, one can provide three relevant callback functions:
 *          - EventPreflight:    the first function that is called by libevent
 *                               for processing this event
 *          - EventHandlerFunc:  called after the Preflight function completes,
 *                               should contain the bulk of event handling
 *          - EventPostflight:   called after Handler completes and will return
 *                               to libevent as soon as it returns
 *
 *       The wrapper functions are also responsible for incrementing the
 *       event_posted_ and event_completed_ counter in EventLoop
 */

// event
typedef void (*EventPreflight)(void*, short);
typedef void (*EventHandlerFunc)(void*, short);
typedef void (*EventPostflight)(void*, short);

// buffer event
typedef void (*BufferEventPreflight)(struct bufferevent*, void*, short);
typedef void (*BufferEventHandlerFunc)(struct bufferevent*, void*, short);
typedef void (*BufferEventPostflight)(struct bufferevent*, void*, short);

// evbuffer event
typedef void (*EvBufferEventPreflight)(struct evbuffer*,
                                       const struct evbuffer_cb_info*,
                                       void*);
typedef void (*EvBufferEventHandlerFunc)(struct evbuffer*,
                                         const struct evbuffer_cb_info*,
                                         void*);
typedef void (*EvBufferEventPostflight)(struct evbuffer*,
                                        const struct evbuffer_cb_info*,
                                        void*);

namespace {

void preflight_noop(void*, short) {}
void postflight_noop(void*, short) {}
void bufferevent_preflight_noop(struct bufferevent*, void*, short) {}
void bufferevent_postflight_noop(struct bufferevent*, void*, short) {}
void evbufferevent_preflight_noop(struct evbuffer*,
                                  const struct evbuffer_cb_info*,
                                  void*) {}
void evbufferevent_postflight_noop(struct evbuffer*,
                                   const struct evbuffer_cb_info*,
                                   void*) {}

inline void bumpEventHandersCalled() {
  if (ThreadID::isWorker()) {
    ++EventLoop::onThisThread()->event_handlers_called_;
  }
}

inline void bumpEventHandlersCompleted() {
  if (ThreadID::isWorker()) {
    ++EventLoop::onThisThread()->event_handlers_completed_;
  }
}

} // namespace

/**
 * comply with event_callback_fn
 */
template <EventHandlerFunc handler,
          EventPreflight preflight = preflight_noop,
          EventPostflight postflight = postflight_noop>
void EventHandler(evutil_socket_t /*fd*/, short what, void* arg) {
  bumpEventHandersCalled();
  preflight(arg, what);
  handler(arg, what);
  postflight(arg, what);
  bumpEventHandlersCompleted();
}

template <BufferEventHandlerFunc handler,
          BufferEventPreflight preflight,
          BufferEventPostflight postflight>
void BufferEventHandlerImpl(struct bufferevent* bev, short what, void* arg) {
  bumpEventHandersCalled();
  preflight(bev, arg, what);
  handler(bev, arg, what);
  postflight(bev, arg, what);
  bumpEventHandlersCompleted();
}

/**
 * comply with bufferevent_event_cb
 */
template <BufferEventHandlerFunc handler,
          BufferEventPreflight preflight = bufferevent_preflight_noop,
          BufferEventPostflight postflight = bufferevent_postflight_noop>
void BufferEventHandler(struct bufferevent* bev, short what, void* arg) {
  return BufferEventHandlerImpl<handler, preflight, postflight>(bev, what, arg);
}

/**
 * comply with bufferevent_data_cb
 */
template <BufferEventHandlerFunc handler,
          BufferEventPreflight preflight = bufferevent_preflight_noop,
          BufferEventPostflight postflight = bufferevent_postflight_noop>
void BufferEventHandler(struct bufferevent* bev, void* arg) {
  // @param what is not used in bufferevent_data_cb
  return BufferEventHandlerImpl<handler, preflight, postflight>(bev, 0, arg);
}

/**
 * comply with evbuffer_cb_func
 */
template <EvBufferEventHandlerFunc handler,
          EvBufferEventPreflight preflight = evbufferevent_preflight_noop,
          EvBufferEventPostflight postflight = evbufferevent_postflight_noop>
void EvBufferEventHandler(struct evbuffer* buffer,
                          const struct evbuffer_cb_info* info,
                          void* arg) {
  bumpEventHandersCalled();
  preflight(buffer, info, arg);
  handler(buffer, info, arg);
  postflight(buffer, info, arg);
  bumpEventHandlersCompleted();
}

}} // namespace facebook::logdevice
