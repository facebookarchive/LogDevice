/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/synchronization/LifoSem.h>
#include <folly/Memory.h>
#include <folly/ScopeGuard.h>

#include "logdevice/common/checks.h"

namespace facebook { namespace logdevice {
namespace detail {

/// Works like a Baton, but performs its handoff via a user-visible
/// eventfd.  This allows the caller to wait for multiple events
/// simultaneously using epoll, poll, select, or a wrapping library
/// like libevent.  You should call consume() instead of wait() if you
/// know that fd is readable, since it will double-check that for you.
/// All bets are off if you do anything to fd besides adding it to an
/// epoll wait set (don't read from, write to, or close it).
///
/// Unlike a Baton, post and wait are cumulative.  It is explicitly allowed
/// to post() twice and then expect two wait()s or consume()s to succeed.
struct EventFdBatonBase : boost::noncopyable {
  const int fd;

  /// Throws an exception if an eventfd can't be opened
  EventFdBatonBase();

  ~EventFdBatonBase() noexcept;

  /// Enqueues a wakeup notification
  void post();

  /// Returns true iff fd is readable afterward, waiting at most
  /// timeoutMillis for that to occur.  Timeout of -1 means wait forever,
  /// and will always return true
  bool poll(int timeoutMillis = -1);

  /// Consumes a preceding post if one is available, returning true if
  /// a post was consumed.  Does not block
  bool consume();

  /// Blocks until there is a corresponding post, then consumes it
  void wait();
};

/// EventFdBaton doesn't actually contain an instance of Atom, but the
/// template arg is used to inject deterministic scheduling behavior
/// under test.  The pass-through methods needn't be pass-through in
/// specialized forms
template <template <typename> class Atom>
struct EventFdBaton : public EventFdBatonBase {
  void post() {
    EventFdBatonBase::post();
  }
  void wait() {
    EventFdBatonBase::wait();
  }
  template <typename Clock, typename Duration>
  bool
  try_wait_until(const std::chrono::time_point<Clock, Duration>& deadline) {
    auto max = std::chrono::time_point<Clock, Duration>::max();
    // deadline support currently unimplemented, and as of this
    // writing, unused.  Consider implementing
    // EventFdBatonBase::try_wait_until if you hit this CHECK.
    CHECK_EQ(
        deadline.time_since_epoch().count(), max.time_since_epoch().count());
    EventFdBatonBase::wait();
    return true;
  }
};
} // namespace detail

template <template <typename> class Atom = std::atomic>
struct LifoEventSemImpl
    : public folly::detail::LifoSemBase<detail::EventFdBaton<Atom>, Atom> {
  struct AsyncWaiter;

  /// Asynchronously performs a wait() operation, arranging for
  /// return_value->fd() to be readable when the wait has completed.
  /// The resulting AsyncWaiter instance is reusable via recycle()
  /// or process().  While LifoEventSem is thread-safe, individual
  /// AsyncWaiter-s are not.
  ///
  /// The semantics of an async wait are roughly equivalent to (but much
  /// more efficient and scalable than)
  ///
  ///   std::thread([&](){
  ///     wait();
  ///     write(fd, ...);
  ///   }).detach();
  ///
  std::unique_ptr<AsyncWaiter> beginAsyncWait() {
    return std::make_unique<AsyncWaiter>(*this);
  }

  /// An AsyncWaiter allows a LifoEventSem wait operation to proceed
  /// asynchronously, with success notification delivered in an
  /// epoll-compatible manner (the readability of fd()).  An AsyncWaiter
  /// may only be destroyed when it is in a notified state (fd()
  /// is readable).  This might or might not be checked.
  ///
  /// AsyncWaiter is designed to work with a level-triggered notification
  /// mechanism.  It cancels neighboring calls to read() and write().
  /// This optimization doesn't work for edge-triggered notification
  /// mechanisms.
  struct AsyncWaiter {
    explicit AsyncWaiter(LifoEventSemImpl<Atom>& owner)
        : owner_(owner), node_(owner.allocateNode()) {
      beginWait();
    }

    ~AsyncWaiter() noexcept {
      // If the fd isn't readable, then the node is still linked into
      // the semaphore
      ld_assert(node_->handoff().poll(0));
    }

    /// The file descriptor that will be readable when the asynchronous
    /// wait started by beginWait has been matched with a post() of the
    /// original LifoEventSem
    int fd() const {
      return node_->handoff().fd;
    }

    /// Consumes the read event pending on fd(), then begins a new async
    /// wait on the owning LifoEventSem.  Throws ShutdownSemError if the
    /// semaphore is shut down.
    void recycle() {
      if (UNLIKELY(recycleAndCheckShutdown())) {
        throw folly::ShutdownSemError("owning semaphore has shut down");
      }
    }

    /// This method encapsulates the logic of processing asynchronous
    /// wait operations and recycling the AsyncWaiter.  Call this from
    /// your epoll (libevent, ...) event loop when you notice that fd()
    /// is readable.  maxCalls is the maximum number of func() invocations
    /// to perform per call to this method.
    ///
    /// Throws ShutdownSemError if sem.isShutdown() and sem.tryWait()
    /// would return false.  Regardless of shutdown fd() remains valid
    /// and readable until this AsyncWaiter is destroyed, giving you time
    /// to remove it from your watch set without races.
    ///
    /// The AsyncWaiter will be recycled even if func throws an exception.
    template <typename Func>
    void process(Func&& func, size_t maxCalls) {
      // we ignore owner_.isShutdown and let notification flow throw the
      // nodes, so that we can implement draining semantics
      if (UNLIKELY(node_->isShutdownNotice())) {
        ld_check(owner_.isShutdown());
        throw folly::ShutdownSemError("semaphore has been shut down");
      }

      try {
        do {
          func();
        } while (--maxCalls > 0 && owner_.tryWait());
      } catch (...) {
        // recycle, but don't throw ShutdownSemError.  In the case of
        // shutdown fd() will remain readable so we can throw on the next
        // call to process(), which will happen very soon.
        recycleAndCheckShutdown();
        throw;
      }
      recycle();
    }

    /// This method is similar to process(f, maxBatchSize), but instead
    /// of calling f() n times, it calls f(n).  If there are multiple
    /// waiters then this method is likely to cause unfairness that you
    /// don't want.  There are three situations I can think of in which you
    /// might consider this micro-optimization: if there is only a single
    /// waiter; if the work required for f(n) is less than n times the
    /// work required for f(1); and if you are completely out of tuning
    /// options and you have profiling data that points here.  If you
    /// fall in the third category please let me know (ngbronson@fb.com).
    ///
    /// processBatch(f, n) is less fair than process(f, n), because with
    /// the former method there is never more than one post() that has
    /// been assigned to this thread but not converted into a call to f().
    /// With the latter there can be up to n post()s in flight.  As an
    /// example, if there are two async waiters using processBatch(f,
    /// 1000), a call to post(500) will likely result in batch sizes of
    /// 499 and 1 to the two waiters.
    ///
    /// Shutdown and error semantics are like process().
    template <typename Func>
    void processBatch(Func&& func, size_t maxBatchSize) {
      if (UNLIKELY(node_->isShutdownNotice())) {
        throw folly::ShutdownSemError("semaphore has been shut down");
      }

      try {
        ld_check(maxBatchSize > 0);
        auto extra = maxBatchSize > 1 ? owner_.tryWait(maxBatchSize - 1) : 0;
        func(1 + extra);
      } catch (...) {
        recycleAndCheckShutdown();
        throw;
      }
      recycle();
    }

    /// Waits for fd() using a one-off poll.  If you are using this a
    /// lot you might not actually need AsyncWaiter or LifoEventSem,
    /// and should consider LifoSem.
    bool poll(int timeoutMillis = -1) {
      return node_->handoff().poll(timeoutMillis);
    }

   private:
    typedef typename LifoEventSemImpl<Atom>::WaitResult WaitResult;

    LifoEventSemImpl<Atom>& owner_;
    typename LifoEventSemImpl<Atom>::UniquePtr node_;

    void beginWait() {
      auto rv = owner_.tryWaitOrPush(*node_);
      if (rv != WaitResult::PUSH) {
        ld_check(rv == WaitResult::DECR || rv == WaitResult::SHUTDOWN);

        // If semaphore decr succeeded immediately the node didn't
        // get pushed, since LifoSemBase is expecting us to elide the
        // baton handoff.  The elision would make the API much more
        // complicated and isn't a big win (we can elide it for all of
        // the subsequent uses), so we just pretend we took the slow path.
        // If the semaphore is shut down we also make the fd readable so
        // that we will immediately trigger the shutdown path.  To avoid
        // a rare (and likely to be untested) code path we don't throw
        // at the moment
        if (UNLIKELY(rv == WaitResult::SHUTDOWN)) {
          node_->setShutdownNotice();
        }
        node_->handoff().post();
      }
    }

    /// Consumes the existing notification and causes this AsyncWaiter
    /// to await a new sem.wait() event.  Returns true if the semaphore
    /// is shut down, false otherwise.  If the semaphore has been shut
    /// down fd() will remain readable.
    bool recycleAndCheckShutdown() {
      // At this point fd is still readable, and we have no wait node
      // linked into the LifoSemBase.  The straightforward implementation
      // here would node_->consume(), then call beginWait().  Since
      // EventFdBaton explicitly allows there to be two pending posts,
      // however, we can optimize by delaying the node_->consume() until we
      // find out if we are going to have to call node_->post().  If so,
      // we let those cancel out to avoid a system call.  This is a win
      // in the case that there is already a post ready in the semaphore,
      // which will happen under high load
      auto rv = owner_.tryWaitOrPush(*node_);
      if (rv == WaitResult::PUSH) {
        // node actually was pushed, we need to remove one from the baton
        if (!node_->handoff().consume()) {
          // If you have gotten this exception it is probably because you
          // called recycle() or process() on the AsyncWaiter before fd()
          // was readable.  It is also possible someone else messed with
          // fd(), reading from it or closing it
          throw std::runtime_error("unable to recycle AsyncWaiter");
        }
      } else {
        ld_check(rv == WaitResult::DECR || rv == WaitResult::SHUTDOWN);
        // in both of these states we want fd() to remain readable,
        // so we just leave the existing value there
      }
      return rv == WaitResult::SHUTDOWN;
    }

    friend LifoEventSemImpl<Atom>;
  };

  explicit LifoEventSemImpl(uint32_t v = 0)
      : folly::detail::LifoSemBase<detail::EventFdBaton<Atom>, Atom>(v) {}

 private:
  /// wait() works, but since we don't reuse the eventfd-s, it is very
  /// expensive.  Making it private doesn't prevent people from casting
  /// to LifoSemBase and calling it, but it means that they will probably
  /// read this comment
  void wait() {
    folly::detail::LifoSemBase<detail::EventFdBaton<Atom>, Atom>::wait();
  }
};

typedef LifoEventSemImpl<> LifoEventSem;
}} // namespace facebook::logdevice
