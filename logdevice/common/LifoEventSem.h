/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <assert.h>

#include <folly/Memory.h>
#include <folly/ScopeGuard.h>
#include <folly/io/async/DelayedDestruction.h>
#include <folly/memory/SanitizeLeak.h>
#include <folly/synchronization/LifoSem.h>

namespace facebook { namespace logdevice {
namespace detail {

/// Wrapper around a eventfd or a pipe used for signaling between threads which
/// can be waited on by epoll or variants. Multiple writes will keep the
/// consumer fd readable until they are all consumed.
class SynchronizationFd {
 public:
  static constexpr int kInfiniteTimeout = -1;
  // Throws if it does not manage to open an eventfd or pipe
  explicit SynchronizationFd();
  SynchronizationFd(SynchronizationFd const&) = delete;
  SynchronizationFd& operator=(SynchronizationFd const&) = delete;
  ~SynchronizationFd();

  /// Post an event to write fd
  void write();
  /// Consume an event from fd. Returns false if no event is available.
  bool read() noexcept;
  /// Waits for fd to become readble but does not consume an event.
  /// Returns false if fd didn't become readable within timeout.
  /// set timeout_ms to kInfiniteTimeout to wait indefinitely
  bool poll(int timeout_ms = kInfiniteTimeout) noexcept;

  /// Returns a read fd which can be waited on using epoll or variants.
  int get_read_fd() const noexcept {
    return fds_[FdType::Read];
  }

 private:
  void check_pid();
  enum FdType { Read = 0, Write = 1, FdCount = 2 };
  int fds_[FdType::FdCount];
  pid_t pid_;
};

/// Works like a Baton, but performs its handoff via a user-visible
/// eventfd or pipe. This allows the caller to wait for multiple events
/// simultaneously using epoll, poll, select, or a wrapping library
/// like libevent.
/// All bets are off if you do anything to fd besides adding it to an
/// epoll wait set (don't read from, write to, or close it).
///
/// Unlike a Baton, post and wait are cumulative.  It is explicitly allowed
/// to post() twice and then expect two wait()s or consume()s to succeed.
/// Unlike a Baton it does not introduce memory barrier on wake up as it does
/// not get any function calls if wake-up goes through user epoll, pool,
/// select or a library. Consumer needs to introduce a barrier by calling
/// get_event_count() before accessing anything modified by producer thread.
template <template <typename> class Atom>
class FdBaton {
 public:
  FOLLY_ALWAYS_INLINE void post() {
    event_count_.fetch_add(1, std::memory_order_release);
    fd_.write();
  }
  FOLLY_ALWAYS_INLINE bool try_wait() noexcept {
    auto res = fd_.read();
    if (res) {
      event_count_.fetch_sub(1, std::memory_order_acquire);
    }
    return res;
  }

  template <typename Rep, typename Period>
  FOLLY_ALWAYS_INLINE bool
  try_wait_for(const std::chrono::duration<Rep, Period>& timeout) noexcept {
    if (FOLLY_LIKELY(try_wait())) {
      return true;
    }
    return try_wait_slow(timeout);
  }

  template <typename Clock, typename Duration>
  FOLLY_ALWAYS_INLINE bool try_wait_until(
      const std::chrono::time_point<Clock, Duration>& deadline) noexcept {
    if (FOLLY_LIKELY(try_wait())) {
      return true;
    }
    return try_wait_slow(deadline - Clock::now());
  }

  FOLLY_ALWAYS_INLINE void wait() noexcept {
    if (FOLLY_UNLIKELY(!try_wait())) {
      try_wait_slow(std::chrono::milliseconds::max());
    }
  }

  FOLLY_ALWAYS_INLINE int get_read_fd() const noexcept {
    return fd_.get_read_fd();
  }

  /// Introduces an acquire memory barrier to match a release one set by post
  FOLLY_ALWAYS_INLINE int get_event_count() noexcept {
    return event_count_.load(std::memory_order_acquire);
  }

  /// Waits for baton to become active but does not consume an event.
  FOLLY_ALWAYS_INLINE void wait_readable() noexcept {
    fd_.poll();
  }

 private:
  template <typename Rep, typename Period>
  bool
  try_wait_slow(const std::chrono::duration<Rep, Period>& timeout) noexcept {
    int timeout_ms = SynchronizationFd::kInfiniteTimeout;
    const auto timeout_ms_unbounded =
        std::chrono::duration_cast<std::chrono::milliseconds>(timeout).count();
    if (timeout_ms_unbounded <= std::numeric_limits<int>::max()) {
      timeout_ms = timeout_ms_unbounded;
    }
    bool res = false;
    if (fd_.poll(timeout_ms)) {
      res = try_wait();
      assert(res);
    }
    return res;
  }
  SynchronizationFd fd_;
  Atom<int> event_count_{0};
};

/// This is a wrapper introduced for usage within LifoSem.
/// LifoSem node can not be larger than 8 bytes and our FdBaton is 12
template <template <typename> class Atom>
class LifoFdBaton {
 public:
  LifoFdBaton() {
    // Since it gets allocated through placement new LSAN does not track
    // ownership correctly so if we have an intentional leak somewhere up the
    // ownership chain LSAN reports it as a leak here.
    folly::annotate_object_leaked(baton_.get());
  }

  ~LifoFdBaton() {
    folly::annotate_object_collected(baton_.get());
  }

  FOLLY_ALWAYS_INLINE void post() {
    baton_->post();
  }

  FOLLY_ALWAYS_INLINE bool try_wait() noexcept {
    return baton_->try_wait();
  }

  template <typename Rep, typename Period>
  FOLLY_ALWAYS_INLINE bool
  try_wait_for(const std::chrono::duration<Rep, Period>& timeout) noexcept {
    return baton_->try_wait_for(timeout);
  }

  template <typename Clock, typename Duration>
  FOLLY_ALWAYS_INLINE bool try_wait_until(
      const std::chrono::time_point<Clock, Duration>& deadline) noexcept {
    return baton_->try_wait_until(deadline);
  }

  FOLLY_ALWAYS_INLINE void wait() noexcept {
    baton_->wait();
  }

  FOLLY_ALWAYS_INLINE int get_read_fd() const noexcept {
    return baton_->get_read_fd();
  }
  FOLLY_ALWAYS_INLINE int get_event_count() noexcept {
    return baton_->get_event_count();
  }
  FOLLY_ALWAYS_INLINE void wait_readable() noexcept {
    baton_->wait_readable();
  }

 private:
  std::unique_ptr<FdBaton<Atom>> baton_{std::make_unique<FdBaton<Atom>>()};
};
} // namespace detail

template <template <typename> class Atom = std::atomic>
class LifoEventSemImpl
    : public folly::detail::LifoSemBase<detail::LifoFdBaton<Atom>, Atom> {
 public:
  class AsyncWaiter;

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
  std::unique_ptr<AsyncWaiter, folly::DelayedDestruction::Destructor>
  beginAsyncWait() {
    return std::unique_ptr<AsyncWaiter, folly::DelayedDestruction::Destructor>(
        new AsyncWaiter(*this));
  }

  /// An AsyncWaiter allows a LifoEventSem wait operation to proceed
  /// asynchronously, with success notification delivered in an
  /// epoll-compatible manner (the readability of fd()).  An AsyncWaiter
  /// may only be destroyed when it is in a notified state (fd()
  /// is readable).
  ///
  /// AsyncWaiter is designed to work with a level-triggered notification
  /// mechanism.  It cancels neighboring calls to read() and write().
  /// This optimization doesn't work for edge-triggered notification
  /// mechanisms.
  class AsyncWaiter : public folly::DelayedDestruction {
   public:
    explicit AsyncWaiter(LifoEventSemImpl<Atom>& owner)
        : owner_(owner), node_(owner.allocateNode()) {
      beginWait();
    }

    /// The file descriptor that will be readable when the asynchronous
    /// wait started by beginWait has been matched with a post() of the
    /// original LifoEventSem
    int fd() const {
      return node_->handoff().get_read_fd();
    }

    FOLLY_ALWAYS_INLINE void wait_readable() noexcept {
      node_->handoff().wait_readable();
    }

    FOLLY_ALWAYS_INLINE bool is_readable() noexcept {
      return node_->handoff().get_event_count() > 0;
    }

    /// Consumes the read event pending on fd(), then begins a new async
    /// wait on the owning LifoEventSem.  Throws ShutdownSemError if the
    /// semaphore is shut down.
    void recycle() {
      if (FOLLY_UNLIKELY(recycleAndCheckShutdown())) {
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
    void process(Func&& func, uint32_t maxCalls) {
      DestructorGuard dg(this);
      // We need to call get_event_count in order to introduce acquire memory
      // barrier as node_->isShutdownNotice() call is not safe without it.
      if (FOLLY_UNLIKELY(node_->handoff().get_event_count() != 1)) {
        return;
      }
      // we ignore owner_.isShutdown and let notification flow throw the
      // nodes, so that we can implement draining semantics
      if (FOLLY_UNLIKELY(node_->isShutdownNotice())) {
        assert(owner_.isShutdown());
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
    void processBatch(Func&& func, uint32_t maxBatchSize) {
      DestructorGuard dg(this);
      // We need to call get_event_count in order to introduce acquire memory
      // barrier as node_->isShutdownNotice() call is not safe without it.
      if (FOLLY_UNLIKELY(node_->handoff().get_event_count() != 1)) {
        return;
      }
      if (FOLLY_UNLIKELY(node_->isShutdownNotice())) {
        throw folly::ShutdownSemError("semaphore has been shut down");
      }

      try {
        assert(maxBatchSize > 0);
        auto extra = maxBatchSize > 1 ? owner_.tryWait(maxBatchSize - 1) : 0ul;
        func(1 + extra);
      } catch (...) {
        recycleAndCheckShutdown();
        throw;
      }
      recycle();
    }

   private:
    ~AsyncWaiter() override {
      // If we are not enqueued we consumed an event from queue but did not
      // process it. If we weren't posted to because of shutdown we should
      // return the event to semaphore as we will not process it.
      if (!owner_.tryRemoveNode(*node_)) {
        // If we are destroying just as we were dequeued from LifoSem but the
        // post did not happen yet we need to wait for a post to happen
        // otherwise we risk a post after we are already destroyed
        node_->handoff().wait_readable();
        // We need to call get_event_count in order to introduce acquire memory
        // barrier as node_->isShutdownNotice() call is not safe without it.
        if (FOLLY_UNLIKELY(node_->handoff().get_event_count() != 1)) {
          // we just waited for fd to become readable so event count should be 1
          // otherwise all bets are off
          return;
        }
        // if it's not shutdown we got notified but we will not process the
        // request so we need to return token to semaphore
        if (!node_->isShutdownNotice()) {
          owner_.post();
        }
      }
    }
    typedef typename LifoEventSemImpl<Atom>::WaitResult WaitResult;

    LifoEventSemImpl<Atom>& owner_;
    typename LifoEventSemImpl<Atom>::UniquePtr node_;

    void beginWait() {
      auto rv = owner_.tryWaitOrPush(*node_);
      if (rv != WaitResult::PUSH) {
        assert(rv == WaitResult::DECR || rv == WaitResult::SHUTDOWN);

        // If semaphore decr succeeded immediately the node didn't
        // get pushed, since LifoSemBase is expecting us to elide the
        // baton handoff.  The elision would make the API much more
        // complicated and isn't a big win (we can elide it for all of
        // the subsequent uses), so we just pretend we took the slow path.
        // If the semaphore is shut down we also make the fd readable so
        // that we will immediately trigger the shutdown path.  To avoid
        // a rare (and likely to be untested) code path we don't throw
        // at the moment
        if (FOLLY_UNLIKELY(rv == WaitResult::SHUTDOWN)) {
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
      // here would node_->tryWait(), then call beginWait().  Since
      // EventFdBaton explicitly allows there to be two pending posts,
      // however, we can optimize by delaying the node_->tryWait() until we
      // find out if we are going to have to call node_->post().  If so,
      // we let those cancel out to avoid a system call.  This is a win
      // in the case that there is already a post ready in the semaphore,
      // which will happen under high load
      auto rv = owner_.tryWaitOrPush(*node_);
      if (rv == WaitResult::PUSH) {
        // node actually was pushed, we need to remove one from the baton
        if (!node_->handoff().try_wait()) {
          // If you have gotten this exception it is probably because you
          // called recycle() or process() on the AsyncWaiter before fd()
          // was readable.  It is also possible someone else messed with
          // fd(), reading from it or closing it
          throw std::runtime_error("unable to recycle AsyncWaiter");
        }
      } else {
        assert(rv == WaitResult::DECR || rv == WaitResult::SHUTDOWN);
        // in both of these states we want fd() to remain readable,
        // so we just leave the existing value there but we must simulate as
        // if we were waiting and got a shutdown notice
        if (rv == WaitResult::SHUTDOWN) {
          node_->setShutdownNotice();
        }
      }
      return rv == WaitResult::SHUTDOWN;
    }

    friend LifoEventSemImpl<Atom>;
  };

  explicit LifoEventSemImpl(uint32_t v = 0)
      : folly::detail::LifoSemBase<detail::LifoFdBaton<Atom>, Atom>(v) {}

 private:
  /// wait() works, but since we don't reuse the SynchronizationFd-s, it is very
  /// expensive.  Making it private doesn't prevent people from casting
  /// to LifoSemBase and calling it, but it means that they will probably
  /// read this comment
  void wait() {
    folly::detail::LifoSemBase<detail::LifoFdBaton<Atom>, Atom>::wait();
  }
};

typedef LifoEventSemImpl<> LifoEventSem;
}} // namespace facebook::logdevice
