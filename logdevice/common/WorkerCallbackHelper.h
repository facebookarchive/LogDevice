/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/Memory.h>

#include "logdevice/common/Request.h"
#include "logdevice/common/WeakRefHolder.h"
#include "logdevice/common/debug.h"
#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice {

class Processor;

// Somewhat reduces the amount of boilerplate needed in the following situation.
// There's a state machine A running on worker thread Wa. It posts a request B
// that runs on an unspecified worker thread Wb and sometimes needs to pass some
// data back to A (e.g. to tell it when it's finished). To do that, B needs to
// post auxiliary request to run on Wa. When the auxiliary request gets
// executed, A might have already been destroyed, and the request needs a way
// to check that.
//
// Usage:
// Add WorkerCallbackHelper<A> as a field of A.
// When creating B, give it a WorkerCallbackHelper::Ticket created with
// WorkerCallbackHelper::ticket() on Wa.
// When B needs to tell something to A, call Ticket::postCallbackRequest()
// with a function that should be executed on Wa. This function gets
// an A* that will be nullptr if the A was destroyed.

class TicketBase {
 public:
  TicketBase();
  explicit TicketBase(std::nullptr_t)
      : workerIdx_(-1), worker_type_(WorkerType::GENERAL) {}
  int postRequest(std::unique_ptr<Request>& rq) const;
  int getWorkerId() const {
    return workerIdx_;
  }

  WorkerType getWorkerType() const {
    return worker_type_;
  }

 private:
  std::weak_ptr<Processor> processor_;
  int workerIdx_;
  WorkerType worker_type_;
};

template <class T>
class WorkerCallbackHelper {
 public:
  using Callback = std::function<void(T*)>;

  class Ticket : public TicketBase {
   public:
    // Creates a ticket that behaves as if it referenced a destroyed object.
    explicit Ticket(std::nullptr_t) : TicketBase(nullptr) {}

    void postCallbackRequest(Callback cb) const {
      std::unique_ptr<Request> rq =
          std::make_unique<Rq>(getWorkerId(), getWorkerType(), ptr_, cb);
      const int rv = postRequest(rq);
      if (rv == -1 && err != E::SHUTDOWN) {
        ld_error("Failed to post request: %s.", error_description(err));
      }
    }

    // True if the referenced WorkerCallbackHelper is still alive.
    // Thread safe.
    explicit operator bool() const {
      return (bool)ptr_;
    }

    // Returns true if the tickets belong to the same WorkerCallbackHelper
    // instance.
    bool operator==(const Ticket& rhs) const {
      return ptr_ == rhs.ptr_;
    }
    bool operator!=(const Ticket& rhs) const {
      return ptr_ != rhs.ptr_;
    }

   private:
    friend class WorkerCallbackHelper;

    typename WeakRefHolder<T>::Ref ptr_;

    explicit Ticket(typename WeakRefHolder<T>::Ref ptr)
        : TicketBase(), ptr_(std::move(ptr)) {}
  };

  explicit WorkerCallbackHelper(T* parent) : parent_(parent) {}
  WorkerCallbackHelper(const WorkerCallbackHelper&) = delete;
  WorkerCallbackHelper& operator=(const WorkerCallbackHelper&) = delete;

  Ticket ticket() const {
    return Ticket(parent_.ref());
  }

  WeakRefHolder<T>& getHolder() {
    return parent_;
  }

 private:
  class Rq : public Request {
   public:
    Rq(int worker_idx,
       WorkerType worker_type,
       typename WeakRefHolder<T>::Ref ptr,
       Callback cb)
        : Request(RequestType::WORKER_CALLBACK_HELPER),
          workerIdx_(worker_idx),
          worker_type_(worker_type),
          ptr_(ptr),
          cb_(cb) {}

    Execution execute() override {
      cb_(ptr_.get());
      return Execution::COMPLETE;
    }

    int getThreadAffinity(int /*nthreads*/) override {
      return workerIdx_;
    }

    WorkerType getWorkerTypeAffinity() override {
      return worker_type_;
    }

   private:
    int workerIdx_;
    WorkerType worker_type_;
    typename WeakRefHolder<T>::Ref ptr_;
    Callback cb_;
  };

  WeakRefHolder<T> parent_;
};

}} // namespace facebook::logdevice
