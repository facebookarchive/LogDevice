/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>

#include "logdevice/common/checks.h"

namespace facebook { namespace logdevice {

/**
 * Utility class that can be used to maintain a weak reference to an object,
 *
 * This utility can be used in lieu of std::weak_ptr when the user wishes to
 * not create a std::shared_ptr to reference the object of type T. The user must
 * create an object of type WeakRefHolder<T> and ensure that it shares the
 * lifetime of the referenced object (possibly by making it a child member).
 *
 * Example:
 *
 *   struct Object {
 *     Object() : holder(this) {}
 *     WeakRefHolder<Object> holder;
 *     int val{0};
 *   };
 *
 *   auto object = std::make_unique<Object>();
 *   Ref ref = object->holder.ref()
 *
 *   {
 *   ld_check(ref);                 // Ref is valid since holder is still alive
 *   Object *o = ref.get();         // One can access the referenced object
 *   ld_check(o);                   // through a call to get()
 *   int v = ref->val;              // or by using the "->" operator.
 *   }
 *
 *   object.reset();
 *   ld_check(!ref);                // Ref is not valid anymore because "object"
 *   ld_check(ref.get() == nullptr);// was destroyed.
 *                                  // Calling operator*() or operator->() here
 *                                  // triggers an assert.
 *
 * Note that WeakRefHolder<T>::Ref does not provide a means to extend the
 * lifetime of the referenced object of type T like std::weak_ptr. This is
 * because Ref internally contains a std::weak_ptr that references a shared_ptr
 * `WeakRefHolder::parent_`.  Extending the lifetime of `WeakRefHolder::parent_`
 * does not extend the lifetime of the object of type T.
 *
 * This is the reason why Ref does not provide a lock() method and the
 * programmer should ensure that the object of type T is not destroyed while
 * holding on to reference to it returned by operator*(), get() or operator->().
 * In particular, beware of races between dereferencing a Ref and destroying
 * the object in another thread:
 *
 * auto* p = ref.get();
 * if (p) {
 *   // Be careful! Another thread could destroy *p after we got it from ref.
 *   // This line is only safe if we know that no other thread can destroy *p.
 *   // E.g. if *p is a state machine living on this Worker thread.
 *   p->foo();
 * }
 *
 * However this interface is useful for state machines that run on a worker
 * thread, perform an async operation (either a storage task or a network
 * request), and when the operation comes back on the same worker thread it
 * needs to check if the state machine is still there.
 */
template <typename T>
class WeakRefHolder {
 public:
  class Ref {
   public:
    explicit Ref(std::weak_ptr<T> ptr) : ptr_(std::move(ptr)) {}
    Ref() {} // Create an invalid ref
    T* get() const {
      return ptr_.lock().get();
    }
    T* operator->() const {
      return get();
    }
    T& operator*() const {
      ld_assert(get());
      return *get();
    }

    // True if the referenced WeakRefHolder is still alive.
    // Thread safe, but inherently racy: WeakRefHolder could have been destroyed
    // by another thread right after this check.
    explicit operator bool() const {
      return !ptr_.expired();
    }

    // Returns true if the two `Ref`s point to the same WeakRefHolder instance.
    // Thread safe. Can be called even if the WeakRefHolder was destroyed.
    bool operator==(const Ref& rhs) const {
      return ptr_.owner_before(rhs.ptr_) && !rhs.ptr_.owner_before(ptr_);
    }
    bool operator==(const std::weak_ptr<void>& rhs) const {
      return ptr_.owner_before(rhs) && !rhs.owner_before(ptr_);
    }
    bool operator!=(const Ref& rhs) const {
      return !(*this == rhs);
    }
    bool operator!=(const std::weak_ptr<void>& rhs) const {
      return !(*this == rhs);
    }

   private:
    std::weak_ptr<T> ptr_;
  };
  explicit WeakRefHolder(T* parent) {
    ld_check(parent != nullptr);
    parent_ = std::shared_ptr<T>(std::make_shared<bool>(true), parent);
  }
  Ref ref() const {
    return Ref(parent_);
  }

 private:
  // Note that this shared_ptr<T> is not used as a shared pointer to T. It's
  // effectively two things packed in a single shared_ptr (see WeakRefHolder
  // constructor): a plain pointer to T (without shared ownership and reference
  // counting) and a reference-counted pointer to a dummy object (of type bool).
  // The ownership of the dummy object is not shared, this is the only non-weak
  // pointer to it; dummy object dies when this WeakRefHolder dies. To check
  // whether the WeakRefHolder is alive, Ref checks whether the dummy object is
  // alive, by checking if weak_ptr is expired).
  std::shared_ptr<T> parent_;
};

template <typename T>
using WeakRef = typename WeakRefHolder<T>::Ref;

}} // namespace facebook::logdevice
