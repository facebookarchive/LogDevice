/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <utility>

namespace facebook { namespace logdevice {

namespace configuration {
class UpdateableConfigBase;
enum class SubscriptionStatus;
using subscription_id_t = uint64_t;
} // namespace configuration

/**
 * An interface for the handle for client-side subscriptions to config updates.
 * Contains no usable methods. Automatically unsubscribes itself on destruction
 * (RAII).
 *
 * Can be moved but not copied.
 */
class ConfigSubscriptionHandle final {
 public:
  /**
   * Default constructor. Creates a subscription handle without owner. Useful
   * for move-assignment at a later point.
   */
  ConfigSubscriptionHandle() noexcept = default;

  /**
   * Constructor for a valid subscription handle.
   *
   * @param owner  Configuration from where to unsubscribe at destruction.
   * @param sub_id The internal subscription ID generated from UpdateableConfig
   */
  ConfigSubscriptionHandle(configuration::UpdateableConfigBase* owner,
                           configuration::subscription_id_t sub_id) noexcept;

  /**
   * Destructor. Unsubscribes from owner, if any.
   * If callback is running now, blocks until the callback returns.
   */
  ~ConfigSubscriptionHandle() noexcept;

  /**
   * Move constructor. Releases other.
   */
  ConfigSubscriptionHandle(ConfigSubscriptionHandle&& other) noexcept;

  /**
   * Move-assignment operator. Unsubscribes this and releases other.
   */
  ConfigSubscriptionHandle&
  operator=(ConfigSubscriptionHandle&& other) noexcept;

  /**
   * Unsubscribe now. Useful if one wants to unsubscribe before or without
   * destroying the handle.
   */
  void unsubscribe() noexcept;

  /**
   * Swap two subscription handles.
   */
  void swap(ConfigSubscriptionHandle& other) noexcept {
    using std::swap;
    swap(owner_, other.owner_);
    swap(sub_id_, other.sub_id_);
  }

 private:
  std::weak_ptr<configuration::UpdateableConfigBase> owner_;
  configuration::subscription_id_t sub_id_;
};

}} // namespace facebook::logdevice

namespace std {
inline void
swap(facebook::logdevice::ConfigSubscriptionHandle& left,
     facebook::logdevice::ConfigSubscriptionHandle& right) noexcept {
  left.swap(right);
}
} // namespace std
