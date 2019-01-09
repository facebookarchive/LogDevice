/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/configuration/UpdateableConfigTmpl.h"

#include "logdevice/include/ConfigSubscriptionHandle.h"

namespace facebook { namespace logdevice { namespace configuration {

UpdateableConfigBase::~UpdateableConfigBase() = default;

ConfigSubscriptionHandle
UpdateableConfigBase::subscribeToUpdates(subscription_callback_t callback) {
  std::lock_guard<std::mutex> guard(mutex_);
  subscription_id_t sub_id = last_id_++;
  callbacks_[sub_id] = callback;

  return ConfigSubscriptionHandle(this, sub_id);
}

ConfigSubscriptionHandle
UpdateableConfigBase::subscribeToUpdates(std::function<void()> callback) {
  subscription_callback_t stored_callback = [=]() -> SubscriptionStatus {
    callback();
    return SubscriptionStatus::KEEP;
  };
  return subscribeToUpdates(std::move(stored_callback));
}

void UpdateableConfigBase::notify() {
  std::lock_guard<std::mutex> guard(mutex_);
  for (auto it = callbacks_.begin(); it != callbacks_.end();) {
    // Unsubscribe only if the callback returned UNSUBSCRIBE
    if (it->second() == SubscriptionStatus::UNSUBSCRIBE) {
      it = callbacks_.erase(it);
    } else {
      ++it;
    }
  }
}

void UpdateableConfigBase::unsubscribeFromUpdates(
    subscription_id_t sub_id) noexcept {
  std::lock_guard<std::mutex> guard(mutex_);
  callbacks_.erase(sub_id);
}
}}} // namespace facebook::logdevice::configuration
