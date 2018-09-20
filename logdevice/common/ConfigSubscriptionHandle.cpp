/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/configuration/UpdateableConfigTmpl.h"

namespace facebook { namespace logdevice {

ConfigSubscriptionHandle::ConfigSubscriptionHandle(
    configuration::UpdateableConfigBase* owner,
    configuration::subscription_id_t sub_id) noexcept
    : owner_(owner->weak_from_this()), sub_id_(sub_id) {}

ConfigSubscriptionHandle::~ConfigSubscriptionHandle() noexcept {
  unsubscribe();
}

ConfigSubscriptionHandle::ConfigSubscriptionHandle(
    ConfigSubscriptionHandle&& other) noexcept
    : owner_(other.owner_), sub_id_(other.sub_id_) {
  other.owner_.reset();
}

ConfigSubscriptionHandle& ConfigSubscriptionHandle::
operator=(ConfigSubscriptionHandle&& other) noexcept {
  if (this != &other) {
    unsubscribe();
    owner_ = other.owner_;
    sub_id_ = other.sub_id_;
    other.owner_.reset();
  }
  return *this;
}

void ConfigSubscriptionHandle::unsubscribe() noexcept {
  if (auto ownerPtr = owner_.lock()) {
    ownerPtr->unsubscribeFromUpdates(sub_id_);
  }
  owner_.reset();
}
}} // namespace facebook::logdevice
