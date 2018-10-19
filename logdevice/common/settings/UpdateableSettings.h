/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>
#include <type_traits>
#include <unordered_map>

#include <boost/make_shared.hpp>
#include <boost/noncopyable.hpp>
#include <boost/program_options.hpp>

#include "logdevice/common/UpdateableSharedPtr.h"
#include "logdevice/common/settings/UpdateableSettings-details.h"
#include "logdevice/common/util.h"

/**
 * @file UpdateableSettings.h
 *
 * UpdateableSettings is a utility for defining settings bundles that can be
 * updated in the presence of concurrent reads and writes. It uses
 * FastUpdateableSharedPtr under the hood.
 *
 * Settings are given flags which define their access scope and how they can be
 * updated. @see SettingFlag.
 *
 * @see logdevice/common/tests/SettingsTest.cpp for an example usage.
 */

namespace facebook { namespace logdevice {

namespace SettingFlag {
// This setting should be exposed to the LogDevice client libaray.
constexpr flag_t CLIENT = 1 << 1;
// This setting is for the server. Can be used alongside CLIENT.
constexpr flag_t SERVER = 1 << 2;
// This setting can only be specified on the command line. It will not be
// exposed to the config and admin command interface. Implies
// REQUIRES_RESTART.
constexpr flag_t CLI_ONLY = 1 << 3;
// After changing this setting, a restart is required for the new value to
// take effect.
constexpr flag_t REQUIRES_RESTART = 1 << 4;
// This setting can only be set by LogDevice code. It will not be exposed to
// any outside interfaces
constexpr flag_t INTERNAL_ONLY = 1 << 5;
// This setting controls a feature that is considered experimental and
// not production ready.
constexpr flag_t EXPERIMENTAL = 1 << 6;
// This setting is deprecated and should not be used.
constexpr flag_t DEPRECATED = 1 << 7;
}; // namespace SettingFlag

namespace SettingsCategory {
constexpr const char* Batching = "Batching and compression";
constexpr const char* Configuration = "Configuration";
constexpr const char* Core = "Core settings";
constexpr const char* Execution = "State machine execution";
constexpr const char* FailureDetector = "Failure detector";
constexpr const char* LogsDB = "LogsDB";
constexpr const char* Monitoring = "Monitoring";
constexpr const char* Network = "Network communication";
constexpr const char* Performance = "Performance";
constexpr const char* ReadPath = "Read path";
constexpr const char* Rebuilding = "Rebuilding";
constexpr const char* Recovery = "Recovery";
constexpr const char* ResourceManagement = "Resource management";
constexpr const char* RocksDB = "RocksDB";
constexpr const char* ReaderFailover = "Reader failover";
constexpr const char* Security = "Security";
constexpr const char* SequencerBoycotting = "Sequencer boycotting";
constexpr const char* Sequencer = "Sequencer State";
constexpr const char* Storage = "Storage";
constexpr const char* Testing = "Testing";
constexpr const char* WritePath = "Write path";
constexpr const char* AdminAPI = "Admin API/server";
}; // namespace SettingsCategory

// Every bandle must implement this interface.
class SettingsBundle {
 public:
  virtual const char* getName() const = 0;
  virtual void defineSettings(SettingEasyInit& init) = 0;
  virtual ~SettingsBundle() {}
};

template <typename T>
class UpdateableSettings {
 public:
  // Until we ship concepts to C++...
  static_assert(std::is_base_of<SettingsBundle, T>::value,
                "UpdateableSettings<T>: T must implement the "
                "SettingsBundle interface");

  /**
   * Construct a UpdateableSettings object. Initializes the underlying T
   * instance's members with default values as specified in T::defineSettings().
   * T must implement the SettingsBundle interface.
   */
  UpdateableSettings() {
    ptr_ = std::make_shared<UpdateableSettingsRaw<T>>();
  }

  /**
   * For settings listed in `override_defaults` the value from
   * `override_defaults` will be used as default value of the setting, instead
   * of the default value given by T::defineSettings().
   * This allows having different default values depending on context,
   * e.g. client, server, tools.
   * All keys in `override_defaults` must name existing settings. This is
   * asserted using ld_check().
   */
  explicit UpdateableSettings(
      const std::unordered_map<std::string, std::string>& override_defaults) {
    ptr_ = std::make_shared<UpdateableSettingsRaw<T>>(override_defaults);
  }

  // An overload to help the compiler disambiguate between the other two
  // single-argument overloads.
  explicit UpdateableSettings(
      std::initializer_list<std::pair<std::string, std::string>>
          override_defaults) {
    ptr_ = std::make_shared<UpdateableSettingsRaw<T>>(
        std::unordered_map<std::string, std::string>(
            override_defaults.begin(), override_defaults.end()));
  }

  /**
   * Construct a UpdateableSettings object and set the data from `data`.
   * This method should only be used in tests as all updates should be made
   * through SettingsUpdater in a non testing environment.
   */
  /* implicit */ UpdateableSettings(T data) {
    ptr_ = std::make_shared<UpdateableSettingsRaw<T>>(std::move(data));
  }
  UpdateableSettings(const UpdateableSettings<T>& other) = default;
  UpdateableSettings(UpdateableSettings<T>&& other) = default;
  UpdateableSettings<T>& operator=(const UpdateableSettings<T>& other) =
      default;
  UpdateableSettings<T>& operator=(UpdateableSettings<T>&& other) = default;

  /**
   * @returns a shared_ptr to the bundle.
   */
  const std::shared_ptr<const T> get() const {
    return ptr_->get();
  }

  /**
   * @returns a shared_ptr to the bundle. This operator is useful for quick
   * acces to a data member of T.
   */
  const std::shared_ptr<const T> operator->() const {
    return get();
  }

  std::shared_ptr<UpdateableSettingsRaw<T>> raw() {
    return ptr_;
  }

  using SubscriptionHandle =
      typename UpdateableSettingsRaw<T>::SubscriptionHandle;

  // Adds a callback to the list of callbacks that will be called whenever
  // update() is called.  It will be called on a dedicated configuration thread,
  // not on your application thread.  This method is thread safe: it grabs a
  // mutex to ensure no callbacks, unsubscribe or other subscribe reqeusts are
  // running at the same time. The returned handle can be used in
  // unsubscribeFromUpdates() or will unsubscribe automatically on destruction
  // (so save it for as long as you want to keep the subscription)
  SubscriptionHandle subscribeToUpdates(std::function<void()> callback) {
    return ptr_->subscribeToUpdates(std::move(callback));
  }

  // Like subscribeToUpdates(), but also calls the callback right now, inside
  // the callAndSubscribeToUpdates() call.
  // Very similar to calling subscribeToUpdates(), then calling the callback.
  // The only difference is that this method calls the callback while holding
  // a mutex that prevents background thread from calling the callback. This
  // prevents certain race conditions.
  SubscriptionHandle callAndSubscribeToUpdates(std::function<void()> callback) {
    return ptr_->callAndSubscribeToUpdates(std::move(callback));
  }

  // Removes the callback that the handle points to from the subscriber list.
  void unsubscribeFromUpdates(SubscriptionHandle& handle) {
    ptr_->unsubscribeFromUpdates(handle);
  }

 private:
  std::shared_ptr<UpdateableSettingsRaw<T>> ptr_;
};

}} // namespace facebook::logdevice
