/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <atomic>
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <utility>

#include <boost/noncopyable.hpp>
#include <folly/Memory.h>
#include <folly/Portability.h>
#include <folly/ThreadLocal.h>
#include <folly/memory/EnableSharedFromThis.h>
#include <folly/stop_watch.h>

#include "logdevice/common/UpdateableSharedPtr.h"
#include "logdevice/common/configuration/ConfigUpdater.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/common/util.h"
#include "logdevice/include/ConfigSubscriptionHandle.h"
#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice {

class ConfigSubscriptionHandle;

namespace configuration {

/**
 * Value of this enum are returned by config update subscribers callbacks.
 * The returned status indicate whether the callback is still interested in
 * keeping the subscription or not.
 */
enum class SubscriptionStatus {
  KEEP,       // Keep this subscription and notify me on future updates.
  UNSUBSCRIBE // Unubscripe me and do not call the callback any more.
};

using subscription_callback_t = std::function<SubscriptionStatus()>;

/**
 * @file A config that may change over time.  Consumers must call get() to
 *       acquire a snapshot of the configuration.
 */

/*
 * The design of updateable configs attempts to satisfy the following
 * requirements:
 *
 * - There are multiple reader threads that want to pull the newest version of
 *   an updateable config.  This happens often so it needs to be cheap.
 *
 * - The are writer threads that periodically want to update the config.
 *
 * - There is an unknown number of requests that hold references to a config.
 *   Old configs must be destroyed when there are no more references to them.
 *
 * - Applications using LogDevice may want to connect to separate LogDevice
 *   clusters.  It should be possible to have multiple updateable configs.
 *
 * Straightforward designs based on bare shared_ptr or intrusive_ptr are not
 * thread-safe (they are not read-write safe).
 *
 * Locks are prohibitively expensive; even folly rwspinlocks (which are a good
 * fit for our need) were measured at ~1us cpu time per read.
 *
 * The end design uses a combination of std::shared_ptr, versioning,
 * thread-local storage and locking. See UpdateableSharedPtr.h for details.
 */

/**
 * Base class for all instances of template class UpdateableConfigTmpl.
 * Implements update callback subscriptions and notifications.
 */
class UpdateableConfigBase
    : public folly::enable_shared_from_this<UpdateableConfigBase>,
      private boost::noncopyable {
 public:
  /**
   * Whenever the config is updated, call the given callback on an unspecified
   * thread.
   *
   * This method is thread-safe.
   *
   * @return Handle that can be used to unsubscribe. Destroying the handle
   *         automatically unsubscribes the callback in a thread-safe manner.
   */
  FOLLY_NODISCARD ConfigSubscriptionHandle
  subscribeToUpdates(subscription_callback_t callback);

  // A short cut subscription if the subscriber doesn't want to control whether
  // the subscription should continue or not in the supplied callback
  FOLLY_NODISCARD ConfigSubscriptionHandle
  subscribeToUpdates(std::function<void()> callback);

 protected:
  /**
   * Protected constructor. Abstract class.
   */
  UpdateableConfigBase() = default;

  /**
   * Protected destructor. No polymorphic destruction.
   */
  ~UpdateableConfigBase();

  /**
   * Call all subscribed callbacks.
   */
  void notify();

  // Callbacks registered via subscribeToUpdates().  Lock is needed
  // since the list will be accessed from different threads.
  std::unordered_map<subscription_id_t, subscription_callback_t> callbacks_;

  // Protects accesses to callbacks_ and other member variables of derived
  // classes.
  mutable std::mutex mutex_;

 private:
  friend class facebook::logdevice::ConfigSubscriptionHandle;
  std::atomic<subscription_id_t> last_id_{0};

  /**
   * Unsubscribe a callback. Called by subscription handles on destruction.
   * If callback is running now, this method blocks until the callback returns.
   */
  void unsubscribeFromUpdates(subscription_id_t sub_id) noexcept;
};

template <class Config, class Overrides>
class UpdateableConfigTmpl : public UpdateableConfigBase {
  using OverridesUpdater =
      std::function<void(const std::shared_ptr<Config>&, Overrides&)>;

 public:
  /**
   * Constructor.
   *
   * An initial Config instance may be provided.  If omitted, the
   * config is null until an update() call.
   *
   */
  explicit UpdateableConfigTmpl(
      std::shared_ptr<Config> initial_config = nullptr);

  /**
   * Destructor.
   */
  ~UpdateableConfigTmpl();

  /**
   * Provides a snapshot of the server configuration as a Config instance.
   *
   * This method is thread-safe.
   */
  std::shared_ptr<Config> get() const {
    return config_.get();
  }

  /**
   * Updates this config with a Config instance, applying any
   * existing, local, configuration overrides.  This can be called, for
   * example, by a background monitoring thread when it detects a change
   * in the config file.
   *
   * Callbacks subscribed via subscribeToServerConfigUpdates() are invoked.
   *
   * This method is thread-safe.
   *
   * @returns 0 on success, -1 on failure. Possible failures:
   *  - INVALID_CONFIG: one of the hooks rejected the config.
   */
  int update(std::shared_ptr<Config> ptr);

  /**
   * Update/cancel local configuration overrides. This is typically
   * invoked by an admin command handler.
   *
   * After the update, overrides are re-applied to the base config
   * to generate a new config that is then published.
   *
   * Callbacks subscribed via subscribeToUpdates() are invoked.
   *
   * This method is thread-safe.
   *
   * @returns 0 on success, -1 on failure
   */
  int updateOverrides(OverridesUpdater update_overrides_cb);

  using Hook = typename std::function<bool(Config&)>;
  using HookList = typename std::list<Hook>;
  using RawHookHandle = typename HookList::iterator;
  class HookHandle {
   public:
    HookHandle() noexcept = default;
    HookHandle(UpdateableConfigTmpl<Config, Overrides>* config,
               RawHookHandle handle) noexcept
        : owner_(config->weak_from_this()), handle_(handle) {}
    HookHandle(HookHandle&& other) noexcept
        : owner_(other.owner_), handle_(other.handle_) {
      other.owner_.reset();
    }
    ~HookHandle() {
      deregister();
    }
    void deregister() {
      if (auto ptr = owner_.lock()) {
        static_cast<UpdateableConfigTmpl<Config, Overrides>*>(ptr.get())
            ->delHook(*this);
      }
      owner_.reset();
    }
    HookHandle& operator=(HookHandle&& other) {
      if (this != &other) {
        deregister();
        owner_ = other.owner_;
        handle_ = other.handle_;
        other.owner_.reset();
      }
      return *this;
    }

   private:
    std::weak_ptr<UpdateableConfigBase> owner_;
    RawHookHandle handle_;

    friend class UpdateableConfigTmpl<Config, Overrides>;
  };

  /**
   * Registers a function that will be called when configuration is updated.
   * If any of registered hooks returns false, configuration is rejected, which
   * manifests itself as update() failing. Hooks may mutate the ServerConfig
   * object before it is published.
   * Returns a handle that will automatically deregister the hook upon
   * destruction. The handle should be associated with the objects or resources
   * that are used in the hook callback, and it should be destroyed before them,
   * to ensure that the callback does not access freed memory.
   */
  HookHandle addHook(Hook hook) {
    std::lock_guard<std::mutex> guard(mutex_);
    return HookHandle(this, hooks_.insert(hooks_.end(), hook));
  }

  void setUpdater(std::shared_ptr<ConfigUpdater> updater) {
    config_updater_ = updater;
  }
  std::shared_ptr<ConfigUpdater> getUpdater() {
    ld_check(config_updater_);
    return config_updater_;
  }

  /**
   * Forces the construction and publishing of a new Config object even
   * if the contents have not changed.  This is used by the RemoteLogsConfig
   * machinery to invoke configs subscription callbacks and invalidate
   * possibly stale caches.
   */
  int invalidate() {
    if (config_updater_) {
      config_updater_->invalidateConfig();
      return 0;
    } else {
      ld_check(false);
      return -1;
    }
  }

  /**
   * Attempts to update the last known version to version. Returns true iff the
   * last known version is successfully updated (version > current last known
   * version)
   */
  bool updateLastKnownVersion(config_version_t version) {
    uint32_t prev = atomic_fetch_max(last_known_version_, version.val());
    return version.val() > prev;
  }

  /**
   * Attempts to update the last known version received though config
   * changed message. Returns true iff the last received version is
   * successfully updated (version > current last received version)
   */
  bool updateLastReceivedVersion(config_version_t version) {
    uint32_t prev = atomic_fetch_max(last_received_version_, version.val());
    return version.val() > prev;
  }

 private:
  /**
   * Removes a hook from the hook list while holding the mutex.
   */
  void delHook(HookHandle& handle) {
    std::lock_guard<std::mutex> guard(mutex_);
    ld_check(handle.owner_.lock().get() == this);
    hooks_.erase(handle.handle_);
  }

  UpdateableSharedPtr<Config> config_;

  // List of registered config hooks
  HookList hooks_;

  // The highest known config version. Used to determine if the current config
  // is stale.
  std::atomic<uint32_t> last_known_version_{0};
  // The highest received config version from CONFIG_CHANGED.
  std::atomic<uint32_t> last_received_version_{0};

  // Updater for this config
  std::shared_ptr<ConfigUpdater> config_updater_;

  // Config provided by normal configuration update mechanisms.
  // without any overrides
  std::shared_ptr<Config> base_config_;

  // Local overrides that are independent of the normal config sources.
  // (e.g. tweaked via the admin interface).
  Overrides config_overrides_;
};

template <class Config, class Overrides>
UpdateableConfigTmpl<Config, Overrides>::UpdateableConfigTmpl(
    std::shared_ptr<Config> initial_config)
    : config_(std::move(initial_config)) {}

template <class Config, class Overrides>
UpdateableConfigTmpl<Config, Overrides>::~UpdateableConfigTmpl() = default;

template <class Config, class Overrides>
int UpdateableConfigTmpl<Config, Overrides>::updateOverrides(
    OverridesUpdater update_overrides_cb) {
  std::shared_ptr<Config> new_config;
  {
    // To ensure this method can return an error leaving the existing
    // and known valid Configuration and Overrides in effect, all updates
    // are performed on copies and then committed only after validation
    // succeeds.
    //
    // Note: We remember the configuration without overrides (committed
    //       to base_config_) and publish the config resulting from applying
    //       any overrides (in configuration_). These copies of the
    //       configuration may be identical.
    std::lock_guard<std::mutex> guard(mutex_);
    ld_check(update_overrides_cb != nullptr);
    // copy() here to avoid rollback logic (restoring base_config_)
    // if the update fails.
    std::shared_ptr<Config> base_config = base_config_->copy();

    auto new_overrides = config_overrides_;
    if (update_overrides_cb) {
      update_overrides_cb(base_config, new_overrides);
    }
    new_config = new_overrides.apply(base_config);

    // Validate config.
    for (auto& hook : hooks_) {
      if (!hook(*new_config)) {
        err = E::INVALID_CONFIG;
        return -1;
      }
    }

    // Using both the base config and the overrides resulted in
    // a valid config. Commit.
    config_overrides_ = std::move(new_overrides);
    base_config_ = std::move(base_config);
  }

  // Publish
  config_.update(std::move(new_config));
  notify();
  return 0;
}

template <class Config, class Overrides>
int UpdateableConfigTmpl<Config, Overrides>::update(
    std::shared_ptr<Config> base_config) {
  folly::stop_watch<std::chrono::milliseconds> watch;
  uint64_t lock_acquired_ms = 0;
  uint64_t overrides_applied_ms = 0;
  uint64_t hooks_executed_ms = 0;
  uint64_t config_updated_ms = 0;
  uint64_t notifications_done_ms = 0;
  std::shared_ptr<Config> new_config;
  {
    // To ensure this method can return an error leaving the existing
    // and known valid Configuration and Overrides in effect, all updates
    // are performed on copies and then committed only after validation
    // succeeds.
    //
    // Note: We remember the configuration without overrides (committed
    //       to base_config_) and publish the config resulting from applying
    //       any overrides (in configuration_). These copies of the
    //       configuration may be identical.
    std::lock_guard<std::mutex> guard(mutex_);
    lock_acquired_ms = watch.lap().count();

    new_config = config_overrides_.apply(base_config);
    overrides_applied_ms = watch.lap().count();
    // Validate config.
    for (auto& hook : hooks_) {
      if (!hook(*new_config)) {
        err = E::INVALID_CONFIG;
        return -1;
      }
    }
    hooks_executed_ms = watch.lap().count();

    base_config_ = std::move(base_config);
  }

  // Publish
  config_.update(std::move(new_config));
  config_updated_ms = watch.lap().count();
  notify();
  notifications_done_ms = watch.lap().count();

  ld_info("Config updated. Timings: Acquiring lock: %lums, applying overrides: "
          "%lums, hook execution: %lums, config update: %lums, notifications: "
          "%lums",
          lock_acquired_ms,
          overrides_applied_ms,
          hooks_executed_ms,
          config_updated_ms,
          notifications_done_ms);
  return 0;
}

} // namespace configuration
}} // namespace facebook::logdevice
