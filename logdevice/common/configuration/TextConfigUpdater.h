/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "logdevice/common/ConfigSource.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/Semaphore.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/configuration/ConfigUpdater.h"
#include "logdevice/common/configuration/LogsConfig.h"
#include "logdevice/common/configuration/ServerConfig.h"
#include "logdevice/common/settings/UpdateableSettings.h"
#include "logdevice/common/stats/Stats.h"

namespace facebook { namespace logdevice {

/**
 * @file Orchestrates config fetching and parsing, allowing config contents to
 * be fetched from various sources.
 */
class TextConfigUpdaterImpl : public ConfigSource::AsyncCallback,
                              public configuration::ConfigUpdater {
 public:
  /**
   * if  target_logs_config is nullptr we will assume that this updates does not
   * manage logs
   */
  explicit TextConfigUpdaterImpl(
      Semaphore& initial_config_sem,
      std::shared_ptr<UpdateableServerConfig> target_server_config,
      std::shared_ptr<UpdateableLogsConfig> target_logs_config,
      UpdateableSettings<Settings> updateable_settings,
      StatsHolder* stats = nullptr)
      : target_server_config_(target_server_config),
        target_logs_config_(target_logs_config),
        updateable_settings_(std::move(updateable_settings)),
        stats_(stats),
        initial_config_sem_(initial_config_sem) {}

  explicit TextConfigUpdaterImpl(
      Semaphore& initial_config_sem,
      std::shared_ptr<UpdateableConfig> updateable_config,
      UpdateableSettings<Settings> updateable_settings =
          UpdateableSettings<Settings>(),
      StatsHolder* stats = nullptr)
      : TextConfigUpdaterImpl(initial_config_sem,
                              updateable_config->updateableServerConfig(),
                              updateable_config->updateableLogsConfig(),
                              std::move(updateable_settings),
                              stats) {}

  /**
   * Registers a `ConfigSource'.  This should be called shortly after
   * construction to enable various config backends to be used.
   */
  void registerSource(std::unique_ptr<ConfigSource>);

  /**
   * Requests a config to be loaded and tracked.  The location should be
   * formatted as "scheme:path" where the scheme is recognized by one of the
   * registered `ConfigSource's.
   *
   * Does not fail if the config itself is invalid.  Call
   * `waitForValidConfig()' afterwards to force an initially valid config.
   *
   * @return 0 on success, -1 if the location specifier is invalid
   */
  int load(const std::string& location,
           std::unique_ptr<LogsConfig> alternative_logs_config,
           const ConfigParserOptions& options = ConfigParserOptions());

  /**
   * Fetches the config from the source of the current config. This is
   * typically invoked when the current config determined to be stale.
   *
   * @returns 0 on success, -1 on failure
   */
  int fetchFromSource() override;

  /**
   * Waits for the initial `load()' to complete.
   *
   * @return 0 on success (config load finished in time and generated a valid
   * config), -1 on failure (timeout or invalid config)
   */
  int waitForInitialLoad(std::chrono::milliseconds timeout);

  /**
   * Invoked by `ConfigSource' instances when an asynchronous get finished (or
   * an update was pushed) and config contents are available.
   *
   * Call with `status' OK if everything was fine, or some other status if the
   * fetch failed.
   *
   * If the source wishes to provide a specific hash (e.g. a source control
   * hash) for the config, it can also populate `hash'.  Otherwise, pass an
   * empty string; this class will calculate a hash of the contents.
   */
  void onAsyncGet(ConfigSource* source,
                  const std::string& path,
                  Status status,
                  ConfigSource::Output config) override;

  /**
   * Forces the construction and publishing of a new Configuration object
   * if the contents have changed. The logs config is reloaded unconditionally.
   * This is used by the RemoteLogsConfig machinery to invoke configs
   * subscription callbacks and invalidate possibly stale caches.
   */
  void invalidateConfig() override {
    update(/*force_reload_logsconfig=*/true);
  }

  // Returns true if the latest load config is valid
  bool hasValidConfig();
  ~TextConfigUpdaterImpl() override;

 private:
  std::weak_ptr<UpdateableServerConfig> target_server_config_;
  std::weak_ptr<UpdateableLogsConfig> target_logs_config_;

  std::vector<std::unique_ptr<ConfigSource>> sources_;
  std::unique_ptr<LogsConfig> alternative_logs_config_;
  ConfigParserOptions config_parser_options_;
  UpdateableSettings<Settings> updateable_settings_;
  StatsHolder* stats_;

  // no config at the beginning considered as not valid config state
  bool isRecentConfigValid_ = false;

  // Reference to initial_config_sem_ defined in TextConfigUpdater
  Semaphore& initial_config_sem_;

  bool invalid_logs_config_{false};

  // Holds states for the main config and (possibly) the included log config.
  // onGetDone() updates these as contents arrive from the sources,
  // then calls update() to try to construct the config.
  struct State {
    ConfigSource* source = nullptr;
    std::string path;
    std::chrono::milliseconds last_loaded_time;
    folly::Optional<ConfigSource::Output> output;
  } main_config_state_, included_config_state_;

  // Set to true at the beginning of destructor. Prevents onAsyncGet() from
  // accessing ConfigSource that may be deleted.
  bool destroying_ = false;

  // Helper method called by load() and onAsyncGet() when a source provides
  // contents of a config, synchronously or asynchronously.  Logs and
  // optionally calls update().
  void onContents(State* state, ConfigSource::Output output, bool call_update);
  // Called after we get some data from source (`main_config_state_' or
  // `included_config_state_' changed), time to try to parse the config text
  // and push a new config.
  // force_reload_logsconfig casues the logsconfig object to be reconstructed
  // regardless of whether the main config has changed.
  void update(bool force_reload_logsconfig = false);

  // Parses a location of the form "scheme:path" and finds the appropriate
  // registered config source.  If none is found, returns nullptr.
  std::pair<ConfigSource*, std::string>
  parseLocation(const std::string& location);
  // Similar to parseMaybeRelativeLocation(), but if the scheme is absent,
  // interprets the location relative to another config source+path.
  std::pair<ConfigSource*, std::string>
  parseMaybeRelativeLocation(const std::string& location,
                             ConfigSource* ref_source,
                             const std::string& ref_path);
  // Based on the file extension, attempts to decompress the contents.
  //
  // NOTE: We decompress as soon as we receive text from sources.  The
  // uncompressed text is cached and hashed.
  //
  // Supported extensions: .gz
  std::string maybeDecompress(const std::string& path,
                              std::string raw_contents);
  // Update local config stat and set last_config_valid stats to 0 or 1
  // depending on validity of most recent received config.
  // Supposed to be called synchronously since update() is locked under mutex
  void setRecentConfigValidity(bool state);

  // Compares new and old server configs
  // Returns 0 if configs are identical
  //         -1 if new_config appears to be older (based on version)
  //         1 if new_config has a more recent version
  int compareServerConfig(const std::shared_ptr<ServerConfig>& old_config,
                          const std::shared_ptr<ServerConfig>& new_config);
};

class TextConfigUpdater : public ConfigSource::AsyncCallback,
                          public configuration::ConfigUpdater {
 public:
  template <typename... Args>
  TextConfigUpdater(Args&&... args)
      : impl_(folly::in_place,
              initial_config_sem_,
              std::forward<Args>(args)...) {}
  template <typename... Args>
  void registerSource(Args&&... args) {
    impl_->registerSource(std::forward<Args>(args)...);
  }
  template <typename... Args>
  int load(Args&&... args) {
    return impl_->load(std::forward<Args>(args)...);
  }
  void onAsyncGet(ConfigSource* source,
                  const std::string& path,
                  Status status,
                  ConfigSource::Output config) override {
    impl_->onAsyncGet(source, path, status, std::move(config));
  }
  int fetchFromSource() override {
    return impl_->fetchFromSource();
  }
  void invalidateConfig() override {
    impl_->invalidateConfig();
  }
  int waitForInitialLoad(std::chrono::milliseconds timeout);

 private:
  // Wakes waitForInitialLoad() after the first config is fetched
  // (successfully or not).
  Semaphore initial_config_sem_;

  folly::Synchronized<TextConfigUpdaterImpl> impl_;
  // NOTE: do not add anything here!
  //
  // impl_ should be the last member of this class so it gets destroyed before
  // anything else
};

}} // namespace facebook::logdevice
