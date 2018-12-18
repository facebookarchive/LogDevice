/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>
#include <deque>
#include <mutex>
#include <unordered_map>

#include <boost/algorithm/string/predicate.hpp>
#include <boost/program_options.hpp>
#include <folly/Format.h>
#include <folly/Optional.h>

#include "logdevice/common/settings/UpdateableSettings.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/common/util.h"
#include "logdevice/include/EnumMap.h"

/**
 * @file SettingsUpdater.h
 *
 * SettingsUpdater is a utility providing various update mechanisms on
 * SettingBundles that have been registered to it. Each update mechanism (or
 * source) has a different order of precedence. Any setting in config file will
 * override what is set from the CLI. Any setting set from the admin command
 * will override the current value.
 *
 * @see logdevice/common/settings/UpdateableSettings.h for how readers can see
 * updates on settings.
 *
 * @see logdevice/common/tests/SettingsTest.cpp for an example usage.
 */

namespace facebook { namespace logdevice {

class SettingsUpdater {
 public:
  SettingsUpdater() {}
  SettingsUpdater(SettingsUpdater&& other) noexcept
      : bundles_(std::move(other.bundles_)),
        settings_(std::move(other.settings_)) {}

  // CURRENT is a pseudo-source. It's a cache of the effective value aggregated
  // from the other sources according to precedence rules.
  enum class Source { INTERNAL = 0, CLI, CLIENT, CONFIG, ADMIN_CMD, CURRENT };
  using SourceNameMap =
      EnumMap<Source,
              std::string,
              /* InvalidEnum */ Source::CURRENT,
              /* Size */ static_cast<int>(Source::CURRENT) + 1>;

  static SourceNameMap& sourceNames();

  // Maintain the state of all settings.
  struct SettingState {
    std::string bundle_name;
    UpdateableSettingsBase::SettingDescriptor descriptor;

    // Keep track of the current value of the setting on a per source basis.
    // The effective value of the setting is the value of the source with a
    // value that has the most priority. Order of priorities is defined by the
    // order of the values in the Source enum (CURRENT omitted). CLI is the
    // source with the least precedence, ADMIN_CMD is the one with the most.
    using SourceMap = std::unordered_map<int, std::vector<std::string>>;
    SourceMap sources;

    // TODO(T8584641): keep more information there. Ideas:
    // - timestamp of the last update;
    // - IP address of the admin command that overrode the setting;
    // - An update history?
  };

  /**
   * @param flag if set, only show the settings that have the given flag. Useful
   *        if you wish to only show CLIENT or SERVER settings.
   * @param bundle Only print settings that belong to the given bundle;
   * @return Help message to describe the settings.
   */
  std::string help(SettingFlag::flag_t flag = 0,
                   folly::Optional<std::string> bundle = folly::none,
                   unsigned line_length = 160,
                   unsigned min_description_length = 50) const;

  /**
   * @return a Markdown document describing all settings. Use this to
   *         generate LogDevice documentation.
   * @param include_deprecated Also include deprecated settings
   * @return Settings documentation in Markdown
   */
  std::string markdownDoc(bool include_deprecated = false) const;

  /**
   * Register a new bundle to contain settings to be updated by this object.
   * @bundle Setting bundle to be registered.
   */
  template <typename T>
  void registerSettings(UpdateableSettings<T> bundle) {
    auto raw = std::static_pointer_cast<UpdateableSettingsBase>(bundle.raw());
    bundles_.push_back(raw);
    for (const auto& i : raw->getSettings()) {
      SettingState setting;
      setting.bundle_name = raw->getName();
      setting.descriptor = i.second;
      setting.sources[static_cast<int>(Source::CURRENT)] =
          i.second.default_value;
      settings_[i.first] = std::move(setting);
    }
  }

  // Returns folly::none if check passed, or error message if check failed.
  using setting_checker_t = std::function<folly::Optional<std::string>(
      const SettingState&,
      const std::string& /* setting_name */)>;
  using fallback_parser_t = std::function<void(int argc, const char** argv)>;

  /**
   * Set some setting comming from CLI
   *
   * @param argc            Number of tokens
   * @param argv            Array of tokens
   * @param checker         a checker function for a setting. Usually one of
   *                        SettingsUpdater::mustBeServerOption or
   *                        SettingsUpdater::mustBeClientOption
   * @param fallback_parser all unrecognized arguments will be passed to the
   *                        fallback_parser if one is set. If no fallback_parser
   *                        is set and there are unrecognized arguments, this
   *                        will throw a boost::program_options::error
   */
  void parseFromCLI(int argc,
                    const char** argv,
                    setting_checker_t checker,
                    fallback_parser_t fallback_parser = nullptr);
  /**
   * Set some settings coming from CLI. This function is similar to parseFromCLI
   * but more convenient for tests.
   *
   * @param setting Setting to be set.
   */
  void setFromCLI(std::unordered_map<std::string, std::string> settings);

  /**
   * Validate settings coming from the config without applying them.
   *
   * Throws a boost::program_options::error if there is a validation error or
   * the setting does not exist.
   *
   * @param settings Settings to be validated.
   */
  void validateFromConfig(
      const std::unordered_map<std::string, std::string>& settings,
      SettingFlag::flag_t match_flag = SettingFlag::CLIENT |
          SettingFlag::SERVER);

  /**
   * Validate and apply settings that are set by LogDevice code internally
   *
   * Throws a boost::program_options::error if there is a validation error or
   * the setting does not exist.
   *
   * @param name  Name of the setting to be changed.
   * @param value New value of the setting.
   */
  void setInternalSetting(std::string name, std::string value);

  /**
   * Validate and apply settings coming from the config.
   *
   * Throws a boost::program_options::error if there is a validation error or
   * the setting does not exist.
   *
   * @param settings   Settings to be validated.
   */
  void setFromConfig(std::unordered_map<std::string, std::string> settings);

  /**
   * Set a setting from the admin command.
   *
   * Throws a boost::program_options::error if there is a validation error or
   * the setting does not exist.
   *
   * @param name  Name of the setting to be changed.
   * @param value New value of the setting.
   */
  void setFromAdminCmd(std::string name, std::string value);

  /**
   * Unset a setting from the admin command.
   *
   * The setting's value will be changed with the value from the source with the
   * highest precedence (ie the value from the config if any, else the value
   * from the CLI if any, else the default value).
   *
   * Throws a boost::program_options::error if there is a validation error or
   * the setting does not exist.
   *
   * @param name of the setting to be unset.
   */
  void unsetFromAdminCmd(std::string name);

  /**
   * Set a setting from the client.
   *
   * Throws a boost::program_options::error if there is a validation error or
   * the setting does not exist.
   *
   * @param name  Name of the setting to be changed.
   * @param value New value of the setting.
   */
  void setFromClient(std::string name, std::string value);

  const std::unordered_map<std::string, SettingState> getState() const;

  /**
   * @param setting Setting to search the value for. This function asserts that
   *                this setting exists.
   * @param src     Source for which to find a value.
   * @return Value of the setting from the given source. Returns folly::none
   *         if there is no value from this source.
   */
  folly::Optional<std::string> getValueFromSource(const std::string& setting,
                                                  Source src) const;

  // Helper functions to pass to parseFromCLI() to filter only server or
  // client settings.
  static folly::Optional<std::string>
  mustBeServerOption(const SettingState& setting,
                     const std::string& setting_name) {
    if (!(setting.descriptor.flags & SettingFlag::SERVER)) {
      return folly::sformat("'{}' must be a server option.", setting_name);
    }
    return folly::none;
  }

  static folly::Optional<std::string>
  mustBeClientOption(const SettingState& setting,
                     const std::string& setting_name) {
    if (!(setting.descriptor.flags & SettingFlag::CLIENT)) {
      return folly::sformat("'{}' must be a client option.", setting_name);
    }
    return folly::none;
  }

  static folly::Optional<std::string>
  eitherClientOrServerOption(const SettingState& setting,
                             const std::string& setting_name) {
    if (!(setting.descriptor.flags & SettingFlag::CLIENT) &&
        !(setting.descriptor.flags & SettingFlag::SERVER)) {
      return folly::sformat(
          "'{}' must be a client or server option.", setting_name);
    }
    return folly::none;
  }

  /**
   * @param setting The setting to get the value of
   * @return Returns the setting value from the highest priority source,
   *         Returns folly::none if the setting does not exist
   */
  folly::Optional<std::string> getValue(const std::string& setting) const;
  /**
   * @return Returns a vector of <setting, value> pairs for all settings
   */
  std::vector<std::pair<std::string, std::string>> getAllValues() const;

 private:
  // set to true once setFromConfig() is called. Once true, prevent any setting
  // that has the REQUIRES_RESTART flag from being changed again.
  bool initial_load_done_{false};

  std::vector<std::shared_ptr<UpdateableSettingsBase>> bundles_;
  std::unordered_map<std::string, SettingState> settings_;

  // Used to protect concurrent accesses for writes on settings from admin
  // commnd thread, config update thread, client main thread, etc.
  // parse().
  mutable std::mutex mutex_;

  /**
   * @param setting Setting to search the value for. This function asserts that
   *                this setting exists.
   * @param src     Source for which to find a value.
   * @return Value of the setting from the given source. Returns an empty vector
   *         if there is no value from this source.
   */
  std::vector<std::string>& valueFromSourceRef(const std::string& setting,
                                               Source src);

  /**
   * Parse some settings and update settings_. Throws
   * boost::program_options::error if something's malformed. Please do all the
   * settings updates using one of the two parse() methods: they log the changes
   * with ld_info().
   *
   * @param arc     Number of settings + 1
   * @param argv    Tokens to set the settings. argv[0] is the binary name.
   * @param source  Source for these updates.
   * @param store   If false, only validate the settings but don't actually
   *                change anything. This is needed as we want to atomically
   *                validate all the settings coming from the config before
   *                applying any of them. If true, validate and if all settings
   *                are valid, apply the changes.
   * @param checker Function that throws when setting should not be accepted.
   * @param remove  If not null, unset these settings for this source. These
   *                settings must be valid (exist in settings_ and be accepted
   *                by checker function), and must not intersect with settings
   *                in argv.
   * @param unrecognized  If null, throw an exception if there are unrecognized
   *                      options in argv. If not null, put the unrecognized
   *                      stuff into `unrecognized` and don't throw an error.
   * @param not_accepted  If null, throw an exception if there are options not
   *                      accepted by checker function. Otherwise, put those
   *                      options into `not_accepted` and don't throw.
   */
  void parse(int argc,
             const char** argv,
             Source source,
             bool store,
             setting_checker_t checker = nullptr,
             const std::vector<std::string>* remove = nullptr,
             std::vector<std::string>* unrecognized = nullptr,
             std::map<std::string, std::string>* not_accepted = nullptr);

  // Wrapper around parse that accepts iterators to std::pair<std::string,
  // std::string> defining pairs of setting keys and values insteads of
  // argc/argv.
  template <typename T>
  void parse(T begin,
             T end,
             Source source,
             bool store,
             setting_checker_t checker,
             const std::vector<std::string>* remove = nullptr,
             std::vector<std::string>* unrecognized = nullptr,
             std::map<std::string, std::string>* not_accepted = nullptr) {
    std::deque<std::string> storage;
    std::vector<const char*> argv;
    argv.push_back("SettingsUpdater");
    for (auto it = begin; it != end; ++it) {
      std::string name = it->first;
      if (!boost::starts_with(name, "--")) {
        name = std::string("--") + name;
      }
      storage.push_back(std::move(name));
      // Saving a pointer into the string's data. Depends on std::deque never
      // invalidating references on insert.
      argv.push_back(storage.back().c_str());
      argv.push_back(it->second.c_str());
    }
    parse(argv.size(),
          argv.data(),
          source,
          store,
          checker,
          remove,
          unrecognized,
          not_accepted);
  }

  /**
   * Must be called after one or more calls to parse() that changed the settings
   * from different sources. Reconciles the current value for each setting
   * considering the order of precedence between sources, then update each
   * Bundle to make the changes available to readers of settings.
   *
   * @param checker Checker function that checks whether a given option could
   *                have changed.
   */
  void update(setting_checker_t checker = nullptr);
};

}} // namespace facebook::logdevice
