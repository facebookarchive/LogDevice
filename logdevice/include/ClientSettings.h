/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <string>
#include <utility>
#include <vector>

#include <folly/Optional.h>

/**
 * @file Allows applications to configure the LogDevice client, before and
 * after constructing the Client instance.
 */

namespace facebook { namespace logdevice {

class ClientSettingsImpl; // private implementation

class ClientSettings {
 public:
  /**
   * Creates an instance with default settings.
   */
  static ClientSettings* create();

  virtual ~ClientSettings() {}

  /**
   * Changes a setting.
   *
   * @param name   setting name, as would be provided on the server command line
   *               (see Settings::addOptions() in logdevice/common/Settings.cpp)
   * @param value  string representation of value, as would be provided on the
   *               server command line
   *
   * Settings that are commonly used on the client:
   *
   * connect-timeout
   *    Connection timeout
   *
   * handshake-timeout
   *    Timeout for LogDevice protocol handshake sequence
   *
   * num-workers
   *    Number of worker threads on the client
   *
   * client-read-buffer-size
   *    Number of records to buffer while reading
   *
   * max-payload-size
   *    The maximum payload size that could be appended by the client
   *
   * ssl-boundary
   *    Enable SSL in cross-X traffic, where X is the setting. Example: if set
   *    to "rack", all cross-rack traffic will be sent over SSL. Can be one of
   *    "none", "node", "rack", "row", "cluster", "dc" or "region". If a value
   *    other than "none" or "node" is specified, --my-location has to be
   *    specified as well.
   *
   * my-location
   *    Specifies the location of the machine running the client. Used for
   *    determining whether to use SSL based on --ssl-boundary. Format:
   *    "{region}.{dc}.{cluster}.{row}.{rack}"
   *
   * client-initial-redelivery-delay
   *    Initial delay to use when downstream rejects a record or gap
   *
   * client-max-redelivery-delay
   *    Maximum delay to use when downstream rejects a record or gap
   *
   * on-demand-logs-config
   *    Set this to true if you want the client to get log configuration on
   *    demand from the server when log configuration is not included in the
   *    main config file.
   *
   * enable-logsconfig-manager
   *    Set this to true if you want to use the internal replicated storage for
   *    logs configuration, this will ignore loading the logs section from the
   *    config file.
   *
   * @returns On success, returns 0. On failure, returns -1 and sets
   *          logdevice::err to:
   *
   *            UNKNOWN_SETTING        name parameter was not recognized
   *            INVALID_SETTING_VALUE  value was invalid (e.g. not numeric for a
   *                                   numeric setting)
   *            INVALID_PARAM          any other error
   */
  int set(const char* name, const char* value);

  // Overload for std::string
  int set(const std::string& name, const std::string& value) {
    return set(name.c_str(), value.c_str());
  }

  // Overload for settings with integral settings, for convenience
  int set(const char* name, int64_t value);

  /**
   * Overload to set any number of settings.  If any one encounters an error, no
   * subsequent ones are set.  A log message is printed describing the one that
   * failed.
   */
  int set(const std::vector<std::pair<std::string, std::string>>& settings);

  /**
   * @param name  setting name
   * @returns If the setting exists, returns the value in a string format,
              otherwise folly::none
   */
  folly::Optional<std::string> get(const std::string& name);

  /**
   * @param name  setting name
   * @returns If the setting was overridden by calling the set() method on this
   *          instance, returns true. False otherwise.
   */
  bool isOverridden(const std::string& name);

  /**
   * @return Returns a vector of <setting, value> pairs for all settings
   */
  std::vector<std::pair<std::string, std::string>> getAll();

 private:
  ClientSettings() {}

  friend class ClientSettingsImpl;
  ClientSettingsImpl* impl(); // downcasts (this)
};

}} // namespace facebook::logdevice
