/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>
#include <memory>
#include <string>
#include <unordered_map>

#include "logdevice/include/Client.h"
#include "logdevice/include/ClientSettings.h"

namespace facebook { namespace logdevice {

class Client;
/**
 * This is the only supported way to create new Client instances. Can be used
 * like this:
 * ```
 * auto client = ClientFactory()
 *     .setSetting("on-demand-logs-config", "true")
 *     .create("path-to-config");
 * ```
 */

class ClientFactory {
 public:
  /**
   * The method that actually creates a Client instance.
   *
   * The shared_ptr is shared between the caller of create() and all instances
   * of Reader and AsyncReader for this client. The Client is shut down when
   * all Reader and AsyncReader objects, as well as all copies of the shared_ptr
   * returned from create() are destroyed.
   *
   * @param config_url     a URL that identifies at a LogDevice configuration
   *                       resource (such as a file) describing the LogDevice
   *                       cluster this client will talk to. The only supported
   *                       formats are currently
   *                       file:<path-to-configuration-file> and
   *                       configerator:<configerator-path>. Examples:
   *                         "file:logdevice.test.conf"
   *                         "configerator:logdevice/logdevice.test.conf"
   *
   * @return on success, a fully constructed LogDevice client object for the
   *         specified LogDevice cluster. On failure nullptr is returned
   *         and logdevice::err is set to
   *           INVALID_PARAM    invalid config URL, cluster name or credentials
   *                            is too log.
   *           TIMEDOUT         timed out while trying to get config
   *           FILE_OPEN        config file could not be opened
   *           FILE_READ        error reading config file
   *           INVALID_CONFIG   various errors in parsing the config
   *           SYSLIMIT         monitoring thread for the config could
   *                            not be started
   */
  std::shared_ptr<Client> create(std::string config_url) noexcept;

  /**
   * Sets the cluster name, only used for debugging purposes
   */
  ClientFactory& setClusterName(std::string v) {
    cluster_name_ = std::move(v);
    return *this;
  }

  /**
   * Specifies the credentials. This may include credentials to present to the
   * LogDevice cluster along with authentication and encryption specifiers.
   */
  ClientFactory& setCredentials(std::string v) {
    credentials_ = std::move(v);
    return *this;
  }

  /**
   * Sets the client construction timeout. This value also serves as the
   * default timeout for methods (appends, etc.) on the created object
   */
  ClientFactory& setTimeout(std::chrono::milliseconds timeout) {
    timeout_ = timeout;
    return *this;
  }

  /**
   * Sets a client setting. See ClientSettings.h for useful examples or
   * Settings.h for an exhaustive listing.
   * Shouldn't be used together with setClientSettings()
   */
  ClientFactory& setSetting(std::string setting_name, std::string value) {
    string_settings_[std::move(setting_name)] = std::move(value);
    return *this;
  }

  /**
   * Shortcut for int values
   */
  ClientFactory& setSetting(std::string setting_name, int64_t value) {
    string_settings_[std::move(setting_name)] = std::to_string(value);
    return *this;
  }

  /**
   * Sets the client session ID. Used for logging to uniquely identify session.
   * If csid is empty, a random one is generated.
   */
  ClientFactory& setCSID(std::string v) {
    csid_ = std::move(v);
    return *this;
  }

  // This interface can be used by command line utilities that have exported the
  // entire ClientSettings bundle as command line arguments.
  // Shouldn't be used with setSetting()
  //
  // Note that a ClientFactory instance for which this method was called will
  // move the supplied `ClientSettings` instance into the newly created client,
  // and so won't pass it on to subsequently created clients. If you want
  // to use this method and re-use the factory to create multiple clients, you
  // have to call it again before each subsequent call to create().
  ClientFactory& setClientSettings(std::unique_ptr<ClientSettings> v);

 private:
  std::string cluster_name_;
  std::chrono::milliseconds timeout_{60000};
  std::string credentials_;
  std::unique_ptr<ClientSettings> client_settings_;
  std::unordered_map<std::string, std::string> string_settings_;
  std::string csid_;
};

}} // namespace facebook::logdevice
