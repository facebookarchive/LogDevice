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

#include "logdevice/common/FileConfigSource.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/configuration/ZookeeperConfigSource.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/settings/UpdateableSettings.h"

namespace facebook { namespace logdevice {

class MyNodeID;
class PluginRegistry;
class StatsHolder;

/**
 * @file Helper class that parses a string like "file:logdevice.test.conf" and
 * creates an UpdateableConfig and an appropriate ConfigUpdater.
 */

class ConfigInit {
  /**
   * @param timeout  Timeout when requesting configs from external services.  A
   *                 create() call may fail if a service fails to provide the
   *                 config before the timeout.
   * @param stats    Object used to update various stat counters.
   *                 No stats are going to be updated if set to nullprt
   */
 public:
  explicit ConfigInit(std::chrono::milliseconds timeout,
                      StatsHolder* stats = nullptr)
      : timeout_(timeout), stats_(stats) {
    ld_check(timeout_.count() >= 0);
  }

  /**
   * Takes an empty UpdateableConfig (possibly with some hooks added) and
   * attaches an updater to it. UpdateableConfig will hold the ownership of the
   * updater. The source of the config is parsed from the given string.
   *
   * Examples of acceptable source strings:
   *   "configerator:logdevice/logdevice.test.conf"
   *   "file:logdevice.test.conf" or just "logdevice.test.conf"
   *
   * @param source                    string specifying the config source
   * @param updateable_config         UpdateableConfig instance
   * @param alternative_logs_config   an alternative log configuration fetcher,
   *                                  in case log data isn't included in the
   *                                  main config file. If null, log config
   *                                  will be read from the file specified in
   *                                  "include_log_config".
   * @param updateable_settings       Server/Client settings will read from this
   *                                  updateable to enable/disable reading the
   *                                  logsconfig from the attached config
   *                                  source.
   * @param options                   options for the config parser.
   *
   * @return 0 on success.  -1 on failure, err is set to:
   *           TIMEDOUT    timed out while trying to connect to config store
   *           FILE_OPEN   file could not be opened
   *           FILE_READ   error reading the file
   *      INVALID_CONFIG   various errors in parsing the config
   *       INVALID_PARAM   @param source has invalid format
   *           SYSLIMIT    config monitoring thread could not be started
   */
  int attach(const std::string& source,
             std::shared_ptr<PluginRegistry>,
             std::shared_ptr<UpdateableConfig> updateable_config,
             std::unique_ptr<LogsConfig> alternative_logs_config,
             UpdateableSettings<Settings> updateable_settings =
                 UpdateableSettings<Settings>(),
             const ConfigParserOptions& options = ConfigParserOptions());

 private:
  std::chrono::milliseconds timeout_;
  StatsHolder* stats_;
};

}} // namespace facebook::logdevice
