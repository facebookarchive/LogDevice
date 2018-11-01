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

#include <folly/dynamic.h>

#include "logdevice/common/plugin/Plugin.h"
#include "logdevice/common/plugin/PluginRegistry.h"
#include "logdevice/common/version.h"

/**
 * @file If the build system can bake build information into the binary, this
 * interface can be implemented (and hooked up via the plugin interface) to
 * provide it.  The server will log the build info at startup and expose it
 * through operational channels (admin commands, ldshell).
 */

namespace facebook { namespace logdevice {

class BuildInfo : public Plugin {
 public:
  static constexpr char const* BUILD_USER_KEY = "str_build_user";
  static constexpr char const* BUILD_PACKAGE_NAME_KEY =
      "str_build_package_name";
  static constexpr char const* BUILD_PACKAGE_VERSION =
      "str_build_package_version";
  static constexpr char const* BUILD_REVISION_KEY = "str_build_revision";
  static constexpr char const* BUILD_REVISION_TIME_KEY =
      "int_build_revision_commit_time";
  static constexpr char const* BUILD_TIME_KEY = "int_build_time";
  static constexpr char const* BUILD_UPSTREAM_REVISION_KEY =
      "str_build_upstream_revision";
  static constexpr char const* BUILD_UPSTREAM_REVISION_TIME_KEY =
      "int_build_upstream_revision_commit_time";

  PluginType type() const override {
    return PluginType::BUILD_INFO;
  }

  // Plugin identifier
  std::string identifier() const override {
    return PluginRegistry::kBuiltin().str();
  }

  // Plugin display name
  std::string displayName() const override {
    return "built-in";
  }

  /**
   * Short string that describes the version.
   */
  virtual std::string version() {
    // By default just returns the version string from version.h, however this
    // can also additionally include SCM revision, user name etc.
    //   char buf[512];
    //   snprintf(buf, sizeof buf,
    //            "%s revision %s (built on %s by %s)",
    //            LOGDEVICE_VERSION,
    //            MyBuildTool::getRevision(),
    //            MyBuildTool::getTime(),
    //            MyBuildTool::getUser());
    //   return buf;
    return LOGDEVICE_VERSION;
  }

  virtual std::string buildUser() {
    return "";
  }

  /**
   * If the build system is complimented by a packaging system, this can
   * provide the package name/information.
   *
   * Example: "logdevice_server_experimental:abcdef01"
   */
  virtual std::string packageNameWithVersion() {
    return "";
  }

  /**
   * If the build system is complimented by a packaging system, this can
   * provide the package name.
   */
  virtual std::string packageName() {
    return "";
  }

  /**
   * If the build system is complimented by a packaging system, this can
   * provide the package version.
   */
  virtual std::string packageVersion() {
    return "";
  }

  /**
   * The time at which the binary was built, specified in Unix
   * time epoch
   */
  virtual uint64_t buildTime() {
    return 0;
  }

  /**
   * The time at which the binary was built, in a human readable format
   *
   * Example: "Wed Jun 21 01:48:55 2017"
   */
  virtual std::string buildTimeStr() {
    return "";
  }

  /**
   * The local commit hash from which this binary was built from.
   */
  virtual std::string buildRevision() {
    return "";
  }

  /**
   * The upstream commit hash from which this binary was built from.
   */
  virtual std::string buildUpstreamRevision() {
    return "";
  }

  /**
   * Time of the last revision, specified in Unix
   * epoch
   */
  virtual uint64_t buildRevisionTime() {
    return 0;
  }

  /**
   *  Time of the last upstream revision, specified in Unix
   *  epoch
   */
  virtual uint64_t buildUpstreamRevisionTime() {
    return 0;
  }

  /**
   * Arbitrary set of key-value pairs that is exposed in the info admin
   * command and in ldshell as a JSON string.
   */
  virtual std::vector<std::pair<std::string, std::string>> fullMap() {
    return std::vector<std::pair<std::string, std::string>>();
  }

  /**
   * Generates a JSON blob containing build specific details,
   * such as time, revision, user, etc.
   *
   * Example: { "str_build_user": "root", "int_build_time": 1498048155 }
   */
  std::string getBuildInfoJson();

  virtual ~BuildInfo() {}

 protected:
  virtual void addBuildJsonFields(folly::dynamic& json);

  void addFieldIfNotEmpty(folly::dynamic& json,
                          const char* name,
                          const std::string& value);

  void addUIntIfNotZero(folly::dynamic& json,
                        const char* name,
                        const uint64_t value);
};

}} // namespace facebook::logdevice
