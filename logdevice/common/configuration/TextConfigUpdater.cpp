/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/configuration/TextConfigUpdater.h"

#include <boost/filesystem.hpp>
#include <folly/compression/Compression.h>
#include <folly/hash/SpookyHashV2.h>

#include "logdevice/common/ConfigSourceLocationParser.h"
#include "logdevice/common/configuration/ParsingHelpers.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/settings/SettingsUpdater.h"
#include "logdevice/include/Err.h"

using namespace facebook::logdevice::configuration;
namespace facebook { namespace logdevice {

// This assumes all sources use '/' for path delimiters which won't
// necessarily be universally true.  Refactor as needed.
static const char PATH_DELIMITER = '/';
// Calculates a 10-character hash of the config (used when the source doesn't
// provide one), exposed by the "info config --hash" admin command
static std::string hash_contents(const std::string& str);
// Compares hashes in ConfigMetadata, return true if they are equal.
static bool hashes_equal(const ServerConfig::ConfigMetadata& a,
                         const ServerConfig::ConfigMetadata& b);

int TextConfigUpdaterImpl::load(
    const std::string& location,
    std::unique_ptr<LogsConfig> alternative_logs_config,
    const ConfigParserOptions& options) {
  std::shared_ptr<UpdateableServerConfig> server_config =
      target_server_config_.lock();
  ld_check(server_config != nullptr);
  alternative_logs_config_ = std::move(alternative_logs_config);
  config_parser_options_ = options;

  std::tie(main_config_state_.source, main_config_state_.path) =
      ConfigSourceLocationParser::parse(sources_, location);
  if (main_config_state_.source == nullptr) {
    err = E::INVALID_PARAM;
    return -1;
  }

  return fetchFromSource();
}

int TextConfigUpdaterImpl::fetchFromSource() {
  // Request main config contents from the source.
  ld_info("Requesting main config \"%s\" (source: %s)",
          main_config_state_.path.c_str(),
          main_config_state_.source->getName().c_str());

  ConfigSource::Output output;
  Status st =
      main_config_state_.source->getConfig(main_config_state_.path, &output);
  if (st == E::OK) {
    onContents(&main_config_state_, std::move(output), true);
    return 0;
  } else if (st == E::NOTREADY) {
    // Not calling update() but returning success, expecting an async result
    // to construct the config.
    return 0;
  } else {
    return -1;
  }
}

bool TextConfigUpdaterImpl::hasValidConfig() {
  std::shared_ptr<UpdateableServerConfig> server_config =
      target_server_config_.lock();
  return server_config->get() != nullptr && !invalid_logs_config_;
}

void TextConfigUpdaterImpl::onAsyncGet(ConfigSource* source,
                                       const std::string& path,
                                       Status status,
                                       ConfigSource::Output output) {
  if (destroying_) {
    // This TextConfigUpdater is being destroyed.
    return;
  }

  if (status != E::OK) {
    initial_config_sem_.post();
    return;
  }

  State* affected_state;
  if (source == main_config_state_.source && path == main_config_state_.path) {
    affected_state = &main_config_state_;
  } else if (source == included_config_state_.source &&
             path == included_config_state_.path) {
    affected_state = &included_config_state_;
  } else {
    ld_info("Fetched config data from %s for path \"%s\" but not interested; "
            "may be a stale request but should be unlikely",
            source->getName().c_str(),
            path.c_str());
    return;
  }

  onContents(affected_state, std::move(output), true);
}

void TextConfigUpdaterImpl::onContents(State* state,
                                       ConfigSource::Output output,
                                       bool call_update) {
  output.contents = maybeDecompress(state->path, std::move(output.contents));

  // NOTE: If decompression failed, `output.contents' is now empty and will
  // soon fail to parse.  This could be handled more gracefully with extra
  // code but probably not worth it.

  if (output.hash.empty()) {
    output.hash = hash_contents(output.contents);
  }

  ld_info("%s config from %s, hash = %s",
          state == &main_config_state_ ? "Main" : "Included log",
          state->source->getName().c_str(),
          output.hash.c_str());

  state->output = std::move(output);
  using namespace std::chrono;
  state->last_loaded_time =
      duration_cast<milliseconds>(system_clock::now().time_since_epoch());
  if (call_update) {
    update();
  }
}

void TextConfigUpdaterImpl::update(bool force_reload_logsconfig) {
  ld_check(main_config_state_.output.hasValue());

  // In most cases (success or error) we'll wake the main thread at the end of
  // the function if it's waiting for the first config.
  auto sem_post_guard =
      folly::makeGuard([this]() { initial_config_sem_.post(); });

  // Wipe `included_config_state_' in case the main config no longer refers to
  // it.  (If it does, the callback will repopulate it.)
  State previous_included_state;
  std::swap(previous_included_state, included_config_state_);

  bool waiting_for_included = false;
  auto parse_include_callback = [&](const char* path,
                                    std::string* contents_out) {
    // This is the callback that the ServerConfig parser invokes when it
    // detects that the main config refers to another location for the logs
    // config.

    std::tie(included_config_state_.source, included_config_state_.path) =
        parseMaybeRelativeLocation(
            path, main_config_state_.source, main_config_state_.path);

    if (!included_config_state_.source) {
      return E::FAILED;
    }

    // If we've already fetched the correct included log config (for example
    // with an async request), return that.
    if (previous_included_state.source == included_config_state_.source &&
        previous_included_state.path == included_config_state_.path &&
        previous_included_state.output.hasValue()) {
      std::swap(included_config_state_, previous_included_state);
      *contents_out = included_config_state_.output->contents;
      return E::OK;
    }

    // Otherwise, request the config from the source.
    ld_info("Requesting included log config \"%s\" (source: %s)",
            included_config_state_.path.c_str(),
            included_config_state_.source->getName().c_str());

    ConfigSource::Output output;
    Status rv = included_config_state_.source->getConfig(
        included_config_state_.path, &output);
    if (rv == E::OK) {
      onContents(&included_config_state_, std::move(output), false);
      // Copy the contents to return to Configuration parser.  Note that this
      // may not be the same as `output.contents' above; onContents() tries to
      // decompress when applicable.
      *contents_out = included_config_state_.output->contents;
    }
    waiting_for_included = rv == E::NOTREADY;
    return rv;
  };

  std::shared_ptr<Configuration> config = Configuration::fromJson(
      main_config_state_.output->contents,
      alternative_logs_config_ ? alternative_logs_config_->copy() : nullptr,
      parse_include_callback,
      config_parser_options_);
  Status config_parse_status = err;

  if (!config || !config->serverConfig()) {
    setRecentConfigValidity(false);
    ld_error("Config update aborted");
    return;
  }

  if (!config->logsConfig() && waiting_for_included) {
    // The main config refers to another location for the log config but the
    // contents have not been fetched yet.  Silently ignore.  There is a
    // request for the logs config inflight with some source; when that
    // comes back, we'll be able to form the full `ServerConfig' object.

    // Don't wake the main thread until then, though.
    sem_post_guard.dismiss();
    return;
  }

  ServerConfig::ConfigMetadata main_config_metadata, included_config_metadata;
  if (included_config_state_.source) {
    included_config_metadata.uri = included_config_state_.source->getName() +
        ':' + included_config_state_.path;
    ConfigSource::Output& included_output =
        included_config_state_.output.value();
    if (included_output.hash.empty()) {
      included_output.hash = hash_contents(included_output.contents);
    }
    included_config_metadata.hash = included_output.hash;
    included_config_metadata.modified_time = included_output.mtime;
    included_config_metadata.loaded_time =
        included_config_state_.last_loaded_time;
  }
  main_config_metadata.uri =
      main_config_state_.source->getName() + ':' + main_config_state_.path;
  main_config_metadata.hash = main_config_state_.output->hash;
  main_config_metadata.modified_time = main_config_state_.output->mtime;
  main_config_metadata.loaded_time = main_config_state_.last_loaded_time;

  config->serverConfig()->setMainConfigMetadata(main_config_metadata);
  config->serverConfig()->setIncludedConfigMetadata(included_config_metadata);

  ConfigUpdateResult server_config_update, zookeeper_config_update,
      logs_config_update;
  server_config_update = pushServerConfig(config->serverConfig());
  zookeeper_config_update = pushZookeeperConfig(config->zookeeperConfig());

  // LogsConfig is a special snowflake. We need to update when:
  // - ServerConfig is changed
  // - Included config (in ServerConfig) has changed (based on hash)
  //   and requests a force update
  // - Force reload flag for LogsConfig is set
  if (server_config_update == ConfigUpdateResult::UPDATED ||
      server_config_update == ConfigUpdateResult::FORCE_RELOAD_LOGSCONFIG ||
      force_reload_logsconfig) {
    logs_config_update = pushLogsConfig(config->logsConfig());
  } else {
    logs_config_update = ConfigUpdateResult::SKIPPED;
  }

  ld_info("Config update result: ServerConfig=%s, ZK=%s, LogsConfig=%s",
          updateResultToString(server_config_update).c_str(),
          updateResultToString(zookeeper_config_update).c_str(),
          updateResultToString(logs_config_update).c_str());

  if (logs_config_update == ConfigUpdateResult::INVALID) {
    ld_error("LogsConfig update (from file) was aborted because: %s",
             error_description(config_parse_status));
    err = E::INVALID_CONFIG;
  }
  invalid_logs_config_ = logs_config_update == ConfigUpdateResult::INVALID;
  setRecentConfigValidity(server_config_update != ConfigUpdateResult::INVALID &&
                          zookeeper_config_update !=
                              ConfigUpdateResult::INVALID &&
                          logs_config_update != ConfigUpdateResult::INVALID);
}

std::pair<ConfigSource*, std::string>
TextConfigUpdaterImpl::parseMaybeRelativeLocation(const std::string& location,
                                                  ConfigSource* ref_source,
                                                  const std::string& ref_path) {
  if (location.find(
          ConfigSourceLocationParser::kLocationSchemeDelimiter.toString()) !=
      std::string::npos) {
    // Scheme explicitly specified in location, parse as absolute
    return ConfigSourceLocationParser::parse(sources_, location);
  }

  std::string full_path;
  if (!location.empty() && location[0] == PATH_DELIMITER) {
    // Absolute path.
    full_path = location;
  } else {
    // Relative path.  Extract path prefix from `ref_path' and attach
    // `location' to it.
    boost::filesystem::path path_prefix(ref_path);
    path_prefix.remove_filename();
    full_path = (path_prefix / location).string();
  }
  return std::make_pair(ref_source, std::move(full_path));
}

std::string TextConfigUpdaterImpl::maybeDecompress(const std::string& path,
                                                   std::string raw_contents) {
  using folly::IOBuf;
  boost::filesystem::path ext = boost::filesystem::path(path).extension();
  if (ext == ".gz") {
    std::unique_ptr<IOBuf> input =
        IOBuf::wrapBuffer(raw_contents.data(), raw_contents.size());
    auto codec = folly::io::getCodec(folly::io::CodecType::GZIP);
    std::unique_ptr<IOBuf> uncompressed;
    try {
      uncompressed = codec->uncompress(input.get());
    } catch (const std::runtime_error& ex) {
      ld_error(
          "gzip decompression of \"%s\" failed: %s", path.c_str(), ex.what());
      return "";
    }
    return std::string(uncompressed->moveToFbString().toStdString());
  } else {
    return raw_contents;
  }
}

static std::string hash_contents(const std::string& str) {
  const uint64_t SEED = 0xbfc655bfa4fd6c49L; // random
  uint64_t h = folly::hash::SpookyHashV2::Hash64(str.data(), str.size(), SEED);
  std::string result;
  for (int i = 0; i < 10; ++i) {
    int d = h % 36;
    h /= 36;
    result += d < 10 ? ('0' + d) : ('a' + d - 10);
  }
  std::reverse(result.begin(), result.end());
  return result;
}

void TextConfigUpdaterImpl::setRecentConfigValidity(bool state) {
  if (isRecentConfigValid_ == state) { // config state did not change
    return;
  }

  isRecentConfigValid_ = state;
  if (isRecentConfigValid_) {
    // Can't use STAT_SET() because we can be called from different threads.
    STAT_DECR(stats_, last_config_invalid);
  } else {
    STAT_INCR(stats_, last_config_invalid);
  }
}

std::string TextConfigUpdaterImpl::updateResultToString(
    TextConfigUpdaterImpl::ConfigUpdateResult result) {
  switch (result) {
    case ConfigUpdateResult::SKIPPED:
      return "skipped";
    case ConfigUpdateResult::INVALID:
      return "invalid";
    case ConfigUpdateResult::UPDATED:
      return "updated";
    case ConfigUpdateResult::FORCE_RELOAD_LOGSCONFIG:
      return "force_reload_logsconfig";
    default:
      ld_assert(false);
      return "";
  }
}

int TextConfigUpdaterImpl::compareServerConfig(
    const std::shared_ptr<ServerConfig>& old_config,
    const std::shared_ptr<ServerConfig>& new_config) {
  if (old_config == nullptr) {
    ld_debug("ServerConfig is not set");
    return 1;
  }

  config_version_t new_version = new_config->getVersion();
  config_version_t old_version = old_config->getVersion();
  RATELIMIT_INFO(std::chrono::seconds(1),
                 1,
                 "Comparing new config (version %u) with existing config "
                 "(version %u)",
                 new_version.val(),
                 old_version.val());
  if (new_version < old_version) {
    ld_error("Not updating config with an older version (%u < %u)",
             new_version.val(),
             old_version.val());
    setRecentConfigValidity(false);
    STAT_INCR(stats_, config_update_old_version);
    return -1;
  } else if (new_version == old_version) {
    auto old_metadata = old_config->getMainConfigMetadata();
    auto new_metadata = new_config->getMainConfigMetadata();

    if (hashes_equal(old_metadata, new_metadata)) {
      ld_info("Received same config as already running "
              "(version: %u - hash: %s).",
              new_version.val(),
              new_metadata.hash.c_str());
      STAT_INCR(stats_, config_update_same_version);
      return 0;
    } else {
      // the hashes don't match. log a warning and proceed with the
      // update assuming it is newer.
      ld_warning("Received config with same version (%u) but mismatched "
                 "hash (%s != %s)",
                 new_version.val(),
                 old_metadata.hash.c_str(),
                 new_metadata.hash.c_str());
      STAT_INCR(stats_, config_update_hash_mismatch);
    }
  }

  return 1;
}

TextConfigUpdaterImpl::ConfigUpdateResult
TextConfigUpdaterImpl::pushServerConfig(
    const std::shared_ptr<ServerConfig>& new_config) {
  std::shared_ptr<UpdateableServerConfig> server_config =
      target_server_config_.lock();
  if (!server_config) {
    ld_debug("Attempting to update ServerConfig after it has been destroyed");
    return ConfigUpdateResult::SKIPPED;
  }

  if (compareServerConfig(server_config->get(), new_config) <= 0) {
    // ServerConfig can have extra includes (logsconfig) which might need
    // a force update. Eventually, we'll pull LogsConfig out of the ServerConfig
    // metadata entirely. When includes have changed, we need to return a
    // force update
    auto old_metadata = server_config->get()->getIncludedConfigMetadata();
    auto new_metadata = new_config->getIncludedConfigMetadata();
    if (hashes_equal(old_metadata, new_metadata)) {
      return ConfigUpdateResult::SKIPPED;
    } else {
      return ConfigUpdateResult::FORCE_RELOAD_LOGSCONFIG;
    }
  } else {
    // Need to increase the stat _before_ publishing config because some
    // integration tests rely on it.
    STAT_INCR(stats_, updated_config);

    int rv = server_config->update(new_config);

    if (rv != 0) {
      STAT_INCR(stats_, config_update_invalid);
      ld_error("Server config update was rejected");
      return ConfigUpdateResult::INVALID;
    } else {
      ld_info("Updated config (version: %u - hash: %s)",
              new_config->getVersion().val(),
              new_config->getMainConfigMetadata().hash.c_str());
      return ConfigUpdateResult::UPDATED;
    }
  }
}

bool TextConfigUpdaterImpl::compareZookeeperConfig(
    const ZookeeperConfig* old_config,
    const ZookeeperConfig* new_config) {
  if (old_config == nullptr && new_config == nullptr) {
    return true;
  } else if ((old_config == nullptr && new_config != nullptr) ||
             (old_config != nullptr && new_config == nullptr)) {
    return false;
  } else {
    // Zookeeper config has no versioning for now, so we cannot compare newer vs
    // older
    return *old_config == *new_config;
  }
}

TextConfigUpdaterImpl::ConfigUpdateResult
TextConfigUpdaterImpl::pushZookeeperConfig(
    const std::shared_ptr<ZookeeperConfig>& new_config) {
  std::shared_ptr<UpdateableZookeeperConfig> updateable_zookeeper_config =
      target_zk_config_.lock();
  if (!updateable_zookeeper_config) {
    ld_debug("Attempting ZK config update, but config doesn't exist anymore");
    return ConfigUpdateResult::SKIPPED;
  }

  if (!compareZookeeperConfig(
          updateable_zookeeper_config->get().get(), new_config.get())) {
    int rv = updateable_zookeeper_config->update(new_config);
    if (rv != 0) {
      ld_error("Zookeeper config update was rejected");
      return ConfigUpdateResult::INVALID;
    } else {
      return ConfigUpdateResult::UPDATED;
    }
  } else {
    return ConfigUpdateResult::SKIPPED;
  }
}

TextConfigUpdaterImpl::ConfigUpdateResult TextConfigUpdaterImpl::pushLogsConfig(
    const std::shared_ptr<LogsConfig>& new_config) {
  std::shared_ptr<UpdateableLogsConfig> updateable_logsconfig =
      target_logs_config_.lock();
  if (!updateable_logsconfig) {
    ld_debug("Attempting log config update, but config doesn't exist anymore");
    return TextConfigUpdaterImpl::ConfigUpdateResult::SKIPPED;
  }

  if (!updateable_settings_->server &&
      (updateable_settings_->on_demand_logs_config ||
       updateable_settings_->force_on_demand_logs_config)) {
    ld_info("Ignoring the 'logs' section in the config file because "
            "'on-demand-logs-config' is ENABLED (Client)");
    // config->logsConfig() contains the alternative_logs
    if (updateable_logsconfig->update(new_config) != 0) {
      return ConfigUpdateResult::INVALID;
    } else {
      return ConfigUpdateResult::UPDATED;
    }
  } else if (!updateable_settings_->enable_logsconfig_manager) {
    // if we didn't get logsconfig as a return of the parsing
    if (!new_config) {
      return ConfigUpdateResult::INVALID;
    } else {
      ld_info("Updating LogsConfig from file: version %lu",
              new_config->getVersion());
      if (updateable_logsconfig->update(new_config) != 0) {
        ld_error("LogsConfig config update got rejected");
        return ConfigUpdateResult::INVALID;
      } else {
        return ConfigUpdateResult::UPDATED;
      }
    }
  } else {
    ld_info("Ignoring changes in the 'logs' section in the config file because "
            "LogsConfigManager is ENABLED");
    // We mark this as a valid update because we don't care about the logs
    // config in that context.
    return ConfigUpdateResult::UPDATED;
  }
}

int TextConfigUpdater::waitForInitialLoad(std::chrono::milliseconds timeout) {
  int rv = initial_config_sem_.timedwait(timeout);
  if (rv != 0) {
    ld_error("Timed out waiting for config after %ld ms", timeout.count());
    err = E::TIMEDOUT;
    return -1;
  }
  if (!impl_->hasValidConfig()) {
    err = E::INVALID_CONFIG;
    return -1;
  }
  return 0;
}

static bool hashes_equal(const ServerConfig::ConfigMetadata& a,
                         const ServerConfig::ConfigMetadata& b) {
  if (a.hash.empty() || b.hash.empty()) {
    return a.hash == b.hash;
  }
  // use std::mismatch here to check hash values as the hash may
  // be truncated (when sent via CONFIG_UPDATED_Message).
  // but we will consider them equal if one is prefix of the other
  auto result =
      std::mismatch(a.hash.begin(), a.hash.end(), b.hash.begin(), b.hash.end());
  return (result.first == a.hash.end() || result.second == b.hash.end());
};

}} // namespace facebook::logdevice
