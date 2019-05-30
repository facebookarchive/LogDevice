/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include <folly/experimental/TestUtil.h>

#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/protocol/MessageType.h"
#include "logdevice/common/settings/UpdateableSettings.h"
#include "logdevice/common/settings/util.h"
#include "logdevice/include/Err.h"
#include "logdevice/include/Record.h"
#include "logdevice/include/debug.h"

namespace facebook { namespace logdevice {

class PluginRegistry;

// Timeout shared by many tests, especially integration tests

#if defined(FOLLY_SANITIZE_ADDRESS) || defined(FOLLY_SANITIZE_THREAD)
// A more generous timeout when running under ASAN/TSAN, particularly for
// Sandcastle
constexpr std::chrono::seconds DEFAULT_TEST_TIMEOUT(240);
#else
constexpr std::chrono::seconds DEFAULT_TEST_TIMEOUT(90);
#endif

// Returns true if LOGDEVICE_TEST_LEAVE_DATA environment variable is set.
// This instructs the tests to not delete temporary directories after the test
// is done.
bool testsShouldLeaveData();

/**
 * Atomically overwrites a file.  (Writes to a temporary file then renames to
 * target file.)
 */
int overwriteConfigFile(const char* path, const std::string& contents);

/**
 * Write config file(s) for the ServerConfig and LogsConfig. If
 * write_logs_config_separately is true, then writes the logs config into a
 * separate file that's in the same directory as the main config and includes
 * it from the main config. Otherwise dumps everything to the file specified.
 */
int overwriteConfig(const char* path,
                    const ServerConfig*,
                    const LogsConfig*,
                    bool write_logs_config_separately);

/**
 * Create a temporary directory to be used by tests.
 *
 * @param name_prefix  prefix for the directory name
 * @param keep_data    if true, don't remove the directory after the returned
 *                     object is destroyed
 *
 *
 * @return  A TemporaryDirectory object representing the newly created
 *          directory.
 *
 */
std::unique_ptr<folly::test::TemporaryDirectory>
createTemporaryDir(const std::string& name_prefix,
                   bool keep_data = testsShouldLeaveData());

inline void writeSimpleConfig(const char* path, int server_generation) {
  std::string contents = "{\n"
                         "  \"cluster\": \"SimpleConfig\",\n"
                         "  \"nodes\": [ {\n"
                         "    \"node_id\": 0,\n"
                         "    \"host\": \"127.0.0.1:4444\",\n"
                         "    \"gossip_address\": 4445,\n"
                         "    \"roles\": [\n"
                         "      \"sequencer\",\n"
                         "      \"storage\"\n"
                         "    ],\n"
                         "    \"sequencer\": true,\n"
                         "    \"weight\": 1,\n"
                         "    \"num_shards\": 2,\n"
                         "    \"generation\": " +
      std::to_string(server_generation) +
      "\n"
      "  } ],\n"
      "  \"logs\": [],\n"
      "  \"metadata_logs\": {\n"
      "     \"nodeset\": [0],\n"
      "     \"replication_factor\": 1\n"
      "  },\n"
      "  \"server_settings\": {\n"
      "     \"enable-logsconfig-manager\": \"false\"\n"
      "  },\n"
      "  \"client_settings\": {\n"
      "     \"enable-logsconfig-manager\": \"false\"\n"
      "  }\n"
      "}\n";
  overwriteConfigFile(path, contents);
}

ServerConfig::NodesConfig createSimpleNodesConfig(size_t nnodes);

/**
 * Create a MetaDataLogsConfig object from an existing NodesConfig. Nodes
 * are selected from the beginning of nodes in @nodes with non-zero weight
 * until either max_metadata_nodes is reached or all nodes are picked.
 *
 * @param nodes                 NodesConfigObject given
 * @param max_metadata_nodes    maximum nodes to store metadata logs,
 *                              the actual number of nodes is also capped by
 *                              the number of non-zero weight nodes in
 *                              NodesConfig.
 * @param max_replication       maximum replication factor for metadata logs,
 *                              the actual replication factor is also capped by
 *                              the actual metadata nodeset size
 * @return                      Configuration::MetaDataLogsConfig object
 */
ServerConfig::MetaDataLogsConfig createMetaDataLogsConfig(
    const ServerConfig::NodesConfig& nodes,
    size_t max_metadata_nodes,
    size_t max_replication,
    NodeLocationScope sync_replication_scope = NodeLocationScope::NODE);

/**
 * Create a MetaDataLogsConfig object from an existing list of node
 * indices.
 *
 * @param positive_weight_nodes List of node IDs with positive weights
 * @param max_replication       maximum replication factor for metadata logs,
 *                              the actual replication factor is also capped by
 *                              the actual metadata nodeset size
 * @return                      Configuration::MetaDataLogsConfig object
 */
ServerConfig::MetaDataLogsConfig createMetaDataLogsConfig(
    std::vector<node_index_t> positive_weight_nodes,
    size_t max_replication,
    NodeLocationScope sync_replication_scope = NodeLocationScope::NODE);

/**
 * Creates a simple config with the specified number of logs and nodes (all of
 * them sequencers).
 *
 *   @param nodes   number of nodes in the cluster
 *   @param logs    how many logs to use (1..logs)
 */
std::shared_ptr<Configuration> createSimpleConfig(size_t nodes, size_t logs);

/**
 * Creates a simple config with the given node configuration and the target
 * number of logs.
 *
 *  @param nodes    node configuration
 *  @param logs     how many logs to use (1..logs)
 */
std::shared_ptr<Configuration>
createSimpleConfig(ServerConfig::NodesConfig nodes, size_t logs);

/**
 * Waits until a condition is satisfied.  The condition is periodically
 * evaluated with sleeping inbetween.
 * The condition is always checked at least once, even if deadline is in the
 * past.
 *
 * @return 0 if the condition was satisfied, -1 if the call timed out.
 */
int wait_until(const char* reason,
               std::function<bool()> cond,
               std::chrono::steady_clock::time_point deadline =
                   std::chrono::steady_clock::time_point::max());

inline int wait_until(std::function<bool()> cond,
                      std::chrono::steady_clock::time_point deadline =
                          std::chrono::steady_clock::time_point::max()) {
  return wait_until(nullptr, cond, deadline);
}

class Reader;
/**
 * Blocking read of `nrecords' from a Reader, swallowing gaps.  Prints
 * progress for easier debugging when reading gets stuck.
 *
 * Returns the number of gaps swallowed, which the caller can promptly ignore.
 */
int read_records_swallow_gaps(
    Reader&,
    size_t nrecords,
    std::vector<std::unique_ptr<DataRecord>>* data_out = nullptr);

// Similar to above but asserts that there were no gaps
inline void read_records_no_gaps(
    Reader& reader,
    size_t nrecords,
    std::vector<std::unique_ptr<DataRecord>>* data_out = nullptr) {
  int ngaps = read_records_swallow_gaps(reader, nrecords, data_out);
  ld_check(ngaps == 0);
}

void PrintTo(MessageType type, ::std::ostream* os);
void PrintTo(Status st, ::std::ostream* os);

// Checks if an environment variable is truthy.
// If specified and true is returned, sets the value of the environment
// into value.
bool getenv_switch(const char* name, std::string* value = nullptr);

// If LOGDEVICE_LOG_LEVEL environment variable is set to a name of a log level,
// return this log level.
folly::Optional<dbg::Level> getLogLevelFromEnv();

// Returns a very long duration if LOGDEVICE_TEST_NO_TIMEOUT environment
// variable is set, otherwise DEFAULT_TEST_TIMEOUT.
std::chrono::milliseconds getDefaultTestTimeout();

/**
 * RAII-style alarm for use in tests.  Kills the process after the specified
 * timeout (unless the instance is destroyed sooner, which defuses the timer).
 *
 * Does not use alarm() but starts a thread.
 */
class Alarm {
 public:
  explicit Alarm(std::chrono::milliseconds timeout)
      : thread_(std::bind(&Alarm::run, this, timeout)) {}

  ~Alarm() {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      cancelled_ = true;
    }
    cv_.notify_one();
    thread_.join();
  }

  void run(std::chrono::milliseconds timeout) {
    if (getenv_switch("LOGDEVICE_TEST_NO_TIMEOUT")) {
      return;
    }

    bool cancelled;
    {
      std::unique_lock<std::mutex> lock(mutex_);
      cancelled = cv_.wait_for(lock, timeout, [&]() { return cancelled_; });
    }
    if (!cancelled) {
      using namespace std::chrono;
      fprintf(stderr,
              "*** TIMEOUT *** test runtime exceeded %g s limit\n",
              duration_cast<duration<double>>(timeout).count());
      std::exit(1);
    }
  }

 private:
  bool cancelled_{false};
  std::mutex mutex_;
  std::condition_variable cv_;
  std::thread thread_;
};

class Processor;
struct Settings;
class ShardedStorageThreadPool;
class StatsHolder;
std::shared_ptr<Processor>
make_test_processor(const Settings& settings,
                    std::shared_ptr<UpdateableConfig> config = nullptr,
                    StatsHolder* stats = nullptr,
                    folly::Optional<NodeID> my_node_id = folly::none);

void gracefully_shutdown_processor(Processor* processor);

std::shared_ptr<PluginRegistry> make_test_plugin_registry();

// verifies that file exists (using findFile()) and returns the filename if it
// does. Throws an instance of std::runtime_error otherwise
std::string verifyFileExists(const std::string& filename);

// Attempts to find a file, give a relative path to search for. The search
// starts in the directory of the currently running program, then walks up the
// filesystem.  To goal is to be able to find e.g. "logdeviced" from a running
// test which is in a subdirectory, regardless of the current working directory.
std::string findFile(const std::string& relative_path,
                     bool require_executable = false);

#define TEST_CONFIG_PATH "logdevice/common/test/configs"
#define TEST_SSL_CERT_PATH "logdevice/common/test/ssl_certs"
#define TEST_SSL_FILE(x) verifyFileExists(TEST_SSL_CERT_PATH "/" x).c_str()
#define TEST_CONFIG_FILE(x) verifyFileExists(TEST_CONFIG_PATH "/" x).c_str()

// Returns the first loopback interface address found, typically "::1" or
// "127.0.0.1"
std::string get_localhost_address_str();

}} // namespace facebook::logdevice
