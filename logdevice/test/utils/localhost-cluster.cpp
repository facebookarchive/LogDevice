/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <iostream>
#include <signal.h>
#include <unordered_map>

#include <boost/filesystem.hpp>
#include <boost/program_options.hpp>
#include <boost/version.hpp>
#include <folly/Format.h>
#include <folly/Singleton.h>

#include "logdevice/common/NodeSetSelectorFactory.h"
#include "logdevice/common/commandline_util.h"
#include "logdevice/common/commandline_util_chrono.h"
#include "logdevice/common/debug.h"
#include "logdevice/test/utils/IntegrationTestUtils.h"

using namespace facebook::logdevice;
namespace fs = boost::filesystem;
using IntegrationTestUtils::Cluster;
using IntegrationTestUtils::ParamScope;

namespace options {
std::string root;
int nnodes = 5;
shard_size_t nshards = 2;
int nlogs = 100;
int nracks = 1;
int replication = -1;
int nodeset_size = -1;
int extra_copies = -1;
int synced_copies = -1;
bool use_existing_metadata = false;
bool enable_logsconfig_manager = true;
bool enable_rebuilding = true;
std::chrono::seconds backlog(-1);
bool scd_enabled = false;
std::vector<std::string> params;
std::string range_name;
std::string config_path = "";
folly::Optional<std::string> cluster_name;
bool use_tcp = false;
bool hash_sequencer_placement = true;
bool traffic_shaping = false;
bool test_flexible_log_sharding = false;
bool with_ssl = false;
} // namespace options

bool done = false;

void parse_command_line(int argc, const char* argv[]) {
  using boost::program_options::bool_switch;
  using boost::program_options::value;

  boost::program_options::options_description desc("Options");
  // clang-format off
  desc.add_options()

    ("help,h",
     "produce this help message and exit")

    ("nnodes,n",
     value<int>(&options::nnodes)
     ->default_value(options::nnodes),
     "number of nodes in the cluster")

    ("nlogs,l",
     value<int>(&options::nlogs)
     ->default_value(options::nlogs),
     "number of logs in the cluster (log IDs start from 1)")

    ("nshards",
     value<shard_size_t>(&options::nshards)
     ->default_value(options::nshards),
     "number of shards in each storage node in the cluster")

    ("nracks",
     value<int>(&options::nracks)
     ->default_value(options::nracks),
     "number of racks in the cluster to spread the nodes accross")

    ("nodeset-size",
     value<int>(&options::nodeset_size),
     "Number of nodes in each log's nodeset. Must be >= replication.")

    ("replication,r",
     value<int>(&options::replication),
     "store and keep this many copies of each record")

    ("extra-copies,x",
     value<int>(&options::extra_copies),
     "write this many extra copies when processing an APPEND")

    ("synced-copies",
     value<int>(&options::synced_copies),
     "sync this many copies to disk when processing an APPEND")

    ("backlog",
     chrono_value(&options::backlog, false),
     "retention period for data")

    ("scd-enabled",
     bool_switch(&options::scd_enabled)
     ->default_value(false),
     "Enable the Single Copy Delivery optimization")

    ("loglevel",
     value<std::string>()
     ->default_value("error")
     ->notifier(dbg::parseLoglevelOption),
     "log level for driver program"
     " (one of: critical, error, warning, info, debug)")

    ("root",
     value<std::string>(&options::root),
     "root path to put all data into (if not specified, a temporary directory "
     "is created and deleted on shutdown)")

    ("range-name",
     value<std::string>(&options::range_name),
     "log range name")

    ("param",
     value<std::vector<std::string> >(&options::params),
     "additional param to pass to all server processes"
     " (e.g. --param loglevel=error)")

    ("use-existing-metadata",
     bool_switch(&options::use_existing_metadata)
     ->default_value(false),
     "Do not provision epoch metadata at startup. Assuming root "
     "directory contains existing metadata")

    ("config-path",
      value<std::string>(&options::config_path),
      "creates a cluster based on the provided config file")

    ("cluster-name",
     value<std::string>()->notifier([](const std::string& val) {
       options::cluster_name = val;
     }),
     "cluster_name property in config. Affects how stats are exported.")

    ("use-tcp",
     bool_switch(&options::use_tcp)
     ->default_value(false),
     "use TCP rather than unix domain sockets for the cluster")

    ("hash-sequencer-placement",
     value<bool>(&options::hash_sequencer_placement)
     ->default_value(options::hash_sequencer_placement),
     "Use hash-based sequencer placement. If false, place all sequencers on "
     "node 0.")

    ("traffic-shaping",
     bool_switch(&options::traffic_shaping)
     ->default_value(options::traffic_shaping),
     "apply a default traffic shaping config (off by default for better perf)")

    ("test-flexible-log-sharding",
     bool_switch(&options::test_flexible_log_sharding)
     ->default_value(options::test_flexible_log_sharding),
     "when enabled, using the \"select-all-shards\" nodeset "
     "selector which puts all shards of all nodes in the storage set of all "
     "logs. This is used to help for the development of Flexible Log Sharding "
     "(T21160618) and should not be used in production.")

    ("with-ssl",
     bool_switch(&options::with_ssl)
     ->default_value(options::with_ssl),
     "Enables SSL for servers/clients. If enabled, valid paths should be "
     "specified for certificates in settings.")
    ;
  // clang-format on

  try {
    boost::program_options::variables_map parsed =
        program_options_parse_no_positional(argc, argv, desc);

    // Check for --help before calling notify(), so that required options
    // aren't required.
    if (parsed.count("help")) {
      std::cout << "Utility that runs a LogDevice cluster on localhost.\n\n"
                << desc;
      exit(0);
    }

    // Surface any errors
    boost::program_options::notify(parsed);
  } catch (const boost::program_options::error& ex) {
    std::cerr << argv[0] << ": " << ex.what() << '\n';
    exit(1);
  }
}

// Parses server params provided on the command line:
//   ./cluster --param loglevel=error --param-storage
//   --per-worker-storage-task-queue-size=8
static void add_params(IntegrationTestUtils::ClusterFactory& factory,
                       const std::vector<std::string>& params,
                       const ParamScope scope) {
  for (const std::string& param : params) {
    size_t pos = param.find('=');
    if (pos == std::string::npos) {
      factory.setParam("--" + param, scope);
    } else {
      std::string key = param.substr(0, pos);
      std::string value = param.substr(pos + 1);

      // NOTE: some params are special-cased, when IntegrationTestUtils needs to
      // know about them (not enough to pass them through to the server process)
      if (key == "rocksdb-partitioned") {
        if (lowerCase(value) == "true" || value == "1") {
          factory.setRocksDBType(
              IntegrationTestUtils::RocksDBType::PARTITIONED);
        } else if (lowerCase(value) == "false" || value == "0") {
          factory.setRocksDBType(IntegrationTestUtils::RocksDBType::SINGLE);
        } else {
          std::cerr << "unexpected value " << value
                    << " of --rocksdb-partitioned" << std::endl;
          exit(1);
        }
      } else {
        factory.setParam("--" + key, value, scope);
      }
    }
  }
}

static int node_simple_command(std::vector<std::string> tokens,
                               Cluster& cluster) {
  ld_check(!tokens.empty());

  if (tokens.size() != 2) {
    std::cout << tokens[0] << " command only expects one argument";
    return -1;
  }

  int nid;
  try {
    nid = folly::to<int>(tokens[1]);
  } catch (const std::exception& e) {
    std::cout << "<nid> argument must be an int" << std::endl;
    return -1;
  }

  if (!cluster.getNodes().count(nid)) {
    std::cout << "<nid> must be a valid node index" << std::endl;
    return -1;
  }

  if (tokens[0] == "replace") {
    return cluster.replace(nid, false);
  } else if (tokens[0] == "start") {
    cluster.getNode(nid).start();
    return 0;
  } else if (tokens[0] == "stop") {
    cluster.getNode(nid).suspend();
    return 0;
  } else if (tokens[0] == "resume") {
    cluster.getNode(nid).resume();
    return 0;
  } else if (tokens[0] == "kill") {
    cluster.getNode(nid).kill();
    return 0;
  } else {
    ld_check(false);
    return -1;
  }
}

static int expand_command(std::vector<std::string> tokens, Cluster& cluster) {
  ld_check(!tokens.empty());

  if (tokens.size() != 2) {
    std::cout << tokens[0] << " command only expects one argument";
    return -1;
  }

  int n;
  try {
    n = folly::to<unsigned int>(tokens[1]);
  } catch (const std::exception& e) {
    std::cout << "<n> argument must be an unsigned int" << std::endl;
    return -1;
  }

  if (n <= 0 || n > 100) {
    std::cout << "<n> must be in range [1, 100]" << std::endl;
    return -1;
  }

  int rv = cluster.expand(n);
  if (rv != 0) {
    return rv;
  }

  std::cout << "The cluster now has " << cluster.getNodes().size() << " nodes"
            << std::endl;
  return rv;
}

static int shrink_command(std::vector<std::string> tokens, Cluster& cluster) {
  ld_check(!tokens.empty());

  if (tokens.size() != 2) {
    std::cout << tokens[0] << " command only expects one argument";
    return -1;
  }

  int n;
  try {
    n = folly::to<unsigned int>(tokens[1]);
  } catch (const std::exception& e) {
    std::cout << "<n> argument must be an unsigned int" << std::endl;
    return -1;
  }

  if (!cluster.getNodes().count(n)) {
    std::cout << "<n> must be a valid node index" << std::endl;
    return -1;
  }

  int rv = cluster.shrink(n);
  if (rv != 0) {
    return rv;
  }

  std::cout << "The cluster now has " << cluster.getNodes().size() << " nodes"
            << std::endl;
  return rv;
}

void printCommandHelp(std::string command,
                      std::string argument,
                      std::string doc) {
  std::cout << folly::format("\t\033[1;34m{} \033[1;33m<{}>\033[0m\t{}",
                             command,
                             argument,
                             doc)
                   .str()
            << std::endl;
}

int shell(Cluster& cluster) {
  char cmd[512];

  typedef std::function<int(std::vector<std::string>, Cluster & cluster)>
      command_func_t;

  std::unordered_map<std::string, command_func_t> commands = {
      {"replace", node_simple_command},
      {"start", node_simple_command},
      {"stop", node_simple_command},
      {"resume", node_simple_command},
      {"kill", node_simple_command},
      {"expand", expand_command},
      {"shrink", shrink_command}};

  std::cout
      << "LogDevice Cluster running. ^C or type \"quit\" or \"q\" to stop. ";
  std::cout << "Commands:" << std::endl << std::endl;
  printCommandHelp("replace",
                   "nid",
                   "Replace a node (kill the old node, wipe the existing data, "
                   "start a replacement). Do not wait for rebuilding.");
  printCommandHelp(
      "start", "nid", "Start a node if it is not already started.");
  printCommandHelp(
      "stop",
      "nid",
      "Pause logdeviced by sending SIGSTOP. Waits for the process to "
      "stop accepting connections.");
  printCommandHelp(
      "resume",
      "nid",
      "Resume logdeviced by sending SIGCONT. Waits for the process to "
      "start accepting connections again.");
  printCommandHelp("kill", "nid", "Kill a node.");
  printCommandHelp("expand", "nid", "Add new nodes to the cluster.");
  printCommandHelp("shrink", "nid", "Remove nodes from the cluster.");
  std::cout << std::endl;

  if (options::enable_logsconfig_manager) {
    // If logs config manager is enabled, print steps to create a log range
    auto cfg = cluster.getConfig()->getServerConfig();
    int nstorage_nodes =
        std::count_if(cfg->getNodes().begin(),
                      cfg->getNodes().end(),
                      [](Configuration::Nodes::value_type kv) {
                        return kv.second.isReadableStorageNode();
                      });

    std::cout << "\033[1;31mTo create a log range:\033[1;0m" << std::endl;
    std::cout << "\tldshell -c " << cluster.getConfigPath() << " logs create"
              << " --from 1 --to " << options::nlogs
              << " --replicate-across \"node: " << std::min(2, nstorage_nodes)
              << "\" test_logs" << std::endl
              << std::endl;

    std::cout << "\033[1;31mTo view existing log ranges: ";
    if (options::nlogs > 0) {
      std::cout << "(We've already created one for you)";
    }
    std::cout << "\033[1;0m" << std::endl;
    std::cout << "\tldshell -c " << cluster.getConfigPath() << " logs show"
              << std::endl
              << std::endl;
  }

#ifdef FB_BUILD_PATHS
  const char* READTESTAPP_RELATIVE_PATH = "logdevice/fb/scripts/readtestapp";
  const char* LOADTEST_RELATIVE_PATH = "logdevice/fb/scripts/loadtest";
  std::string readtestapp_path =
      IntegrationTestUtils::findBinary(READTESTAPP_RELATIVE_PATH);
  std::string loadtest_path =
      IntegrationTestUtils::findBinary(LOADTEST_RELATIVE_PATH);
  if (!readtestapp_path.empty()) {
#if BOOST_VERSION >= 106000
    readtestapp_path = fs::relative(fs::path(readtestapp_path)).string();
#endif
    std::cout << "\033[1;31mTo start a tailer for all logs:\033[1;0m"
              << std::endl;
    std::cout << "\t" << readtestapp_path << " --config-path "
              << cluster.getConfigPath() << " --logs 1.." << options::nlogs
              << " --client-authentication=false"
              << " --tail" << std::endl
              << std::endl;
  }
  if (!loadtest_path.empty()) {
#if BOOST_VERSION >= 106000
    loadtest_path = fs::relative(fs::path(loadtest_path)).string();
#endif
    std::cout
        << "\033[1;31mTo generate synthetic write load on all logs:\033[1;0m"
        << std::endl;
    std::cout << "\t" << loadtest_path << " --config-path "
              << cluster.getConfigPath() << " --logs 1.." << options::nlogs
              << " -n -1 --rate 10" << std::endl
              << std::endl;
  }
#else
  const char* TAIL_RELATIVE_PATH = "ldtail";
  const char* WRITE_RELATIVE_PATH = "ldwrite";
  std::string tail_path = IntegrationTestUtils::findBinary(TAIL_RELATIVE_PATH);
  std::string write_path =
      IntegrationTestUtils::findBinary(WRITE_RELATIVE_PATH);
  if (!write_path.empty()) {
#if BOOST_VERSION >= 106000
    write_path = fs::relative(fs::path(write_path)).string();
#endif
    std::cout << "\033[1;31mTo write to log 1:\033[1;0m" << std::endl;
    std::cout << "\t echo hello | " << write_path << " "
              << cluster.getConfigPath() << " 1"
              << "\n\n";
  }

  if (!tail_path.empty()) {
#if BOOST_VERSION >= 106000
    tail_path = fs::relative(fs::path(tail_path)).string();
#endif
    std::cout << "\033[1;31mTo start a tailer for log 1:\033[1;0m" << std::endl;
    std::cout << "\t" << tail_path << " " << cluster.getConfigPath() << " 1"
              << " --follow"
              << "\n\n";
  }
#endif

  auto& first_node = cluster.getNode(0);
  std::cout << "\033[1;31mTo tail the error log of a node:\033[1;0m"
            << std::endl;
  std::cout << "\ttail -f " << first_node.getLogPath() << std::endl
            << std::endl;
  auto first_node_cmd_addr = first_node.getCommandSockAddr();
  std::cout << "\033[1;31mTo send an admin command to a node:\033[1;0m"
            << std::endl;
  if (first_node_cmd_addr.isUnixAddress()) {
    std::cout << "\techo info | nc -U " << first_node_cmd_addr.getPath();
  } else {
    std::cout << "\techo info | nc 0 " << first_node_cmd_addr.port();
  }
  std::cout << std::endl;

  // The user can stop by sending SIGTERM or SIGINT
  struct sigaction sa;
  sa.sa_handler = [](int /*sig*/) { done = true; };
  sigemptyset(&sa.sa_mask);
  sa.sa_flags = 0;
  ld_check(sigaction(SIGTERM, &sa, nullptr) == 0);
  ld_check(sigaction(SIGINT, &sa, nullptr) == 0);

  while (!done) {
    std::cout << "cluster> ";
    std::cin.getline(cmd, 512);
    if (!std::cin) {
      std::cout << std::endl;
      break;
    }
    std::vector<std::string> tokens;
    folly::split(' ', cmd, tokens, true /* ignoreEmpty */);
    if (tokens.empty()) {
      continue;
    }
    if (tokens[0] == "quit" || tokens[0] == "q") {
      break;
    }
    auto it = commands.find(tokens[0]);
    if (it == commands.end()) {
      std::cout << "Unknown command \"" << cmd << "\"" << std::endl;
      continue;
    }
    if (it->second(tokens, cluster) != 0) {
      std::cout << "Command failed." << std::endl;
    }
  }

  return 0;
}

int main(int argc, const char* argv[]) {
  logdeviceInit();
  parse_command_line(argc, argv);

  IntegrationTestUtils::ClusterFactory factory;
  factory.setNumLogs(options::nlogs);
  factory.setNumLogsConfigManagerLogs(options::nlogs);
  factory.useDefaultTrafficShapingConfig(options::traffic_shaping);
  if (options::hash_sequencer_placement) {
    factory.useHashBasedSequencerAssignment();
  }

  if (!options::use_existing_metadata) {
    factory.doPreProvisionEpochMetaData();
    factory.allowExistingMetaData();
  }

  logsconfig::LogAttributes log_attrs =
      factory.createDefaultLogAttributes(options::nnodes);
  if (!options::range_name.empty()) {
    factory.setLogGroupName(options::range_name);
  }
  if (options::replication != -1) {
    log_attrs.set_replicationFactor(options::replication);
  }
  if (options::extra_copies != -1) {
    log_attrs.set_extraCopies(options::extra_copies);
  }
  if (options::synced_copies != -1) {
    log_attrs.set_syncedCopies(options::synced_copies);
  }
  if (options::backlog.count() >= 0) {
    log_attrs.set_backlogDuration(options::backlog);
  }
  if (options::scd_enabled) {
    log_attrs.set_scdEnabled(true);
  }
  if (options::nodeset_size != -1) {
    log_attrs.set_nodeSetSize(options::nodeset_size);
  }
  if (options::use_tcp) {
    factory.useTcp();
  }
  if (!options::with_ssl) {
    factory.noSSLAddress();
  }

  factory.setLogAttributes(log_attrs);

  logsconfig::LogAttributes event_log_attrs;
  event_log_attrs.set_backlogDuration(folly::none);
  event_log_attrs.set_replicationFactor(
      options::nodeset_size == -1
          ? std::min(5, (options::nnodes + 1) / 2)
          : std::min(5, (options::nodeset_size + 1) / 2));
  factory.setEventLogAttributes(event_log_attrs);

  if (!options::root.empty()) {
    factory.setRootPath(options::root);
  }

  if (options::cluster_name.hasValue()) {
    factory.setClusterName(options::cluster_name.value());
  }

  if (options::nodeset_size != -1) {
    std::shared_ptr<NodeSetSelector> selector =
        NodeSetSelectorFactory::create(NodeSetSelectorType::RANDOM);
    factory.setProvisionNodeSetSelector(selector);
  }

  if (options::test_flexible_log_sharding) {
    std::shared_ptr<NodeSetSelector> selector =
        NodeSetSelectorFactory::create(NodeSetSelectorType::SELECT_ALL_SHARDS);
    factory.setProvisionNodeSetSelector(selector);
    factory.setParam("--write-shard-id-in-copyset", "true");
    factory.setParam("--epoch-metadata-use-new-storage-set-format", "true");
  }

  if (options::enable_logsconfig_manager) {
    factory.enableLogsConfigManager();
  }
  if (options::enable_rebuilding) {
    factory.setParam("--disable-rebuilding", "false");
  }

  factory.eventLogMode(
      IntegrationTestUtils::ClusterFactory::EventLogMode::SNAPSHOTTED);
  factory.setNumDBShards(options::nshards);
  factory.setNumRacks(options::nracks);

  add_params(factory, options::params, ParamScope::ALL);
  std::unique_ptr<Cluster> cluster = nullptr;
  try {
    if (options::config_path != "") {
      auto config = Configuration::fromJsonFile(options::config_path.c_str());
      if (config == nullptr) {
        ld_error("Unable to load config. Could not create cluster");
        return 1;
      }
      cluster = factory.create(*config);
    } else {
      cluster = factory.create(options::nnodes);
    }
  } catch (const std::exception& e) {
    ld_error("Could not create cluster: %s", e.what());
    return 1;
  }
  return shell(*cluster);
}
