/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <cstdlib>

#include "folly/dynamic.h"
#include "folly/json.h"

#include "logdevice/lib/ClientImpl.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/test/utils/IntegrationTestBase.h"
#include "logdevice/test/utils/IntegrationTestUtils.h"

using namespace facebook::logdevice;
using namespace facebook::logdevice::IntegrationTestUtils;

namespace {
struct Params {
#define FIELD(type, name, default_val) \
  type name{default_val};              \
  Params& set_##name(type val) {       \
    this->name = val;                  \
    return *this;                      \
  }

  FIELD(int, node_count, 25)
  FIELD(int, max_boycott_count, 1)
  FIELD(std::chrono::milliseconds, controller_check_period, 100)
  FIELD(std::chrono::milliseconds,
        controller_aggregation_period,
        std::chrono::seconds{5})
  FIELD(std::chrono::milliseconds,
        controller_response_timeout,
        std::chrono::seconds{1})
  FIELD(std::chrono::milliseconds, boycott_grace_period, 0)
  FIELD(std::chrono::milliseconds, boycott_duration, DEFAULT_TEST_TIMEOUT)
  // use a low sensitivity to not require as many nodes
  FIELD(unsigned int, boycott_std, 1)
  FIELD(unsigned int, required_client_count, 1)
  FIELD(double, remove_worst_percentage, 0.0)
  FIELD(unsigned int, send_worst_client_count, 1)
  FIELD(bool, observe_only, false)
#undef FIELD
};

/**
 * Used to asyncronously append to logs. Uses RAII to ensure that the
 * thread is stopped.
 */
class AppendThread {
 public:
  AppendThread() = default;
  // copy-constructible to have it in vector
  AppendThread(const AppendThread& other) {
    // don't copy the thread though
    stop_ = other.stop_.load();
  }

  // see comments in copy constructor
  AppendThread& operator=(const AppendThread& other) {
    stop_ = other.stop_.load();
    return *this;
  }

  ~AppendThread() {
    stop();
  }

  void start(Client* client, unsigned int log_count) {
    std::vector<node_index_t> outlier_nodes;
    outlier_nodes.reserve(log_count);
    for (node_index_t i = 0; i < log_count; ++i) {
      outlier_nodes.emplace_back(i);
    }

    start(client, std::move(outlier_nodes));
  }

  void start(Client* client, std::vector<node_index_t> outlier_nodes) {
    stop();

    stop_ = false;
    append_thread_ = std::thread([=] {
      while (!stop_) {
        for (auto outlier : outlier_nodes) {
          client->appendSync(logid_t{static_cast<uint64_t>(outlier + 1)}, ".");
        }
      }
    });
  }

  void stop() {
    stop_ = true;
    if (append_thread_.joinable()) {
      append_thread_.join();
    }
  }

 private:
  std::atomic<bool> stop_{false};
  std::thread append_thread_;
};

class NodeStatsControllerIntegrationTest
    : public IntegrationTestBase,
      public ::testing::WithParamInterface<bool /*use-rmsd*/> {
 public:
  void initializeCluster(Params params) {
    auto msString = [](std::chrono::milliseconds duration) {
      return std::to_string(duration.count()) + "ms";
    };

    cluster =
        IntegrationTestUtils::ClusterFactory{} /* allow two controllers */

            // All tests will be run twice, once with the legacy outlier
            // detection algorithm, once with the new outlier detection
            // algorithm from common/OutlierDetection.h.
            .setParam(
                "--node-stats-boycott-use-rmsd", GetParam() ? "true" : "false")

            .setParam("--node-stats-max-boycott-count",
                      std::to_string(params.max_boycott_count))
            /* Increase the speed at which nodes check if they're
             * controllers */
            .setParam("--node-stats-controller-check-period",
                      msString(params.controller_check_period))
            /* Period at which controllers collect stats */
            .setParam("--node-stats-controller-aggregation-period",
                      msString(params.controller_aggregation_period))
            /* Delay between sending a message and aggregating the
             * replies */
            .setParam("--node-stats-controller-response-timeout",
                      msString(params.controller_response_timeout))
            .setParam("--node-stats-boycott-grace-period",
                      msString(params.boycott_grace_period))
            .setParam("--node-stats-boycott-duration",
                      msString(params.boycott_duration))
            .setParam("--node-stats-boycott-required-std-from-mean",
                      std::to_string(params.boycott_std))
            .setParam("--node-stats-remove-worst-percentage",
                      std::to_string(params.remove_worst_percentage))
            .setParam("--node-stats-boycott-required-client-count",
                      std::to_string(params.required_client_count))
            .setParam("--node-stats-send-worst-client-count",
                      std::to_string(params.send_worst_client_count))
            // Use a relatively big relative margin to ensure the tests are not
            // flaky.
            .setParam("--node-stats-boycott-relative-margin", "0.5")
            .setParam("--boycotts-observe-only",
                      params.observe_only ? "true" : "false")
            .setParam("--use-sequencer-affinity", "true")
            /* will lazily assign sequencers and enable gossip */
            .useHashBasedSequencerAssignment()
            /* speed up gossips */
            .setParam("--gossip-interval", "10ms")
            /* put each node in a separate rack */
            .setNumRacks(params.node_count)
            /* Can't currently create 0 logs, create 1 and then overwrite it */
            .setNumLogs(1)
            .create(params.node_count);

    setOneLogPerNode();
    waitForSequencersToActivate();

    int controller_count = params.max_boycott_count + 1;
    // wait for controllers to activate
    wait_until(std::string(std::to_string(controller_count) +
                           " controllers are chosen")
                   .c_str(),
               [&] { return getActiveControllerCount() == controller_count; });
  }

  /**
   * Sets the affinity to the num_logs first nodes in the nodes_config
   * Assumes that each node has a separate location. This can be achieved by
   * setting the numRacks equal to the amount of created nodes
   */
  void setOneLogPerNode() {
    auto full_config = cluster->getConfig()->get();
    auto nodes = full_config->serverConfig()->getNodes();

    auto logs_config = std::make_shared<configuration::LocalLogsConfig>();

    auto config_tree = logsconfig::LogsConfigTree::create();
    const auto defaults =
        logsconfig::DefaultLogAttributes{}.with_replicationFactor(1);
    for (unsigned int i = 0; i < nodes.size(); ++i) {
      ASSERT_TRUE(config_tree->addLogGroup(
          "/",
          std::to_string(i),
          logid_range_t({logid_t{i + 1}, logid_t{i + 1}}),
          defaults.with_sequencerAffinity(
              logsconfig::Attribute<folly::Optional<std::string>>(
                  nodes.at(i).location.value().toString()))));
    }
    logs_config->setLogsConfigTree(std::move(config_tree));
    logs_config->markAsFullyLoaded();

    cluster->writeConfig(full_config->serverConfig().get(), logs_config.get());
    cluster->waitForConfigUpdate();
  }

  void waitForSequencersToActivate() {
    // make sure that this client never sends stats to make the remaining parts
    // of the test more deterministic
    auto client = createClient({{"node-stats-send-period", "1h"}});

    wait_until(
        "All sequencers are activated",
        [client = client.get(), node_count = cluster->getNodes().size()] {
          bool is_all_activated = true;
          for (int log = 1; log <= node_count; ++log) {
            is_all_activated = is_all_activated &&
                client->appendSync(logid_t(log), ".") != LSN_INVALID;
          }
          return is_all_activated;
        });
  }

  int getActiveControllerCount() {
    int controller_count = 0;
    for (const auto& node : cluster->getNodes()) {
      controller_count += node.second->stats()["is_controller"];
    }

    return controller_count;
  }

  std::shared_ptr<Client>
  createClient(std::vector<std::pair<std::string, std::string>> settings = {}) {
    std::unique_ptr<ClientSettings> client_settings{ClientSettings::create()};

    // put defaults at the start of settings, then they will be overridden by
    // any non-default values
    std::vector<std::pair<std::string, std::string>> with_defaults{
        {"node-stats-send-period", "1s"}, {"use-sequencer-affinity", "true"}};
    std::move(
        settings.begin(), settings.end(), std::back_inserter(with_defaults));

    for (auto val : with_defaults) {
      EXPECT_EQ(0, client_settings->set(val.first.c_str(), val.second.c_str()))
          << "Invalid settings " << val.first << ":" << val.second;
    }

    // use a small timeout not to get stuck
    auto client = cluster->createClient(
        std::chrono::milliseconds{50}, std::move(client_settings));

    return client;
  }

  void updateSettings(ServerConfig::SettingsConfig changed_settings) {
    auto other = cluster->getConfig()->getServerConfig();

    ServerConfig::SettingsConfig new_settings =
        other->getServerSettingsConfig();

    for (const auto& kv : changed_settings) {
      new_settings[kv.first] = kv.second;
    }

    std::shared_ptr<ServerConfig> new_config =
        ServerConfig::fromData(other->getClusterName(),
                               configuration::NodesConfig(other->getNodes()),
                               other->getMetaDataLogsConfig(),
                               ServerConfig::PrincipalsConfig(),
                               ServerConfig::SecurityConfig(),
                               ServerConfig::TraceLoggerConfig(),
                               ServerConfig::TrafficShapingConfig(),
                               ServerConfig::ZookeeperConfig(),
                               new_settings,
                               other->getClientSettingsConfig());

    ASSERT_TRUE(new_config != nullptr) << "Invalid setting given";

    cluster->writeServerConfig(new_config.get());
  }

  void setErrorInjection(Client* client,
                         const std::vector<node_index_t>& outlier_nodes,
                         const std::vector<double>& fail_ratios) {
    ld_check(outlier_nodes.size() == fail_ratios.size());

    std::unordered_map<logid_t, double> outlier_logs;
    outlier_logs.reserve(outlier_nodes.size());
    for (int i = 0; i < outlier_nodes.size(); ++i) {
      outlier_logs.emplace(
          logid_t{static_cast<uint64_t>(outlier_nodes[i]) + 1}, fail_ratios[i]);
    }

    auto raw_client = static_cast<ClientImpl*>(client);
    raw_client->setAppendErrorInjector(
        // fail every append with the status SEQNOBUFS
        AppendErrorInjector{Status::SEQNOBUFS, std::move(outlier_logs)});
  }

  // utility function that assumes that all outliers should be boycotted
  void
  waitUntilBoycottsOnAllNodes(const std::vector<node_index_t>& outlier_nodes) {
    waitUntilBoycottsOnAllNodes(outlier_nodes, outlier_nodes.size());
  }

  // only boycott_count boycotts may occur, and the entire cluster must be in
  // agreement
  void
  waitUntilBoycottsOnAllNodes(const std::vector<node_index_t>& outlier_nodes,
                              int boycott_count) {
    std::string wait_until_str{"All nodes have agreed on the " +
                               std::to_string(boycott_count) +
                               " outliers to boycott"};
    wait_until(wait_until_str.c_str(), [&] {
      std::vector<std::set<node_index_t>> boycotts_on_nodes(
          cluster->getNodes().size());

      for (auto& node : cluster->getNodes()) {
        for (auto boycott_entry : node.second->gossipBoycottState()) {
          if (boycott_entry.second) {
            // the node is boycotted
            boycotts_on_nodes[node.first].emplace(
                // names are "N" + node_index. Simply remove the N
                std::stoi(boycott_entry.first.substr(1)));
            ld_info("N%i thinks that %s is BOYCOTTED",
                    node.first,
                    boycott_entry.first.c_str());
          }
        }
      }

      return
          // check that the boycotted elements are part of the outlier nodes and
          // that it's the correct size
          std::all_of(
              boycotts_on_nodes.cbegin(),
              boycotts_on_nodes.cend(),
              [&](const auto& set) {
                return set.size() == boycott_count &&
                    // the boycotts should all be any of the given outliers
                    std::all_of(set.cbegin(),
                                set.cend(),
                                [&](const auto& boycotted_node) {
                                  return std::find(outlier_nodes.cbegin(),
                                                   outlier_nodes.cend(),
                                                   boycotted_node) !=
                                      outlier_nodes.cend();
                                });
              }) &&
          // check that every element is equal to the one before => all
          // elements are equal
          std::equal(boycotts_on_nodes.cbegin() + 1,
                     boycotts_on_nodes.cend(),
                     boycotts_on_nodes.cbegin());
    });
  }

  // the default log for a node is its index + 1
  logid_t getDefaultLog(node_index_t node_index) {
    return logid_t{static_cast<uint64_t>(node_index + 1)};
  }

  node_index_t getSequencerForLog(Client* client, logid_t log) {
    SequencerState seq_state;
    while (true) {
      if (IntegrationTestUtils::getSeqState(client, log, seq_state, true) ==
          Status::OK) {
        break;
      }
    }
    return seq_state.node.index();
  }

  std::unique_ptr<Cluster> cluster;
};
} // namespace

TEST_P(NodeStatsControllerIntegrationTest, ControllerSelection) {
  initializeCluster(Params{}.set_max_boycott_count(1).set_node_count(3));
  wait_until("2 controllers", [&] { return getActiveControllerCount() == 2; });

  // 2 boycotts => 3 controllers
  updateSettings({{"node-stats-max-boycott-count", "4"}});
  wait_until("3 controllers", [&] { return getActiveControllerCount() == 3; });

  // 0 boycotts => 0 controllers
  updateSettings({{"node-stats-max-boycott-count", "0"}});
  wait_until("0 controllers", [&] { return getActiveControllerCount() == 0; });

  // 100 boycotts => 101 controllers, but we only have 3 nodes => only 3
  // controllers
  updateSettings({{"node-stats-max-boycott-count", "100"}});
  wait_until("3 controllers (Controller count > node count)",
             [&] { return getActiveControllerCount() == 3; });
}

TEST_P(NodeStatsControllerIntegrationTest, BoycottNodeZeroSuccess) {
  unsigned int node_count = 5;
  node_index_t outlier_node{3};

  initializeCluster(
      Params{}.set_max_boycott_count(1).set_node_count(node_count));

  auto client = createClient();
  setErrorInjection(client.get(), {outlier_node}, {1.0});

  AppendThread appender;
  appender.start(client.get(), node_count);

  waitUntilBoycottsOnAllNodes({outlier_node});
}

TEST_P(NodeStatsControllerIntegrationTest, BoycottNode50PercentFail) {
  unsigned int node_count = 5;
  node_index_t outlier_node{3};

  initializeCluster(
      Params{}.set_max_boycott_count(1).set_node_count(node_count));

  auto client = createClient();
  setErrorInjection(client.get(), {outlier_node}, {0.5});

  AppendThread appender;
  appender.start(client.get(), node_count);

  waitUntilBoycottsOnAllNodes({outlier_node});
}

TEST_P(NodeStatsControllerIntegrationTest, Boycott1Node2Outliers) {
  unsigned int node_count = 5;
  std::vector<node_index_t> outlier_nodes{1, 3};

  initializeCluster(
      Params{}.set_max_boycott_count(1).set_node_count(node_count));

  auto client = createClient();

  setErrorInjection(client.get(), outlier_nodes, {0.5, 0.5});

  AppendThread appender;
  appender.start(client.get(), node_count);

  if (GetParam()) {
    // With the new outlier detection algorithm, we should not pick any outlier.
    waitUntilBoycottsOnAllNodes({});
  } else {
    // 1 boycott, even though we have two outliers. Makes sure that the boycott
    // on all nodes is the same one
    waitUntilBoycottsOnAllNodes(outlier_nodes, 1);
  }
}

TEST_P(NodeStatsControllerIntegrationTest, Boycott2Nodes) {
  unsigned int node_count = 10;
  std::vector<node_index_t> outlier_nodes{1, 3};

  initializeCluster(
      Params{}.set_max_boycott_count(2).set_node_count(node_count));

  auto client = createClient();

  setErrorInjection(client.get(), outlier_nodes, {0.5, 0.5});

  AppendThread appender;
  appender.start(client.get(), node_count);

  waitUntilBoycottsOnAllNodes(outlier_nodes);
}

TEST_P(NodeStatsControllerIntegrationTest, BoycottManyOutliersNoBoycotts) {
  unsigned int node_count = 5;
  std::vector<node_index_t> outlier_nodes{1};

  initializeCluster(Params{}
                        .set_max_boycott_count(1)
                        .set_node_count(node_count)
                        // have shorter duration to first have one boycott, and
                        // then let it reset and boycott 0 because many are bad
                        .set_boycott_duration(std::chrono::seconds{10}));

  auto client = createClient();

  setErrorInjection(client.get(), outlier_nodes, {0.5});

  AppendThread appender;
  appender.start(client.get(), node_count);

  waitUntilBoycottsOnAllNodes(outlier_nodes);

  appender.stop();

  outlier_nodes = {1, 2, 3};
  std::vector<double> fail_ratios(outlier_nodes.size(), 0.5);
  setErrorInjection(client.get(), outlier_nodes, fail_ratios);

  appender.start(client.get(), node_count);

  waitUntilBoycottsOnAllNodes({}); // no boycotts anymore
}

TEST_P(NodeStatsControllerIntegrationTest, RemoveWorstClients) {
  unsigned int node_count = 5;
  std::vector<node_index_t> outlier_nodes{3};

  initializeCluster(Params{}
                        .set_max_boycott_count(1)
                        // send the worst 2 clients separately
                        .set_send_worst_client_count(2)
                        // don't remove any bad clients at first
                        .set_remove_worst_percentage(0)
                        .set_node_count(node_count)
                        .set_boycott_duration(std::chrono::seconds{10}));

  std::vector<std::shared_ptr<Client>> clients;

  int client_count = 5;

  // add 4 good clients
  for (int i = 0; i < client_count - 1; ++i) {
    clients.emplace_back(createClient());
  }

  // and 1 bad clients
  auto outlier_client = createClient();
  setErrorInjection(outlier_client.get(), outlier_nodes, {0.5});
  clients.emplace_back(std::move(outlier_client));

  std::vector<AppendThread> appenders(clients.size());
  for (int i = 0; i < clients.size(); ++i) {
    appenders[i].start(clients[i].get(), node_count);
  }

  waitUntilBoycottsOnAllNodes(outlier_nodes);

  // remove the 20% (1) worst clients. Now there should be no more boycotts
  updateSettings({{"node-stats-remove-worst-percentage", "0.2"}});

  waitUntilBoycottsOnAllNodes({}); // no boycotts anymore
}

TEST_P(NodeStatsControllerIntegrationTest, Require2Clients) {
  unsigned int node_count = 5;
  node_index_t outlier_node{3};

  initializeCluster(Params{}
                        .set_max_boycott_count(1)
                        .set_required_client_count(1)
                        .set_boycott_duration(std::chrono::seconds{10})
                        .set_node_count(node_count));

  auto client = createClient();
  setErrorInjection(client.get(), {outlier_node}, {0.5});

  AppendThread appender;
  appender.start(client.get(), node_count);

  waitUntilBoycottsOnAllNodes({outlier_node});

  // require 2 clients, but because we only have 1 => no more boycotts
  updateSettings({{"node-stats-boycott-required-client-count", "2"}});

  waitUntilBoycottsOnAllNodes({});
}

TEST_P(NodeStatsControllerIntegrationTest, SendWorstClientCount) {
  unsigned int node_count = 5;
  std::vector<node_index_t> outlier_nodes{3};

  initializeCluster(Params{}
                        .set_max_boycott_count(1)
                        // remove 20% of the worst clients
                        .set_remove_worst_percentage(0.2)
                        // don't send the bad clients separately
                        .set_send_worst_client_count(0)
                        .set_node_count(node_count)
                        .set_boycott_duration(std::chrono::seconds{10}));

  std::vector<std::shared_ptr<Client>> clients;

  int client_count = 5;

  // add 4 good clients
  for (int i = 0; i < client_count - 1; ++i) {
    clients.emplace_back(createClient());
  }

  // and 1 bad clients
  auto outlier_client = createClient();
  setErrorInjection(outlier_client.get(), outlier_nodes, {0.5});
  clients.emplace_back(std::move(outlier_client));

  std::vector<AppendThread> appenders(clients.size());
  for (int i = 0; i < clients.size(); ++i) {
    appenders[i].start(clients[i].get(), node_count);
  }

  waitUntilBoycottsOnAllNodes(outlier_nodes);

  // separate the worst client from the others. And because the 20% worst
  // clients get removed, the worst node should be removed and no boycotts
  // should occur
  updateSettings({{"node-stats-send-worst-client-count", "1"}});

  waitUntilBoycottsOnAllNodes({}); // no boycotts anymore
}

TEST_P(NodeStatsControllerIntegrationTest, Disable) {
  unsigned int node_count = 5;
  node_index_t outlier_node{3};

  initializeCluster(Params{}
                        .set_max_boycott_count(1)
                        .set_node_count(node_count)
                        .set_boycott_duration(std::chrono::hours{1}));

  auto client = createClient();
  setErrorInjection(client.get(), {outlier_node}, {0.5});

  AppendThread appender;
  appender.start(client.get(), node_count);

  waitUntilBoycottsOnAllNodes({outlier_node});

  updateSettings({{"node-stats-max-boycott-count", "0"}});

  waitUntilBoycottsOnAllNodes({});
}

TEST_P(NodeStatsControllerIntegrationTest, DisableZeroDuration) {
  unsigned int node_count = 5;
  node_index_t outlier_node{3};

  initializeCluster(Params{}
                        .set_max_boycott_count(1)
                        .set_node_count(node_count)
                        .set_boycott_duration(std::chrono::hours{1}));

  auto client = createClient();
  setErrorInjection(client.get(), {outlier_node}, {0.5});

  AppendThread appender;
  appender.start(client.get(), node_count);

  waitUntilBoycottsOnAllNodes({outlier_node});

  updateSettings({{"node-stats-boycott-duration", "0s"}});

  waitUntilBoycottsOnAllNodes({});
}

TEST_P(NodeStatsControllerIntegrationTest, DisableObserveOnly) {
  unsigned int node_count = 5;
  node_index_t outlier_node{3};

  initializeCluster(Params{}
                        .set_max_boycott_count(1)
                        .set_node_count(node_count)
                        .set_observe_only(false)
                        .set_boycott_duration(std::chrono::hours{1}));

  auto client = createClient();
  setErrorInjection(client.get(), {outlier_node}, {0.5});

  EXPECT_EQ(outlier_node,
            getSequencerForLog(client.get(), getDefaultLog(outlier_node)));

  AppendThread appender;
  appender.start(client.get(), node_count);

  waitUntilBoycottsOnAllNodes({outlier_node});

  EXPECT_NE(outlier_node,
            getSequencerForLog(client.get(), getDefaultLog(outlier_node)));

  updateSettings({{"boycotts-observe-only", "true"}});

  wait_until("Sequencer is back to normal", [&] {
    return outlier_node ==
        getSequencerForLog(client.get(), getDefaultLog(outlier_node));
  });

  // all the nodes should still consider the node boycotted, but it should not
  // affect sequencer placement
  EXPECT_TRUE(std::all_of(
      cluster->getNodes().begin(),
      cluster->getNodes().end(),
      [&](auto& node_entry) {
        auto boycotts = node_entry.second->gossipBoycottState();
        auto boycott_count = node_entry.second->stats()["boycotts_seen"];
        return boycotts["N" + std::to_string(outlier_node)] == true &&
            boycott_count == 1;
      }));
}

TEST_P(NodeStatsControllerIntegrationTest, NoisyTest) {
  unsigned int node_count = 5;
  node_index_t outlier_node{3};

  initializeCluster(Params{}
                        .set_max_boycott_count(1)
                        .set_boycott_duration(std::chrono::seconds(10))
                        .set_node_count(node_count));

  auto client = createClient();
  setErrorInjection(client.get(), {outlier_node}, {0.5});

  AppendThread appender;
  appender.start(client.get(), node_count);

  waitUntilBoycottsOnAllNodes({outlier_node});

  // make noise and expect none to be boycotted
  std::vector<node_index_t> noisy_nodes;
  for (node_index_t i = 0; i < node_count; ++i) {
    noisy_nodes.emplace_back(i);
  }
  // 0.5% fail ratio
  std::vector<double> fail_ratios(node_count, 0.005);

  setErrorInjection(client.get(), noisy_nodes, fail_ratios);

  waitUntilBoycottsOnAllNodes({});

  // but then have a real outlier in the noise, make sure it gets boycotted
  fail_ratios[outlier_node] = 0.5;
  setErrorInjection(client.get(), noisy_nodes, fail_ratios);

  waitUntilBoycottsOnAllNodes({outlier_node});
}

TEST_P(NodeStatsControllerIntegrationTest, Reset) {
  unsigned int node_count = 5;
  node_index_t outlier_node{3};

  initializeCluster(Params{}
                        .set_max_boycott_count(1)
                        // way longer time than the test
                        .set_boycott_duration(std::chrono::hours{1})
                        .set_node_count(node_count));

  auto client = createClient();
  setErrorInjection(client.get(), {outlier_node}, {0.5});

  AppendThread appender;
  appender.start(client.get(), node_count);

  waitUntilBoycottsOnAllNodes({outlier_node});

  // no more outliers
  setErrorInjection(client.get(), {}, {});

  cluster->getNode(0).resetBoycott(outlier_node);

  waitUntilBoycottsOnAllNodes({});
}

// This test tests that everything works correctly when AppenderRequest doesn't
// set sequencer_node_
TEST_P(NodeStatsControllerIntegrationTest, NoSequencerNode) {
  auto cluster = IntegrationTestUtils::ClusterFactory().deferStart().create(1);

  std::unique_ptr<ClientSettings> settings(ClientSettings::create());
  ASSERT_EQ(0, settings->set("on-demand-logs-config", "true"));
  std::shared_ptr<Client> client = cluster->createIndependentClient(
      std::chrono::milliseconds(100), std::move(settings));
  ASSERT_TRUE((bool)client);

  // Sends an append and expects it to timeout before routing it (because it's
  // blocked on fetching LogsConfig)
  char data[20];
  lsn_t lsn = client->appendSync(logid_t(1), Payload(data, sizeof data));

  EXPECT_EQ(LSN_INVALID, lsn);
  EXPECT_EQ(E::TIMEDOUT, err);

  cluster->start();
  wait_until("NODE_STATS_REPLY received", [&]() {
    Stats client_stats =
        checked_downcast<ClientImpl&>(*client).stats()->aggregate();
    return client_stats
               .per_message_type_stats[int(MessageType::NODE_STATS_REPLY)]
               .message_received > 0;
  });
}

TEST_P(NodeStatsControllerIntegrationTest, AdminCommand) {
  unsigned int node_count = 5;
  node_index_t outlier_node{3};

  initializeCluster(Params{}
                        .set_max_boycott_count(1)
                        // way longer time than the test
                        .set_boycott_duration(std::chrono::hours{1})
                        .set_node_count(node_count));

  auto client = createClient();
  setErrorInjection(client.get(), {outlier_node}, {0.5});

  AppendThread appender;
  appender.start(client.get(), node_count);

  waitUntilBoycottsOnAllNodes({outlier_node});

  // Using hardcoded node id 3 as controller for now, assuming this doesn't
  // change. Might want to use the placement algorithm here as well to figure
  // out where it is or try all of them
  std::string reply =
      cluster->getNode(3).sendCommand("info append_outliers --json");
  bool found_bad = false;
  auto parsed = folly::parseJson(reply.substr(0, reply.rfind("END")));
  for (auto& row : parsed["rows"]) {
    auto node_id = std::atoi(row[0].asString().c_str());
    auto failures = std::atoi(row[2].asString().c_str());
    auto successes = std::atoi(row[1].asString().c_str());
    auto is_outlier = std::atoi(row[4].asString().c_str());
    auto failure_rate =
        static_cast<float>(failures) / static_cast<float>(successes);

    ld_info("Node failure rate %u is %f", node_id, failure_rate);
    if (failure_rate >= 0.4) {
      ld_assert_eq(outlier_node, node_id);
      found_bad = true;
    }
    ld_assert_eq(node_id == outlier_node, is_outlier);
  }
  ld_assert(found_bad);
}

INSTANTIATE_TEST_CASE_P(NodeStatsControllerIntegrationTest,
                        NodeStatsControllerIntegrationTest,
                        ::testing::Bool());
