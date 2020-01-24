/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <boost/python.hpp>
#include <folly/futures/Promise.h>

#include "logdevice/clients/python/util/util.h"
#include "logdevice/common/ThriftCodec.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/ServerConfig.h"
#include "logdevice/common/configuration/nodes/NodesConfigLegacyConverter.h"
#include "logdevice/common/configuration/nodes/NodesConfigurationAPI.h"
#include "logdevice/common/configuration/nodes/NodesConfigurationCodec.h"
#include "logdevice/common/configuration/nodes/NodesConfigurationManagerFactory.h"
#include "logdevice/common/plugin/ZookeeperClientFactory.h"
#include "logdevice/include/Client.h"
#include "logdevice/lib/ClientImpl.h"
#include "logdevice/lib/ClientSettingsImpl.h"
#include "thrift/lib/cpp2/protocol/JSONProtocol.h"
#include "thrift/lib/cpp2/protocol/Serializer.h"

using namespace boost::python;
using namespace facebook::logdevice;

namespace facebook { namespace logdevice { namespace configuration {
namespace nodes {

using apache::thrift::BinarySerializer;
using apache::thrift::SimpleJSONSerializer;

object parseException;
object apiException;

namespace {
NodesConfigurationAPI* getNodesConfigurationAPI(Client& client) {
  auto& client_impl = static_cast<ClientImpl&>(client);
  auto nc_api = client_impl.getNodesConfigurationAPI();
  ld_check(nc_api != nullptr);
  return nc_api;
}
} // namespace

static object get_nodes_configuration(Client& client) {
  auto nc_api = getNodesConfigurationAPI(client);
  auto nc = nc_api->getConfig();
  if (nc == nullptr) {
    auto exception = "NodesConfiguration is invalid or not yet ready";
    throw_python_exception(apiException, object(exception));
  }

  auto nc_binary = ThriftCodec::serialize<BinarySerializer>(
      NodesConfigurationThriftConverter::toThrift(*nc));

  return object(
      handle<>(PyBytes_FromStringAndSize(nc_binary.c_str(), nc_binary.size())));
}

static std::string nodes_configuration_to_json(std::string binary_config) {
  auto nc =
      ThriftCodec::deserialize<BinarySerializer, thrift::NodesConfiguration>(
          Slice::fromString(binary_config));
  if (nc == nullptr) {
    throw_python_exception(
        parseException,
        object("Failed to parse the NodesConfiguration thrift binary"));
  }
  return ThriftCodec::serialize<SimpleJSONSerializer>(*nc);
}

static object json_to_nodes_configuration(std::string json_config) {
  auto nc_thrift = ThriftCodec::deserialize<SimpleJSONSerializer,
                                            thrift::NodesConfiguration>(
      Slice::fromString(json_config));
  if (nc_thrift == nullptr) {
    throw_python_exception(
        parseException,
        object("Failed to parse the NodesConfiguration thrift json"));
  }

  auto nc_binary = ThriftCodec::serialize<BinarySerializer>(*nc_thrift);

  return object(
      handle<>(PyBytes_FromStringAndSize(nc_binary.c_str(), nc_binary.size())));
}

static std::shared_ptr<NodesConfiguration>
nodes_configuration_from_string(std::string binary_config) {
  auto nc_thrift =
      ThriftCodec::deserialize<BinarySerializer, thrift::NodesConfiguration>(
          Slice::fromString(binary_config));
  if (nc_thrift == nullptr) {
    auto exception = folly::sformat(
        "Failed to parse config binary: {}", errorStrings()[err].name);
    throw_python_exception(parseException, object(exception.c_str()));
  }

  auto nc = NodesConfigurationThriftConverter::fromThrift(*nc_thrift);
  if (nc == nullptr) {
    auto exception = folly::sformat(
        "Failed to parse config binary: {}", errorStrings()[err].name);
    throw_python_exception(parseException, object(exception.c_str()));
  }
  return nc;
}

static void overwrite_nodes_configuration(Client& client,
                                          std::string binary_config) {
  auto nc = nodes_configuration_from_string(binary_config);
  auto nc_api = getNodesConfigurationAPI(client);

  folly::Promise<Status> p;
  nc_api->overwrite(
      nc, [&](auto status, auto /* new_config */) { p.setValue(status); });
  if (p.getSemiFuture().get() != Status::OK) {
    auto exception = folly::sformat(
        "Overwriting config failed: {}", errorStrings()[err].name);
    throw_python_exception(apiException, object(exception.c_str()));
  }
}

static std::unique_ptr<NodesConfigurationStore>
build_nodes_configuration_store(const Configuration& config) {
  const auto& server_config = config.serverConfig();

  // 1. Build the settings instance that will be passed to the factory
  auto client_settings = std::make_unique<ClientSettingsImpl>();
  client_settings->getSettingsUpdater()->setFromConfig(
      server_config->getClientSettingsConfig());
  client_settings->set("admin-client-capabilities", "true");

  // 2. Build the NodesConfigurationStore
  std::shared_ptr<ZookeeperClientFactory> zk_client_factory =
      client_settings->getPluginRegistry()
          ->getSinglePlugin<ZookeeperClientFactory>(
              PluginType::ZOOKEEPER_CLIENT_FACTORY);
  auto store = NodesConfigurationStoreFactory::create(
      config,
      *client_settings->getSettings().get(),
      std::move(zk_client_factory));
  if (store == nullptr) {
    auto exception =
        folly::sformat("Failed to create NodesConfigurationStore: {}",
                       errorStrings()[err].name);
    throw_python_exception(apiException, object(exception.c_str()));
  }
  return store;
}

static void do_provision_write(const Configuration& config,
                               const NodesConfiguration& nc) {
  // 1. Serialize the NodesConfiguration
  auto nc_serialized = NodesConfigurationCodec::serialize(nc);
  if (nc_serialized.empty()) {
    auto exception = folly::sformat(
        "Failed to serialize NodesConfiguration: {}", errorStrings()[err].name);
    throw_python_exception(parseException, object(exception.c_str()));
  }

  // 2. Build the NCS
  auto store = build_nodes_configuration_store(config);

  // 4. Do the actual write
  auto st = store->updateConfigSync(
      nc_serialized, NodesConfigurationStore::Condition::overwrite());
  if (st != Status::OK) {
    auto exception =
        folly::sformat("Writing config failed: {}", errorStrings()[st].name);
    throw_python_exception(apiException, object(exception.c_str()));
  }
}

static void provision_empty_nodes_configuration(std::string config_json) {
  // 1. Parse the passed config json
  auto config = Configuration::fromJson(config_json, nullptr);
  if (config == nullptr) {
    auto exception = folly::sformat(
        "Failed to parse config json: {}", errorStrings()[err].name);
    throw_python_exception(parseException, object(exception.c_str()));
  }
  err = Status::OK;

  // 2. Do the actual write
  do_provision_write(*config, NodesConfiguration());
}

static void provision_initial_nodes_configuration(std::string config_json) {
  // 1. Parse the passed config json
  auto config = Configuration::fromJson(config_json, nullptr);
  if (config == nullptr) {
    auto exception = folly::sformat(
        "Failed to parse config json: {}", errorStrings()[err].name);
    throw_python_exception(parseException, object(exception.c_str()));
  }
  err = Status::OK;

  // 2. Convert the ServerConfig into a NodesConfiguration
  const auto& server_config = config->serverConfig();
  auto nc = NodesConfigLegacyConverter::fromLegacyNodesConfig(
      server_config->getNodesConfig(),
      server_config->getMetaDataLogsConfig(),
      server_config->getVersion());

  // 3. Do the actual write
  do_provision_write(*config, *nc);
}

static bool nodes_configuration_exists(std::string config_json) {
  // 1. Parse the passed config json
  auto config = Configuration::fromJson(config_json, nullptr);
  if (config == nullptr) {
    auto exception = folly::sformat(
        "Failed to parse config json: {}", errorStrings()[err].name);
    throw_python_exception(parseException, object(exception.c_str()));
  }

  // 2. Build the NCS
  auto store = build_nodes_configuration_store(*config);

  // 3. Check if the config exists
  auto status = store->getConfigSync(nullptr);
  if (status != Status::OK && status != Status::NOTFOUND) {
    auto exception = folly::sformat(
        "Getting config failed: {}", errorStrings()[status].name);
    throw_python_exception(apiException, object(exception.c_str()));
  }
  return status == Status::OK;
}

static std::string update_server_config_with_nodes_configuration(
    std::string server_config_str,
    std::string binary_nodes_configuration) {
  auto config = Configuration::fromJson(server_config_str, nullptr);
  if (config == nullptr) {
    auto exception = folly::sformat(
        "Failed to parse config json: {}", errorStrings()[err].name);
    throw_python_exception(parseException, object(exception.c_str()));
  }
  err = Status::OK;

  auto nc = nodes_configuration_from_string(binary_nodes_configuration);

  NodesConfig nodes_config;
  NodesConfigLegacyConverter::toLegacyNodesConfig(*nc, &nodes_config);

  auto metadata_cfg = config->serverConfig()->getMetaDataLogsConfig();
  NodesConfigLegacyConverter::toLegacyMetadataLogsConfig(*nc, metadata_cfg);

  auto new_server_config = config->serverConfig()
                               ->withNodes(std::move(nodes_config))
                               ->withMetaDataLogsConfig(metadata_cfg);
  return new_server_config->toString(
      config->logsConfig().get(), config->zookeeperConfig().get());
}

static std::string normalize_server_config(std::string server_config_str) {
  auto config = Configuration::fromJson(server_config_str, nullptr);
  if (config == nullptr) {
    auto exception = folly::sformat(
        "Failed to parse config json: {}", errorStrings()[err].name);
    throw_python_exception(parseException, object(exception.c_str()));
  }
  return config->toString();
}

static object
nodes_configuration_from_server_config(std::string server_config_str) {
  auto server_config = ServerConfig::fromJson(server_config_str);
  if (server_config == nullptr) {
    auto exception = folly::sformat(
        "Failed to parse config binary: {}", errorStrings()[err].name);
    throw_python_exception(
        parseException, object("failed to parse server config json"));
  }
  err = Status::OK;

  auto nc = NodesConfigLegacyConverter::fromLegacyNodesConfig(
      server_config->getNodesConfig(),
      server_config->getMetaDataLogsConfig(),
      server_config->getVersion());

  auto nc_thrift = NodesConfigurationThriftConverter::toThrift(*nc);
  auto nc_str = ThriftCodec::serialize<BinarySerializer>(nc_thrift);

  return object(
      handle<>(PyBytes_FromStringAndSize(nc_str.c_str(), nc_str.size())));
}

}}}} // namespace facebook::logdevice::configuration::nodes

BOOST_PYTHON_MODULE(nodes_configuration_manager) {
  using namespace facebook::logdevice::configuration::nodes;

  parseException =
      createExceptionClass("NodesConfigurationManagerParseException");
  apiException = createExceptionClass("NodesConfigurationManagerApiException");

  def("get_nodes_configuration", get_nodes_configuration, R"(
    Returns the serialized binary NodesConfiguration (without the
    NodesConfigurationWrapper) fetched from the NodesConfigurationAPI.

    @raises NodesConfigurationManagerApiException if the current config is
      invalid or not ready.
  )");
  def("nodes_configuration_to_json", nodes_configuration_to_json, R"(
    Converts a serialized binary NodesConfiguration (without the
    NodesConfigurationWrapper) into json.

    @param binary_config: The serialized binary NodesConfiguration

    @raises NodesConfigurationManagerParseException if the schema files are not
      readable or if the binary NodesConfiguration is invalid.
  )");
  def("json_to_nodes_configuration", json_to_nodes_configuration, R"(
    Converts the JSON NodesConfiguration (without the
    NodesConfigurationWrapper) into its binary format.

    @param json_config: The serialized JSON NodesConfiguration

    @raises NodesConfigurationManagerParseException if the schema files are not
      readable or if it fails to convert the JSON to the binary format.
  )");
  def("overwrite_nodes_configuration", overwrite_nodes_configuration, R"(

    Overwrites the NodesConfiguration using the NodesConfigurationAPI.

    @param client: A logdevice client on which the NodesConfigurationManager is
      running.

    @param binary_config: The binary serialized nodes configuration that will
      get written.

    @raises NodesConfigurationManagerParseException if the binary
      NodesConfiguration is invalid.

    @raises NodesConfigurationManagerApiException when the overwrite operation
      fails.

  )");

  def("update_server_config_with_nodes_configuration",
      update_server_config_with_nodes_configuration,
      R"(
    Returns a JSON ServerConfig that's the result of merging the passed
    JSON ServerConfig with the the passed binary NodesConfiguration.

    @param server_config_str: The JSON serialized ServerConfig

    @param binary_nodes_configuration: The binary serialized NodesConfiguration

    @raises NodesConfigurationManagerParseException if either the ServerConfig
      or the NodesConfiguration are not parsable.
  )");

  def("normalize_server_config",
      normalize_server_config,
      R"(
    Reserializes the passed server config with the cpp ServerConfig serializer
    to allow cleaner comparison with other cpp serialized server configs (e.g.
    the one returned by update_server_config_with_nodes_configuration).

    @param server_config_str: The JSON serialized ServerConfig

    @raises NodesConfigurationManagerParseException if the ServerConfig is not
      parsable.
  )");

  def("nodes_configuration_from_server_config",
      nodes_configuration_from_server_config,
      R"(
    Returns a binary serialized NodesConfiguration that's extracted from the
    passed JSON ServerConfig.

    @param server_config_str: The JSON serialized ServerConfig

    @raises NodesConfigurationManagerParseException if either the ServerConfig
      is not parsable.
  )");

  def("provision_empty_nodes_configuration",
      provision_empty_nodes_configuration,
      R"(

    Bootstraps the initial NodesConfigurationStore (e.g. creates znode / file)
    and then writes an initial empty NodesConfiguration.

    @param server_config_str: The JSON serialized ServerConfig

    @raises NodesConfigurationManagerParseException if the ServerConfig
      is not parsable.

    @raises NodesConfigurationManagerApiException when the provision operation
      fails or if it fails to create the NodesConfigurationStore.
  )");

  def("provision_initial_nodes_configuration",
      provision_initial_nodes_configuration,
      R"(

    Bootstraps the initial NodesConfigurationStore (e.g. creates znode / file)
    and then writes the initial NodesConfiguration from a ServerConfig. It's
    intended to be used during the migration period to move clusters to the NC
    for the first time. When all the clusters are on NC and new clusters are
    turned up with NC enabled, this binding will be removed.

    @param server_config_str: The JSON serialized ServerConfig


    @raises NodesConfigurationManagerParseException if either the ServerConfig
      or the NodesConfiguration are not parsable.

    @raises NodesConfigurationManagerApiException when the provision operation
      fails or if it fails to create the NodesConfigurationStore.

  )");

  def("nodes_configuration_exists",
      nodes_configuration_exists,
      R"(
    Check if the NodesConfiguration exists in zookeeper or not.

    @param server_config_str: The JSON serialized ServerConfig


    @raises NodesConfigurationManagerParseException if either the ServerConfig
      is not parsable.

    @raises NodesConfigurationManagerApiException when the read operation fails
      or if we fail to build the store.

  )");
}
