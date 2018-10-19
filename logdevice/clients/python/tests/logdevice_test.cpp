/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/clients/python/util/util.h"
#include "logdevice/include/Client.h"
#include "logdevice/test/utils/IntegrationTestUtils.h"

using namespace boost::python;
using namespace facebook::logdevice;
using namespace facebook::logdevice::IntegrationTestUtils;

namespace {
void set_root_path(ClusterFactory* factory, std::string path) {
  factory->setRootPath(path);
}

void set_rocks_db_type(ClusterFactory* factory, RocksDBType rocks_db_type) {
  factory->setRocksDBType(rocks_db_type);
}

void enable_logsconfig_manager(ClusterFactory* factory) {
  factory->enableLogsConfigManager();
}

boost::shared_ptr<Cluster> create_cluster(ClusterFactory* factory, int nodes) {
  auto cluster = factory->create(nodes);
  return boost::shared_ptr<Cluster>(cluster.release());
}

boost::shared_ptr<Cluster> create_from_json(ClusterFactory* factory,
                                            std::string json) {
  auto cluster = factory->create(
      *(Configuration::fromJson(json, nullptr, nullptr, ConfigParserOptions())
            .get()));
  return boost::shared_ptr<Cluster>(cluster.release());
}

boost::shared_ptr<Client> create_client(Cluster* cluster) {
  auto client = cluster->createClient();
  return make_boost_shared_ptr_from_std_shared_ptr(client);
}

list logs_config(Cluster* cluster) {
  object json = import("json");
  return extract<list>(json.attr("loads")(
      cluster->getConfig()->get()->logsConfig()->toString()));
}

dict server_config(Cluster* cluster) {
  object json = import("json");
  return extract<dict>(json.attr("loads")(
      cluster->getConfig()->get()->serverConfig()->toString()));
}

} // namespace

BOOST_PYTHON_MODULE(test) {
  enum_<RocksDBType>("RocksDBType")
      .value("SINGLE", RocksDBType::SINGLE)
      .value("PARTITIONED", RocksDBType::PARTITIONED);

  class_<ClusterFactory>("ClusterFactory")
      .def("set_root_path", &set_root_path)
      .def("set_rocks_db_type", &set_rocks_db_type)
      .def("enable_logsconfig_manager", &enable_logsconfig_manager)
      .def("create", &create_cluster)
      .def("create_from_json", &create_from_json);

  class_<Cluster, boost::shared_ptr<Cluster>, boost::noncopyable>(
      "Cluster", no_init)
      .def("stop", &Cluster::stop)
      .def("create_client", &create_client)
      .add_property("logs_config", &logs_config)
      .add_property("server_config", &server_config)
      .add_property("config_path", &Cluster::getConfigPath);
}
