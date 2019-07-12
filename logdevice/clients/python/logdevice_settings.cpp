/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <boost/python.hpp>

#include "logdevice/admin/settings/AdminServerSettings.h"
#include "logdevice/clients/python/util/util.h"
#include "logdevice/common/plugin/CommonBuiltinPlugins.h"
#include "logdevice/common/plugin/DynamicPluginLoader.h"
#include "logdevice/common/plugin/PluginRegistry.h"
#include "logdevice/common/plugin/StaticPluginLoader.h"
#include "logdevice/common/settings/GossipSettings.h"
#include "logdevice/common/settings/RebuildingSettings.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/settings/SettingsUpdater.h"
#include "logdevice/common/settings/UpdateableSettings.h"
#include "logdevice/lib/ClientPluginHelper.h"
#include "logdevice/server/ServerSettings.h"
#include "logdevice/server/locallogstore/LocalLogStoreSettings.h"
#include "logdevice/server/locallogstore/RocksDBSettings.h"

using namespace boost::python;
using namespace facebook::logdevice;

std::unordered_map<std::string, std::string>
dict_to_map(boost::python::dict input) {
  std::unordered_map<std::string, std::string> input_settings;
  stl_input_iterator<object> begin(input.keys()), end;
  std::for_each(begin, end, [&](object key) {
    auto key_str = extract_string(key, "settings key");
    auto value =
        extract_string(input.get(key),
                       ("settings value for key\"" + key_str + "\"").c_str(),
                       true /* coerce */);
    input_settings.insert(std::make_pair(key_str, value));
  });

  return input_settings;
}

void validate_server_settings(boost::python::dict input) {
  if (input.is_none()) {
    return;
  }

  UpdateableSettings<ServerSettings> server_settings;
  UpdateableSettings<RebuildingSettings> rebuilding_settings;
  UpdateableSettings<LocalLogStoreSettings> locallogstore_settings;
  UpdateableSettings<GossipSettings> gossip_settings;
  UpdateableSettings<Settings> settings;
  UpdateableSettings<RocksDBSettings> rocksdb_settings;
  UpdateableSettings<AdminServerSettings> admin_server_settings;

  auto settings_updater = std::make_shared<SettingsUpdater>();
  settings_updater->registerSettings(server_settings);
  settings_updater->registerSettings(rebuilding_settings);
  settings_updater->registerSettings(locallogstore_settings);
  settings_updater->registerSettings(gossip_settings);
  settings_updater->registerSettings(settings);
  settings_updater->registerSettings(rocksdb_settings);
  settings_updater->registerSettings(admin_server_settings);

  std::shared_ptr<PluginRegistry> plugin_registry =
      std::make_shared<PluginRegistry>(
          createPluginVector<DynamicPluginLoader,
                             StaticPluginLoader,
                             BuiltinPluginProvider>());
  plugin_registry->addOptions(settings_updater.get());

  auto input_settings = dict_to_map(input);
  try {
    settings_updater->validateFromConfig(input_settings, SettingFlag::SERVER);
  } catch (const boost::program_options::error& ex) {
    throw_python_exception(PyExc_ValueError, str(ex.what()));
  }
}

void validate_client_settings(boost::python::dict input) {
  if (input.is_none()) {
    return;
  }

  UpdateableSettings<RebuildingSettings> rebuilding_settings;
  UpdateableSettings<GossipSettings> gossip_settings;
  UpdateableSettings<Settings> settings;

  auto settings_updater = std::make_shared<SettingsUpdater>();
  settings_updater->registerSettings(rebuilding_settings);
  settings_updater->registerSettings(gossip_settings);
  settings_updater->registerSettings(settings);

  std::shared_ptr<PluginRegistry> plugin_registry =
      std::make_shared<PluginRegistry>(getClientPluginProviders());
  plugin_registry->addOptions(settings_updater.get());

  auto input_settings = dict_to_map(input);
  try {
    settings_updater->validateFromConfig(input_settings, SettingFlag::CLIENT);
  } catch (const boost::program_options::error& ex) {
    throw_python_exception(PyExc_ValueError, str(ex.what()));
  }
}

BOOST_PYTHON_MODULE(settings) {
  def("validate_server_settings", &validate_server_settings);
  def("validate_client_settings", &validate_client_settings);
}
