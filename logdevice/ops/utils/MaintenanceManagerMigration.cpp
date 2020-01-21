/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <boost/core/null_deleter.hpp>
#include <boost/python.hpp>

#include "logdevice/include/Err.h"
#include "logdevice/ops/utils/MaintenanceManagerUtils.h"

using namespace boost::python;

BOOST_PYTHON_MODULE(maintenance_manager_migration) {
  using namespace facebook::logdevice;

  auto status = enum_<Status>("Status");
  status.value("OK", E::OK);
  status.value("FAILED", E::FAILED);
  status.value("PARTIAL", E::PARTIAL);

  def("migrateToMaintenanceManager",
      maintenance::migrateToMaintenanceManager,
      R"DOC(
      migrateToMaintenanceManager(Client)
      Goes over EventLogRebuildingSet for the cluster and creates corresponding
      maintenance for all shards in the Maintenance Log
    )DOC");
}
