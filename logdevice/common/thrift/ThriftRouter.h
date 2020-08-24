/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <memory>

#include "logdevice/common/NodeID.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/settings/UpdateableSettings.h"

namespace facebook { namespace logdevice {

struct Settings;
class ThriftClientFactory;

namespace thrift {
class LogDeviceAPIAsyncClient;
} // namespace thrift

/**
 * Provides API to create Thrift clients connected to specific LogDevice nodes.
 * This interface bridges the gap between core Thrift (operating on physical
 * addresses) and the rest of LogDevice (mostly using nodes' ids and own service
 * discovery).
 */
class ThriftRouter {
 public:
  /**
   * Creates a new client for Thrift API on the node with given ID.
   *
   * @return New Thrift client or nullptr if unable to router.
   */
  virtual std::unique_ptr<thrift::LogDeviceAPIAsyncClient>
      getApiClient(node_index_t) = 0;

  virtual ~ThriftRouter() = default;
};

/**
 * NCM-aware implementation of ThriftRouter which uses dynamic service discovery
 * to resolve nodes' IDs into physical addresses.
 */
class NcmThriftRouter : public ThriftRouter {
 public:
  /**
   * Creates a new router.
   *
   * @param client_factory Factory to create new Thrift clients
   * @param settings       Global settings to be used for routing decisions
   */
  NcmThriftRouter(ThriftClientFactory* client_factory,
                  UpdateableSettings<Settings> settings,
                  std::shared_ptr<UpdateableNodesConfiguration> nodes);

  std::unique_ptr<thrift::LogDeviceAPIAsyncClient>
      getApiClient(node_index_t) override;

 private:
  ThriftClientFactory* client_factory_;
  UpdateableSettings<Settings> settings_;
  // TODO(T70882102): Close connectons whose peer addrees has changed
  std::shared_ptr<UpdateableNodesConfiguration> nodes_;
};

}} // namespace facebook::logdevice
