/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/thrift/ThriftRouter.h"

#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/nodes/ServerAddressRouter.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/if/gen-cpp2/LogDeviceAPIAsyncClient.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/thrift/ThriftClientFactory.h"

using facebook::logdevice::thrift::LogDeviceAPIAsyncClient;

namespace facebook { namespace logdevice {

NcmThriftRouter::NcmThriftRouter(
    ThriftClientFactory* client_factory,
    UpdateableSettings<Settings> settings,
    std::shared_ptr<UpdateableNodesConfiguration> nodes)
    : client_factory_(client_factory),
      settings_(std::move(settings)),
      nodes_(std::move(nodes)) {
  ld_check(client_factory_);
  ld_check(nodes_);
}

std::unique_ptr<LogDeviceAPIAsyncClient>
NcmThriftRouter::getApiClient(node_index_t nid) {
  configuration::nodes::ServerAddressRouter router;
  const auto* node_svc = nodes_->get()->getNodeServiceDiscovery(nid);
  auto maybe_address = node_svc != nullptr
      ? router.getThriftAddress(nid, *node_svc, *settings_.get())
      : folly::none;
  if (!maybe_address.hasValue() || !maybe_address->valid()) {
    err = E::NOTINCONFIG;
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    1,
                    "Cannot find Thrift address to connect to node N%d",
                    nid);
    return nullptr;
  }
  auto callback_executor = Worker::onThisThread(/*enforce_worker*/ false);
  return client_factory_->createClient<LogDeviceAPIAsyncClient>(
      maybe_address->getSocketAddress(), callback_executor);
}
}} // namespace facebook::logdevice
