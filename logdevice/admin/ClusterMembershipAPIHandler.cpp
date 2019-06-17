/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/admin/ClusterMembershipAPIHandler.h"

#include "logdevice/admin/AdminAPIHandlerBase.h"
#include "logdevice/admin/AdminAPIUtils.h"
#include "logdevice/admin/cluster_membership/RemoveNodesHandler.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/configuration/nodes/NodesConfigurationManager.h"
#include "logdevice/common/membership/StorageStateTransitions.h"
#include "logdevice/common/types_internal.h"

namespace facebook { namespace logdevice {

using namespace facebook::logdevice::configuration::nodes;
using namespace facebook::logdevice::admin::cluster_membership;

folly::SemiFuture<std::unique_ptr<thrift::RemoveNodesResponse>>
ClusterMembershipAPIHandler::semifuture_removeNodes(
    std::unique_ptr<thrift::RemoveNodesRequest> req) {
  if (auto failed = failIfMMDisabled(); failed) {
    return *failed;
  }

  auto nodes_configuration = processor_->getNodesConfiguration();
  const auto& cluster_state = processor_->cluster_state_;

  RemoveNodesHandler handler{};
  auto res = handler.buildNodesConfigurationUpdates(
      req->node_filters, *nodes_configuration, *cluster_state);

  if (res.hasError()) {
    return folly::makeSemiFuture<std::unique_ptr<thrift::RemoveNodesResponse>>(
        std::move(res).error());
  }

  auto remove_result = std::move(res).value();

  return applyNodesConfigurationUpdates(std::move(remove_result.update))
      .via(this->getThreadManager())
      .thenValue(
          [removed_nodes = std::move(remove_result.to_be_removed)](
              std::shared_ptr<const NodesConfiguration>
                  nodes_configuration) mutable
          -> folly::SemiFuture<std::unique_ptr<thrift::RemoveNodesResponse>> {
            auto resp = std::make_unique<thrift::RemoveNodesResponse>();
            resp->set_removed_nodes(std::move(removed_nodes));
            resp->set_new_nodes_configuration_version(
                nodes_configuration->getVersion().val());
            return std::move(resp);
          });
}

folly::SemiFuture<std::shared_ptr<const NodesConfiguration>>
ClusterMembershipAPIHandler::applyNodesConfigurationUpdates(
    NodesConfiguration::Update update) {
  auto ncm = processor_->getNodesConfigurationManager();
  ld_check(ncm);

  if (update.empty()) {
    return ncm->getConfig();
  }

  auto [res_promise, res_future] =
      folly::makePromiseContract<std::shared_ptr<const NodesConfiguration>>();
  ncm->update(std::move(update),
              [promise = std::move(res_promise)](
                  Status status,
                  std::shared_ptr<const NodesConfiguration> cfg) mutable {
                if (status != Status::OK) {
                  // TODO caputre and pass the actual NodesConfiguration failure
                  // reason.
                  thrift::NodesConfigurationManagerError err;
                  err.set_message(error_description(status));
                  err.set_error_code(static_cast<int32_t>(status));
                  promise.setException(std::move(err));
                  return;
                };
                promise.setValue(std::move(cfg));
              });

  return std::move(res_future);
}

}} // namespace facebook::logdevice
