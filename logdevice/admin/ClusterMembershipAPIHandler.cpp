/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/admin/ClusterMembershipAPIHandler.h"

#include <chrono>

#include <folly/futures/Retrying.h>

#include "logdevice/admin/AdminAPIHandlerBase.h"
#include "logdevice/admin/AdminAPIUtils.h"
#include "logdevice/admin/cluster_membership/AddNodesHandler.h"
#include "logdevice/admin/cluster_membership/BootstrapClusterHandler.h"
#include "logdevice/admin/cluster_membership/BumpNodeGenerationHandler.h"
#include "logdevice/admin/cluster_membership/MarkShardsAsProvisionedHandler.h"
#include "logdevice/admin/cluster_membership/RemoveNodesHandler.h"
#include "logdevice/admin/cluster_membership/UpdateNodesHandler.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/ThriftCodec.h"
#include "logdevice/common/configuration/nodes/NodeIndicesAllocator.h"
#include "logdevice/common/configuration/nodes/NodesConfigurationManager.h"
#include "logdevice/common/membership/StorageStateTransitions.h"
#include "logdevice/common/types_internal.h"

namespace facebook { namespace logdevice {

using namespace facebook::logdevice::configuration::nodes;
using namespace facebook::logdevice::admin::cluster_membership;

namespace {
/**
 * Returns a retrying policy used by folly::futures::retrying. The policy
 * VERSION_MISMATCH errors 5 times.
 */
std::function<folly::Future<bool>(size_t, const folly::exception_wrapper&)>
get_retrying_policy() {
  return folly::futures::retryingPolicyCappedJitteredExponentialBackoff(
      /*max_tries=*/5,
      /*backoff_min=*/std::chrono::milliseconds(50),
      /*backoff_max*/ std::chrono::milliseconds(50),
      /*jitter_param=*/0.5,
      folly::ThreadLocalPRNG(),
      [](size_t, const folly::exception_wrapper& wrapper) {
        // Retry as long as it's an NCM VERSION_MISMATCH error. Don't retry
        // otherwise.
        auto ex =
            wrapper.get_exception<thrift::NodesConfigurationManagerError>();
        return (ex != nullptr &&
                *(ex->get_error_code()) ==
                    static_cast<uint32_t>(Status::VERSION_MISMATCH));
      });
}

template <class T>
std::string debugString(T obj) {
  return facebook::logdevice::ThriftCodec::serialize<
      apache::thrift::SimpleJSONSerializer>(obj);
}

} // namespace

folly::SemiFuture<std::unique_ptr<thrift::RemoveNodesResponse>>
ClusterMembershipAPIHandler::semifuture_removeNodes(
    std::unique_ptr<thrift::RemoveNodesRequest> request) {
  if (auto failed = failIfMMDisabled(); failed) {
    return *failed;
  }
  return folly::futures::retrying(
      get_retrying_policy(),
      [req = std::move(request),
       processor = processor_,
       thread_manager = getThreadManager()](size_t trial)
          -> folly::SemiFuture<std::unique_ptr<thrift::RemoveNodesResponse>> {
        ld_info("Handling removeNodes request (trial #%ld): %s",
                trial,
                debugString(*req).c_str());
        auto nodes_configuration = processor->getNodesConfiguration();
        const auto& cluster_state = processor->cluster_state_;

        RemoveNodesHandler handler{};
        auto res = handler.buildNodesConfigurationUpdates(
            req->node_filters, *nodes_configuration, *cluster_state);

        if (res.hasError()) {
          return folly::makeSemiFuture<
              std::unique_ptr<thrift::RemoveNodesResponse>>(
              std::move(res).error());
        }

        auto remove_result = std::move(res).value();
        return applyNodesConfigurationUpdates(
                   processor, std::move(remove_result.update))
            .via(thread_manager)
            .thenValue([removed_nodes = std::move(remove_result.to_be_removed)](
                           std::shared_ptr<const NodesConfiguration>
                               nodes_configuration) mutable {
              auto resp = std::make_unique<thrift::RemoveNodesResponse>();
              resp->set_removed_nodes(std::move(removed_nodes));
              resp->set_new_nodes_configuration_version(
                  nodes_configuration->getVersion().val());
              return resp;
            });
      });
}

folly::SemiFuture<std::unique_ptr<thrift::AddNodesResponse>>
ClusterMembershipAPIHandler::semifuture_addNodes(
    std::unique_ptr<thrift::AddNodesRequest> request) {
  if (auto failed = failIfMMDisabled(); failed) {
    return *failed;
  }

  return folly::futures::retrying(
      get_retrying_policy(),
      [req = std::move(request),
       processor = processor_,
       thread_manager = getThreadManager()](size_t trial)
          -> folly::SemiFuture<std::unique_ptr<thrift::AddNodesResponse>> {
        ld_info("Handling addNodes request (trial #%ld): %s",
                trial,
                debugString(*req).c_str());
        auto nodes_configuration = processor->getNodesConfiguration();

        AddNodesHandler handler{};
        auto res =
            handler.buildNodesConfigurationUpdates(req->new_node_requests,
                                                   *nodes_configuration,
                                                   NodeIndicesAllocator{});

        if (res.hasError()) {
          return folly::makeSemiFuture<
              std::unique_ptr<thrift::AddNodesResponse>>(
              std::move(res).error());
        }

        auto add_result = std::move(res).value();

        return applyNodesConfigurationUpdates(
                   processor, std::move(add_result.update))
            .via(thread_manager)
            .thenValue(
                [added_nodes = std::move(add_result.to_be_added)](
                    std::shared_ptr<const NodesConfiguration> new_cfg) mutable {
                  auto resp = std::make_unique<thrift::AddNodesResponse>();
                  for (const auto& added : added_nodes) {
                    thrift::NodeConfig node_cfg;
                    fillNodeConfig(node_cfg, added, *new_cfg);
                    resp->added_nodes.push_back(std::move(node_cfg));
                    resp->set_new_nodes_configuration_version(
                        new_cfg->getVersion().val());
                  }
                  return resp;
                });
      });
}

folly::SemiFuture<std::unique_ptr<thrift::UpdateNodesResponse>>
ClusterMembershipAPIHandler::semifuture_updateNodes(
    std::unique_ptr<thrift::UpdateNodesRequest> request) {
  if (auto failed = failIfMMDisabled(); failed) {
    return *failed;
  }

  return folly::futures::retrying(
      get_retrying_policy(),
      [req = std::move(request),
       processor = processor_,
       thread_manager = getThreadManager()](size_t trial)
          -> folly::SemiFuture<std::unique_ptr<thrift::UpdateNodesResponse>> {
        ld_info("Handling updateNodes request (trial #%ld): %s",
                trial,
                debugString(*req).c_str());
        auto nodes_configuration = processor->getNodesConfiguration();

        UpdateNodesHandler handler{};
        auto res = handler.buildNodesConfigurationUpdates(
            req->get_node_requests(), *nodes_configuration);

        if (res.hasError()) {
          return folly::makeSemiFuture<
              std::unique_ptr<thrift::UpdateNodesResponse>>(
              std::move(res).error());
        }

        auto update_result = std::move(res).value();

        return applyNodesConfigurationUpdates(
                   processor, std::move(update_result.update))
            .via(thread_manager)
            .thenValue(
                [updated_nodes = std::move(update_result.to_be_updated)](
                    std::shared_ptr<const NodesConfiguration> new_cfg) mutable {
                  auto resp = std::make_unique<thrift::UpdateNodesResponse>();
                  for (auto updated : updated_nodes) {
                    thrift::NodeConfig node_cfg;
                    fillNodeConfig(node_cfg, updated, *new_cfg);
                    resp->updated_nodes.push_back(std::move(node_cfg));
                    resp->set_new_nodes_configuration_version(
                        new_cfg->getVersion().val());
                  }
                  return resp;
                });
      });
}

folly::SemiFuture<std::unique_ptr<thrift::MarkShardsAsProvisionedResponse>>
ClusterMembershipAPIHandler::semifuture_markShardsAsProvisioned(
    std::unique_ptr<thrift::MarkShardsAsProvisionedRequest> request) {
  if (auto failed = failIfMMDisabled(); failed) {
    return *failed;
  }

  return folly::futures::retrying(
      get_retrying_policy(),
      [req = std::move(request),
       processor = processor_,
       thread_manager = getThreadManager()](size_t trial)
          -> folly::SemiFuture<
              std::unique_ptr<thrift::MarkShardsAsProvisionedResponse>> {
        ld_info("Handling markShardsAsProvisioned request (trial #%ld): %s",
                trial,
                debugString(*req).c_str());
        auto nodes_configuration = processor->getNodesConfiguration();

        MarkShardsAsProvisionedHandler handler{};
        auto update_result = handler.buildNodesConfigurationUpdates(
            req->get_shards(), *nodes_configuration);

        return applyNodesConfigurationUpdates(
                   processor, std::move(update_result.update))
            .via(thread_manager)
            .thenValue([shards = std::move(update_result.updated_shards)](
                           std::shared_ptr<const NodesConfiguration>
                               new_cfg) mutable {
              auto resp =
                  std::make_unique<thrift::MarkShardsAsProvisionedResponse>();
              resp->set_updated_shards(mkShardSet(std::move(shards)));
              resp->set_new_nodes_configuration_version(
                  new_cfg->getVersion().val());
              return resp;
            });
      });
}

folly::SemiFuture<std::unique_ptr<thrift::BumpGenerationResponse>>
ClusterMembershipAPIHandler::semifuture_bumpNodeGeneration(
    std::unique_ptr<thrift::BumpGenerationRequest> request) {
  if (auto failed = failIfMMDisabled(); failed) {
    return *failed;
  }
  return folly::futures::retrying(
      get_retrying_policy(),
      [req = std::move(request),
       processor = processor_,
       thread_manager = getThreadManager()](size_t trial)
          -> folly::SemiFuture<
              std::unique_ptr<thrift::BumpGenerationResponse>> {
        ld_info("Handling bumpNodeGeneration request (trial #%ld): %s",
                trial,
                debugString(*req).c_str());
        auto nodes_configuration = processor->getNodesConfiguration();

        BumpNodeGenerationHandler handler{};
        auto bump_result = handler.buildNodesConfigurationUpdates(
            req->node_filters, *nodes_configuration);

        return applyNodesConfigurationUpdates(
                   processor, std::move(bump_result.update))
            .via(thread_manager)
            .thenValue([bumped_nodes = std::move(bump_result.to_be_bumped)](
                           std::shared_ptr<const NodesConfiguration>
                               nodes_configuration) mutable {
              auto resp = std::make_unique<thrift::BumpGenerationResponse>();
              resp->set_bumped_nodes(std::move(bumped_nodes));
              resp->set_new_nodes_configuration_version(
                  nodes_configuration->getVersion().val());
              return resp;
            });
      });
}

folly::SemiFuture<std::unique_ptr<thrift::BootstrapClusterResponse>>
ClusterMembershipAPIHandler::semifuture_bootstrapCluster(
    std::unique_ptr<thrift::BootstrapClusterRequest> request) {
  return folly::futures::retrying(
      get_retrying_policy(),
      [req = std::move(request),
       processor = processor_,
       thread_manager = getThreadManager()](size_t trial)
          -> folly::SemiFuture<
              std::unique_ptr<thrift::BootstrapClusterResponse>> {
        ld_info("Handling bootstrapCluster request (trial #%ld): %s",
                trial,
                debugString(*req).c_str());
        auto nodes_configuration = processor->getNodesConfiguration();

        BootstrapClusterHandler handler{};
        auto res =
            handler.buildNodesConfigurationUpdates(*req, *nodes_configuration);

        if (res.hasError()) {
          return folly::makeSemiFuture<
              std::unique_ptr<thrift::BootstrapClusterResponse>>(
              std::move(res).error());
        }

        auto update_result = std::move(res).value();

        return applyNodesConfigurationUpdates(
                   processor, std::move(update_result.update))
            .via(thread_manager)
            .thenValue(
                [](std::shared_ptr<const NodesConfiguration> new_cfg) mutable
                -> folly::SemiFuture<
                    std::unique_ptr<thrift::BootstrapClusterResponse>> {
                  auto resp =
                      std::make_unique<thrift::BootstrapClusterResponse>();
                  resp->set_new_nodes_configuration_version(
                      new_cfg->getVersion().val());
                  return std::move(resp);
                });
      });
}

folly::SemiFuture<std::shared_ptr<const NodesConfiguration>>
ClusterMembershipAPIHandler::applyNodesConfigurationUpdates(
    Processor* processor,
    NodesConfiguration::Update update) {
  auto ncm = processor->getNodesConfigurationManager();
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
