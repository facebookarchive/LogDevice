/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/admin/MaintenanceAPIHandler.h"

#include "logdevice/admin/maintenance/APIUtils.h"
#include "logdevice/admin/maintenance/MaintenanceLogWriter.h"
#include "logdevice/admin/maintenance/MaintenanceManager.h"
#include "logdevice/common/request_util.h"

using namespace facebook::logdevice::thrift;
using namespace facebook::logdevice::maintenance;

using MaintenanceOut =
    folly::Expected<ClusterMaintenanceState, MaintenanceError>;

using ListMaintenanceDefs =
    folly::Expected<std::vector<MaintenanceDefinition>, MaintenanceError>;

namespace facebook { namespace logdevice {
// get maintenances
folly::SemiFuture<std::unique_ptr<MaintenanceDefinitionResponse>>
MaintenanceAPIHandler::semifuture_getMaintenances(
    std::unique_ptr<MaintenancesFilter> request) {
  auto failed = failIfMMDisabled();
  if (failed) {
    return *failed;
  }
  if (request == nullptr) {
    return InvalidRequest("Cannot accept nullptr filter");
  }
  /**
   * We should always get the latest ClusterMaintenanceState directly from the
   * ClusterMaintenanceStateMachine. We know that the maintenance manager
   * might be already running with an older version until the next run but if
   * we used that version then the user will be confused. A user who just
   * added a maintenance should find it in the results of this call.
   *
   * The side-effect of this is that we will return a list of maintenances
   * that might not include some of the currently active maintenances because
   * maintenance manager still didn't react to the new one.
   *
   * The maintenance is enriched with the safety check results cached in
   * maintenance manager.
   *
   */
  ld_check(maintenance_manager_);
  return maintenance_manager_->getLatestMaintenanceState()
      .via(this->getThreadManager())
      .thenValue(
          // We get expected<vec<definition>, maintenance error>
          [request = std::move(request)](
              auto&& value) -> std::unique_ptr<MaintenanceDefinitionResponse> {
            if (value.hasError()) {
              // The exception will be passed through to the user of the API on
              // the client side as expected.
              value.error().throwThriftException();
              ld_assert(false);
            }
            auto v = std::make_unique<MaintenanceDefinitionResponse>();
            v->set_maintenances(
                APIUtils::filterMaintenances(*request, value.value()));
            return v;
          });
}

folly::SemiFuture<ListMaintenanceDefs>
MaintenanceAPIHandler::applyAndGetMaintenances(
    std::vector<MaintenanceDefinition> defs) {
  auto worker_type = ClusterMaintenanceStateMachine::workerType(processor_);
  auto worker_index =
      worker_id_t(ClusterMaintenanceStateMachine::getWorkerIndex(
          processor_->getWorkerCount(worker_type)));
  // We need to execute getting the state and finding existing maintenance on
  // the RSM worker to ensure consistency.
  // The flow is `defs -> RSM -> new_defs+existing_defs`
  // Then we will augment the last impact from the maintenance manager after we
  // get that list.
  // Callback that fulfills the promise on the worker thread of the state
  // machine
  auto cb = [defs = std::move(defs),
             this](folly::Promise<ListMaintenanceDefs> promise) mutable {
    // NOTE: This code runs on the RSM's Worker.
    auto sm = Worker::onThisThread()->cluster_maintenance_state_machine_;
    if (!sm) {
      ld_warning("No ClusterMaintenanceStateMachine available on this worker. "
                 "Returning E::NOTREADY.");
      promise.setValue(folly::makeUnexpected(MaintenanceError(
          E::NOTREADY,
          "No ClusterMaintenanceStateMachine running on this machine!")));
      return;
    }
    if (!sm->isFullyLoaded()) {
      ld_warning("ClusterMaintenanceStateMachine is not fully loaded yet. "
                 "Cannot fulfill requests.");
      promise.setValue(folly::makeUnexpected(MaintenanceError(
          E::NOTREADY, "The ClusterMaintenanceState is not fully loaded yet")));
      return;
    }
    const auto& current_state = sm->getCurrentState();
    std::vector<MaintenanceDefinition> existing;
    std::vector<MaintenanceDefinition> new_defs;
    for (auto& def : defs) {
      // Do we have an equivalent maintenance already? in this case we
      // return the existing ones.
      auto found = APIUtils::findEquivalentMaintenance(
          current_state.get_maintenances(), def);
      if (found) {
        // ignore the input's group_id and use the one we have in the
        // state.
        existing.push_back(std::move(*found));
      } else {
        // This is new.
        new_defs.push_back(std::move(def));
      }
    }
    // Let's write the new maintenances and get the combined results back.
    // NOTE: The continuation is also executed on the RSM's worker thread.
    applyAndMerge(new_defs, existing)
        .toUnsafeFuture()
        .thenValue([this, promise = std::move(promise)](
                       ListMaintenanceDefs&& value) mutable {
          if (value.hasError()) {
            promise.setValue(folly::makeUnexpected(value.error()));
          }
          // Augment the combined results with safety checker information. This
          // gets executed on the MaintenanceManager's execution context.
          maintenance_manager_
              ->augmentWithProgressInfo(std::move(value).value())
              // inline future to promise fulfillment. Runs on MM's execution
              // context.
              .toUnsafeFuture()
              .thenValue(
                  [promise = std::move(promise)](auto&& augmented) mutable {
                    promise.setValue(std::move(augmented));
                  });
        });
  };

  // Dispatch the operation to be executed on the RSM's worker thread.
  return fulfill_on_worker<ListMaintenanceDefs>(
      processor_, worker_index, worker_type, std::move(cb));
}

folly::SemiFuture<ListMaintenanceDefs> MaintenanceAPIHandler::applyAndMerge(
    std::vector<MaintenanceDefinition> new_defs,
    std::vector<MaintenanceDefinition> existing) {
  if (new_defs.empty()) {
    // No need to write a delta in this case really. No new definitions.
    return existing;
  }
  MaintenanceDelta delta;
  delta.set_apply_maintenances(std::move(new_defs));
  return MaintenanceLogWriter::writeDelta(processor_, delta)
      // We want the continuation on the same thread to reduce unnecessary
      // context switching since the contiuation is a very simple merge.
      .toUnsafeFuture()
      .thenValue([existing = std::move(existing), delta](
                     MaintenanceOut&& value) mutable -> ListMaintenanceDefs {
        if (value.hasError()) {
          return folly::makeUnexpected(value.error());
        }
        // Now let's get the newly applied maintenances and combine with
        // existing.
        // We are iterating over the input maintenances we were trying to
        // create. Then we will find them again in the returned state by
        // ID.
        for (const auto& def : delta.get_apply_maintenances()) {
          // find that maintenance in the state and push that to the
          // existing list.
          auto found = APIUtils::findMaintenanceByID(
              def.group_id_ref().value(), value->get_maintenances());
          // We should always find the newly added maintenance if the
          // operation didn't fail in the RSM.
          ld_check(found);
          existing.push_back(std::move(*found));
        }
        // augment everything with safety check results.
        return existing;
      });
}

// apply a new maintenance
folly::SemiFuture<std::unique_ptr<MaintenanceDefinitionResponse>>
MaintenanceAPIHandler::semifuture_applyMaintenance(
    std::unique_ptr<MaintenanceDefinition> definition) {
  auto failed = failIfMMDisabled();
  if (failed) {
    return *failed;
  }
  if (definition == nullptr) {
    return InvalidRequest("MaintenanceDefinition is required");
  }
  ld_check(maintenance_manager_);
  /**
   * Steps of creating a maintenance.
   *   1- Perform basic input validation on the supplied maintenance
   *   definition
   *   2- Expand the maintenance into N maintenances in a list and for
   * each maintenance ensure that all the required fields that we expect
   * per maintenance are set.
   *     - All nodes _must_ have the node_index set. This is the only
   * field needed in NodeID and the rest will be unset.
   *     - Shards with shard_index = -1 are expanded into a ShardSet
   * according to the number of shards per node as configured at the time
   * of the request.
   *     - If group is True, a single maintenance definition is created.
   *     - If group is False, we will create a list of maintenance
   * definitions where each has all the shards and sequencers for every
   * given node that matches. The group in this case will try to achieve
   * all operations (storage and sequencer) on a single node in one go.
   * Each group gets its own ID.
   *   3- We try to find if we have existing groups that are "equivalent" to
   *   some of the expanded ones. If yes, we won't create them again, Instead
   * we will return the existing definitions for these.
   *   4- For the new maintenances, We submit the list of groups as a single
   * Delta to the state machine and wait until we hear back from the RSM.
   *   5- For the returned maintenances and existing ones, we augment the
   * definitions with the last_check_impact_result from the maintenance
   * manager if we have any. 6- Combine results and return.
   */

  // Step (1)
  auto validation = APIUtils::validateDefinition(*definition);
  if (validation) {
    return *validation;
  }
  auto nodes_config = processor_->getNodesConfiguration();
  // Step (2)
  folly::Expected<std::vector<MaintenanceDefinition>, InvalidRequest>
      expanded_maintenances =
          APIUtils::expandMaintenances(*definition, nodes_config);
  if (expanded_maintenances.hasError()) {
    return expanded_maintenances.error();
  }
  // Step (3, 4, 5, and 6)
  return applyAndGetMaintenances(std::move(expanded_maintenances).value())
      .toUnsafeFuture()
      .thenValue([](ListMaintenanceDefs&& result) {
        if (result.hasError()) {
          // Throw the right thrift exception.
          result.error().throwThriftException();
          ld_assert(false);
        }
        auto v = std::make_unique<MaintenanceDefinitionResponse>();
        v->set_maintenances(result.value());
        return v;
      });
}

// remove a maintenance
folly::SemiFuture<std::unique_ptr<RemoveMaintenancesResponse>>
MaintenanceAPIHandler::semifuture_removeMaintenances(
    std::unique_ptr<RemoveMaintenancesRequest> request) {
  auto failed = failIfMMDisabled();
  if (failed) {
    return *failed;
  }
  if (request == nullptr ||
      !APIUtils::isMaintenancesFilterSet(request->get_filter())) {
    return InvalidRequest("Cannot accept nullptr/empty "
                          "RemoveMaintenanceRequest/MaintenancesFilter");
  }

  ld_check(maintenance_manager_);
  /*
   */
  MaintenancesFilter filter = request->get_filter();
  return maintenance_manager_->getLatestMaintenanceState()
      .via(this->getThreadManager())
      .thenValue(
          // We get expected<vec<definition>, maintenance error>
          [this, filter, request = std::move(request)](auto&& value) {
            if (value.hasError()) {
              // The exception will be passed through to the user of the API
              // on the client side as expected.
              value.error().throwThriftException();
              ld_assert(false);
            }
            // We need to find the maintenances that we are going to remove
            // now since the returned state from the RSM will not contain them
            // after removal.
            auto filtered = APIUtils::filterMaintenances(filter, value.value());
            // We have the filtered results that we can return if the removal
            // is successful. Execute the remove
            MaintenanceDelta delta;
            delta.set_remove_maintenances(*request);

            // Write the delta and if that was successful, return the filtered
            // list. If not, pass through the returned error.
            return MaintenanceLogWriter::writeDelta(
                       processor_, std::move(delta))
                // We want the continuation on the same thread to reduce
                // unnecessary context switching since the contiuation is a
                // very simple merge.
                .toUnsafeFuture()
                .thenValue(
                    [filtered = std::move(filtered)](MaintenanceOut&& value)
                        -> std::unique_ptr<RemoveMaintenancesResponse> {
                      if (value.hasError()) {
                        value.error().throwThriftException();
                        ld_assert(false);
                      }
                      auto response =
                          std::make_unique<RemoveMaintenancesResponse>();
                      response->set_maintenances(filtered);
                      return response;
                    });
          });
}

// unblock rebuilding (marks shards unrecoverable)
folly::SemiFuture<std::unique_ptr<UnblockRebuildingResponse>>
MaintenanceAPIHandler::semifuture_unblockRebuilding(
    std::unique_ptr<UnblockRebuildingRequest> /*request*/) {
  // This does not depend on maintenance manager
  return folly::makeSemiFuture<std::unique_ptr<UnblockRebuildingResponse>>(
      NotSupported("Not Implemented!"));
}
}} // namespace facebook::logdevice
