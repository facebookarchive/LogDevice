/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>
#include <unordered_set>

#include "logdevice/common/EpochMetaData.h"
#include "logdevice/common/NodeSetSelector.h"
#include "logdevice/common/ShardID.h"
#include "logdevice/common/configuration/Configuration.h"

namespace facebook { namespace logdevice {

/**
 * @file  Contains classes of EpochMetaData::Updater type for common epoch
 *        metadata operations.
 */

/**
 * Given the configuration and the current epoch metadata, determine if
 * a reconfiguration of the log is needed (in particular, a nodeset change),
 * and if so, return the updated epoch metadata for epoch store transaction.
 *
 * Currently used when: (1) sequencer is activated, (2) cluster nodes or log
 * configuration changed, (3) periodically for nodeset rebalancing
 * (see nodeset-adjustment-period setting).
 *
 * Possible outcomes:
 *  - If no changes are needed, returns UpdateResult::UNCHANGED.
 *  - If there was an error, returns UpdateResult::FAILED and assigns err.
 *  - If `info` was nullptr, and new metadata was produced,
 *    returns UpdateResult::CREATED.
 *  - If nodeset_params need to be changed while the rest of `info`
 *    stays the same, returns UpdateResult::NODESET_PARAMS_CHANGED. Such change
 *    to EpochMetaData doesn't need to be recorded in metadata log and therefore
 *    effective_since and WRITTEN_IN_METADATALOG flag don't need to be
 *    changed.
 *  - If the nodeset changed but nothing else (other than the signature) then
 *    it returns UpdateResult::NONSUBSTANTIAL_RECONFIGURATION. This can be used
 *    to infer that just the storage state of some of the nodes changed and that
 *    we can defer the reactivation operation.
 *  - If `info` needs to be changed in a more substantial way
 *    (e.g. updated nodeset or replication property), returns
 *    UpdateResult::SUBSTANTIAL_RECONFIGURATION, updates effective_since epoch
 *    to be the same as `epoch`, and removes WRITTEN_IN_METADATALOG flag.
 *
 * @param log_id  The data log id for which metadata is being manipulated.
 * @param info
 *   In-out argument. In: existing metadata in epoch store; nullptr if not
 *   provisioned yet. Out: new metadata. If the provided value is not nullptr,
 *   the EpochMetaData is modified in place. Otherwise a new EpochMetaData
 *   is created.
 * @param config  contains configuration for logs config.
 * @param nodes_configuration    contains configuration of nodes in the cluster
 * @param target_nodeset_size, nodeset_seed
 *   Target nodeset size and seed to pass to nodeset selector and to put into
 *   the new EpochMetaData. If folly::none, kept unchanged; if it's a newly
 *   provisioned log, uses nodeset size from log attributes and seed 0.
 * @param nodeset_selector  Used for picking a nodeset. If nullptr, a new
 *   nodeset selector will be created according to config.
 * @param provision_if_empty
 *   If `info` is null or empty, provisions a new one if this is
 *   true. If false, the operation will fail with E::EMPTY.
 * @param update_if_exists
 *   If `info` is not null and not empty, updates it if this is
 *   true. If false, the operation will fail with E::EXISTS.
 *   If `info` is not empty and not disabled and doesn't have
 *   WRITTEN_IN_METADATALOG flag, then update_if_exists must be false.
 * @param force_update
 *   Update the metadata even if the nodeset doesn't change.
 */
EpochMetaData::UpdateResult updateMetaDataIfNeeded(
    logid_t log_id,
    std::unique_ptr<EpochMetaData>& info,
    const Configuration& config,
    const configuration::nodes::NodesConfiguration& nodes_configuration,
    folly::Optional<nodeset_size_t> target_nodeset_size,
    folly::Optional<uint64_t> nodeset_seed,
    NodeSetSelector* nodeset_selector,
    bool use_storage_set_format,
    bool provision_if_empty = false,
    bool update_if_exists = true,
    bool force_update = false);

// This class is only used in tests and in deprecated metadata-utility.
// Normally metadata updates happen together with activating sequencer using
// EpochMetaDataUpdateToNextEpoch.
class CustomEpochMetaDataUpdater final : public EpochMetaData::Updater {
 public:
  /**
   * Create an EpochMetaDataUpdater object
   * @param config  contains configuration for logs config.
   * @param nodes_configuration    contains configuration of nodes in the
   * cluster
   * @param nodeset_selector       nodeset_selector for picking a nodeset
   * @param use_storage_set_format Use the new copyset serialization format for
   *                               Flexible Log Sharding.
   *                               TODO(T15517759): remove this options once all
   *                               production tiers are converted to use this
   *                               option by default.
   * @param provision_if_empty     if there is no entry in the epoch store,
   *                               provisions a new one, if this is true. If
   *                               false, the operation will fail with
   *                               E::EMPTY
   * @param update_if_exists       if there is an existing entry in the epoch
   *                               store, updates it, if this is true. If false,
   *                               the operation will fail with E::EXISTS
   * @param force_update           update the metadata even if the nodeset
   *                               doesn't change
   */
  CustomEpochMetaDataUpdater(
      std::shared_ptr<Configuration> config,
      std::shared_ptr<const configuration::nodes::NodesConfiguration>
          nodes_configuration,
      std::shared_ptr<NodeSetSelector> nodeset_selector,
      bool use_storage_set_format,
      bool provision_if_empty = false,
      bool update_if_exists = true,
      bool force_update = false)
      : config_(std::move(config)),
        nodes_configuration_(std::move(nodes_configuration)),
        nodeset_selector_(std::move(nodeset_selector)),
        use_storage_set_format_(use_storage_set_format),
        provision_if_empty_(provision_if_empty),
        update_if_exists_(update_if_exists),
        force_update_(force_update) {
    ld_check(config_ != nullptr);
    ld_check(nodeset_selector_ != nullptr);
  }

  EpochMetaData::UpdateResult operator()(logid_t log_id,
                                         std::unique_ptr<EpochMetaData>& info,
                                         MetaDataTracer* tracer) override;

 private:
  // Internal function that helps processes changes to
  // the log config. Return an enum that describes
  // the type of changes that occured.
  EpochMetaData::UpdateResult
  processConfigChanges(std::unique_ptr<EpochMetaData>& metadata,
                       const ReplicationProperty& replication,
                       bool force_update,
                       epoch_metadata_version::type metadata_version,
                       folly::Optional<nodeset_size_t> target_nodeset_size,
                       folly::Optional<uint64_t> nodeset_seed,
                       NodeSetSelector::Result& selected);

  const std::shared_ptr<Configuration> config_;
  const std::shared_ptr<const configuration::nodes::NodesConfiguration>
      nodes_configuration_;
  const std::shared_ptr<NodeSetSelector> nodeset_selector_;
  const bool use_storage_set_format_;
  const bool provision_if_empty_;
  const bool update_if_exists_;
  const bool force_update_;
};

/**
 * Used for performing epoch store transaction when sequencer activation is
 * needed. Atomically fetches the next epoch number for _logid_ in the
 * epoch store, increment it and stores it back to the epochstore.
 *
 * If no epoch metadata exists in epoch store, creates it
 * (if config allows sequencers to provision epoch store).
 *
 * If the next epoch number reaches EPOCH_MAX, the update will fail with
 * err = E::TOOBIG.
 * If acceptable_activation_epoch is set, and the epoch that
 * sequencer would activate into is different, will fail with err = E::ABORTED.
 *
 * Along the way, makes updates to the EpochMetaData if needed
 * (if config allows sequencers to do that).
 * E.g. updates replication factor if it was changed in log attributes, and
 * generates a new nodeset if nodes config or log attributes changed.
 * This update is done in one of two ways:
 *  (1) EpochMetaDataUpdateToNextEpoch calls updateMetaDataIfNeeded() on the
 *      current metadata from epoch store. This mode is used when activating
 *      sequencer for the first time in a process.
 *  (2) The creator of EpochMetaDataUpdateToNextEpoch builds an updated
 *      EpochMetaData beforehand (usually by calling updateMetaDataIfNeeded()
 *      on the metadata of a currently running sequencer) and passes it
 *      to EpochMetaDataUpdateToNextEpoch as updated_metadata.
 *      In this case, EpochMetaDataUpdateToNextEpoch just increments epoch in
 *      updated_metadata and makes no other changes. However, the update becomes
 *      conditional:
 *       - current metadata in epoch store must have epoch equal to
 *         updated_metadata->epoch, just like with acceptable_activation_epoch,
 *       - current metadata in epoch store must be non-empty, non-disabled, and
 *         written to metadata log.
 *      If these conditions are not met, fails with err = E::ABORTED.
 *      This mode is used when something triggered a metadata update, e.g. nodes
 *      were added to the cluster, or nodeset_size changed in log attributes.
 *
 *  @return
 *  UpdateResult::SUBSTANTIAL_RECONFIGURATION    successfully bumped the epoch
 *
 *  UpdateResult::CREATED    successfully provisioned new metadata (only if the
 *                           config allows it)
 *
 *  UpdateResult::FAILED     some error occurred
 */
class EpochMetaDataUpdateToNextEpoch final : public EpochMetaData::Updater {
 public:
  explicit EpochMetaDataUpdateToNextEpoch(
      std::shared_ptr<Configuration> config = nullptr,
      std::shared_ptr<const configuration::nodes::NodesConfiguration>
          nodes_configuration = nullptr,
      std::shared_ptr<EpochMetaData> updated_metadata = nullptr,
      folly::Optional<epoch_t> acceptable_activation_epoch = folly::none,
      bool use_storage_set_format = false,
      bool provision_if_empty = true)
      : config_(std::move(config)),
        nodes_configuration_(std::move(nodes_configuration)),
        updated_metadata_(updated_metadata),
        acceptable_activation_epoch_(acceptable_activation_epoch),
        use_storage_set_format_(use_storage_set_format),
        provision_if_empty_(provision_if_empty) {
    // If metadata was provided, make the write conditional on that metadata
    // being up-to-date.
    if (updated_metadata_ != nullptr) {
      if (acceptable_activation_epoch_.hasValue()) {
        ld_check_eq(acceptable_activation_epoch_.value().val(),
                    updated_metadata_->h.epoch.val());
      } else {
        acceptable_activation_epoch_ = updated_metadata_->h.epoch;
      }
    }
  }

  EpochMetaData::UpdateResult operator()(logid_t log_id,
                                         std::unique_ptr<EpochMetaData>& info,
                                         MetaDataTracer* tracer) override;

  // Helper that returns E::OK if the epoch can be bumped without regenerating
  // the metadata, an error code explaining the reason otherwise
  Status
  canEpochBeBumpedWithoutProvisioning(logid_t log_id,
                                      std::unique_ptr<EpochMetaData>& info,
                                      bool tolerate_notfound = false);

  // Helper that returns true if this updater is allowed to provision/update the
  // metadata (as opposed to just bumping the epoch)
  bool canSequencerProvision();

 private:
  const std::shared_ptr<Configuration> config_;
  const std::shared_ptr<const configuration::nodes::NodesConfiguration>
      nodes_configuration_;
  std::shared_ptr<EpochMetaData> updated_metadata_;
  folly::Optional<epoch_t> acceptable_activation_epoch_;
  bool use_storage_set_format_;
  bool provision_if_empty_;
};

/**
 * An EpochMetaData::Updater functor class which is used to mark an existing
 * EpochMetaData object as written in metadata log by setting the corresponding
 * flag in its header.
 *
 * If the compare_equality argument is supplied, verifies
 * that metadata that is being modified is substantially (e.g. everything except
 * for the epoch field) identical to the one supplied. If it isn't, sets err to
 * E::STALE and returns UpdateResult::FAILED
 *
 *  @return    UpdateResult::UNCHANGED  EpochMetaData is already marked as
 *                                      written
 *
 *             UpdateResult::SUBSTANTIAL_RECONFIGURATION    EpochMetaData has
 *                                                          been successfully
 *                                                          marked as written
 *
 *             UpdateResult::FAILED     EpochMetaData is not valid or check with
 *                                      compare_equality failed
 */
class EpochMetaDataUpdateToWritten : public EpochMetaData::Updater {
 public:
  explicit EpochMetaDataUpdateToWritten(
      std::shared_ptr<const EpochMetaData> compare_equality = nullptr)
      : compare_equality_(std::move(compare_equality)) {}

  EpochMetaData::UpdateResult operator()(logid_t log_id,
                                         std::unique_ptr<EpochMetaData>& info,
                                         MetaDataTracer* tracer) override;

 private:
  std::shared_ptr<const EpochMetaData> compare_equality_;
};

// Used for performing epoch store transaction when sequencer activation is not
// needed. Changes EpochMetaData's nodeset_params to new_params. The transaction
// is conditioned on the next_epoch in the epoch store being `required_epoch`,
// otherwise fails with E::ABORTED.
class EpochMetaDataUpdateNodeSetParams : public EpochMetaData::Updater {
 public:
  EpochMetaDataUpdateNodeSetParams(epoch_t required_epoch,
                                   EpochMetaData::NodeSetParams new_params)
      : required_epoch_(required_epoch), new_params_(new_params) {}
  EpochMetaData::UpdateResult operator()(logid_t log_id,
                                         std::unique_ptr<EpochMetaData>& info,
                                         MetaDataTracer* tracer) override;

 private:
  epoch_t required_epoch_;
  EpochMetaData::NodeSetParams new_params_;
};

/**
 * An EpochMetaData::Updater functor class which is used to mark an existing
 * EpochMetaData object as not written in metadata log by clearing the
 * corresponding flag in its header.
 *
 *  @return    UpdateResult::UNCHANGED  the flag is already cleared
 *
 *             UpdateResult::SUBSTANTIAL_RECONFIGURATION    EpochMetaData has
 *                                                          been successfully
 *                                                          marked as not
 *                                                          written
 *
 *             UpdateResult::FAILED     EpochMetaData is not valid
 */
class EpochMetaDataClearWrittenBit : public EpochMetaData::Updater {
 public:
  explicit EpochMetaDataClearWrittenBit() {}

  EpochMetaData::UpdateResult operator()(logid_t log_id,
                                         std::unique_ptr<EpochMetaData>& info,
                                         MetaDataTracer* tracer) override;
};

/**
 * An EpochMetaData::Updater functor class which is used to mark an existing
 * EpochMetaData object as disabled.
 *
 *  @return    UpdateResult::UNCHANGED  EpochMetaData is already marked as
 *                                      disabled or does not exist
 *
 *             UpdateResult::SUBSTANTIAL_RECONFIGURATION   EpochMetaData has
 *                                                         been successfully
 *                                                         marked as disabled
 *
 *             UpdateResult::FAILED     EpochMetaData is not valid
 */
class DisableEpochMetaData : public EpochMetaData::Updater {
 public:
  EpochMetaData::UpdateResult operator()(logid_t log_id,
                                         std::unique_ptr<EpochMetaData>& info,
                                         MetaDataTracer* tracer) override;
};

/**
 * An EpochMetaData::Updater functor class which is used to read the metadata.
 * Doesn't modify anything.
 *
 *  @return    UpdateResult::UNCHANGED  EpochMetaData read successfully
 *
 *             UpdateResult::FAILED     couldn't read EpochMetaData or it's
 *                                      empty
 */
class ReadEpochMetaData : public EpochMetaData::Updater {
 public:
  EpochMetaData::UpdateResult operator()(logid_t log_id,
                                         std::unique_ptr<EpochMetaData>& info,
                                         MetaDataTracer* tracer) override;
};

/**
 * A simple EpochMetaData::Updater functor class which just calls the submitted
 * function as the callback. Used in FileEpochStore and tests.
 */
class SimpleEpochMetaDataUpdater final : public EpochMetaData::Updater {
 public:
  using func_t = std::function<
      EpochMetaData::UpdateResult(logid_t, std::unique_ptr<EpochMetaData>&)>;
  explicit SimpleEpochMetaDataUpdater(func_t func) : func_(std::move(func)) {}
  EpochMetaData::UpdateResult operator()(logid_t log_id,
                                         std::unique_ptr<EpochMetaData>& info,
                                         MetaDataTracer* /*tracer*/) override {
    return func_(log_id, info);
  }

 private:
  func_t func_;
};

}} // namespace facebook::logdevice
