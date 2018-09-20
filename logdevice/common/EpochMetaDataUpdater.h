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

#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/EpochMetaData.h"
#include "logdevice/common/NodeSetSelector.h"
#include "logdevice/common/ShardID.h"

namespace facebook { namespace logdevice {

/**
 * @file  Contains classes of EpochMetaData::Updater type for common epoch
 *        metadata operations.
 */

/**
 * EpochMetaDataUpdaterBase is a descendant of EpochMetaData::Updater.
 * Used to provision/update EpochMetaData in the EpochStore. Its
 * generateNewMetaData() method performs update based on the given configuration
 * and nodeset selector.
 */
class EpochMetaDataUpdaterBase : public EpochMetaData::Updater {
 protected:
  /**
   * If the given EpochMetaData object is valid, the previous nodeset is also
   * provided to the nodeset selector. If it decides that epoch metadata needs
   * to be changed (either nodeset or replication factor or both), it replaces
   * the nodeset and replication factor of the given EpochMetaData object with
   * the updated values, and changes its effective_since epoch to be the same as
   * the epoch field of the object. If everything is successful, the functor
   * returns EpochMetaData::UpdateResult::UPDATED. If nothing needs to be
   * changed, it returns EpochMetaData::UpdateResult::UNCHANGED without changing
   * the given epoch metadata. It returns EpochMetaData::UpdateResult::FAILED if
   * failure occurs.
   *
   * On the other hand, if the given object is empty, will provision the object
   * with replication factor and nodeset computed from the config, and set epoch
   * and effective_since to be the initial epoch (EPOCH_MIN).
   *
   * @param log_id                 the data log id for which metadata is being
   *                               manipulated
   * @param info                   the incoming metadata. Can be nullptr if
   *                               there is no metadata provisioned yet.
   * @param config                 cluster configuration
   * @param nodeset_selector       nodeset_selector for picking a nodeset
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
  EpochMetaData::UpdateResult
  generateNewMetaData(logid_t log_id,
                      std::unique_ptr<EpochMetaData>& info,
                      std::shared_ptr<Configuration> config,
                      std::shared_ptr<NodeSetSelector> nodeset_selector,
                      MetaDataTracer* tracer,
                      bool use_storage_set_format,
                      bool provision_if_empty = false,
                      bool update_if_exists = true,
                      bool force_update = false);
};

class EpochMetaDataUpdater final : public EpochMetaDataUpdaterBase {
 public:
  /**
   * Create an EpochMetaDataUpdater object
   * @param config                 cluster configuration
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
  EpochMetaDataUpdater(std::shared_ptr<Configuration> config,
                       std::shared_ptr<NodeSetSelector> nodeset_selector,
                       bool use_storage_set_format,
                       bool provision_if_empty = false,
                       bool update_if_exists = true,
                       bool force_update = false)
      : config_(std::move(config)),
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
  const std::shared_ptr<Configuration> config_;
  const std::shared_ptr<NodeSetSelector> nodeset_selector_;
  const bool use_storage_set_format_;
  const bool provision_if_empty_;
  const bool update_if_exists_;
  const bool force_update_;
};

/**
 * Atomically fetches the next epoch number for _logid_ in the
 * epoch store, increment it and stores it back to the epochstore.
 * If the next epoch number reaches EPOCH_MAX, the update will fail with
 * err = E::TOOBIG. If acceptable_activation_epoch is set, and the epoch that
 * sequencer would activate in is different, will fail with err = E::ABORTED.
 *
 * The current log metadata information (e.g., nodeset, replication factor)
 * is also retrieved atomically with the next epoch number.
 *
 * If the config is passed in the constructor and it allows sequencers to
 * provision metadata, then EpochMetaDataUpdateToNextEpoch will provision epoch
 * store metadata for an unprovisioned log, or will update the metadata for a
 * log that has metadata that doesn't match the config.
 *
 *  @return    UpdateResult::UPDATED    successfully bumped the epoch
 *
 *             UpdateResult::CREATED    successfully provisioned new metadata
 *                                      (only if the config allows it)
 *
 *             UpdateResult::FAILED     couldn't bump the epoch
 */
class EpochMetaDataUpdateToNextEpoch final : public EpochMetaDataUpdaterBase {
 public:
  explicit EpochMetaDataUpdateToNextEpoch(
      std::shared_ptr<Configuration> config = nullptr,
      folly::Optional<epoch_t> acceptable_activation_epoch = folly::none,
      bool use_storage_set_format = false,
      bool provision_if_empty = true)
      : config_(std::move(config)),
        acceptable_activation_epoch_(acceptable_activation_epoch),
        use_storage_set_format_(use_storage_set_format),
        provision_if_empty_(provision_if_empty) {}

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

  // Since the CF is called with metadata that was written, it contains the
  // epoch of the _next_ sequencer, not of the one that we activated now.
  // Modifying the epoch/metadata here, so that the EpochStore CF is called with
  // this sequencer's epoch instead.
  std::unique_ptr<EpochMetaData>
  getCompletionMetaData(const EpochMetaData*) override;

 private:
  const std::shared_ptr<Configuration> config_;
  folly::Optional<epoch_t> acceptable_activation_epoch_;
  bool use_storage_set_format_;
  bool provision_if_empty_;
};

/**
 * An EpochMetaData::Updater functor class which is used to mark an existing
 * EpochMetaData object as written in metadata log by setting the corresponding
 * flag in its header.
 *
 * If the compare_equality_ argument is supplied, verifies
 * that metadata that is being modified is substantially (e.g. everything except
 * for the epoch field) identical to the one supplied. If it isn't, sets err to
 * E::STALE and returns UpdateResult::FAILED
 *
 *  @return    UpdateResult::UNCHANGED  EpochMetaData is already marked as
 *                                      written
 *
 *             UpdateResult::UPDATED    EpochMetaData has been successfully
 *                                      marked as written
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

/**
 * An EpochMetaData::Updater functor class which is used to mark an existing
 * EpochMetaData object as not written in metadata log by clearing the
 * corresponding flag in its header.
 *
 *  @return    UpdateResult::UNCHANGED  the flag is already cleared
 *
 *             UpdateResult::UPDATED    EpochMetaData has been successfully
 *                                      marked as not written
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
 *             UpdateResult::UPDATED    EpochMetaData has been successfully
 *                                      marked as disabled
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
