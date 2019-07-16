/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/EpochMetaDataUpdater.h"

#include "logdevice/common/MetaDataTracer.h"
#include "logdevice/common/NodeSetSelectorFactory.h"
#include "logdevice/common/configuration/nodes/utils.h"
#include "logdevice/common/debug.h"
#include "logdevice/lib/ClientProcessor.h"

namespace facebook { namespace logdevice {

using UpdateResult = EpochMetaData::UpdateResult;

EpochMetaData::UpdateResult CustomEpochMetaDataUpdater::
operator()(logid_t log_id,
           std::unique_ptr<EpochMetaData>& info,
           MetaDataTracer* tracer) {
  UpdateResult res =
      updateMetaDataIfNeeded(log_id,
                             info,
                             *config_,
                             *nodes_configuration_,
                             /* target_nodeset_size */ folly::none,
                             /* nodeset_seed */ folly::none,
                             nodeset_selector_.get(),
                             use_storage_set_format_,
                             provision_if_empty_,
                             update_if_exists_,
                             force_update_);
  if (res == UpdateResult::ONLY_NODESET_PARAMS_CHANGED ||
      res == UpdateResult::NONSUBSTANTIAL_RECONFIGURATION ||
      res == UpdateResult::CREATED ||
      res == UpdateResult::SUBSTANTIAL_RECONFIGURATION) {
    if (tracer) {
      tracer->setAction(MetaDataTracer::Action::PROVISION_METADATA);
      tracer->setNewMetaData(*info);
    }
  } else {
    ld_check(res == UpdateResult::UNCHANGED || res == UpdateResult::FAILED);
  }
  return res;
}

/*
 * This function processes changes to the log configuration
 * as well as the nodeset parameters. It classifies changes
 * between the old and the new state in the following categories:
 * 1. Nothing changed.
 * 2. One or more of the the nodeset parameters changed: signature, seed, target
 * nodeset size.
 * 3. There were substantial reconfigurations. E.g, the replication factor was
 * changed.
 *
 * The caller can combine this information along with the result from the
 * NodeSetSelector to determine what actions are needed:
 * 1. Immediately perform sequencer reactivation.
 * 2. Immediately update epoch metadata but no sequencer reactivation is needed.
 * 3. Perform sequencer reactivation but delay the step for some time.
 *
 */
EpochMetaData::UpdateResult
processConfigChanges(std::unique_ptr<EpochMetaData>& metadata,
                     const ReplicationProperty& replication,
                     bool force_update,
                     epoch_metadata_version::type metadata_version,
                     folly::Optional<nodeset_size_t> target_nodeset_size,
                     folly::Optional<uint64_t> nodeset_seed,
                     const NodeSetSelector::Result& selected) {
  if (!force_update && metadata->replication == replication &&
      !metadata->disabled() && metadata->h.version >= metadata_version) {
    // No change to config. Check the nodeset params.
    if (target_nodeset_size.value() ==
            metadata->nodeset_params.target_nodeset_size &&
        nodeset_seed.value() == metadata->nodeset_params.seed &&
        selected.signature == metadata->nodeset_params.signature) {
      return EpochMetaData::UpdateResult::UNCHANGED;
    } else {
      // Only need to update nodeset params.
      // No need to reset effective_since and write a metadata log record.
      return UpdateResult::ONLY_NODESET_PARAMS_CHANGED;
    }
  }

  // More than just the nodeset params changed in the config. Most likely
  // the replication factor changed.
  return UpdateResult::SUBSTANTIAL_RECONFIGURATION;
}

/*
 * This function processes changes to the epoch metadata and based on the
 * changes provides a recommendation to the caller about actions to perform:
 * update epoch metadata only, update epoch metadata and perform sequencer
 * reactivation, or delay sequencer reactivation. The table below summarizes how
 * this recommendation is made. The header in each column is the entity that
 * changed.
 *
 *------------------------------------------------------------------------------
 *   NS   | replFactor  | targetNodesetSize  |   val of enum UpdateResult     |
 * change | or          | or seed or NC hash |                                |
 *        | forceUpdate | or signature       |                                |
 *        | or          |                    |                                |
 *        | logDisabled |                    |                                |
 *-----------------------------------------------------------------------------
 *   N    |     N       |         N          | UNCHANGED                      |
 *-----------------------------------------------------------------------------
 *   N    |     N       |         Y          | ONLY_NODESET_PARAMS_CHANGED    |
 *-----------------------------------------------------------------------------
 *   *    |     Y       |         *          | SUBSTANTIAL_RECONFIGURATION    |
 *-----------------------------------------------------------------------------
 *   Y    |     N       |         N          | NONSUBSTANTIAL_RECONFIGURATION |
 *-----------------------------------------------------------------------------
 *   Y    |     N       |  Y (NC hash only)  | NONSUBSTANTIAL_RECONFIGURATION |
 *-----------------------------------------------------------------------------
 *   Y    |     N       |         Y          | SUBSTANTIAL_RECONFIGURATION    |
 *-----------------------------------------------------------------------------
 *
 *
 */
UpdateResult updateMetaDataIfNeeded(
    logid_t log_id,
    std::unique_ptr<EpochMetaData>& metadata,
    const Configuration& config,
    const configuration::nodes::NodesConfiguration& nodes_configuration,
    folly::Optional<nodeset_size_t> target_nodeset_size,
    folly::Optional<uint64_t> nodeset_seed,
    NodeSetSelector* nodeset_selector,
    bool use_storage_set_format,
    bool provision_if_empty,
    bool update_if_exists,
    bool force_update) {
  const std::shared_ptr<LogsConfig::LogGroupNode> logcfg =
      config.getLogGroupByIDShared(log_id);
  if (!logcfg) {
    err = E::NOTFOUND;
    return UpdateResult::FAILED;
  }

  // If the given metadata is empty, provision it with an initial metadata
  // Otherwise, update the metadata given
  const bool prev_metadata_exists = metadata && !metadata->isEmpty();
  if (!prev_metadata_exists && !provision_if_empty) {
    RATELIMIT_INFO(std::chrono::seconds(10),
                   10,
                   "Metadata not found for log %lu",
                   log_id.val_);
    err = E::EMPTY;
    return UpdateResult::FAILED;
  }

  if (prev_metadata_exists && !update_if_exists) {
    ld_error("Metadata already provisioned for log %lu", log_id.val_);
    err = E::EXISTS;
    return UpdateResult::FAILED;
  }

  if (!target_nodeset_size.hasValue()) {
    if (prev_metadata_exists &&
        metadata->nodeset_params.target_nodeset_size != 0) {
      target_nodeset_size = metadata->nodeset_params.target_nodeset_size;
    } else {
      target_nodeset_size =
          logcfg->attrs().nodeSetSize().value().value_or(NODESET_SIZE_MAX);
    }
  }

  if (!nodeset_seed.hasValue()) {
    if (prev_metadata_exists) {
      nodeset_seed = metadata->nodeset_params.seed;
    } else {
      nodeset_seed = 0;
    }
  }

  // Select a nodeset.
  std::unique_ptr<NodeSetSelector> nodeset_selector_ptr;
  if (!nodeset_selector) {
    NodeSetSelectorType nodeset_selector_type =
        config.serverConfig()->getMetaDataLogsConfig().nodeset_selector_type;
    ld_check(nodeset_selector_type != NodeSetSelectorType::INVALID);
    nodeset_selector_ptr =
        NodeSetSelectorFactory::create(nodeset_selector_type);
    nodeset_selector = nodeset_selector_ptr.get();
    ld_check(nodeset_selector != nullptr);
  }
  auto selected = nodeset_selector->getStorageSet(
      log_id,
      &config,
      nodes_configuration,
      target_nodeset_size.value(),
      nodeset_seed.value(),
      prev_metadata_exists ? metadata.get() : nullptr,
      nullptr);

  UpdateResult result =
      prev_metadata_exists ? UpdateResult::UNCHANGED : UpdateResult::CREATED;

  ReplicationProperty replication =
      ReplicationProperty::fromLogAttributes(logcfg->attrs());
  epoch_metadata_version::type metadata_version =
      epoch_metadata_version::versionToWrite(config.serverConfig());

  switch (selected.decision) {
    case NodeSetSelector::Decision::FAILED:
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      5,
                      "NodeSetSelector failed to generate new nodeset for log "
                      "%lu",
                      log_id.val_);
      err = E::FAILED;
      return UpdateResult::FAILED;
    case NodeSetSelector::Decision::KEEP:
      if (!prev_metadata_exists) {
        ld_critical("INTERNAL ERROR: NodeSet selector returned Decision::KEEP "
                    "for an invalid epoch metadata that needs to be "
                    "provisioned! logid: %lu",
                    log_id.val_);
        // Should be enforced by the nodeset selector.
        ld_check(false);
        err = E::INTERNAL;
        return UpdateResult::FAILED;
      }
      // No change in nodeset. Any change is due to change in the config.
      result = processConfigChanges(metadata,
                                    replication,
                                    force_update,
                                    metadata_version,
                                    target_nodeset_size,
                                    nodeset_seed,
                                    selected);
      if (result == UpdateResult::UNCHANGED) {
        return result;
      }

      break;
    case NodeSetSelector::Decision::NEEDS_CHANGE:
      // The nodeset changed. We need to perform metadata log updates and
      // sequencer reactivation immediately or later depending on what events
      // triggered the change. But we only need to process this information if
      // the config already doesn't have significant changes that requires us to
      // reactivate the sequencer.
      if (prev_metadata_exists) {
        // Figure out what else changed on the log config or the nodeset params.
        UpdateResult configResult = processConfigChanges(metadata,
                                                         replication,
                                                         force_update,
                                                         metadata_version,
                                                         target_nodeset_size,
                                                         nodeset_seed,
                                                         selected);

        // Now reconcile the config changes with the nodeset changes to
        // generate the final result.
        if (configResult == UpdateResult::UNCHANGED) {
          result = UpdateResult::NONSUBSTANTIAL_RECONFIGURATION;
        } else if (configResult == UpdateResult::ONLY_NODESET_PARAMS_CHANGED &&
                   target_nodeset_size.value() ==
                       metadata->nodeset_params.target_nodeset_size &&
                   nodeset_seed.value() == metadata->nodeset_params.seed) {
          // just the signature changed
          result = UpdateResult::NONSUBSTANTIAL_RECONFIGURATION;
        } else {
          // Nodeset as well as nodeset parameters changed. Or one of the log
          // config options, like the replication factor, changed.
          result = UpdateResult::SUBSTANTIAL_RECONFIGURATION;
        }
      }

      break;
  }

  ld_check(result >= UpdateResult::ONLY_NODESET_PARAMS_CHANGED);
  if (metadata == nullptr) {
    metadata = std::make_unique<EpochMetaData>();
  }

  if (!prev_metadata_exists) {
    if (selected.storage_set.empty()) {
      ld_critical("INTERNAL ERROR: NodeSet selector returned empty nodeset "
                  "for log %lu whose epoch metadata needs to be provisioned!",
                  log_id.val_);
      // should be enforced by the nodeset selector
      ld_check(false);
      err = E::INTERNAL;
      return UpdateResult::FAILED;
    }

    // provision metadata with the initial epoch
    metadata->h.epoch = metadata->h.effective_since = EPOCH_MIN;
    // use the configured `metadata_version'
    metadata->h.version = metadata_version;
  } else if (result != UpdateResult::ONLY_NODESET_PARAMS_CHANGED) {
    // epoch remains the same
    // update effective_since to be the same as epoch
    metadata->h.effective_since = epoch_t(metadata->h.epoch.val_);
  }

  metadata->replication = replication;

  // update the version to metadata_version if applicable
  if (metadata->h.version < metadata_version) {
    metadata->h.version = metadata_version;
  }

  // update nodeset and nodeset_params
  metadata->nodeset_params.signature = selected.signature;
  metadata->nodeset_params.target_nodeset_size = target_nodeset_size.value();
  metadata->nodeset_params.seed = nodeset_seed.value();
  if (selected.decision == NodeSetSelector::Decision::NEEDS_CHANGE) {
    ld_check(result != UpdateResult::ONLY_NODESET_PARAMS_CHANGED);
    metadata->setShards(selected.storage_set);
    metadata->weights = selected.weights;
    metadata->replication_conf_changed_at = RecordTimestamp::now();
  }

  // clear the DISABLED flag as well
  metadata->h.flags &= ~MetaDataLogRecordHeader::DISABLED;

  if (use_storage_set_format) {
    // Enable the new copyset serialization format
    metadata->h.flags |= MetaDataLogRecordHeader::HAS_STORAGE_SET;
  } else {
    metadata->h.flags &= ~MetaDataLogRecordHeader::HAS_STORAGE_SET;
  }

  if (result != UpdateResult::ONLY_NODESET_PARAMS_CHANGED) {
    // since this is a newly generated metadata, by default it is not yet
    // written to the metadata log
    metadata->h.flags &= ~MetaDataLogRecordHeader::WRITTEN_IN_METADATALOG;
    ld_check(!metadata->writtenInMetaDataLog());
  }

  ld_check(!metadata->disabled());

  if (!metadata->isValid()) {
    ld_critical(
        "INTERNAL ERROR: Updated epoch metadata is invalid for log %lu: %s",
        log_id.val_,
        metadata->toString().c_str());
    // nodeset selector should enforce that epoch metadata is valid
    ld_check(false);
    err = E::INTERNAL;
    return UpdateResult::FAILED;
  }

  return result;
}

Status EpochMetaDataUpdateToNextEpoch::canEpochBeBumpedWithoutProvisioning(
    logid_t log_id,
    std::unique_ptr<EpochMetaData>& info,
    bool tolerate_notfound) {
  ld_check(!info || info->isValid());
  if (!info) {
    if (!tolerate_notfound) {
      RATELIMIT_ERROR(std::chrono::seconds(10),
                      10,
                      "Attempt to bump epoch for log %lu but the epoch store "
                      "does not have provisioned metadata for it!",
                      log_id.val_);
    }
    return E::NOTFOUND;
  }
  if (info->isEmpty()) {
    ld_error("Attempt to bump epoch for log %lu but the epoch store content is "
             "empty!",
             log_id.val_);
    return E::EMPTY;
  }

  if (info->disabled()) {
    // The log is disabled in epoch store.
    return E::DISABLED;
  }
  return E::OK;
}

bool EpochMetaDataUpdateToNextEpoch::canSequencerProvision() {
  bool res = false;
  if (config_) {
    auto& ml_config = config_->serverConfig()->getMetaDataLogsConfig();
    res = ml_config.sequencers_provision_epoch_store;
    ld_check(!res || ml_config.sequencers_write_metadata_logs);
  }
  return res;
}

UpdateResult EpochMetaDataUpdateToNextEpoch::
operator()(logid_t log_id,
           std::unique_ptr<EpochMetaData>& info,
           MetaDataTracer* tracer) {
  // do not allow calling this with metadata logids
  if (log_id <= LOGID_INVALID || log_id > LOGID_MAX) {
    err = E::INVALID_PARAM;
    ld_check(false);
    return UpdateResult::FAILED;
  }
  if (info && !info->isValid()) {
    ld_error("Attempt to bump epoch for log %lu but the epoch store content is "
             "invalid!",
             log_id.val_);
    err = E::FAILED;
    return UpdateResult::FAILED;
  }

  // Do this check early to report preemption even if other stopping conditions
  // are hit (e.g. not written to metadata log).
  // Note that `info` comes from epoch store, so it contains the _next_ epoch,
  // i.e. the epoch the newly activated sequencer is going to get.
  if (acceptable_activation_epoch_.hasValue() &&
      acceptable_activation_epoch_.value() !=
          (info ? info->h.epoch : EPOCH_MIN)) {
    RATELIMIT_INFO(std::chrono::seconds(10),
                   2,
                   "Aborting metadata update because epoch changed in epoch "
                   "store: wanted %u, got %u",
                   acceptable_activation_epoch_.value().val(),
                   info ? info->h.epoch.val() : EPOCH_MIN.val());
    err = E::ABORTED;
    return EpochMetaData::UpdateResult::FAILED;
  }

  // Checking if this job (that should run on a sequencer) is responsible for
  // provisioning metadata. If it is, sequencers should also be responsible for
  // writing metadata logs, and the written bit should be enabled in the
  // metadata.
  bool provisioning_enabled = canSequencerProvision();
  Status can_activate_without_provisioning =
      canEpochBeBumpedWithoutProvisioning(log_id,
                                          info,
                                          /*tolerate_notfound=*/true);
  if (!provisioning_enabled && (can_activate_without_provisioning != E::OK)) {
    err = can_activate_without_provisioning;
    return UpdateResult::FAILED;
  }

  if (info && info->h.epoch == EPOCH_MAX) {
    // Note: we actually do not return EPOCH_MAX to the sequencer, despite
    // it is a valid epoch. Otherwise we need to store an invalid metadata
    // to the epoch store. This should be OK since it is unlikely logs are
    // running out of epochs.
    err = E::TOOBIG;
    return UpdateResult::FAILED;
  }

  // default result is SUBSTANTIAL_RECONFIGURATION, could be changed to CREATED
  // or FAILED by the updater if sequencers provision logs
  UpdateResult res = UpdateResult::SUBSTANTIAL_RECONFIGURATION;

  if (updated_metadata_ != nullptr) {
    // New metadata was provided to us from outside.
    // Check the conditions and use it.
    if (!info || info->isEmpty() || info->disabled() ||
        !info->writtenInMetaDataLog()) {
      RATELIMIT_ERROR(
          std::chrono::seconds(10),
          2,
          "Epoch store has invalid metadata or no WRITTEN_IN_METADATA_LOG flag "
          "for log %lu, while we have a running sequencer that thinks that "
          "metadata is written to metadata log. This is unexpected. Aborting "
          "metadata update. Tried to update to %s, epoch store has %s",
          log_id.val(),
          updated_metadata_->toString().c_str(),
          info ? info->toString().c_str() : "null");
      err = E::FAILED;
      return UpdateResult::FAILED;
    }

    // acceptable_activation_epoch_ (assigned in constructor) must have taken
    // care of that.
    ld_check(info->h.epoch == updated_metadata_->h.epoch);

    *info = *updated_metadata_;
    if (tracer) {
      tracer->setAction(MetaDataTracer::Action::PROVISION_METADATA);
    }
  } else {
    // Update or provision metadata if needed.
    bool provisioning_allowed = !info || info->isEmpty() || info->disabled() ||
        info->writtenInMetaDataLog();

    if (provisioning_enabled && provisioning_allowed) {
      ld_check(config_);
      ld_check(nodes_configuration_);
      res = updateMetaDataIfNeeded(log_id,
                                   info,
                                   *config_,
                                   *nodes_configuration_,
                                   folly::none,
                                   folly::none,
                                   /* nodeset_selector */ nullptr,
                                   use_storage_set_format_,
                                   provision_if_empty_);
      if (res == UpdateResult::FAILED) {
        return res;
      }
      if (res == UpdateResult::UNCHANGED) {
        // We're going to bump epoch even if nodeset doesn't need changing.
        res = UpdateResult::SUBSTANTIAL_RECONFIGURATION;
      } else if (tracer) {
        tracer->setAction(MetaDataTracer::Action::PROVISION_METADATA);
      }
    } else {
      // We didn't consider updating the nodeset, either because nodeset
      // updating is turned off or because the current nodeset is not written to
      // metadata log yet. If it's the latter, we'll re-check it after metadata
      // is written.
      ld_assert(canEpochBeBumpedWithoutProvisioning(log_id, info) == E::OK);
    }
  }
  ld_check(info && info->isValid());

  ++info->h.epoch.val_;
  info->setEpochIncrementAt();
  if (info->h.epoch <= EPOCH_MIN) {
    ld_critical("Unexpected - bumped epoch in EpochStore to epoch %u, "
                "should be >= 2",
                info->h.epoch.val());
    ld_check(false);
    return UpdateResult::FAILED;
  }
  ld_check(info->isValid());

  if (tracer) {
    tracer->setNewMetaData(*info);
  }

  return res;
}

EpochMetaData::UpdateResult EpochMetaDataUpdateToWritten::
operator()(logid_t log_id,
           std::unique_ptr<EpochMetaData>& info,
           MetaDataTracer* tracer) {
  if (!info) {
    ld_error("Attempt to mark EpochMetaData for log %lu as written in "
             "metadata log but there is no metadata in the epoch store",
             log_id.val_);
    return EpochMetaData::UpdateResult::FAILED;
  }
  if (info->isEmpty() || !info->isValid()) {
    ld_error("Attempt to mark EpochMetaData for log %lu as written in "
             "metadata log but the content is invalid!",
             log_id.val_);
    return EpochMetaData::UpdateResult::FAILED;
  }

  if (info->writtenInMetaDataLog()) {
    ld_info("Attempt to mark EpochMetaData for log %lu as written in "
            "but it is already marked as written.",
            log_id.val_);
    return EpochMetaData::UpdateResult::UNCHANGED;
  }

  if (compare_equality_) {
    // comparing everything except the epoch field
    if (!compare_equality_->identicalInMetaDataLog(*info)) {
      ld_error("Attempt to mark EpochMetaData for log %lu as written in "
               "metadata log but metadata in epoch store (%s) differs from the "
               "one that was written (%s).",
               log_id.val_,
               info->toString().c_str(),
               compare_equality_->toString().c_str());
      err = E::STALE;
      return EpochMetaData::UpdateResult::FAILED;
    }
  }

  info->h.flags |= MetaDataLogRecordHeader::WRITTEN_IN_METADATALOG;
  if (tracer) {
    tracer->setAction(MetaDataTracer::Action::SET_WRITTEN_BIT);
    tracer->setNewMetaData(*info);
  }
  return EpochMetaData::UpdateResult::SUBSTANTIAL_RECONFIGURATION;
}

EpochMetaData::UpdateResult EpochMetaDataUpdateNodeSetParams::
operator()(logid_t log_id,
           std::unique_ptr<EpochMetaData>& info,
           MetaDataTracer* tracer) {
  if (!info || info->isEmpty() || !info->isValid() ||
      info->h.epoch == EPOCH_INVALID) {
    ld_error(
        "Attempt to update nodeset params in epoch store for log %lu epoch %u "
        "but the metadata is missing or invalid in the epoch store.",
        log_id.val(),
        required_epoch_.val());
    err = E::FAILED;
    return EpochMetaData::UpdateResult::FAILED;
  }

  if (info->h.epoch != required_epoch_) {
    RATELIMIT_INFO(std::chrono::seconds(10),
                   2,
                   "Not updating nodeset params for log %lu epoch %u to %s "
                   "because epoch was increased to %u.",
                   log_id.val(),
                   required_epoch_.val(),
                   new_params_.toString().c_str(),
                   info->h.epoch.val());
    err = E::ABORTED;
    return EpochMetaData::UpdateResult::FAILED;
  }

  if (info->nodeset_params == new_params_) {
    RATELIMIT_INFO(
        std::chrono::seconds(10),
        2,
        "Not updating nodeset params for log %lu epoch %u to %s because it "
        "already has this value.",
        log_id.val(),
        required_epoch_.val(),
        new_params_.toString().c_str());
    return EpochMetaData::UpdateResult::UNCHANGED;
  }

  info->nodeset_params = new_params_;
  if (tracer) {
    tracer->setAction(MetaDataTracer::Action::UPDATE_NODESET_PARAMS);
    tracer->setNewMetaData(*info);
  }
  return EpochMetaData::UpdateResult::SUBSTANTIAL_RECONFIGURATION;
}

EpochMetaData::UpdateResult EpochMetaDataClearWrittenBit::
operator()(logid_t log_id,
           std::unique_ptr<EpochMetaData>& info,
           MetaDataTracer* tracer) {
  if (!info || (info->isValid() && info->isEmpty())) {
    ld_info("Attempt to mark EpochMetaData for log %lu as unwritten "
            "but no metadata exists for this log.",
            log_id.val_);
    return EpochMetaData::UpdateResult::UNCHANGED;
  }

  if (!info->isValid()) {
    ld_error("Attempt to mark EpochMetaData for log %lu as unwritten "
             "but the existing content is invalid!",
             log_id.val_);
    return EpochMetaData::UpdateResult::FAILED;
  }

  if (!info->writtenInMetaDataLog()) {
    ld_info("Attempt to mark EpochMetaData for log %lu as unwritten "
            "but it is already marked as such.",
            log_id.val_);
    return EpochMetaData::UpdateResult::UNCHANGED;
  }

  info->h.flags &= ~MetaDataLogRecordHeader::WRITTEN_IN_METADATALOG;
  if (tracer) {
    tracer->setAction(MetaDataTracer::Action::CLEAR_WRITTEN_BIT);
    tracer->setNewMetaData(*info);
  }
  return EpochMetaData::UpdateResult::SUBSTANTIAL_RECONFIGURATION;
}

EpochMetaData::UpdateResult DisableEpochMetaData::
operator()(logid_t log_id,
           std::unique_ptr<EpochMetaData>& info,
           MetaDataTracer* tracer) {
  if (!info || (info->isValid() && info->isEmpty())) {
    ld_info("Attempt to mark EpochMetaData for log %lu as disabled "
            "but no metadata exists for this log.",
            log_id.val_);
    return EpochMetaData::UpdateResult::UNCHANGED;
  }

  if (!info->isValid()) {
    ld_error("Attempt to mark EpochMetaData for log %lu as disabled "
             "but the existing content is invalid!",
             log_id.val_);
    return EpochMetaData::UpdateResult::FAILED;
  }

  if (info->disabled()) {
    ld_info("Attempt to mark EpochMetaData for log %lu as disabled "
            "but it is already marked as disabled.",
            log_id.val_);
    return EpochMetaData::UpdateResult::UNCHANGED;
  }

  info->h.flags |= MetaDataLogRecordHeader::DISABLED;
  // bump the epoch to avoid inconsistency with metadata logs in case like
  // disabling a half-provisioned log
  info->h.epoch =
      (info->h.epoch == EPOCH_MAX ? EPOCH_MAX
                                  : epoch_t(info->h.epoch.val_ + 1));
  info->setEpochIncrementAt();
  if (tracer) {
    tracer->setAction(MetaDataTracer::Action::DISABLE);
    tracer->setNewMetaData(*info);
  }
  return EpochMetaData::UpdateResult::SUBSTANTIAL_RECONFIGURATION;
}

EpochMetaData::UpdateResult ReadEpochMetaData::
operator()(logid_t log_id,
           std::unique_ptr<EpochMetaData>& info,
           MetaDataTracer* /* tracer */) {
  if (!info) {
    err = E::NOTFOUND;
    return EpochMetaData::UpdateResult::FAILED;
  }
  if (info->isEmpty()) {
    err = E::EMPTY;
    return EpochMetaData::UpdateResult::FAILED;
  }

  if (!info->isValid()) {
    RATELIMIT_ERROR(
        std::chrono::seconds(10),
        10,
        "Attempt to read EpochMetaData for log %lu but its content is invalid!",
        log_id.val_);
    err = E::FAILED;
    return EpochMetaData::UpdateResult::FAILED;
  }
  return EpochMetaData::UpdateResult::UNCHANGED;
}
}} // namespace facebook::logdevice
