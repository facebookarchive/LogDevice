/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "EpochMetaDataUpdater.h"
#include "logdevice/common/MetaDataTracer.h"
#include "logdevice/common/NodeSetSelectorFactory.h"
#include "logdevice/common/debug.h"

#include "logdevice/lib/ClientProcessor.h"

namespace facebook { namespace logdevice {

EpochMetaData::UpdateResult EpochMetaDataUpdater::
operator()(logid_t log_id,
           std::unique_ptr<EpochMetaData>& info,
           MetaDataTracer* tracer) {
  return generateNewMetaData(log_id,
                             info,
                             config_,
                             nodeset_selector_,
                             tracer,
                             use_storage_set_format_,
                             provision_if_empty_,
                             update_if_exists_,
                             force_update_);
}

EpochMetaData::UpdateResult EpochMetaDataUpdaterBase::generateNewMetaData(
    logid_t log_id,
    std::unique_ptr<EpochMetaData>& info,
    std::shared_ptr<Configuration> config,
    std::shared_ptr<NodeSetSelector> nodeset_selector,
    MetaDataTracer* tracer,
    bool use_storage_set_format,
    bool provision_if_empty,
    bool update_if_exists,
    bool force_update) {
  const LogsConfig::LogGroupNode* logcfg = config->getLogGroupByIDRaw(log_id);
  if (!logcfg) {
    err = E::NOTFOUND;
    return EpochMetaData::UpdateResult::FAILED;
  }

  // If the given metadata is empty, provision it with an initial metadata
  // Otherwise, update the metadata given
  const bool metadata_needs_provisioning = !info || info->isEmpty();
  EpochMetaData::UpdateResult success_res = info
      ? EpochMetaData::UpdateResult::UPDATED
      : EpochMetaData::UpdateResult::CREATED;
  if (metadata_needs_provisioning && !provision_if_empty) {
    RATELIMIT_INFO(std::chrono::seconds(10),
                   10,
                   "Metadata not found for log %lu",
                   log_id.val_);
    err = E::EMPTY;
    return EpochMetaData::UpdateResult::FAILED;
  }
  if (!metadata_needs_provisioning && !update_if_exists) {
    ld_error("Metadata already provisioned for log %lu", log_id.val_);
    err = E::EXISTS;
    return EpochMetaData::UpdateResult::FAILED;
  }
  if (!info) {
    info = std::make_unique<EpochMetaData>();
  }
  const StorageSet* old_storage_set =
      metadata_needs_provisioning ? nullptr : &info->shards;
  std::unique_ptr<StorageSet> new_storage_set;
  NodeSetSelector::Decision decision;
  NodeSetSelector::Options selector_options;

  std::tie(decision, new_storage_set) = nodeset_selector->getStorageSet(
      log_id, config, old_storage_set, &selector_options);

  switch (decision) {
    case NodeSetSelector::Decision::FAILED:
      ld_error("NodeSetSelector failed to generate new nodeset for log "
               "%lu: %s",
               log_id.val_,
               error_name(err));
      // coalesce errs set by nodeset selector to E::FAILED and propagate to
      // the epoch store
      err = E::FAILED;
      return EpochMetaData::UpdateResult::FAILED;
    case NodeSetSelector::Decision::KEEP:
      if (metadata_needs_provisioning) {
        ld_critical("INTERNAL ERROR: NodeSet selector returned Decision::KEEP "
                    "for an invalid epoch metadata that needs to be "
                    "provisioned! logid: %lu",
                    log_id.val_);
        // should be enforced by the nodeset selector
        ld_check(false);
        err = E::INTERNAL;
        return EpochMetaData::UpdateResult::FAILED;
      }

      if (!force_update &&
          (ReplicationProperty::fromLogAttributes(logcfg->attrs()) ==
           info->replication) &&
          !info->disabled()) {
        // keep the existing metadata only when:
        // 1) no change in the nodeset for the log, and
        // 2) replication property stays the same,
        // 3) existing metadata is not disabled, and
        // 4) force_update flag wasn't set
        return EpochMetaData::UpdateResult::UNCHANGED;
      }
      break;
    case NodeSetSelector::Decision::NEEDS_CHANGE:
      break;
  }

  epoch_metadata_version::type metadata_version =
      epoch_metadata_version::versionToWrite(config->serverConfig());

  if (metadata_needs_provisioning) {
    if (new_storage_set == nullptr) {
      ld_critical("INTERNAL ERROR: NodeSet selector returned nullptr nodeset "
                  "for log %lu whose epoch metadata needs to be provisioned!",
                  log_id.val_);
      // should be enforced by the nodeset selector
      ld_check(false);
      err = E::INTERNAL;
      return EpochMetaData::UpdateResult::FAILED;
    }

    // provision metadata with the initial epoch
    info->h.epoch = info->h.effective_since = EPOCH_MIN;
    // use the configured `metadata_version'
    info->h.version = metadata_version;
  } else {
    // epoch remains the same
    // update effective_since to be the same as epoch
    info->h.effective_since = epoch_t(info->h.epoch.val_);
  }
  // update replication property
  info->replication = ReplicationProperty::fromLogAttributes(logcfg->attrs());

  // update the version to metadata_version if applicable
  if (info->h.version < metadata_version) {
    info->h.version = metadata_version;
  }

  // update the nodeset
  if (new_storage_set) {
    info->setShards(*new_storage_set);
  }

  info->nodesconfig_hash.assign(
      config->serverConfig()->getStorageNodesConfigHash());
  info->h.flags |= MetaDataLogRecordHeader::HAS_NODESCONFIG_HASH;

  // since this is a newly generated metadata, by default it is not yet written
  // to the metadata log
  info->h.flags &= ~MetaDataLogRecordHeader::WRITTEN_IN_METADATALOG;
  // clear the DISABLED flag as well
  info->h.flags &= ~MetaDataLogRecordHeader::DISABLED;

  if (use_storage_set_format) {
    // Enable the new copyset serialization format
    info->h.flags |= MetaDataLogRecordHeader::HAS_STORAGE_SET;
  } else {
    info->h.flags &= ~MetaDataLogRecordHeader::HAS_STORAGE_SET;
  }

  ld_check(!info->writtenInMetaDataLog());
  ld_check(!info->disabled());

  if (!info->isValid()) {
    ld_critical(
        "INTERNAL ERROR: Updated epoch metadata is invalid for log %lu: %s",
        log_id.val_,
        info->toString().c_str());
    // nodeset selector should enforce that epoch metadata is valid
    ld_check(false);
    err = E::INTERNAL;
    return EpochMetaData::UpdateResult::FAILED;
  }

  if (!info->matchesConfig(log_id, config, /* output_errors */ true)) {
    ld_critical("INTERNAL ERROR: Updated epoch metadata is inconsistent with "
                "config for log %lu: %s",
                log_id.val_,
                info->toString().c_str());
    // nodeset selector should enforce that epoch metadata is
    // consistent with the configuration
    ld_check(false);
    // However, since metadata is valid, we allow sequencer activation here
    // anyway
  }

  if (tracer) {
    tracer->setAction(MetaDataTracer::Action::PROVISION_METADATA);
    tracer->setNewMetaData(*info);
  }

  return success_res; // UPDATED or CREATED
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

EpochMetaData::UpdateResult EpochMetaDataUpdateToNextEpoch::
operator()(logid_t log_id,
           std::unique_ptr<EpochMetaData>& info,
           MetaDataTracer* tracer) {
  // do not allow calling this with metadata logids
  if (log_id <= LOGID_INVALID || log_id > LOGID_MAX) {
    err = E::INVALID_PARAM;
    ld_check(false);
    return EpochMetaData::UpdateResult::FAILED;
  }
  if (info && !info->isValid()) {
    ld_error("Attempt to bump epoch for log %lu but the epoch store content is "
             "invalid!",
             log_id.val_);
    err = E::FAILED;
    return EpochMetaData::UpdateResult::FAILED;
  }

  // Checking if this job (that should run on a sequencer) is responsible for
  // provisioning metadata. If it is, sequencers should also be responsible for
  // writing metadata logs, and the written bit should be enabled in the
  // metadata.
  bool can_provision = canSequencerProvision();
  Status can_activate_without_provisioning =
      canEpochBeBumpedWithoutProvisioning(log_id,
                                          info,
                                          /*tolerate_notfound=*/true);
  if (!can_provision && (can_activate_without_provisioning != E::OK)) {
    err = can_activate_without_provisioning;
    return EpochMetaData::UpdateResult::FAILED;
  }

  if (info && info->h.epoch == EPOCH_MAX) {
    // Note: we actually do not return EPOCH_MAX to the sequencer, despite
    // it is a valid epoch. Otherwise we need to store an invalid metadata
    // to the epoch store. This should be OK since it is unlikely logs are
    // running out of epochs.
    err = E::TOOBIG;
    return EpochMetaData::UpdateResult::FAILED;
  }

  // default result is updated, could be changed to CREATED or FAILED by the
  // updater if sequencers provision logs
  EpochMetaData::UpdateResult res = EpochMetaData::UpdateResult::UPDATED;
  if (can_provision) {
    ld_check(config_);
    bool provisioning_required = !info || !info->matchesConfig(log_id, config_);
    // We cannot re-provision metadata if we have valid metadata that has not
    // been written to the metadata logs
    bool provisioning_allowed = !info || info->isEmpty() || info->disabled() ||
        info->writtenInMetaDataLog();
    if (provisioning_required && provisioning_allowed) {
      NodeSetSelectorType nodeset_selector_type = config_->serverConfig()
                                                      ->getMetaDataLogsConfig()
                                                      .nodeset_selector_type;
      ld_check(nodeset_selector_type != NodeSetSelectorType::INVALID);
      auto nodeset_selector =
          NodeSetSelectorFactory::create(nodeset_selector_type);
      res = generateNewMetaData(log_id,
                                info,
                                config_,
                                std::move(nodeset_selector),
                                tracer,
                                use_storage_set_format_,
                                provision_if_empty_,
                                true /* update_if_exists */,
                                true /* force_udpate */);
      // If we detected changes are required, they should happen
      ld_check(res != EpochMetaData::UpdateResult::UNCHANGED);
      if (res == EpochMetaData::UpdateResult::FAILED) {
        return res;
      }
    } else {
      // TODO: (T15881519) reluctant sequencer activation. We want to activate
      // the sequencer with the old metadata until it manages to write it to the
      // metadata log, then reactivate whenever that succeeds with the new
      // metadata
      ld_assert(canEpochBeBumpedWithoutProvisioning(log_id, info) == E::OK);
    }
  }
  ld_check(info && info->isValid());

  // Note that the following check is done before incrementing the epoch. This
  // is because we store the _next_ epoch in epoch store, but here we want to
  // check against the epoch with which this sequencer will be activated (i.e.
  // the one we have read from ZK)
  if (acceptable_activation_epoch_.hasValue() &&
      acceptable_activation_epoch_.value() != info->h.epoch) {
    err = E::ABORTED;
    return EpochMetaData::UpdateResult::FAILED;
  }

  ++info->h.epoch.val_;
  if (info->h.epoch <= EPOCH_MIN) {
    ld_critical("Unexpected - bumped epoch in EpochStore to epoch %u, "
                "should be >= 2",
                info->h.epoch.val());
    ld_check(false);
    return EpochMetaData::UpdateResult::FAILED;
  }
  ld_check(info->isValid());
  // Updating metadata in tracer
  if (tracer) {
    tracer->setNewMetaData(*info);
  }
  return res;
}

std::unique_ptr<EpochMetaData>
EpochMetaDataUpdateToNextEpoch::getCompletionMetaData(
    const EpochMetaData* src) {
  if (!src) {
    return nullptr;
  }
  auto res = std::make_unique<EpochMetaData>(*src);
  ld_check(res->h.epoch > EPOCH_MIN);
  --res->h.epoch.val_;
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
    if (!compare_equality_->isSubstantiallyIdentical(*info)) {
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
  return EpochMetaData::UpdateResult::UPDATED;
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
  return EpochMetaData::UpdateResult::UPDATED;
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
  if (tracer) {
    tracer->setAction(MetaDataTracer::Action::DISABLE);
    tracer->setNewMetaData(*info);
  }
  return EpochMetaData::UpdateResult::UPDATED;
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
