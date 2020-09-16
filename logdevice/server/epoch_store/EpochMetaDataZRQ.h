/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>

#include <folly/Memory.h>

#include "logdevice/common/MetaDataTracer.h"
#include "logdevice/common/debug.h"
#include "logdevice/server/epoch_store/EpochStoreEpochMetaDataFormat.h"
#include "logdevice/server/epoch_store/ZookeeperEpochStoreRequest.h"

namespace facebook { namespace logdevice {

/**
 * @file Update EpochMetaData in the ZookeeperEpoch Store using the
 *       given @updater. If the znode is empty, or does not exist, use @updater
 *       to provision initial metadata with the inital epoch.
 */

class EpochMetaDataZRQ : public ZookeeperEpochStoreRequest {
 public:
  EpochMetaDataZRQ(logid_t logid,
                   EpochStore::CompletionMetaData cf,
                   std::shared_ptr<EpochMetaData::Updater> updater,
                   MetaDataTracer tracer,
                   EpochStore::WriteNodeID write_node_id,
                   std::shared_ptr<const NodesConfiguration> cfg,
                   folly::Optional<NodeID> my_node_id)
      : ZookeeperEpochStoreRequest(logid),
        cf_meta_data_(std::move(cf)),
        updater_(updater),
        cfg_(std::move(cfg)),
        tracer_(std::move(tracer)),
        write_node_id_(write_node_id),
        my_node_id_(my_node_id) {
    ld_check(cf_meta_data_);
    ld_check(updater_);
    ld_check(cfg_);
  }

  Status
  legacyDeserializeIntoLogMetaData(std::string value,
                                   LogMetaData& log_metadata) const override {
    EpochMetaData metadata{};
    NodeID node_id;
    int rv = EpochStoreEpochMetaDataFormat::fromLinearBuffer(
        value.data(), value.size(), &metadata, logid_, *cfg_, &node_id);
    if (rv != 0 && err != E::EMPTY) {
      // err set in fromLinearBuffer
      ld_check(err == E::BADMSG || err == E::INVALID_PARAM);
      return err;
    }
    if (node_id.isNodeID()) {
      log_metadata.epoch_store_properties.last_writer_node_id.assign(node_id);
    }

    ld_assert(metadata.isValid() || (metadata.isEmpty() && err == E::EMPTY));
    log_metadata.current_epoch_metadata = std::move(metadata);
    return Status::OK;
  }

  // see ZookeeperEpochStoreRequest.h
  NextStep applyChanges(LogMetaData& log_metadata,
                        bool value_existed) override {
    std::unique_ptr<EpochMetaData> metadata;
    if (value_existed) {
      // Move the data into a unique ptr for comptability with existing APIs
      // and later we will move it back.
      metadata = std::make_unique<EpochMetaData>(
          std::move(log_metadata.current_epoch_metadata));
    }

    if (metadata) {
      tracer_.setOldMetaData(*metadata);
    }
    // Note: we allow the znode buffer to be empty (but not malformed)
    // in such case, create and write metadata with the initial epoch.
    // metadata can be nullptr if znode does not exist yet.
    EpochMetaData::UpdateResult result =
        (*updater_)(logid_, metadata, &tracer_);
    NextStep next_step{NextStep::FAILED};
    switch (result) {
      case EpochMetaData::UpdateResult::UNCHANGED:
        // no need to change metadata, do not continue to write
        next_step = NextStep::STOP;
        err = E::UPTODATE;
        break;
      case EpochMetaData::UpdateResult::FAILED:
        next_step = NextStep::FAILED;
        break;
      case EpochMetaData::UpdateResult::CREATED:
        ld_check(metadata && metadata->isValid());
        next_step = NextStep::PROVISION;
        break;
      case EpochMetaData::UpdateResult::ONLY_NODESET_PARAMS_CHANGED:
      case EpochMetaData::UpdateResult::NONSUBSTANTIAL_RECONFIGURATION:
      case EpochMetaData::UpdateResult::SUBSTANTIAL_RECONFIGURATION:
        ld_check(metadata && metadata->isValid());
        next_step = NextStep::MODIFY;
        break;
    };

    if (metadata) {
      ld_check(result == EpochMetaData::UpdateResult::FAILED ||
               metadata->isValid());
      log_metadata.current_epoch_metadata = std::move(*metadata);
    }
    return next_step;
  }

  // see ZookeeperEpochStoreRequest.h
  std::string getZnodePath(const std::string& rootpath) const override {
    ld_check(logid_ != LOGID_INVALID);
    return rootpath + "/" + std::to_string(logid_.val_) + "/" + znodeName;
  }

  void postCompletion(Status st,
                      LogMetaData&& log_metadata,
                      RequestExecutor& executor) override {
    tracer_.trace(st);
    // eventually transfer metadata_ to the completion function
    auto completion = std::make_unique<EpochStore::CompletionMetaDataRequest>(
        cf_meta_data_,
        worker_idx_,
        worker_type_,
        st,
        logid_,
        std::make_unique<EpochMetaData>(
            std::move(log_metadata.current_epoch_metadata)),
        std::make_unique<EpochStoreMetaProperties>(
            std::move(log_metadata.epoch_store_properties)));

    std::unique_ptr<Request> rq(std::move(completion));
    int rv = executor.postWithRetrying(rq);

    if (rv != 0 && err != E::SHUTDOWN) {
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      1,
                      "Got an unexpected status "
                      "code %s from Processor::postWithRetrying(), dropping "
                      "request for log %lu",
                      error_name(err),
                      logid_.val_);
      ld_check(false);
    }
  }

  // see ZookeeperEpochStoreRequest.h
  int composeZnodeValue(LogMetaData& log_metadata,
                        char* buf,
                        size_t size) const override {
    ld_check(buf);
    // must have valid metadata
    ld_check(log_metadata.current_epoch_metadata.isValid());

    // Only record the NodeID for requests that are written by the sequencer
    // node.
    switch (write_node_id_) {
      case EpochStore::WriteNodeID::MY:
        if (!my_node_id_.has_value() || !my_node_id_->isNodeID()) {
          ld_check(false);
          err = E::INTERNAL;
          return -1;
        }
        log_metadata.epoch_store_properties.last_writer_node_id.assign(
            my_node_id_.value());
        break;
      case EpochStore::WriteNodeID::KEEP_LAST:
        // Do nothing
        break;
      case EpochStore::WriteNodeID::NO:
        log_metadata.epoch_store_properties.last_writer_node_id.reset();
        break;
    }

    folly::Optional<NodeID> node_id_to_write =
        log_metadata.epoch_store_properties.last_writer_node_id;
    return EpochStoreEpochMetaDataFormat::toLinearBuffer(
        log_metadata.current_epoch_metadata, buf, size, node_id_to_write);
  }

  static constexpr const char* znodeName = "sequencer";

 private:
  // completion function to call
  const EpochStore::CompletionMetaData cf_meta_data_;

  std::shared_ptr<EpochMetaData::Updater> updater_;
  // true if the writer's NodeID should be written to the metadata
  std::shared_ptr<const NodesConfiguration> cfg_;

  MetaDataTracer tracer_;

  EpochStore::WriteNodeID write_node_id_;

  // Can be folly::none when it's being called from the tooling.
  folly::Optional<NodeID> my_node_id_;
};

}} // namespace facebook::logdevice
