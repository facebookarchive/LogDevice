/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/Memory.h>

#include "logdevice/common/EpochStoreEpochMetaDataFormat.h"
#include "logdevice/common/MetaDataTracer.h"
#include "logdevice/common/debug.h"
#include "logdevice/server/ZookeeperEpochStoreRequest.h"

namespace facebook { namespace logdevice {

/**
 * @file Update EpochMetaData in the ZookeeperEpoch Store using the
 *       given @updater. If the znode is empty, or does not exist, use @updater
 *       to provision initial metadata with the inital epoch.
 */

class EpochMetaDataZRQ : public ZookeeperEpochStoreRequest {
 public:
  EpochMetaDataZRQ(logid_t logid,
                   epoch_t epoch,
                   EpochStore::CompletionMetaData cf,
                   ZookeeperEpochStore* store,
                   std::shared_ptr<EpochMetaData::Updater> updater,
                   MetaDataTracer tracer,
                   EpochStore::WriteNodeID write_node_id,
                   std::shared_ptr<const NodesConfiguration> cfg,
                   folly::Optional<NodeID> my_node_id)
      : ZookeeperEpochStoreRequest(logid, epoch, std::move(cf), store),
        updater_(updater),
        cfg_(std::move(cfg)),
        tracer_(std::move(tracer)),
        write_node_id_(write_node_id),
        my_node_id_(my_node_id) {
    ld_check(updater_);
    ld_check(cfg_);
  }

  // see ZookeeperEpochStoreRequest.h
  NextStep onGotZnodeValue(const char* value, int len) override {
    ld_check(epoch_ == EPOCH_INVALID); // must not yet be initialized

    std::unique_ptr<EpochMetaData> metadata;
    if (value) {
      // the znode exists
      metadata = std::make_unique<EpochMetaData>();
      NodeID node_id;
      int rv = EpochStoreEpochMetaDataFormat::fromLinearBuffer(
          value, len, metadata.get(), logid_, *cfg_, &node_id);
      if (rv != 0 && err != E::EMPTY) {
        // err set in fromLinearBuffer
        ld_check(err == E::BADMSG || err == E::INVALID_PARAM);
        return NextStep::FAILED;
      }
      if (node_id.isNodeID()) {
        if (!meta_properties_) {
          meta_properties_ = std::make_unique<EpochStoreMetaProperties>();
        }
        meta_properties_->last_writer_node_id.assign(node_id);
      }

      ld_assert(
          metadata &&
          (metadata->isValid() || (metadata->isEmpty() && err == E::EMPTY)));
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
      // update epoch_ and metadata_ member in the request
      epoch_ = metadata->h.epoch;
      metadata_ = std::move(metadata);
      ld_check(result == EpochMetaData::UpdateResult::FAILED ||
               metadata_->isValid());
    }
    return next_step;
  }

  // see ZookeeperEpochStoreRequest.h
  std::string getZnodePath() const override {
    ld_check(logid_ != LOGID_INVALID);
    return store_->rootPath() + "/" + std::to_string(logid_.val_) + "/" +
        znodeName;
  }

  void postCompletion(Status st) override {
    tracer_.trace(st);
    store_->postCompletion(
        // eventually transfer metadata_ to the completion function
        std::make_unique<EpochStore::CompletionMetaDataRequest>(
            cf_meta_data_,
            worker_idx_,
            st,
            logid_,
            std::move(metadata_),
            std::move(meta_properties_)));
  }

  // see ZookeeperEpochStoreRequest.h
  int composeZnodeValue(char* buf, size_t size) override {
    ld_check(buf);
    // must have valid metadata
    ld_check(metadata_ != nullptr);
    ld_check(metadata_->isValid());
    ld_check(metadata_->h.epoch == epoch_);

    // Only record the NodeID for requests that are written by the sequencer
    // node.
    folly::Optional<NodeID> node_id_to_write;
    switch (write_node_id_) {
      case EpochStore::WriteNodeID::MY:
        node_id_to_write = my_node_id_.value();
        if (!node_id_to_write->isNodeID()) {
          ld_check(false);
          err = E::INTERNAL;
          return -1;
        }
        break;
      case EpochStore::WriteNodeID::KEEP_LAST:
        if (meta_properties_) {
          node_id_to_write = meta_properties_->last_writer_node_id;
        }
        break;
      case EpochStore::WriteNodeID::NO:
        break;
    }
    return EpochStoreEpochMetaDataFormat::toLinearBuffer(
        *metadata_, buf, size, node_id_to_write);
  }

  static constexpr const char* znodeName = "sequencer";

 private:
  std::shared_ptr<EpochMetaData::Updater> updater_;
  // true if the writer's NodeID should be written to the metadata
  std::shared_ptr<const NodesConfiguration> cfg_;

  // epoch metadata read from or written to the epoch store, whichever happened
  // last.
  std::unique_ptr<EpochMetaData> metadata_;

  // MetaProperties that will be passed to the CF
  std::unique_ptr<EpochStoreMetaProperties> meta_properties_;

  MetaDataTracer tracer_;

  EpochStore::WriteNodeID write_node_id_;


  // Can be folly::none when it's being called from the tooling.
  folly::Optional<NodeID> my_node_id_;
};

}} // namespace facebook::logdevice
