/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/server/RebuildingMarkerChecker.h"

#include <tuple>

#include "logdevice/common/RetryHandler.h"

namespace facebook { namespace logdevice {

RebuildingMarkerChecker::RebuildingMarkerChecker(
    const std::unordered_map<shard_index_t, membership::ShardState>& shards,
    NodeID my_node_id,
    configuration::NodesConfigurationAPI* nc_api,
    ShardedLocalLogStore* sharded_store)
    : shards_(shards),
      my_node_id_(std::move(my_node_id)),
      nc_api_(nc_api),
      sharded_store_(sharded_store) {
  ld_check(sharded_store_);
}

folly::Expected<
    std::unordered_map<shard_index_t, RebuildingMarkerChecker::CheckResult>,
    E>
RebuildingMarkerChecker::checkAndWriteMarkers() {
  std::unordered_set<shard_index_t> not_missing_data_shards;
  std::unordered_map<shard_index_t, CheckResult> result;
  for (const auto& [shard, _] : shards_) {
    auto res = isNewShard(shard) ? writeMarker(shard) : readMarker(shard);
    result[shard] = res;
    if (res == CheckResult::SHARD_NOT_MISSING_DATA) {
      not_missing_data_shards.insert(shard);
    }
  }

  auto nc_status = markShardsAsProvisioned(not_missing_data_shards);
  if (nc_status != Status::OK) {
    ld_error("Failed marking shards as provisioned: %s (%s)",
             error_name(nc_status),
             error_description(nc_status));
    return folly::makeUnexpected(nc_status);
  }

  return result;
}

bool RebuildingMarkerChecker::isNewShard(shard_index_t shard) const {
  return my_node_id_.generation() <= 1 ||
      shards_.at(shard).storage_state == membership::StorageState::PROVISIONING;
}

RebuildingMarkerChecker::CheckResult
RebuildingMarkerChecker::writeMarker(shard_index_t shard) {
  RebuildingCompleteMetadata metadata;
  LocalLogStore::WriteOptions options;
  LocalLogStore* store = sharded_store_->getByIndex(shard);
  ld_check(store);
  if (store->acceptingWrites() != E::DISABLED) {
    int rv = store->writeStoreMetadata(metadata, options);
    if (rv == 0) {
      return CheckResult::SHARD_NOT_MISSING_DATA;
    } else {
      ld_error("Could not write RebuildingCompleteMetadata for shard %u: %s",
               shard,
               error_description(err));
      if (store->acceptingWrites() != E::DISABLED) {
        return CheckResult::UNEXPECTED_ERROR;
      }
      return CheckResult::SHARD_ERROR;
    }
  } else {
    ld_error("Not writing RebuildingCompleteMetadata for shard %u because the "
             "local store is not accepting writes",
             shard);
    return CheckResult::SHARD_DISABLED;
  }
}

RebuildingMarkerChecker::CheckResult
RebuildingMarkerChecker::readMarker(shard_index_t shard) {
  LocalLogStore* store = sharded_store_->getByIndex(shard);
  ld_check(store);
  RebuildingCompleteMetadata meta;
  int rv = store->readStoreMetadata(&meta);
  if (rv == 0) {
    return CheckResult::SHARD_NOT_MISSING_DATA;
  } else if (err == E::NOTFOUND) {
    ld_info("Did not find RebuildingCompleteMetadata for shard %u. Waiting "
            "for the shard to be rebuilt...",
            shard);
    return CheckResult::SHARD_MISSING_DATA;
  } else {
    // It's likely that the failing disk on which this shard resides has not
    // been repaired yet. Once the disk is repaired, logdeviced will be
    // restarted and we will try reading the marker again.
    ld_error("Error reading RebuildingCompleteMetadata for shard %u: %s",
             shard,
             error_description(err));
    if (store->acceptingWrites() != E::DISABLED) {
      return CheckResult::UNEXPECTED_ERROR;
    }
    return CheckResult::SHARD_ERROR;
  }
}

Status RebuildingMarkerChecker::markShardsAsProvisioned(
    const std::unordered_set<shard_index_t>& shards) {
  std::unordered_set<shard_index_t> to_be_updated;
  for (auto shard : shards) {
    if (shards_[shard].storage_state !=
        membership::StorageState::PROVISIONING) {
      continue;
    }
    to_be_updated.insert(shard);
  }
  if (to_be_updated.empty() || nc_api_ == nullptr) {
    return Status::OK;
  }

  auto result = RetryHandler<Status>::syncRun(
      [&](size_t trial_num) {
        ld_info("Will mark shards %s as PROVISIONED in the NodesConfiguration "
                "(trial #%ld)",
                toString(to_be_updated).c_str(),
                trial_num);

        using namespace configuration::nodes;
        using namespace membership;

        auto storage_membership = nc_api_->getConfig()->getStorageMembership();
        auto storage_version = storage_membership->getVersion();

        // Build the MARK_SHARD_PROVISIONED update
        NodesConfiguration::Update update;
        update.storage_config_update =
            std::make_unique<StorageConfig::Update>();
        update.storage_config_update->membership_update =
            std::make_unique<StorageMembership::Update>(storage_version);
        for (auto shard : shards) {
          auto shard_id = ShardID(my_node_id_.index(), shard);
          if (storage_membership->getShardState(shard_id)->storage_state !=
              StorageState::PROVISIONING) {
            continue;
          }
          update.storage_config_update->membership_update->addShard(
              shard_id,
              {
                  StorageStateTransition::MARK_SHARD_PROVISIONED,
                  Condition::EMPTY_SHARD | Condition::LOCAL_STORE_READABLE |
                      Condition::NO_SELF_REPORT_MISSING_DATA,
              });
        }

        // Apply the update
        Status update_status;
        folly::Baton<> b;
        nc_api_->update(
            std::move(update),
            [&](Status st, std::shared_ptr<const NodesConfiguration>) {
              update_status = st;
              b.post();
            });
        b.wait();
        return update_status;
      },
      [](const Status& st) { return st == Status::VERSION_MISMATCH; },
      /* num_tries */ 10,
      /* backoff_min */ std::chrono::seconds(1),
      /* backoff_max */ std::chrono::seconds(60),
      /* jitter_param */ 0.25);

  return result.hasValue() ? result.value() : result.error();
}

}} // namespace facebook::logdevice
