/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/locallogstore/LocalLogStore.h"

#include <folly/hash/SpookyHashV2.h>

#include "logdevice/common/LocalLogStoreRecordFormat.h"
#include "logdevice/common/Random.h"
#include "logdevice/common/SimpleEnumMap.h"
#include "logdevice/common/configuration/ServerConfig.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/protocol/STORE_Message.h"
#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice {

void FlushCallback::deactivate() {
  LocalLogStore* store = registered_with.load();
  if (store != nullptr) {
    store->unregisterOnFlushCallback(*this);
  }
}

int LocalLogStore::findTime(logid_t /*log_id*/,
                            std::chrono::milliseconds /*timestamp*/,
                            lsn_t* /*lo*/,
                            lsn_t* /*hi*/,
                            bool /*approximate*/,
                            bool /*allow_blocking_io*/,
                            std::chrono::steady_clock::time_point
                            /*deadline*/) const {
  err = E::NOTSUPPORTED;
  return -1;
}

int LocalLogStore::findKey(logid_t /*log_id*/,
                           std::string /*key*/,
                           lsn_t* /*lo*/,
                           lsn_t* /*hi*/,
                           bool /*approximate*/,
                           bool /*allow_blocking_io*/) const {
  err = E::NOTSUPPORTED;
  return -1;
}

void LocalLogStore::normalizeTimeRanges(RecordTimeIntervals&) const {}

int LocalLogStore::registerOnFlushCallback(FlushCallback& cb) {
  std::unique_lock<std::mutex> lock(flushing_mtx_);
  ld_check(!cb.links.is_linked());
  if (!cb.links.is_linked()) {
    on_flush_cbs_.push_back(cb);
    cb.registered_with = this;
    return 0;
  }
  err = E::INVALID_PARAM;
  return -1;
}

void LocalLogStore::unregisterOnFlushCallback(FlushCallback& cb) {
  std::unique_lock<std::mutex> lock(flushing_mtx_);
  if (cb.registered_with.load() == this) {
    cb.links.unlink();
    cb.registered_with = nullptr;
  }
}

void LocalLogStore::broadcastFlushEvent(FlushToken flushedUpTo) {
  std::unique_lock<std::mutex> lock(flushing_mtx_);
  for (auto& cb : on_flush_cbs_) {
    cb(this, flushedUpTo);
  }
}

bool LocalLogStoreReadFilter::operator()(logid_t,
                                         lsn_t,
                                         const ShardID* const copyset_original,
                                         const copyset_size_t copyset_size,
                                         const csi_flags_t flags,
                                         RecordTimestamp /* min_ts */,
                                         RecordTimestamp /* max_ts */) {
  const bool may_filter =
      scd_my_shard_id_.isValid() || !required_in_copyset_.empty();

  // Parse the copyset only if necessary
  if (!may_filter) {
    return true;
  }

  if ((flags & LocalLogStoreRecordFormat::CSI_FLAG_DRAINED) != 0) {
    // Record was re-replicated to other nodes. We are not responsible
    // for shipping it. We should not be in the copyset.
    dd_assert(
        !scd_my_shard_id_.isValid() ||
            std::find(copyset_original,
                      copyset_original + copyset_size,
                      scd_my_shard_id_) == copyset_original + copyset_size,
        "Record with DRAINED flag has copyset containing local ShardID");
    return false;
  }

  // If we are a donor shard for rebuilding, check if the copyset contains at
  // least one rebuilding shard. If that's not the case, we will filter the
  // record.
  if (!required_in_copyset_.empty()) {
    copyset_off_t i = 0;
    for (; i < copyset_size; ++i) {
      // We expect requried_in_copyset_ to be smaller than the size of
      // the copyset, hence the use of std::find on a  vector instead of
      // a std::unordered_set() here.
      if (std::find(required_in_copyset_.begin(),
                    required_in_copyset_.end(),
                    copyset_original[i]) != required_in_copyset_.end()) {
        break;
      }
    }
    if (i == copyset_size) {
      // We could not find a recipient in the copyset that's in the
      // required_in_copyset_ list, filter this record.
      return false;
    }
  }

  if (flags & LocalLogStoreRecordFormat::CSI_FLAG_HOLE) {
    // Do not filter out holes if we are in a non-rebuilding context
    return true;
  }

  if (!scd_my_shard_id_.isValid()) {
    // There is no SCD filtering here. Just ship the record.
    return true;
  }

  //
  // We are in Single Copy Delivery mode.
  //

  // Before doing the SCD calculation, prepare the effective copyset.
  const ShardID* copyset;
  copyset_custsz_t<8> copyset_reordered;
  if (scd_copyset_reordering_ == SCDCopysetReordering::NONE) {
    copyset = copyset_original;
  } else {
    copyset_reordered.assign(copyset_original, copyset_original + copyset_size);
    applyCopysetReordering(copyset_reordered.data(), copyset_size);
    copyset = copyset_reordered.data();
  }

  // The following filtering logic ensures that, assuming copysets are
  // consistent accross all copies of the same record, at least one shard will
  // ship each copy, and if possible only one shard will ship it.
  //
  // This is guaranteed in three steps:
  // 1/ If local SCD is enabled (i.e. client_location_ is not null),
  //    consider local shards (shards in the same region as the client) first.
  //    The first non-down local shard in a record's copyset will ship that
  //    record.
  //    If local SCD is not enabled or there are no local shards in the
  //    copyset, switch to normal SCD logic.
  // 2/ If this storage node is among the last "copyset_size-replication"
  //    non-down recipients in the record's copyset, then ship the record.
  // 3/ If this storage shard is the first non-down recipient in the record's
  //    copyset, then ship the record.
  //
  // Note that for all steps we ship the record even if we see ourself in
  // the known down list because that is how the reader gets informed that we
  // are back up and we should be removed from that list.

  auto is_down = [&](int i) {
    // We expect scd_known_down_ to always be empty or of a very small size,
    // hence the use of std::find on a folly::small_vector over
    // std::unordered_set here.
    return std::find(scd_known_down_.begin(),
                     scd_known_down_.end(),
                     copyset[i]) != scd_known_down_.end();
  };

  auto is_me = [&](int i) { return copyset[i] == scd_my_shard_id_; };

  if (client_location_) {
    // 1/ Local scd logic
    auto nc = getNodesConfiguration();
    auto is_local = [&](int i) {
      if (auto node = nc->getNodeServiceDiscovery(copyset[i].node())) {
        const folly::Optional<NodeLocation>& location = node->location;
        if (location.hasValue()) {
          if (client_location_->sharesScopeWith(
                  location.value(), NodeLocationScope::REGION)) {
            return true;
          }
        } else {
          RATELIMIT_WARNING(std::chrono::seconds(10),
                            10,
                            "Local SCD: Node N%d has no location",
                            copyset[i].node());
        }
      }
      return false;
    };
    for (copyset_off_t i = 0; i < copyset_size; ++i) {
      if (is_local(i)) {
        if (is_me(i)) {
          return true;
        } else if (!is_down(i)) {
          return false;
        }
      }
    }
  }

  // 2/ Normal scd logic
  if (scd_replication_ > 0) {
    int n_extras = copyset_size - scd_replication_;
    int n_not_down_seen = 0;
    for (int i = copyset_size - 1; i >= 0 && n_not_down_seen < n_extras; --i) {
      if (is_me(i)) {
        return true;
      }
      if (is_down(i)) {
        continue;
      }
      ++n_not_down_seen;
    }
  }
  // 3/
  for (copyset_off_t i = 0; i < copyset_size; ++i) {
    if (is_me(i)) {
      return true;
    }
    if (is_down(i)) {
      continue;
    }
    return false;
  }

  // All shards in the copyset are in the known down list, and our shard is not
  // in the copyset. This should not happen in practice, but let's be safe and
  // ship this copy.
  return true;
}

void LocalLogStoreReadFilter::applyCopysetReordering(
    ShardID* copyset,
    copyset_size_t copyset_size) const {
  // NOTE: It is unsafe to change an algorithm when it is already deployed.
  // The correctness of SCD depends on servers' shuffling being in sync.  Add
  // a new version instead.
  switch (scd_copyset_reordering_) {
    case SCDCopysetReordering::HASH_SHUFFLE: {
      // We need to do two things: 1) hash the copyset 2) shuffle it based on
      // the hash.  For shuffling we use xorshift128 for its fast
      // initialisation.  For hashing we use SpookyV2 which conveniently
      // produces 128-bit output, just the right number to seed xorshift128.
      //
      // The choice of hashing the copyset is not obvious compared to
      // something else like the RecordID.  Hashing the copyset ensures that
      // any consecutive records with the same copyset (if the sticky copysets
      // feature is being used on the write path) will be served by the same
      // node, allowing other nodes not to have to read the records (assuming
      // the copyset index is used).

      // Prior to flexible log sharding (T15517759), copysets were only made of
      // node_index_t. Convert our array of ShardIDs to the old format so that
      // the hashing remains deterministic. If we break the determinism of the
      // hashing, this will cause SCD to not function properly.

      // Using randomly generated seeds
      copysetShuffle(
          copyset, copyset_size, 0x59d2d101d78f02ad, 0x430bfb34b1cd41e1);
      break;
    }
    case SCDCopysetReordering::HASH_SHUFFLE_CLIENT_SEED: {
      // Same methodology as in HASH_SHUFFLE.
      // What changes is that instead of randomly generated seeds for hashing,
      // we now use the session id hash of the client that triggered the read.
      copysetShuffle(copyset, copyset_size, csid_hash_pt1, csid_hash_pt2);
      break;
    }
    case SCDCopysetReordering::NONE:
      // Asserting because we don't expect to get called but just returning in
      // release builds is fine
      ld_check(false);
      break;
    default:
      ld_critical("Unsupported SCDCopysetReordering %d!",
                  static_cast<int>(scd_copyset_reordering_));
      std::abort();
  }
}

std::shared_ptr<const NodesConfiguration>
LocalLogStoreReadFilter::getNodesConfiguration() const {
  return updateable_config_->getNodesConfiguration();
}

void LocalLogStoreReadFilter::setUpdateableConfig(
    std::shared_ptr<UpdateableConfig> config) {
  updateable_config_ = config;
}

void LocalLogStoreReadFilter::copysetShuffle(ShardID* copyset,
                                             copyset_size_t copyset_size,
                                             uint64_t hash_pt1,
                                             uint64_t hash_pt2) const {
  folly::small_vector<node_index_t, 4> node_ids;
  node_ids.resize(copyset_size);
  for (size_t i = 0; i < copyset_size; ++i) {
    node_ids[i] = copyset[i].node();
  }

  // Seeds for SpookyHash
  uint64_t h1 = hash_pt1, h2 = hash_pt2;

  folly::hash::SpookyHashV2::Hash128(
      node_ids.data(), copyset_size * sizeof(node_index_t), &h1, &h2);
  if (UNLIKELY(h1 == 0 && h2 == 0)) {
    // xorshift requires non-zero seed
    ++h2;
  }
  XorShift128PRNG rng;
  rng.seed(h1, h2);
  simple_shuffle(copyset, copyset + copyset_size, rng);
}

namespace {
SimpleEnumMap<IteratorState, const char*>
    state_names({{IteratorState::ERROR, "ERROR"},
                 {IteratorState::WOULDBLOCK, "WOULDBLOCK"},
                 {IteratorState::AT_END, "AT_END"},
                 {IteratorState::LIMIT_REACHED, "LIMIT_REACHED"},
                 {IteratorState::AT_RECORD, "AT_RECORD"}});
}

std::ostream& operator<<(std::ostream& os, IteratorState state) {
  return os << state_names[state];
}

}} // namespace facebook::logdevice
