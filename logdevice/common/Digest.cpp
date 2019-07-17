/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/Digest.h"

#include <folly/Format.h>

#include "logdevice/common/debug.h"
#include "logdevice/common/protocol/RECORD_Message.h"
#include "logdevice/common/types_internal.h"

namespace facebook { namespace logdevice {

bool Digest::RecordMetadata::operator==(const RecordMetadata& rhs) const {
  return flags == rhs.flags && wave_or_seal_epoch == rhs.wave_or_seal_epoch;
}

bool Digest::RecordMetadata::operator<(const RecordMetadata& rhs) const {
  auto get = [](const RecordMetadata& m) {
    // Precedence rules:
    // 1) if the record is written by recovery, it has a higher precedence
    //    over records/amends that are not.
    // 2) for two records both written by recovery, the one with higher
    //    seal_epoch (stored as wave) has higher precedence.
    // 3) for two records both not written by recovery, the one with higher
    //    wave has higher precedence

    const bool written_by_recovery =
        (m.flags & RECORD_Header::WRITTEN_BY_RECOVERY);
    uint32_t wave_or_seal_epoch = m.wave_or_seal_epoch;
    if (!written_by_recovery && wave_or_seal_epoch == 0) {
      // adjust the wave number so that records written by legacy recovery
      // (wave == 0 but no WRITTEN_BY_RECOVERY) take precedence over
      // records written by sequencers
      // TODO 11866467: the wave == 0 logic is kept for back-comptiability,
      // remove once all deployments are upgraded
      wave_or_seal_epoch = std::numeric_limits<uint32_t>::max();
    }
    return std::make_tuple(written_by_recovery ? 1 : 0, wave_or_seal_epoch);
  };

  return get(*this) < get(rhs);
}

std::string Digest::RecordMetadata::toString() const {
  return folly::sformat("flags: {:x}, {:s}: {:d}",
                        flags,
                        (flags | RECORD_Header::WRITTEN_BY_RECOVERY)
                            ? "seal epoch"
                            : "wave number",
                        wave_or_seal_epoch);
}

Digest::RecordMetadata
Digest::RecordMetadata::fromRecord(const DataRecordOwnsPayload& record) {
  return RecordMetadata{record.flags_,
                        // TODO 11866467: record.extra_metadata_ can be absent
                        // if storage node is running with an older version
                        record.extra_metadata_
                            ? record.extra_metadata_->header.wave
                            : (record.flags_ & RECORD_Header::HOLE ? 0 : 1)};
}

Digest::Digest(
    logid_t log_id,
    epoch_t epoch,
    const EpochMetaData& epoch_metadata,
    epoch_t seal_epoch,
    const std::shared_ptr<const NodesConfiguration>& nodes_configuration,
    Options options)
    : log_id_(log_id),
      epoch_(epoch),
      seal_epoch_(seal_epoch),
      failure_domain_(epoch_metadata.shards,
                      *nodes_configuration,
                      epoch_metadata.replication),
      options_(options) {
  ld_check(seal_epoch > EPOCH_INVALID);
}

bool Digest::needsMutation(Entry& entry,
                           std::set<ShardID>* successfully_stored,
                           std::set<ShardID>* amend_metadata,
                           std::set<ShardID>* conflict_copies,
                           bool complete_digest) {
  ld_check(successfully_stored != nullptr);
  ld_check(amend_metadata != nullptr);
  ld_check(conflict_copies != nullptr);

  successfully_stored->clear();
  amend_metadata->clear();
  conflict_copies->clear();

  if (entry.record == nullptr) {
    // no nodes send us digest records or all digest nodes for this entry has
    // been removed but the entry still exists, should no happen.
    RATELIMIT_CRITICAL(std::chrono::seconds(10),
                       10,
                       "Digest entry exists while no target record state found "
                       "for this entry!");
    ld_check(false);
    return false;
  }

  const RecordMetadata metadata_highest =
      RecordMetadata::fromRecord(*entry.record);

  // used when complete_digest is set
  std::set<ShardID> highest_nodes;
  for (const auto& kv : entry.node_info) {
    if (kv.second == metadata_highest &&
        kv.second.flags & RECORD_Header::WRITTEN_BY_RECOVERY &&
        seal_epoch_.val_ == kv.second.wave_or_seal_epoch &&
        !entry.byte_offset_changed) {
      // the node has sent us what was stored before by the same epoch
      // recovery instance. It's likely that the epoch recovery has been
      // restarted. Consider the node fully stored.
      successfully_stored->insert(kv.first);
    } else if ((kv.second.flags ^ metadata_highest.flags) &
               RECORD_Header::HOLE) {
      // the record sent by this node has a different hole/record property
      // from the highest precedence metadata in the digest, consider it
      // to be a conflict copy whose payload needs to be replaced
      conflict_copies->insert(kv.first);
    } else {
      // otherwise, epoch recovery just needs to amend the record metadata but
      // not the payload
      amend_metadata->insert(kv.first);
    }

    if (complete_digest && kv.second == metadata_highest) {
      highest_nodes.insert(kv.first);
    }
  }

  if (!conflict_copies->empty()) {
    return true;
  }

  if (!complete_digest && !amend_metadata->empty()) {
    // for a complete digest, it is OK to leave some extra copies without
    // amending, as long as successful copies can form a copyset.
    return true;
  }

  // consider the record up-to-date in the following cases
  // 1.  the record has the highest precedence; AND:
  // 2.1 we have a complete digest (AUTHORITATIVE_COMPLETE or
  //                                NON_AUTHORITATIVE); OR
  // 2.2 the record was written by epoch recovery at the same epoch
  const auto& eligible_nodes =
      complete_digest ? highest_nodes : *successfully_stored;

  failure_domain_.resetAttributeCounters();
  for (auto idx : eligible_nodes) {
    failure_domain_.setShardAttribute(idx, true);
  }

  return !failure_domain_.canReplicate(true);
}

void Digest::onRecord(ShardID from,
                      std::unique_ptr<DataRecordOwnsPayload> record) {
  esn_t esn = lsn_to_esn(record->attrs.lsn);
  Entry& entry = entries_[esn];

  RecordMetadata metadata = RecordMetadata::fromRecord(*record);
  ld_check(entry.record != nullptr || entry.node_info.empty());
  entry.node_info[from] = metadata;

  if (entry.record == nullptr ||
      RecordMetadata::fromRecord(*entry.record) < metadata) {
    entry.record = std::move(record);
  }
}

esn_t Digest::applyBridgeRecords(esn_t last_known_good,
                                 esn_t* tail_record_out) {
  // for each node, store the RecordMetadata of the highest bridge
  // record received so far (as iterator moves forward)
  std::unordered_map<ShardID, RecordMetadata, ShardID::Hash> bridges;
  for (auto eit = entries_.begin(); eit != entries_.end(); ++eit) {
    for (auto nit = eit->second.node_info.begin();
         nit != eit->second.node_info.end();
         ++nit) {
      // update the highest bridge received if possible
      if (nit->second.flags & RECORD_Header::BRIDGE) {
        auto result = bridges.insert(*nit);
        if (!result.second) {
          // a bridge record already exist, update only if the new record is
          // of higher precedence
          ld_check(result.first->second.flags & RECORD_Header::BRIDGE);
          if (result.first->second < nit->second) {
            result.first->second = nit->second;
          }
        }
      }
    }

    // apply the bridge record received so far to each digest entry,
    // overwriting the entry if needed
    std::unique_ptr<DataRecordOwnsPayload>& existing_record =
        eit->second.record;
    ld_check(existing_record != nullptr);

    // already collected the bridge information upto this record, it is OK to
    // remove the bridge flag in the record to be mutated. This is also needed
    // to get the correct set of nodes in needsMutation(). The new bridge
    // record will be computed later in this function
    existing_record->flags_ &= ~RECORD_Header::BRIDGE;

    for (const auto& kv : bridges) {
      if (RecordMetadata::fromRecord(*existing_record) < kv.second) {
        // the record should actually transform to be a hole because of
        // a previously received bridge record. replacing the record

        auto extra_metadata = std::make_unique<ExtraMetadata>();
        // we only care about wave for this hole in extra metadata
        extra_metadata->header.wave = kv.second.wave_or_seal_epoch;

        RECORD_flags_t flags = RECORD_Header::WRITTEN_BY_RECOVERY |
            RECORD_Header::HOLE | RECORD_Header::DIGEST;

        // create a hole record from the existing record
        std::unique_ptr<DataRecordOwnsPayload> hole_record(
            new DataRecordOwnsPayload(
                existing_record->logid,
                // empty payload
                Payload(),
                existing_record->attrs.lsn,
                // using the timestamp of the existing record
                existing_record->attrs.timestamp,
                flags,
                std::move(extra_metadata),
                std::shared_ptr<BufferedWriteDecoder>(),
                0, // batch_offset
                std::move(existing_record->attrs.offsets),
                /*invalid_checksum=*/false));

        // this will free the payload the previous record
        existing_record = std::move(hole_record);
      }

      // Note: do not update node_info so that the copy of each node in
      // node_info will guaranteed to be amended/replaced with the new hole
    }
  }

  // last step: recompute the new bridge record esn, with the following rules:
  // 1) the bridge record is placed in the first esn immediately after the last
  //    entry in the digest with a real data record (i.e., not a hole/bridge);
  // 2) If the ESN does not exist (last record is ESN_MAX, unlikely), do not
  //    place bridge record;
  // 3) if there is no record in digest whose esn > last_known_good; the bridge
  //    esn will be either:
  //      1) ESN_INVALID if last_known_good is ESN_INVALID and digest is empty
  //                     and options disallow storing bridge for empty epochs
  //                     or last_known_good is ESN_MAX;
  //      2) last_known_good + 1     otherwise
  //
  // Note that the bridge esn may already has a hole stored in the digest
  // entry, it is fine and it will be later converted to a bridge record by
  // adding the BRIDGE flag in the mutation phase.
  esn_t last_record_esn = last_known_good;
  for (auto eit = entries_.crbegin(); eit != entries_.crend(); ++eit) {
    if (!eit->second.isHolePlug()) {
      last_record_esn = std::max(last_record_esn, eit->first);
      break;
    }
  }

  if (tail_record_out) {
    *tail_record_out = last_record_esn;
  }

  if (last_record_esn == ESN_INVALID && empty() &&
      !options_.bridge_for_empty_epoch) {
    // empty epoch, do not plug bridge
    return ESN_INVALID;
  }

  const esn_t bridge =
      (last_record_esn == ESN_MAX ? ESN_INVALID
                                  : esn_t(last_record_esn.val_ + 1));

  // if the bridge esn exists in the digest, apply the BRIDGE flag so that
  // needsMutation() can get a correct amend set
  auto it_bridge = entries_.find(bridge);
  if (it_bridge != entries_.end()) {
    it_bridge->second.record->flags_ |= RECORD_Header::BRIDGE;
  }

  return bridge;
}

int Digest::recomputeOffsetsWithinEpoch(
    esn_t last_known_good,
    folly::Optional<OffsetMap> offsets_within_epoch) {
  ld_check(digest_start_esn_ != ESN_INVALID);
  // nothing to change
  if (entries_.empty()) {
    return 0;
  }

  // if last_known_good is invalid, recalculate OffsetMap of the whole epoch
  if (last_known_good == ESN_INVALID) {
    // To make offsets_within_epoch valid
    OffsetMap om;
    om.setCounter(BYTE_OFFSET, 0);
    offsets_within_epoch = std::move(om);
  }

  // if offsets_within_epoch is not given, figure out offsets_within_epoch from
  // digest entries
  if (!offsets_within_epoch.hasValue()) {
    ld_check(last_known_good >= digest_start_esn_);
    auto iter = entries_.find(last_known_good);
    if (iter == entries_.end()) {
      RATELIMIT_ERROR(
          std::chrono::seconds(10),
          10,
          "cannot get logid:%lu e%un%u's OffsetMap from digest entry",
          log_id_.val_,
          epoch_.val_,
          last_known_good.val_);
      // abort correct byte offset
      return -1;
    }
    ld_check(iter->second.record != nullptr);

    if (iter->second.record->extra_metadata_ != nullptr) {
      offsets_within_epoch =
          iter->second.record->extra_metadata_->offsets_within_epoch;
    } else {
      offsets_within_epoch = OffsetMap();
    }

  } else {
    // if offsets_within_epoch is given, in this case last_known_good should be
    // exactly digest_start_esn_.val_ - 1
    ld_check(last_known_good.val_ == digest_start_esn_.val_ - 1);
  }

  if (!offsets_within_epoch.value().isValid()) {
    // Last Known Good's OffsetMap is invalid. There is nothing we can do.
    return -1;
  }

  // correct OffsetMap from last_known_good
  for (auto& kv : entries_) {
    if (kv.first <= last_known_good) {
      continue;
    }

    auto& entry = kv.second;
    OffsetMap payload_size_map;
    // TODO(T33977412) Add record counter offset based on settings
    if (!entry.isHolePlug() && !entry.isBridgeRecord()) {
      payload_size_map.setCounter(
          BYTE_OFFSET, entry.getPayload().size() - entry.getChecksumBytes());
    }
    offsets_within_epoch.value() = OffsetMap::mergeOffsets(
        std::move(offsets_within_epoch.value()), payload_size_map);

    ld_check(entry.record != nullptr);

    // if offsets_within_epoch doesn't match, marked it has been changed
    // this can ensure that mutation phase will amend the copy's metadata
    // to have the correct OffsetMap
    // update rebuild_matadata_
    if (entry.record->extra_metadata_ != nullptr &&
        entry.record->extra_metadata_->offsets_within_epoch !=
            offsets_within_epoch.value()) {
      entry.record->extra_metadata_->offsets_within_epoch =
          offsets_within_epoch.value();
      entry.record->flags_ |= RECORD_Header::INCLUDE_OFFSET_WITHIN_EPOCH;
      entry.byte_offset_changed = true;
    }
  }

  // on succeed
  return 0;
}

void Digest::removeNodeIf(std::function<bool(ShardID shard)> should_remove) {
  for (auto eit = entries_.begin(); eit != entries_.end(); ++eit) {
    auto& node_info = eit->second.node_info;
    for (auto nit = node_info.begin(); nit != node_info.end();) {
      if (should_remove(nit->first)) {
        // Note that here we removed a node but did not update Entry::record.
        // If the node removed is the only one that sent us the highest record,
        // then we have a stale record. But this is still fine, as the record
        // was still an authentic record written by logdevice and the consensus
        // result should not change if the esn was previously acknowledged to
        // the client.
        nit = node_info.erase(nit);
      } else {
        ++nit;
      }
    }

    // see doc block of removeNode(), we do not change the target record state
    ld_check(eit->second.record != nullptr);
  }
}

void Digest::removeNode(ShardID shard) {
  removeNodeIf([shard](ShardID n) { return n == shard; });
}

void Digest::filter(const std::set<ShardID>& nodes_to_keep) {
  removeNodeIf([&nodes_to_keep](ShardID n) { return !nodes_to_keep.count(n); });
}

void Digest::trim(esn_t last_known_good) {
  for (auto it = entries_.begin(); it != entries_.end();) {
    if (it->first <= last_known_good) {
      it = entries_.erase(it);
    } else {
      // the smallest entry already > lng
      break;
    }
  }

  digest_start_esn_ =
      esn_t(std::max(digest_start_esn_.val_, last_known_good.val_ + 1));
}

const Digest::Entry* Digest::findEntry(esn_t esn) const {
  auto it = entries_.find(esn);
  if (it == entries_.end()) {
    return nullptr;
  }
  return &it->second;
}

}} // namespace facebook::logdevice
