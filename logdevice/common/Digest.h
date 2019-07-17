/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <map>
#include <memory>
#include <set>
#include <string>

#include <folly/Optional.h>

#include "logdevice/common/DataRecordOwnsPayload.h"
#include "logdevice/common/EpochMetaData.h"
#include "logdevice/common/FailureDomainNodeSet.h"
#include "logdevice/common/NodeID.h"
#include "logdevice/common/ShardID.h"
#include "logdevice/common/protocol/STORE_Message.h"
#include "logdevice/common/types_internal.h"

namespace facebook { namespace logdevice {

class RECORD_Message;

/**
 * @file Digest collects data and gap records delivered by nodes in
 *       the recovery set. It also collects metadata associated with the record
 *       such as the wave number (if written by sequencer) or the seal epoch
 *       (if written by epoch recovery). RECORD messages are delivered in
 *       response to START sent by the EpochRecovery machine that owns this
 *       digest.
 *
 *       For each ESN of a RECORD message the Digest records the metadata and
 *       node id of all nodes that have delivered that record. Digest also keeps
 *       track of the fully record received that has the highest precedence.
 *       It makes use of the such information to determine whether a particular
 *       esn needs mutation by epoch recovery, and if so, determine sets of
 *       that 1) already have the up-to-date copy; 2) needs to amend its record
 *       metadata but no need to send payload; 3) needs to resolve the hole-copy
 *       conflict and overwrite the existing record.
 *
 *       see MutationsWithImmutableConsensus in test/RecoveryTest.cpp for an
 *       example of record precedence rules and how Digest resolves the
 *       consensus.
 *
 *       TODO 5904734: more description about the bridge record.
 *
 *       A brief explanation on why epoch recovery does not pay special
 *       attention to records with EPOCH_BEGIN flag like it does to the
 *       bridge record: considering the following two cases:
 *         1) consensus LNG of the epoch > ESN_INVALID and LNG has been moved
 *            in this epoch. In this case, the record with smallest esn must
 *            have fully replicated with the EPOCH_BEGIN stored by the
 *            sequencer. Epoch recovery does not need to perform anything
 *            regarding to the record.
 *
 *         2) consensus LNG of the epoch > ESN_INVALID and LNG has not been
 *            moved in this epoch. This can happen if sequencer gets reactivated
 *            to an new epoch and gets LNG > ESN_INVALID for the new epoch. In
 *            such case, EPOCH_BEGIN may or may not replicate by log recovery.
 *            But it won't affect correctness or consistency of the epoch,
 *            the worst case is that the reads will fallback to use f-majority
 *            based gap detection if EPOCH_BEGIN is not present.
 *
 *         3) consensus LNG of the epoch == ESN_INVALID: in such case, the
 *            record with EPOCH_BEGIN may or may not be fully replicated, and
 *            the esn might even be plugged a hole. However, instead of trying
 *            to amend the EPOCH_BEGIN flag like bridge record, epoch recovery
 *            can simply just ignore it. The reason is that epoch recovery must
 *            ensure the consistency of esn from lng_ + 1 to the end of epoch,
 *            in such case, ESN 1 will always guarantee to exist  by epoch
 *            recovery by either being a normal record, a hole or a bridge
 *            record. On the read path, the readers can always treat ESN 1 as
 *            the beginning of an epoch since it is the smallest esn possible
 *            that can be allocated. The reader will ingore subsequent
 *            EPOCH_BEGIN flags it encountered for the same epoch, if any.
 */

class Digest {
 public:
  struct RecordMetadata {
    // relevant flags of concern:
    // WRITTEN_BY_RECOVERY: the record is written by epoch recovery
    // HOLE:                the record is a hole plug
    // BRIDGE:              the record is a special hole indicating the end of
    //                      an epoch
    RECORD_flags_t flags;

    // if WRITTEN_BY_RECOVERY is set in the flags, store the seal epoch of
    // the epoch recovery instance that wrote the record. Otherwise, this is
    // the wave number from sequencer in which the record was written
    uint32_t wave_or_seal_epoch;

    bool operator==(const RecordMetadata& rhs) const;

    // determine the record precedence
    bool operator<(const RecordMetadata& rhs) const;

    RecordMetadata& operator=(const RecordMetadata& rhs) {
      if (&rhs != this) {
        flags = rhs.flags;
        wave_or_seal_epoch = rhs.wave_or_seal_epoch;
      }
      return *this;
    }

    std::string toString() const;

    // extract the record metadata from a record
    static RecordMetadata fromRecord(const DataRecordOwnsPayload& record);
  };

  struct Entry {
    // keep the record of the highest precedence received. can be a normal
    // record or a hole plug. When the digest phase completed, this will be
    // the final determined state of the LSN and will be properly replicated
    // to the mutation and cleaning set by epoch recovery.
    std::unique_ptr<DataRecordOwnsPayload> record;

    // keep the record metadata for this LSN slot receievd from all shards
    // participating the digest, indexed by their ShardID
    std::map<ShardID, RecordMetadata> node_info;

    // offset_with_in epoch has been changed by recovery and is different from
    // the original record
    bool byte_offset_changed = false;

    bool isHolePlug() const {
      return record && (record->flags_ & RECORD_Header::HOLE);
    }

    bool isBridgeRecord() const {
      return record && (record->flags_ & RECORD_Header::BRIDGE);
    }

    const Payload& getPayload() const {
      ld_check(!isHolePlug() && record != nullptr);
      return record->payload;
    }

    size_t getChecksumBytes() const {
      // TODO T31241526: support checksum in epoch recovery
      // if (record->flags_ & STORE_Header::CHECKSUM) {
      //   return record->flags_ & STORE_Header::CHECKSUM_64BIT ? 8 : 4;
      // }
      // currently digest records do not contain checksum
      return 0;
    }
  };

  using EntryMap = std::map<esn_t, Entry>;
  using const_iterator = EntryMap::const_iterator;
  using iterator = EntryMap::iterator;

  // settings for computing digest, tail attribute and bridge record
  struct Options {
    bool bridge_for_empty_epoch;
  };

  /**
   * Create a Digest object.
   *
   * @param seal_epoch              seal epoch of the epoch recovery instance,
   *                                used to determine if mutaiton is needed
   *
   * @param nodes_configuration     nodes configuration
   *
   * @param options                 options for computing the digest,
   *                                by default:
   *                                 - do not store bridge record for empty
   *                                   epochs
   */
  Digest(logid_t log_id,
         epoch_t epoch,
         const EpochMetaData& epoch_metadata,
         epoch_t seal_epoch,
         const std::shared_ptr<const NodesConfiguration>& nodes_configuration,
         Options options = {false});

  /**
   * Adds a new record to the digest.
   *
   * @param node    node the record was received from
   * @param record  a data record object representing either a regular record,
   *                or a hole plug (determined by record.flags_ and an empty
   *                payload)
   */
  void onRecord(ShardID from, std::unique_ptr<DataRecordOwnsPayload> record);

  /**
   * Trim the digest, discarding entries whose esn <= @param last_known_good
   */
  void trim(esn_t last_known_good);

  /**
   * Removes the node from the digest. This is used before mutation phased
   * started, filtering out nodes that 1) did not finish the digest; or 2)
   * are not qualify to store mutation records (e.g., in draining).
   *
   * Important note: for each LSN of which the given _shard_ sent a copy. This
   * only removes the node from the list of nodes (i.e., Entry::node_info) with
   * record metadata, but will not change the target state of the LSN
   * (i.e., Entry::record).
   *
   * The reason for this is that we have to preserve the final target state
   * computed from the set of DIGESTED nodes when digest phase completes.
   * The important property of the set is that it is a f-majority that
   * intersects with any valid copyset of the epoch. The mutation and
   * cleaning set, on the other hand, does not have to be f-majority so that
   * the result computed solely from nodes in mutation and cleaning set
   * may not be correct.
   */
  void removeNode(ShardID shard);

  /**
   * Intersects all copysets and plugsets with the given set. Equivalent to
   * calling removeNode() for all nodes that are _not_ in nodes_to_keep.
   */
  void filter(const std::set<ShardID>& nodes_to_keep);

  /**
   * Transform entries in the digest, taking into account of bridge records
   * received during the digest. Specifically:
   *    - some entires will be transformed into holes because of bridge record
   *      previously received
   *    - all bridge flags in entries of the digests will be removed. existing
   *      bridge records become hole plugs.
   *    - the new bridge esn for the epoch will be returned
   *
   * Note that this function is called when epoch recovery has finished
   * collecting digest from nodes in the recovery set.
   *
   * @param last_known_good     last known good esn collected by epoch
   *                            recovery for the epoch. Used to compute the new
   *                            bridge esn.
   * @param tail_record_out     output parameter for the esn of the tail record
   *                            (last non-hole record) in the digest
   */
  esn_t applyBridgeRecords(esn_t last_known_good, esn_t* tail_record_out);

  /**
   * since some records might be recognized as holes during recovery, we need
   * to recalculate OffsetMap during digest. the process will start from
   * last know good record and recalculate the OffsetMap of each record
   * afterwards.
   *
   * Note that this function is called when epoch recovery has finished
   * collecting digest from nodes in the recovery set.
   *
   * @param last_known_good         epoch sequence number where byteoffset
   *                                recompute will start
   *
   * @param offsets_within_epoch    the OffsetMap of the corresponding
   *                                last_known_good, If it is not given digest
   *                                will try to check if it has the record with
   *                                last_known_good in its entries and get
   *                                offsets from that record. If
   *                                offsets_within_epoch is not given and also
   *                                last_known_good entry's OffsetMap is not
   *                                found. recomputeOffsetsWithinEpoch will stop
   *                                and return -1, otherwise on succeed it will
   *                                return 0
   */
  int recomputeOffsetsWithinEpoch(
      esn_t last_known_good,
      folly::Optional<OffsetMap> offsets_within_epoch = folly::none);

  /**
   * Checks if mutation is needed for the given entry. Things to check include
   * sufficient replication based on replication factor and failure domains,
   * conflicts between copies and holes, as well as wave number or seal epoch
   * of the record.
   *
   * @param entry                  Digest entry to perform the check
   * @param successfully_stored    output parameter, will store set of nodes
   *                               that have the up-to-date record
   * @param amend_metadata         output parameter, will store set of nodes
   *                               that requires record metadata to be amended,
   *                               but not the payload
   * @param conflict_copies        output parameter, will store set of nodes
   *                               whose record (w/ its metadata) needs to
   *                               be overwritten
   *
   * @param complete_digest        set it to true if the set of nodes that
   *                               finished digesting has every single fully
   *                               authoritative node in the nodeset. Having
   *                               a complete digest can relax the mutation
   *                               requirement if it is certain  there is no way
   *                               for a conflict to emerge ever after.
   *
   * Note: time complexity O(n) in current implementation, where n is the size
   *       of the digest node set.
   *
   */
  bool needsMutation(Entry& entry,
                     std::set<ShardID>* successfully_stored,
                     std::set<ShardID>* amend_metadata,
                     std::set<ShardID>* conflict_copies,
                     bool complete_digest = false);

  const_iterator begin() const {
    return entries_.begin();
  }
  const_iterator end() const {
    return entries_.end();
  }
  iterator begin() {
    return entries_.begin();
  }
  iterator end() {
    return entries_.end();
  }

  size_t size() const {
    return entries_.size();
  }
  bool empty() const {
    return size() == 0;
  }

  void clear() {
    entries_.clear();
    digest_start_esn_ = ESN_INVALID;
  }

  // @return: esn of the last entry in the digest, ESN_INVALID if the digest
  //          is empty
  esn_t getLastEsn() const {
    const auto rit = entries_.crbegin();
    return rit == entries_.crend() ? ESN_INVALID : rit->first;
  }

  // @return: esn of the first entry in the digest, ESN_INVALID if the digest
  //          is empty
  esn_t getFirstEsn() const {
    const auto rit = entries_.cbegin();
    return rit == entries_.cend() ? ESN_INVALID : rit->first;
  }

  // Find the digest entry for the given _esn_
  // @return  pointer to the entry requested, nullptr if entry cannot be found
  const Entry* findEntry(esn_t esn) const;

  void setDigestStartEsn(esn_t digest_start_esn) {
    digest_start_esn_ = digest_start_esn;
  }

 private:
  logid_t log_id_;

  // epoch which is under digesting
  epoch_t epoch_;

  // Seal epoch of the epoch recovery instance owning the digest
  const epoch_t seal_epoch_;

  // an ordered map of digest entries, indexed by esn
  EntryMap entries_;

  // boolean attribute: true  - the node already sent us the record that exactly
  //                            matches the record to be replicated.
  //                    false - otherwise
  using FailureDomain = FailureDomainNodeSet<bool>;

  // track the location-based failure domain information for the nodes
  // participating in the Digest
  FailureDomain failure_domain_;

  Options options_;

  void removeNodeIf(std::function<bool(ShardID shard)> should_remove);

  esn_t digest_start_esn_ = ESN_INVALID;
};

}} // namespace facebook::logdevice
