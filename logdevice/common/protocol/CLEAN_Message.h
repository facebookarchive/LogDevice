/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/NodeID.h"
#include "logdevice/common/ShardID.h"
#include "logdevice/common/TailRecord.h"
#include "logdevice/common/protocol/FixedSizeMessage.h"
#include "logdevice/common/types_internal.h"

namespace facebook { namespace logdevice {

/**
 * @file CLEAN messages are sent by sequencers towards the end of recovery of
 * an epoch. A message contains an epoch number and indicates that:
 *
 * - This node participated in an attempt to recover that epoch after a
 *   sequencer crash. The epoch is clean; there is no need to touch records
 *   in it anymore. If the epoch recovery attempt in which this node
 *   participated ultimately does not succeed, more attempts to recover the
 *   epoch may be undertaken later by other sequencers. All such attempts are
 *   guaranteed not to introduce gap/copy conflicts with the epoch strand on
 *   this node.
 *
 * - The node should check its view of the last clean epoch for the log. If
 *   there are epochs in the exclusive range (last_clean_epoch, epoch), the
 *   node should purge records from those epochs. We infer that those epochs
 *   were recovered but this node did not participate, so we need to delete
 *   records that are not known to be Good.
 *
 * - Finally, the node should update its view of the last clean epoch. After
 *   that, it is free to process RELEASE messages in the next epoch.
 *
 * A non-obvious effect of CLEAN messages is on nodes that *do not* receive
 * them. If a node did not get a CLEAN message for an epoch but starts
 * getting RELEASE messages in a later epoch, it infers that the epoch was
 * recovered but it did not participate, so it also needs to purge
 * not-known-Good records.
 *
 * For purging and byte offsets, more metadata are added to the CLEAN
 * message, which are expected to eventually be stored as per-epoch log
 * metadata on storage nodes.
 */

using CLEAN_flags_t = uint16_t;

struct CLEAN_Header {
  logid_t log_id; // log and epoch that this CLEAN message is for
  epoch_t epoch;
  recovery_id_t recovery_id;

  // clean flags, for handling of future compatibility, unused currently
  CLEAN_flags_t flags;

  // sequencer epoch of the log recovery procedure, which is the epoch of
  // the sequencer at the time when the recovery started
  epoch_t sequencer_epoch;

  // last known good esn of the epoch. This is a consensus collected from
  // nodes which finished digest.
  esn_t last_known_good;

  // last esn of the recovery digest, also a result of consensus
  esn_t last_digest_esn;

  // the number of nodes that did not participate this epoch recovery procedure
  nodeset_size_t num_absent_nodes;

  // accumulative byte offset of end of the epoch
  // size of epoch in bytes
  // TODO(T35659884) : remove from header, this variable is not used as it
  // is replaced with OffsetMap. Can simply delete all places except in
  // serialize and deserialize of CLEAN_Message.
  uint64_t epoch_end_offset_DEPRECATED;

  // size of epoch in bytes
  // TODO(T35659884) : remove from header, this variable is not used as it
  // is replaced with OffsetMap. Can simply delete all places except in
  // serialize and deserialize of CLEAN_Message.
  uint64_t epoch_size_DEPRECATED;

  // Shard that this CLEAN is for.
  shard_index_t shard;

  // if set, TailRecord is valid
  static const CLEAN_flags_t INCLUDE_TAIL_RECORD = 1u << 0; //=1
} __attribute__((__packed__));

class CLEAN_Message : public Message {
 public:
  explicit CLEAN_Message(const CLEAN_Header& header,
                         TailRecord tail_record,
                         OffsetMap epoch_size_map,
                         StorageSet absent_nodes = StorageSet{});

  CLEAN_Message(const CLEAN_Message&) noexcept = delete;
  CLEAN_Message(CLEAN_Message&&) noexcept = delete;
  CLEAN_Message& operator=(const CLEAN_Message&) = delete;
  CLEAN_Message& operator=(CLEAN_Message&&) = delete;

  // see Message.h
  void serialize(ProtocolWriter&) const override;
  Disposition onReceived(const Address&) override {
    // Receipt handler lives in PurgeCoordinator::onReceived(); this should
    // never get called.
    std::abort();
  }
  void onSent(Status st, const Address& to) const override;
  static Message::deserializer_t deserialize;

  // Helper for deserializing `absent_nodes_`.
  static StorageSet readAbsentNodes(ProtocolReader& reader,
                                    const CLEAN_Header& hdr);

  CLEAN_Header header_;
  // tail_record_.offsets_map_ contains size on epoch_end_offset
  TailRecord tail_record_;
  // Contains information on epoch size
  OffsetMap epoch_size_map_;
  StorageSet absent_nodes_;
};

}} // namespace facebook::logdevice
