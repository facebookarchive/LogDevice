/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/CHECK_SEAL_onReceived.h"

#include <folly/Memory.h>

#include "logdevice/common/Sender.h"
#include "logdevice/common/configuration/ServerConfig.h"
#include "logdevice/common/protocol/CHECK_SEAL_REPLY_Message.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/ServerWorker.h"
#include "logdevice/server/read_path/LogStorageStateMap.h"

namespace facebook { namespace logdevice {

namespace {
static void prepareReplyWithHighestSeal(CHECK_SEAL_REPLY_Header& reply_out,
                                        folly::Optional<Seal> normal,
                                        folly::Optional<Seal> soft) {
  ld_check(reply_out.status == E::OK);

  if (!normal.hasValue() ||
      (soft.hasValue() && soft.value().epoch > normal.value().epoch)) {
    reply_out.sealed_epoch = soft.value().epoch;
    reply_out.sequencer = soft->seq_node;
    reply_out.seal_type = CheckSealType::SOFT;
  } else {
    reply_out.sealed_epoch = normal.value().epoch;
    reply_out.sequencer = normal->seq_node;
    reply_out.seal_type = CheckSealType::NORMAL;
  }

  if (!normal.hasValue() && soft.hasValue()) {
    RATELIMIT_INFO(
        std::chrono::seconds(1),
        5,
        "Soft seal for log:%lu is [epoch:%s, seq:%s], but normal seal"
        " is absent; src_rqid:%lu",
        reply_out.log_id.val_,
        folly::to<std::string>(soft->epoch.val_).c_str(),
        soft->seq_node.toString().c_str(),
        reply_out.rqid.val());
  }
}

static void sendReply(const Address& to, CHECK_SEAL_REPLY_Header reply) {
  ld_spew("Sending CHECK_SEAL reply to %s(%s), src_rqid:%lu, log:%lu, "
          "sealed_epoch:%u, type:%d, preemptor_node:%s, status:%s",
          to.id_.node_.toString().c_str(),
          Sender::describeConnection(to).c_str(),
          reply.rqid.val(),
          reply.log_id.val_,
          reply.sealed_epoch.val_,
          (int)reply.seal_type,
          reply.sequencer.toString().c_str(),
          error_name(reply.status));

  int rv = Worker::onThisThread()->sender().sendMessage(
      std::make_unique<CHECK_SEAL_REPLY_Message>(reply), to);
  if (rv != 0) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    1,
                    "Failed to send CHECK_SEAL_REPLY message to %s, err:%s",
                    to.id_.node_.toString().c_str(),
                    error_name(err));
  }
}
} // namespace

Message::Disposition CHECK_SEAL_onReceived(CHECK_SEAL_Message* msg,
                                           const Address& from) {
  const CHECK_SEAL_Header& header = msg->getHeader();

  ServerWorker* w = ServerWorker::onThisThread();
  ServerProcessor* processor = w->processor_;

  const shard_size_t n_shards = w->getNodesConfiguration()->getNumShards();
  shard_index_t shard = header.shard;
  ld_check(shard != -1);

  if (shard >= n_shards) {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    10,
                    "Got CHECK_SEAL message from %s with invalid shard %u, "
                    "this node only has %u shards",
                    Sender::describeConnection(from).c_str(),
                    shard,
                    n_shards);
    return Message::Disposition::NORMAL;
  }

  CHECK_SEAL_REPLY_Header reply_hdr = {header.rqid,
                                       header.wave,
                                       header.log_id,
                                       E::OK,
                                       EPOCH_INVALID,
                                       CheckSealType::NONE,
                                       NodeID(),
                                       shard};

  if (Worker::settings().disable_check_seals) {
    reply_hdr.status = E::NOTSUPPORTED;
    sendReply(from, reply_hdr);
    return Message::Disposition::NORMAL;
  }

  if (!processor->runningOnStorageNode()) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    5,
                    "Received a CHECK_SEAL request for log:%lu from %s"
                    " on a non-storage node",
                    header.log_id.val_,
                    Sender::describeConnection(from).c_str());
    reply_hdr.status = E::NOTSTORAGE;
    sendReply(from, reply_hdr);
    return Message::Disposition::NORMAL;
  }

  LogStorageState* log_state =
      processor->getLogStorageStateMap().insertOrGet(header.log_id, shard);
  if (!log_state) {
    reply_hdr.status = E::FAILED;
    sendReply(from, reply_hdr);
    return Message::Disposition::NORMAL;
  }

  folly::Optional<Seal> normal_seal =
      log_state->getSeal(LogStorageState::SealType::NORMAL);
  folly::Optional<Seal> soft_seal =
      log_state->getSeal(LogStorageState::SealType::SOFT);

  if (!normal_seal.hasValue() && !soft_seal.hasValue()) {
    const logid_t log_id = header.log_id;
    int rv = log_state->recoverSeal(
        [reply_hdr, log_id, from](Status status, LogStorageState::Seals seals) {
          CHECK_SEAL_REPLY_Header reply = reply_hdr;
          reply.status = (status == E::OK) ? E::OK : E::FAILED;

          folly::Optional<Seal> recovered_normal =
              seals.getSeal(LogStorageState::SealType::NORMAL);
          folly::Optional<Seal> recovered_soft =
              seals.getSeal(LogStorageState::SealType::SOFT);

          ld_debug(
              "Recovered seals for log:%lu, [status:%s,"
              " normal_seal:%s, soft_seal:%s], rqid:%lu",
              log_id.val_,
              error_name(status),
              recovered_normal.hasValue()
                  ? folly::to<std::string>(recovered_normal->epoch.val_).c_str()
                  : "not present",
              recovered_soft.hasValue()
                  ? folly::to<std::string>(recovered_soft->epoch.val_).c_str()
                  : "not present",
              reply.rqid.val());

          if (status == E::OK) {
            prepareReplyWithHighestSeal(
                reply, recovered_normal, recovered_soft);
          }
          sendReply(from, reply);
        });

    if (rv == 0) {
      WORKER_STAT_INCR(check_seal_req_recover_seal);
      ld_debug("Recovering seals for log:%lu", header.log_id.val_);
    } else {
      // Log has permanent error.
      ld_check(err == E::FAILED);
      reply_hdr.status = E::FAILED;
      sendReply(from, reply_hdr);
      return Message::Disposition::NORMAL;
    }

    return Message::Disposition::NORMAL;
  }

  prepareReplyWithHighestSeal(reply_hdr, normal_seal, soft_seal);
  sendReply(from, reply_hdr);
  return Message::Disposition::NORMAL;
}

}} // namespace facebook::logdevice
