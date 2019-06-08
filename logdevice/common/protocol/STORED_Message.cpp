/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/protocol/STORED_Message.h"

#include <memory>

#include "logdevice/common/Appender.h"
#include "logdevice/common/ClientIdxAllocator.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/RebuildingTypes.h"
#include "logdevice/common/Request.h"
#include "logdevice/common/RequestType.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Socket.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/protocol/ProtocolReader.h"
#include "logdevice/common/protocol/ProtocolWriter.h"
#include "logdevice/common/stats/Stats.h"

namespace facebook { namespace logdevice {

STORED_Message::STORED_Message(const STORED_Header& header,
                               lsn_t rebuilding_version,
                               uint32_t rebuilding_wave,
                               log_rebuilding_id_t rebuilding_id,
                               FlushToken flushToken,
                               ServerInstanceId serverInstanceId,
                               ShardID rebuildingRecipient)
    : Message(MessageType::STORED, calcTrafficClass(header)),
      header_(header),
      rebuilding_version_(rebuilding_version),
      rebuilding_wave_(rebuilding_wave),
      rebuilding_id_(rebuilding_id),
      flushToken_(flushToken),
      serverInstanceId_(serverInstanceId),
      rebuildingRecipient_(rebuildingRecipient) {}

MessageReadResult STORED_Message::deserialize(ProtocolReader& reader) {
  STORED_Header hdr;
  hdr.shard = -1; // Default for old protocols
  reader.read(&hdr, STORED_Header::headerSize(reader.proto()));

  lsn_t rebuilding_version = LSN_INVALID;
  uint32_t rebuilding_wave = 0;
  FlushToken flushToken = FlushToken_INVALID;
  ServerInstanceId serverInstanceId = ServerInstanceId_INVALID;
  log_rebuilding_id_t rebuilding_id = LOG_REBUILDING_ID_INVALID;
  if (hdr.flags & STORED_Header::REBUILDING) {
    reader.read(&rebuilding_version);
    reader.read(&rebuilding_wave);
    reader.read(&flushToken);
    reader.read(&serverInstanceId);
    reader.read(&rebuilding_id);
  }

  ShardID rebuildingRecipient;
  if (hdr.status == E::REBUILDING) {
    reader.read(&rebuildingRecipient);

    if (!rebuildingRecipient.isValid()) {
      // The other end should provide a valid rebuilding recipient when
      // replied with E::REBUILDING
      ld_error("PROTOCOL ERROR: got a STORED message for record %s "
               "with E::REBUILDING but no valid rebuildingRecipient "
               "is provided.",
               hdr.rid.toString().c_str());
      return reader.errorResult(E::BADMSG);
    }
  }

  return reader.result([&] {
    return new STORED_Message(hdr,
                              rebuilding_version,
                              rebuilding_wave,
                              rebuilding_id,
                              flushToken,
                              serverInstanceId,
                              rebuildingRecipient);
  });
}

void STORED_Message::serialize(ProtocolWriter& writer) const {
  writer.write(&header_, STORED_Header::headerSize(writer.proto()));
  if (header_.flags & STORED_Header::REBUILDING) {
    writer.write(rebuilding_version_);
    writer.write(rebuilding_wave_);
    writer.write(flushToken_);
    writer.write(serverInstanceId_);
    writer.write(rebuilding_id_);
  }
  if (header_.status == E::REBUILDING) {
    writer.write(rebuildingRecipient_);
  }
}

Message::Disposition
STORED_Message::handleOneMessage(const STORED_Header& header,
                                 ShardID from,
                                 ShardID rebuildingRecipient) {
  Appender* appender{
      // Appender that sent the corresponding STORE
      Worker::onThisThread()->activeAppenders().map.find(header.rid)};

  if (!appender) { // a reply from an extra will often hit this path
    ld_debug("Appender for record %s sent to %s not found",
             header.rid.toString().c_str(),
             from.toString().c_str());
    return Disposition::NORMAL;
  }

  ld_assert(header.rid == Appender::KeyExtractor()(*appender));

  return appender->onReply(header, from, rebuildingRecipient)
      ? Disposition::ERROR
      : Disposition::NORMAL;
}

Message::Disposition STORED_Message::onReceivedCommon(const Address& from) {
  if (from.isClientAddress()) {
    ld_error("PROTOCOL ERROR: got a STORED message for record %s from "
             "client %s. STORED can only arrive from servers",
             header_.rid.toString().c_str(),
             Sender::describeConnection(from).c_str());
    err = E::PROTO;
    return Disposition::ERROR;
  }

  Worker* w = Worker::onThisThread();

  shard_index_t shard_idx = header_.shard;
  ld_check(shard_idx != -1);
  ShardID shard(from.id_.node_.index(), shard_idx);

  if (Worker::settings().hold_store_replies) {
    // Appender that sent the corresponding STORE
    Appender* appender{w->activeAppenders().map.find(header_.rid)};

    if (!appender) { // a reply from an extra will often hit this path
      ld_debug("Appender for record %s sent to %s not found",
               header_.rid.toString().c_str(),
               Sender::describeConnection(from).c_str());
      return Disposition::NORMAL;
    }

    // There's a possible race condition here.  repliesExpected() can decrease
    // in some error conditions, like a socket closing, but we don't recheck
    // the condition in that case.  In fact, we don't know whether one of the
    // replies we're holding came in on that socket, or whether it will now
    // never come.  That's one reason why this is only for tests.
    if (appender->repliesHeld() + 1 < appender->repliesExpected()) {
      appender->holdReply(header_, shard, rebuildingRecipient_);
      return Disposition::NORMAL;
    } else {
      // This is the last reply.  Time to process them all!

      // Because the Appender may be deleted after any call, we need to
      // move out the list of replies here.
      auto replies = appender->takeHeldReplies();

      for (auto& reply : replies) {
        auto rv =
            handleOneMessage(reply.hdr, reply.from, reply.rebuildingRecipient);
        if (rv == Disposition::ERROR) {
          ld_info("Got an error on proccessing a held STORED message, but "
                  "not closing connection.");
        }
      }
      // Fall through to normal processing for current message.
    }
  }

  return handleOneMessage(header_, shard, rebuildingRecipient_);
}

/**
 * Objects of class SendSTOREDRequest are Requests that Workers use to
 * arrange for the delivery of a reply to a chained STORE
 * request. Such replies are commonly sent through direct client
 * connections to the originator of the chain (a sequencer node). If
 * the client (incoming) connection from the sequencer was assigned to
 * a Worker other than the one that is handling the STORE, the Worker
 * handling the STORE posts a SendSTOREDRequest that the Processor
 * delivers to the Worker that the connection was assigned to.
 */
class SendSTOREDRequest : public Request {
 public:
  SendSTOREDRequest(std::unique_ptr<STORED_Message> msg,
                    ClientID to,
                    worker_id_t target_worker)
      : Request(RequestType::SEND_STORED),
        msg_(std::move(msg)),
        to_(to),
        target_worker_(target_worker) {}

  int getThreadAffinity(int /*nthreads*/) override {
    // route the request to the Worker that the incoming ("client")
    // connection was assigned to
    return target_worker_.val_;
  }

  int8_t getExecutorPriority() const override {
    return msg_->header_.flags & STORED_Header::REBUILDING
        ? folly::Executor::LO_PRI
        : folly::Executor::HI_PRI;
  }

  Request::Execution execute() override {
    execute(std::move(msg_), to_);
    return Execution::COMPLETE;
  }

  static void execute(std::unique_ptr<STORED_Message> msg, ClientID to) {
    // The ClientID we're sending to may come from an unreliable source: from
    // inside a STORE message (in chain-sending case). The ClientID might have
    // been freed and reused after the STORE_Message was formed.
    // Before sending the STORED to this ClientID, check that the corresponding
    // socket exists, that it's handshaken and that the other end is a server;
    // this is done mostly to avoid overly strong error messages that Sender
    // would otherwise produce. If it's not the right server or the server was
    // restarted, it'll just reject our message based on epoch number and wave.
    ld_check(to.valid());
    Sender& sender = Worker::onThisThread()->sender();
    auto socket_proxy = sender.getSocketProxy(to);
    if (socket_proxy == nullptr) {
      RATELIMIT_WARNING(
          std::chrono::seconds(10),
          1,
          "Dropping a STORED for %s for delivery to C%u because the "
          "corresponding socket no longer exist. This should be rare.",
          msg->header_.rid.toString().c_str(),
          to.getIdx());
      return;
    }
    const Socket* sock = socket_proxy->get();
    ld_check(sock != nullptr);
    if (!sock->isHandshaken() || sock->peerIsClient()) {
      RATELIMIT_WARNING(
          std::chrono::seconds(10),
          1,
          "Dropping a STORED for %s for delivery to C%u because the "
          "corresponding socket is not handshaken or is connected to a client. "
          "This is probably a ClientID collision. This should be rare.",
          msg->header_.rid.toString().c_str(),
          to.getIdx());
    }

    int rv = sender.sendMessage(std::move(msg), to);
    if (rv != 0) {
      RATELIMIT_INFO(std::chrono::seconds(10),
                     1,
                     "Failed to send a STORED message for %s to %s: %s",
                     msg->header_.rid.toString().c_str(),
                     Sender::describeConnection(Address(to)).c_str(),
                     error_description(err));
    }
  }

 private:
  std::unique_ptr<STORED_Message> msg_;
  const ClientID to_; // id of incoming ("client") connection
                      // to send the request to
  worker_id_t target_worker_;
};

void STORED_Message::createAndSend(const STORED_Header& header,
                                   ClientID send_to,
                                   lsn_t rebuilding_version,
                                   uint32_t rebuilding_wave,
                                   log_rebuilding_id_t rebuilding_id,
                                   FlushToken flushToken,
                                   ShardID rebuildingRecipient) {
  ld_check(send_to.valid()); // must have been set by onReceived()
  Worker* worker = Worker::onThisThread();

  if (header.status != E::OK) {
    WORKER_STAT_INCR(node_stored_unsuccessful_total);
    // increment specific stats counters for different reasons
    switch (header.status) {
      case E::PREEMPTED:
        WORKER_STAT_INCR(node_stored_preempted_sent);
        break;
      case E::NOSPC:
        WORKER_STAT_INCR(node_stored_out_of_space_sent);
        break;
      case E::FAILED:
        RATELIMIT_ERROR(std::chrono::seconds(10),
                        10,
                        "INTERNAL ERROR: Sending STORED with E::FAILED. "
                        "This should never happen.");
        ld_check(false);
        break;
      case E::DISABLED:
        WORKER_STAT_INCR(node_stored_disabled_sent);
        break;
      case E::DROPPED:
        WORKER_STAT_INCR(node_stored_dropped_sent);
        break;
      case E::FORWARD:
        break;
      // the following apply to cases we fail early upon receiving a
      // STORE message
      case E::NOTSTORAGE:
        WORKER_STAT_INCR(node_stored_not_storage_sent);
        break;
      case E::REBUILDING:
        // must provide a valid rebuilding recipient
        ld_check(rebuildingRecipient.isValid());
        WORKER_STAT_INCR(node_stored_rebuilding_sent);
        break;
      case E::SHUTDOWN:
        break;
      case E::CHECKSUM_MISMATCH:
        break;
      default:
        RATELIMIT_ERROR(std::chrono::seconds(1),
                        10,
                        "Unexpected error code %u (%s)",
                        (unsigned)header.status,
                        error_name(header.status));
        break;
    }
  }

  ClientIdxAllocator& client_idx_allocator =
      worker->processor_->clientIdxAllocator();
  auto target_worker = client_idx_allocator.getWorkerId(send_to);
  if (target_worker.first != WorkerType::GENERAL) {
    if (target_worker.first == WorkerType::MAX) {
      // client_id is a closed or nonexistent connection. This is ok: probably
      // the connection was closed while we were processing the store. Ignore
      // message. StoreStateMachine machine should take care of retransmission.
      ld_debug("Dropping a STORED for %s for delivery to %d as client_id is "
               "no longer valid.",
               header.rid.toString().c_str(),
               send_to.getIdx());
    } else {
      RATELIMIT_WARNING(
          std::chrono::seconds(10),
          10,
          "Dropping a STORED for %s for delivery to %u because this "
          "client_id refers to a gossip or background connection (on %s). "
          "This is probably a ClientID collision. Should be rare.",
          header.rid.toString().c_str(),
          send_to.getIdx(),
          Worker::getName(target_worker.first, target_worker.second).c_str());
    }
  } else {
    auto serverInstanceId = worker->processor_->getServerInstanceId();
    auto msg = std::make_unique<STORED_Message>(header,
                                                rebuilding_version,
                                                rebuilding_wave,
                                                rebuilding_id,
                                                flushToken,
                                                serverInstanceId,
                                                rebuildingRecipient);

    if (target_worker.second == worker->idx_) {
      // the connection to origin is handled by this Worker thread
      SendSTOREDRequest::execute(std::move(msg), send_to);
    } else {
      // the connection to origin is handled by another Worker
      // thread. Have that Worker send the reply. Hopefully we will be
      // able to skip this step by having the storage task reply
      // directly to the correct thread.
      ld_debug(
          "%s is passing a STORED for %s to %s for delivery to %s",
          worker->getName().c_str(),
          header.rid.toString().c_str(),
          Worker::getName(target_worker.first, target_worker.second).c_str(),
          Sender::describeConnection(Address(send_to)).c_str());

      std::unique_ptr<Request> send_stored =
          std::make_unique<SendSTOREDRequest>(
              std::move(msg), send_to, target_worker.second);
      worker->processor_->postImportant(send_stored);
    }
  }
}

std::vector<std::pair<std::string, folly::dynamic>>
STORED_Message::getDebugInfo() const {
  std::vector<std::pair<std::string, folly::dynamic>> res;

  auto flagsToString = [](STORED_flags_t flags) {
    folly::small_vector<std::string, 4> strings;
#define FLAG(x)                   \
  if (flags & STORED_Header::x) { \
    strings.emplace_back(#x);     \
  }
    FLAG(SYNCED)
    FLAG(OVERLOADED)
    FLAG(AMENDABLE_DEPRECATED)
    FLAG(REBUILDING)
    FLAG(PREMPTED_BY_SOFT_SEAL_ONLY)
    FLAG(LOW_WATERMARK_NOSPC)
#undef FLAG
    return folly::join('|', strings);
  };

  auto add = [&](const char* key, folly::dynamic val) {
    res.emplace_back(key, std::move(val));
  };

  add("log_id", header_.rid.logid.val());
  add("lsn", lsn_to_string(header_.rid.lsn()));
  add("wave", header_.wave);
  add("status", error_name(header_.status));
  add("redirect", header_.redirect.toString());
  add("flags", flagsToString(header_.flags));
  add("shard", header_.shard);
  add("rebuilding_version", lsn_to_string(rebuilding_version_));
  if (header_.flags && STORED_Header::REBUILDING) {
    add("rebuilding_wave", rebuilding_wave_);
    add("rebuilding_id", rebuilding_id_.val());
    add("flush_token", flushToken_);
    add("server_instance_id", serverInstanceId_);
    add("rebuilding_recipient", rebuildingRecipient_.toString());
  }

  return res;
}

}} // namespace facebook::logdevice
