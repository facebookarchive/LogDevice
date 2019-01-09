/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/protocol/RELEASE_Message.h"

#include <cstdlib>

#include "logdevice/common/Address.h"
#include "logdevice/common/AllSequencers.h"
#include "logdevice/common/MetaDataLogWriter.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Sequencer.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/debug.h"
#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice {

RELEASE_Message::RELEASE_Message(const RELEASE_Header& header)
    : Message(MessageType::RELEASE, TrafficClass::READ_TAIL), header_(header) {}

void RELEASE_Message::serialize(ProtocolWriter& writer) const {
  writer.write(&header_, RELEASE_Header::headerSize(writer.proto()));
}

MessageReadResult RELEASE_Message::deserialize(ProtocolReader& reader) {
  RELEASE_Header hdr{};
  // Defaults for old protocols
  hdr.shard = -1;
  hdr.release_type = ReleaseType::GLOBAL;

  reader.read(&hdr, RELEASE_Header::headerSize(reader.proto()));

  reader.allowTrailingBytes();
  return reader.result([&] { return new RELEASE_Message(hdr); });
}

void RELEASE_Message::onSent(Status st, const Address& to) const {
  if (st == Status::PROTONOSUPPORT) {
    // We get here if we attempt to send a per-epoch RELEASE message to a node
    // running an older version. This is fine. Per-epoch RELEASE messages are
    // best effort and only affect the ability of readers to read past the
    // global last-released LSN.
    ld_check(header_.release_type == ReleaseType::PER_EPOCH);
    RATELIMIT_INFO(std::chrono::seconds(10),
                   1,
                   "Failed to send a per-epoch RELEASE for record %s to %s "
                   "because of old protocol",
                   header_.rid.toString().c_str(),
                   Sender::describeConnection(to).c_str());
    return;
  }

  std::shared_ptr<Sequencer> sequencer =
      Worker::onThisThread()->processor_->allSequencers().findSequencer(
          header_.rid.logid);

  if (!sequencer) {
    // for metadata logs, it is possible that the meta sequencer is destroyed
    // before some releases are sent.
    if (!MetaDataLog::isMetaDataLog(header_.rid.logid)) {
      RATELIMIT_CRITICAL(std::chrono::seconds(1),
                         10,
                         "INTERNAL ERROR: unable to find a sequencer for "
                         "log %lu",
                         header_.rid.logid.val_);
    }
    return;
  }

  if (st != Status::OK) {
    RATELIMIT_LEVEL((st == Status::TIMEDOUT || st == Status::CONNFAILED)
                        ? dbg::Level::DEBUG
                        : dbg::Level::WARNING,
                    std::chrono::seconds(1),
                    1,
                    "Failed to send a RELEASE for record %s to %s: %s. ",
                    header_.rid.toString().c_str(),
                    Sender::describeConnection(to).c_str(),
                    error_description(st));

    sequencer->schedulePeriodicReleases();
    return;
  }

  ld_check(!to.isClientAddress());
  sequencer->noteReleaseSuccessful(
      ShardID(to.asNodeID().index(), header_.shard),
      compose_lsn(header_.rid.epoch, header_.rid.esn),
      header_.release_type);
}

bool RELEASE_Message::warnAboutOldProtocol() const {
  return false;
}

const std::string& release_type_to_string(ReleaseType release_type) {
  static const std::string GLOBAL("GLOBAL");
  static const std::string PER_EPOCH("PER_EPOCH");
  static const std::string INVALID("INVALID");
  switch (release_type) {
    case ReleaseType::GLOBAL:
      return GLOBAL;
    case ReleaseType::PER_EPOCH:
      return PER_EPOCH;
    default:
      return INVALID;
  }
}

std::vector<std::pair<std::string, folly::dynamic>>
RELEASE_Message::getDebugInfo() const {
  std::vector<std::pair<std::string, folly::dynamic>> res;

  auto add = [&res](const char* key, folly::dynamic val) {
    res.emplace_back(key, std::move(val));
  };
  add("log_id", toString(header_.rid.logid));
  add("upto_lsn", lsn_to_string(header_.rid.lsn()));
  add("release_type", release_type_to_string(header_.release_type));
  add("shard", header_.shard);
  return res;
}

}} // namespace facebook::logdevice
