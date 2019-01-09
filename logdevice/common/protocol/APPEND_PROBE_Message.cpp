/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/protocol/APPEND_PROBE_Message.h"

#include <folly/Memory.h>

#include "logdevice/common/AllSequencers.h"
#include "logdevice/common/AppendRequestBase.h"
#include "logdevice/common/MetaDataLog.h"
#include "logdevice/common/MetaDataLogWriter.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Sequencer.h"
#include "logdevice/common/SequencerBatching.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/protocol/APPEND_PROBE_REPLY_Message.h"
#include "logdevice/common/util.h"
#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice {

// This function is the meat of APPEND_PROBE message processing.  Based on the
// log ID and append size, it decides if the client should proceed with the
// append, or if it should abort because the append would likely be denied.
static Status calculate_response(const Address& from,
                                 const APPEND_PROBE_Header& header) {
  Worker* w = Worker::onThisThread();
  Processor* processor = w->processor_;
  if (w->shuttingDown()) {
    return E::SHUTDOWN;
  }

  if (MetaDataLog::isMetaDataLog(header.logid)) {
    // Metadata logs go through a different codepath (MetaDataLogWriter) from
    // normal appends which does not enforce limits.  Writes are small and
    // infrequent.
    return E::OK;
  }

  // Check if the node is at the overall limit of inflight appender sizes
  // *across all logs*.
  // TODO factor the implementation of this check outta here (t11847850)
  if (w->totalSizeOfAppenders_ + header.wire_size >
      Worker::settings().max_total_appenders_size_hard /
          Worker::settings().num_workers) {
    RATELIMIT_INFO(std::chrono::seconds(10),
                   2,
                   "Denying APPEND_PROBE from %s for log %lu because "
                   "maximum total size of appenders %zu has been reached",
                   Sender::describeConnection(from).c_str(),
                   header.logid.val(),
                   Worker::settings().max_total_appenders_size_hard);
    return E::SEQNOBUFS;
  }

  // Check that the sequencer for this log has room in its sliding window.
  std::shared_ptr<Sequencer> sequencer =
      processor->allSequencers().findSequencer(header.logid);
  if (sequencer) {
    const size_t appends_inflight = sequencer->getNumAppendsInFlight();
    // with per-epoch sequencers, if the sequencer has not yet gotten a valid
    // epoch or all epochs are evicted, getNumAppendsInFlight() and
    // getMaxWindowSize() will both return 0. We still want to accept this
    // append since it may activate the sequencer.
    if (appends_inflight > 0 &&
        appends_inflight >= sequencer->getMaxWindowSize()) {
      RATELIMIT_INFO(std::chrono::seconds(10),
                     2,
                     "Denying APPEND_PROBE from %s for log %lu because "
                     "maximum append window size %zu has been reached",
                     Sender::describeConnection(from).c_str(),
                     header.logid.val(),
                     sequencer->getMaxWindowSize());
      return E::SEQNOBUFS;
    }
  } else {
    // We do not have a sequencer for this log.  The append may activate it;
    // don't deny the append because of this.
  }

  // If sequencer batching is on, there is a separate limit
  // (`Settings::max_total_buffered_append_size') on the amount it will let
  // sit in BufferedWriter.  Check if we're at that limit.
  Status rv = processor->sequencerBatching().appendProbe();
  if (rv != E::OK) {
    RATELIMIT_INFO(std::chrono::seconds(10),
                   2,
                   "Denying APPEND_PROBE from %s for log %lu because "
                   "sequencer batching is out of buffer space",
                   Sender::describeConnection(from).c_str(),
                   header.logid.val());
    return rv;
  }

  return E::OK;
}

template <>
Message::Disposition APPEND_PROBE_Message::onReceived(Address const& from) {
  Status status = calculate_response(from, header_);
  APPEND_PROBE_REPLY_Header replyhdr{
      header_.rqid,
      header_.logid,
      status,
      NodeID(),
      APPEND_PROBE_REPLY_flags_t(0),
  };
  auto msg = std::make_unique<APPEND_PROBE_REPLY_Message>(replyhdr);
  int rv = Worker::onThisThread()->sender().sendMessage(std::move(msg), from);
  if (rv != 0) {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    3,
                    "Failed to send APPEND_PROBE_REPLY(rqid=%ld, log=%ld, "
                    "status=%s) message "
                    "to %s: error %s.",
                    header_.rqid.val(),
                    header_.logid.val(),
                    error_name(status),
                    from.toString().c_str(),
                    error_name(err));
  }
  if (status == E::OK) {
    WORKER_STAT_INCR(append_probes_passed);
    // We don't bump `append_received' here like we do below because the
    // AppendRequest state machine on the client will continue and follow up
    // with a proper APPEND message; we'll count the append then.
  } else {
    StatsHolder* stats = Worker::stats();
    STAT_INCR(stats, append_probes_denied);
    // The probe failed; this will be reflected as a failed append on the
    // client so count it as a received and failed append.  (Normally we bump
    // `append_received' when an APPEND message comes in but in this case the
    // failed probe prevents the client from sending one.)
    STAT_INCR(stats, append_received);
    STAT_INCR(stats, append_failed);
    WORKER_LOG_STAT_INCR(header_.logid, append_failed);
  }
  return Disposition::NORMAL;
}

template <>
void APPEND_PROBE_Message::onSent(Status st, const Address& to) const {
  if (st != E::OK) {
    Worker* w = Worker::onThisThread();
    auto it = w->runningAppends().map.find(header_.rqid);
    if (it != w->runningAppends().map.end()) {
      it->second->onProbeSendError(st, to.asNodeID());
    }
  }
}

template <>
bool APPEND_PROBE_Message::warnAboutOldProtocol() const {
  return false;
}

}} // namespace facebook::logdevice
