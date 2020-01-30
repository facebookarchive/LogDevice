/**
 * Copyright (c) 2019-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/replicated_state_machine/LogBasedRSMSnapshotStore.h"

#include <string>
#include <zstd.h>

#include "logdevice/common/AppendRequest.h"
#include "logdevice/common/Checksum.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/SyncSequencerRequest.h"
#include "logdevice/common/TailRecord.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/client_read_stream/AllClientReadStreams.h"
#include "logdevice/common/client_read_stream/ClientReadStreamBufferFactory.h"

namespace facebook { namespace logdevice {

int LogBasedRSMSnapshotStore::postRequestWithRetrying(
    std::unique_ptr<Request>& rq) const {
  return processor_->postWithRetrying(rq);
}

void LogBasedRSMSnapshotStore::postAppendRequest(lsn_t snapshot_ver,
                                                 std::string payload,
                                                 completion_cb_t cb) {
  auto snapshot_log = snapshot_log_id_;
  auto append_cb = [cb, snapshot_ver, snapshot_log](
                       Status st, const DataRecord& r) {
    lsn_t snapshot_ver_written = st == E::OK ? snapshot_ver : LSN_INVALID;
    ld_debug("Wrote snapshot ver:%s, as lsn:%s in snapshot log:%lu, st:%s",
             lsn_to_string(snapshot_ver_written).c_str(),
             lsn_to_string(r.attrs.lsn).c_str(),
             snapshot_log.val_,
             error_name(st));
    cb(st, snapshot_ver_written);
  };
  std::unique_ptr<AppendRequest> req = std::make_unique<AppendRequest>(
      nullptr,
      snapshot_log_id_,
      AppendAttributes(),
      PayloadHolder::copyBuffer(payload.data(), payload.size()),
      snapshot_append_timeout_,
      append_cb);
  req->bypassWriteTokenCheck();
  std::unique_ptr<Request> base_req = std::move(req);
  int rv = postRequestWithRetrying(base_req);
  if (rv) {
    ld_error("Failed with err:%s, while writing snapshot for log:%lu, "
             "base ver:%s",
             error_name(err),
             snapshot_log_id_.val_,
             lsn_to_string(snapshot_ver).c_str());
    cb(E::FAILED, snapshot_ver);
    return;
  }
}

read_stream_id_t LogBasedRSMSnapshotStore::createBasicReadStream(
    lsn_t start_lsn,
    lsn_t until_lsn,
    ClientReadStreamDependencies::record_cb_t on_record,
    ClientReadStreamDependencies::gap_cb_t on_gap,
    ClientReadStreamDependencies::health_cb_t health_cb) {
  const auto rsid = processor_->issueReadStreamID();
  auto deps = std::make_unique<ClientReadStreamDependencies>(
      rsid,
      snapshot_log_id_,
      "",
      on_record,
      on_gap,
      nullptr /* on done cb */,
      nullptr, /* metadata cache */
      health_cb);

  RATELIMIT_INFO(
      std::chrono::seconds(1),
      1,
      "Creating ClientReadStream for log:%lu to read snapshot record at lsn:%s",
      snapshot_log_id_.val_,
      lsn_to_string(start_lsn).c_str());
  auto read_stream = std::make_unique<ClientReadStream>(
      rsid,
      snapshot_log_id_,
      start_lsn,
      until_lsn,
      processor_->settings()->client_read_flow_control_threshold,
      ClientReadStreamBufferType::CIRCULAR,
      100,
      std::move(deps),
      processor_->config_);

  // SCD adds complexity and may incur latency on storage node failures. Since
  // replicated state machines should be low volume logs, we can afford to not
  // use that optimization.
  read_stream->forceNoSingleCopyDelivery();
  Worker* w = Worker::onThisThread();
  ld_check(w->processor_ == processor_);
  w->clientReadStreams().insertAndStart(std::move(read_stream));
  return rsid;
}

int LogBasedRSMSnapshotStore::extractVersion(
    const std::string& payload_str,
    RSMSnapshotHeader& header_out) const {
  Payload pl(payload_str.data(), payload_str.size());
  const auto header_sz = RSMSnapshotHeader::deserialize(pl, header_out);
  if (header_sz < 0) {
    ld_error("Failed to deserialize header of snapshot");
    err = E::BADMSG;
    return -1;
  }
  ld_debug("log:%lu, snapshot header(base version:%s, delta ptr:%s)",
           snapshot_log_id_.val_,
           lsn_to_string(header_out.base_version).c_str(),
           lsn_to_string(header_out.delta_log_read_ptr).c_str());
  return 0;
}

bool LogBasedRSMSnapshotStore::isWritable() const {
  if (!writable_) {
    ld_info("Store is not configured as writable");
    return false;
  }

  NodeID my_node_id = processor_->getMyNodeID();
  ld_check(my_node_id.isNodeID());

  auto w = Worker::onThisThread();
  auto cs = w->getClusterState();
  ld_check(cs != nullptr);

  // The node responsible for snapshotting is the first node
  // that's alive according to the failure detector.
  return cs->getFirstNodeAlive() == my_node_id.index();
}

void LogBasedRSMSnapshotStore::writeSnapshot(lsn_t snapshot_ver,
                                             std::string snapshot_blob,
                                             completion_cb_t cb) {
  postAppendRequest(snapshot_ver, std::move(snapshot_blob), std::move(cb));
}

int LogBasedRSMSnapshotStore::getLastReleasedLsn(
    std::function<void(Status st, lsn_t last_released_lsn)>
        get_last_released_cb) {
  logid_t logid = snapshot_log_id_;
  auto ssr_cb = [get_last_released_cb, logid](
                    Status st,
                    NodeID /*seq*/,
                    lsn_t next_lsn,
                    std::unique_ptr<LogTailAttributes> tail_attributes,
                    std::shared_ptr<const EpochMetaDataMap> /*metadata_map*/,
                    std::shared_ptr<TailRecord> /*tail_record*/,
                    folly::Optional<bool> is_log_empty) {
    Status orig_st = st;
    lsn_t read_upto{LSN_INVALID};
    const lsn_t tail_lsn = next_lsn <= LSN_OLDEST ? LSN_OLDEST : next_lsn - 1;
    if (st != E::OK) {
      ld_error("Got st:%s while fetching version for log:%lu, next_lsn:%s",
               error_name(st),
               logid.val_,
               lsn_to_string(next_lsn).c_str());
    } else if (!tail_attributes) {
      ld_error(
          "Got st:%s but no tail attributes while fetching version for log:%lu",
          error_name(st),
          logid.val_);
      st = E::FAILED;
    } else {
      if (tail_attributes->last_released_real_lsn == LSN_INVALID) {
        st = E::EMPTY;
        // Assert that last_release_real_lsn being invalid means log is empty
        ld_check(is_log_empty.value() == 1);
      } else {
        read_upto = tail_attributes->last_released_real_lsn;
      }
    }

    ld_debug(
        "SSR cb, log:%lu, orig_st:%s, st:%s, next_lsn:%s, tail_lsn:%s, "
        "last_released_real_lsn(%s), is_log_empty(%s), read_upto:%s",
        logid.val_,
        error_name(orig_st),
        error_name(st),
        lsn_to_string(next_lsn).c_str(),
        lsn_to_string(tail_lsn).c_str(),
        tail_attributes
            ? lsn_to_string(tail_attributes->last_released_real_lsn).c_str()
            : "no tail attributes",
        is_log_empty.hasValue()
            ? folly::to<std::string>(is_log_empty.value()).c_str()
            : "no value",
        lsn_to_string(read_upto).c_str());
    get_last_released_cb(st, read_upto);
  };

  // Since SyncSequencerRequest internally does retry on E::AGAIN, we don't need
  // to call getLastReleasedLsn() repeatedly
  auto ss_req = std::make_unique<SyncSequencerRequest>(
      snapshot_log_id_,
      SyncSequencerRequest::INCLUDE_TAIL_ATTRIBUTES |
          SyncSequencerRequest::INCLUDE_IS_LOG_EMPTY,
      ssr_cb);
  auto w = Worker::onThisThread(false);
  if (w) {
    ss_req->setWorkerType(w->worker_type_);
    ss_req->setThreadIdx(w->idx_.val());
  }
  std::unique_ptr<Request> req = std::move(ss_req);
  return postRequestWithRetrying(req);
}

void LogBasedRSMSnapshotStore::getVersion(snapshot_ver_cb_t ver_cb) {
  auto snapshot_cb = [ver_cb](
                         Status st,
                         std::string /* unused */,
                         RSMSnapshotStore::SnapshotAttributes snapshot_attrs) {
    ver_cb(st, snapshot_attrs.base_version);
  };

  // Read the snapshot blob to extract base version from the header
  getSnapshot(
      std::max(last_snapshot_attrs_.base_version, LSN_OLDEST), snapshot_cb);
}

void LogBasedRSMSnapshotStore::getDurableVersion(snapshot_ver_cb_t cb) {
  getVersion(std::move(cb));
}

bool LogBasedRSMSnapshotStore::onSnapshotRecord(
    lsn_t min_ver,
    snapshot_cb_t cb,
    Status st,
    lsn_t last_released_real_lsn,
    const std::unique_ptr<DataRecord>& record) {
  Payload& p = record->payload;
  std::string snapshot_blob((char*)p.data(), p.size());
  RSMSnapshotHeader header_out;
  int rv = extractVersion(snapshot_blob, header_out);
  SnapshotAttributes snapshot_attrs(
      header_out.base_version, record->attrs.timestamp);
  if (!rv && st == E::OK) {
    if (last_released_real_lsn_ < last_released_real_lsn) {
      setCachedItems(last_released_real_lsn, snapshot_blob, snapshot_attrs);
    }
  }
  ld_debug("getSnapshot()'s onRecord() cb for log:%lu, "
           "last_released_real_lsn:%s, base_ver:%s, "
           "snapshot_attrs(ts:%lu), "
           "snapshot size:%zu, rv:%d",
           snapshot_log_id_.val_,
           lsn_to_string(last_released_real_lsn).c_str(),
           lsn_to_string(header_out.base_version).c_str(),
           snapshot_attrs.timestamp.count(),
           p.size(),
           rv);
  Status st_to_forward = st;
  if (rv) {
    st_to_forward = E::BADMSG;
  } else if (min_ver > header_out.base_version) {
    st_to_forward = E::STALE;
  }
  cb(st_to_forward, std::move(snapshot_blob), std::move(snapshot_attrs));
  return true;
}

void LogBasedRSMSnapshotStore::onGotLastReleased(Status st,
                                                 lsn_t last_released_real_lsn,
                                                 lsn_t min_ver,
                                                 snapshot_cb_t cb) {
  if (st == E::OK) {
    // do nothing, we either need to read snapshot log, OR
    // return cached version
  } else if (st == E::EMPTY || st == E::FAILED) {
    ld_info("Returning %s for log:%lu, last_released_real_lsn:%s",
            error_name(st),
            snapshot_log_id_.val_,
            lsn_to_string(last_released_real_lsn).c_str());
    cb(st, "", SnapshotAttributes(LSN_INVALID, std::chrono::milliseconds(0)));
    return;
  } else {
    ld_error(
        "Failed with st=%s, log:%lu", error_name(st), snapshot_log_id_.val_);
    cb(st, "", SnapshotAttributes(LSN_INVALID, std::chrono::milliseconds(0)));
    return;
  }

  if (last_released_real_lsn_ >= last_released_real_lsn &&
      last_snapshot_attrs_.base_version >= min_ver) {
    RATELIMIT_INFO(std::chrono::seconds(1),
                   2,
                   "Returning cached snapshot; last_released_real_lsn_:%s, "
                   "last_released_real_lsn:%s, base ver:%s, min_ver:%s",
                   lsn_to_string(last_released_real_lsn_).c_str(),
                   lsn_to_string(last_released_real_lsn).c_str(),
                   lsn_to_string(last_snapshot_attrs_.base_version).c_str(),
                   lsn_to_string(min_ver).c_str());
    used_cache_++;
    cb(E::OK, latest_snapshot_blob_, last_snapshot_attrs_);
    return;
  }

  logid_t snapshot_log_id = snapshot_log_id_;
  createBasicReadStream(
      last_released_real_lsn,                          // start
      last_released_real_lsn,                          // end
      [this, min_ver, cb, st, last_released_real_lsn]( // on record cb
          std::unique_ptr<DataRecord>& record) {
        return onSnapshotRecord(
            min_ver, cb, st, last_released_real_lsn, record);
      },
      [cb, snapshot_log_id](const GapRecord& gap) { // on gap cb
        ld_info("Received GAP(type:%s, lo:%s, hi:%s) while reading snapshot "
                "log:%lu",
                gapTypeToString(gap.type).c_str(),
                lsn_to_string(gap.lo).c_str(),
                lsn_to_string(gap.hi).c_str(),
                snapshot_log_id.val_);
        cb(E::NOTFOUND,
           "",
           SnapshotAttributes(LSN_INVALID, std::chrono::milliseconds(0)));
        return true;
      },
      nullptr /* on health cb */);
}

void LogBasedRSMSnapshotStore::getSnapshot(lsn_t min_ver, snapshot_cb_t cb) {
  auto get_last_released_cb = [this, min_ver, cb](
                                  Status st, lsn_t last_released_real_lsn) {
    onGotLastReleased(st, last_released_real_lsn, min_ver, cb);
  };

  if (getLastReleasedLsn(std::move(get_last_released_cb))) {
    cb(E::FAILED,
       "",
       SnapshotAttributes(LSN_INVALID, std::chrono::milliseconds(0)));
    return;
  }
}

void LogBasedRSMSnapshotStore::setCachedItems(
    lsn_t last_released_real_lsn,
    std::string snapshot_blob,
    SnapshotAttributes snapshot_attrs) {
  last_released_real_lsn_ = last_released_real_lsn;
  latest_snapshot_blob_ = snapshot_blob;
  last_snapshot_attrs_ = snapshot_attrs;
}

}} // namespace facebook::logdevice
