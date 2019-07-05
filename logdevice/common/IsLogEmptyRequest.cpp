/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/IsLogEmptyRequest.h"

#include <folly/Memory.h>

#include "logdevice/common/EventLoop.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/protocol/IS_LOG_EMPTY_Message.h"

namespace facebook { namespace logdevice {
// See header file for explanation
const long IsLogEmptyRequest::WAVE_TIMEOUT_LOWER_BOUND_MIN;
const long IsLogEmptyRequest::WAVE_TIMEOUT_LOWER_BOUND_MAX;

Request::Execution IsLogEmptyRequest::execute() {
  // Check parameters
  if (log_id_ == LOGID_INVALID) {
    ld_error("Invalid log ID");
    finalize(E::INVALID_PARAM, false, /*delete_this=*/false);
    return Execution::COMPLETE;
  }

  if (MetaDataLog::isMetaDataLog(log_id_)) {
    ld_error("%lu is a metadata log ID, which is not supported "
             "by IsLogEmptyRequest.",
             log_id_.val());
    finalize(E::INVALID_PARAM, false, /*delete_this=*/false);
    return Execution::COMPLETE;
  }

  // Insert request into map for worker to track it
  auto insert_result = Worker::onThisThread()->runningIsLogEmpty().map.insert(
      std::make_pair(id_, std::unique_ptr<IsLogEmptyRequest>(this)));
  ld_check(insert_result.second);

  registerForShardAuthoritativeStatusUpdates();

  initNodeSetFinder();
  return Execution::CONTINUE;
}

void IsLogEmptyRequest::initNodeSetFinder() {
  nodeset_finder_ = makeNodeSetFinder();
  nodeset_finder_->start();
}

std::shared_ptr<const configuration::nodes::NodesConfiguration>
IsLogEmptyRequest::getNodesConfiguration() const {
  return Worker::onThisThread()->getNodesConfiguration();
}

std::unique_ptr<NodeSetFinder> IsLogEmptyRequest::makeNodeSetFinder() {
  return std::make_unique<NodeSetFinder>(
      log_id_,
      client_timeout_,
      [this](Status status) {
        ld_check_in(
            status,
            ({E::OK, E::TIMEDOUT, E::INVALID_PARAM, E::FAILED, E::ACCESS}));
        this->start(status);
      },
      // Currently we enforce that IsLogEmptyRequest finds nodesets through
      // reading the metadata logs. The reason is to make sure that this client
      // eventually receives SHARD_STATUS_UPDATE messages via its metadata log
      // read streams.
      // TODO(T25938692) use metadata logs v2
      NodeSetFinder::Source::METADATA_LOG);
}

void IsLogEmptyRequest::initStorageSetAccessor() {
  ld_check(nodeset_finder_);
  shards_ = nodeset_finder_->getUnionStorageSet(*getNodesConfiguration());
  ReplicationProperty minRep = nodeset_finder_->getNarrowestReplication();
  ld_debug("Building StorageSetAccessor with %lu shards in storage set, "
           "replication %s",
           shards_.size(),
           minRep.toString().c_str());

  StorageSetAccessor::ShardAccessFunc shard_access =
      [this](ShardID shard, const StorageSetAccessor::WaveInfo&) {
        return this->sendTo(shard);
      };

  StorageSetAccessor::CompletionFunc completion = [this](Status st) {
    this->completion(st);
  };

  StorageSetAccessor::CompletionCondition completion_cond = [this]() {
    // This is called once we have an f-majority of responses, and will force
    // execution to continue until we have sufficient consensus among
    // nodes (or hit either the client or grace period timeout).
    completion_cond_called_ = true;
    bool res = haveNonEmptyCopyset() || haveEmptyFMajority() || haveDeadEnd();
    completion_cond_allowed_termination_ |= res;
    // If it passed once, should never fail later
    ld_check_eq(res, completion_cond_allowed_termination_);
    return res;
  };

  nodeset_accessor_ = makeStorageSetAccessor(
      getNodesConfiguration(), shards_, minRep, shard_access, completion);
  nodeset_accessor_->setGracePeriod(grace_period_, completion_cond);
  nodeset_accessor_->setWaveTimeout(getWaveTimeoutInterval(client_timeout_));
  failure_domain_ = makeFailureDomain(shards_, getNodesConfiguration(), minRep);
}

chrono_interval_t<std::chrono::milliseconds>
IsLogEmptyRequest::getWaveTimeoutInterval(std::chrono::milliseconds timeout) {
  // Clamp min to allowed range
  long min = (long)std::round(timeout.count() / 10.0);
  min = std::max(WAVE_TIMEOUT_LOWER_BOUND_MIN,
                 std::min(min, WAVE_TIMEOUT_LOWER_BOUND_MAX));
  // Stick to a max of 10s regardless of the client timeout.
  return chrono_interval_t<std::chrono::milliseconds>{
      std::chrono::milliseconds(min), std::chrono::milliseconds(10000)};
}

void IsLogEmptyRequest::onShardStatusChanged(bool initialize) {
  // nodeset accessor may be not constructed yet.
  if (nodeset_accessor_) {
    if (!initialize) {
      // May only differ when we're still starting up
      ld_check(!haveShardAuthoritativeStatusDifferences());
    }

    const auto fd_auth_statuses_before =
        failure_domain_->getShardAuthoritativeStatusMap();
    const auto na_auth_statuses_before =
        nodeset_accessor_->getFailureDomainShardAuthoritativeStatusMap();

    applyShardStatus(initialize);

    // A node may have become underreplicated or unavailable, such that we no
    // longer expect to get an f-majority of responses. We might as well finish
    // early in this case. If all nodes that couldn't give us a response were
    // in rebuilding, consider it PARTIAL; otherwise consider it FAILED.
    // This is needed because the NodeSetAccessor does not catch cases where we
    // already have a dead end on startup, and there are also other cases we'd
    // consider a dead end for this particular use case that NodeSetAccessor
    // doesn't do.
    if (!haveFmajorityOfResponses() && haveDeadEnd()) {
      RATELIMIT_INFO(std::chrono::seconds(10),
                     2,
                     "After a change in shard status, we hit a dead end -- we "
                     "won't get an accurate answer right now. Considering log "
                     "%lu non-empty. Shard statuses according to request's "
                     "FailureDomain: %s",
                     log_id_.val(),
                     getHumanReadableShardStatuses().c_str());
      completion(E::FAILED); // this will correctly decide the final status
    } else {
      // NodeSetAccessor won't be ready when we initialize this request's
      // failure domain, but will update itself when we start it.
      if (!initialize) {
        bool done = nodeset_accessor_->onShardStatusChanged();
        if (!done) {
          ld_check(!haveShardAuthoritativeStatusDifferences(
              &fd_auth_statuses_before, &na_auth_statuses_before));
        } else {
          // `this` was destroyed by nodeset_accessor_'s completion callback
        }
      }
    }
  }
}

ShardAuthoritativeStatusMap&
IsLogEmptyRequest::getShardAuthoritativeStatusMap() {
  return Worker::onThisThread()
      ->shardStatusManager()
      .getShardAuthoritativeStatusMap();
}

void IsLogEmptyRequest::applyShardStatus(bool initialize_unknown) {
  const auto& shard_status_map = getShardAuthoritativeStatusMap();
  for (const ShardID shard : nodeset_accessor_->getShards()) {
    const auto auth_st =
        shard_status_map.getShardStatus(shard.node(), shard.shard());
    setShardAuthoritativeStatus(shard, auth_st, initialize_unknown);
  }
}

void IsLogEmptyRequest::setShardAuthoritativeStatus(ShardID shard,
                                                    AuthoritativeStatus auth_st,
                                                    bool initialize_unknown) {
  if (!failure_domain_->containsShard(shard)) {
    ld_warning("Given authoritative status for %s, which is not known by "
               "failure detector! Auth status: %s",
               shard.toString().c_str(),
               toString(auth_st).c_str());
    return; // ignore shard, might be newly added node
  }

  shard_status_t shard_st = 0;
  int rv = failure_domain_->getShardAttribute(shard, &shard_st);
  if (rv != 0 && !initialize_unknown) {
    ld_error("Failed to find state of shard %s (now auth st %s), ignoring",
             shard.toString().c_str(),
             toString(auth_st).c_str());
    return;
  }

  shard_st = (auth_st == AuthoritativeStatus::UNDERREPLICATION ||
              auth_st == AuthoritativeStatus::UNAVAILABLE)
      ? shard_st | SHARD_IS_REBUILDING
      : shard_st & ~SHARD_IS_REBUILDING;
  failure_domain_->setShardAttribute(shard, shard_st);
  failure_domain_->setShardAuthoritativeStatus(shard, auth_st);
}

std::unique_ptr<StorageSetAccessor> IsLogEmptyRequest::makeStorageSetAccessor(
    const std::shared_ptr<const configuration::nodes::NodesConfiguration>&
        nodes_configuration,
    StorageSet shards,
    ReplicationProperty minRep,
    StorageSetAccessor::ShardAccessFunc node_access,
    StorageSetAccessor::CompletionFunc completion) {
  return std::make_unique<StorageSetAccessor>(
      log_id_,
      shards,
      nodes_configuration,
      minRep,
      node_access,
      completion,
      StorageSetAccessor::Property::FMAJORITY,
      client_timeout_);
}

void IsLogEmptyRequest::start(Status status) {
  if (status != E::OK) {
    ld_error("Unable to get the set of nodes to send IsLogEmpty requests to "
             "for log %lu: %s.",
             log_id_.val_,
             error_description(status));
    finalize(status, false);
    return;
  }

  initStorageSetAccessor();
  ld_check(nodeset_accessor_ != nullptr);
  ld_check(!callback_called_);
  onShardStatusChanged(/*initialize=*/true);
  if (!callback_called_) {
    nodeset_accessor_->start();
  }
}

StorageSetAccessor::SendResult IsLogEmptyRequest::sendTo(ShardID shard) {
  const auto& nodes_configuration = getNodesConfiguration();
  if (!nodes_configuration->isNodeInServiceDiscoveryConfig(shard.node())) {
    ld_error("Cannot find node at index %u", shard.node());
    return {StorageSetAccessor::Result::PERMANENT_ERROR, Status::NOTFOUND};
  }

  NodeID to(shard.node());
  IS_LOG_EMPTY_Header header = {id_, log_id_, shard.shard()};
  auto msg = std::make_unique<IS_LOG_EMPTY_Message>(header);
  if (Worker::onThisThread()->sender().sendMessage(std::move(msg), to) != 0) {
    if (err == E::PROTONOSUPPORT) {
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      10,
                      "IS_LOG_EMPTY is not supported by the server at %s",
                      Sender::describeConnection(to).c_str());
      return {StorageSetAccessor::Result::PERMANENT_ERROR, err};
    } else {
      return {StorageSetAccessor::Result::TRANSIENT_ERROR, err};
    }
  }

  return {StorageSetAccessor::Result::SUCCESS, err};
}

void IsLogEmptyRequest::finalize(Status status, bool empty, bool delete_this) {
  ld_check(!callback_called_);
  callback_called_ = true;
  if (status == E::OK) {
    ld_check(haveEmptyFMajority() || haveNonEmptyCopyset());
  }
  switch (status) {
    case E::OK:
    case E::PARTIAL:
    case E::TIMEDOUT:
    case E::FAILED:
    case E::INVALID_PARAM:
    case E::ACCESS:
      break;
    default:
      ld_error("IsLogEmpty[%lu] finished with unexpected status %s; returning "
               "FAILED instead",
               log_id_.val(),
               error_name(status));
      ld_check(false);
      status = E::FAILED;
  }
  callback_(*this, status, empty);
  if (delete_this) {
    deleteThis();
  }
}

IsLogEmptyRequest::~IsLogEmptyRequest() {
  const Worker* worker = Worker::onThisThread(false /* enforce_worker */);
  if (!worker) {
    // The request has not made it to a Worker. Do not call the callback.
    return;
  }

  if (!callback_called_) {
    // This can happen if the request or client gets torn down while the
    // request is still processing
    ld_check(worker->shuttingDown());
    ld_warning("IsLogEmptyRequest destroyed while still processing");
    callback_(*this, E::SHUTDOWN, false);
  }
}

void IsLogEmptyRequest::onMessageSent(ShardID to, Status status) {
  if (status != E::OK) {
    if (status == E::PROTONOSUPPORT) {
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      10,
                      "IS_LOG_EMPTY is not supported by the server for "
                      "shard %s",
                      to.toString().c_str());
      // Let this be handled as if we received the below reply. The shard will
      // not be asked again, and we will check if we've hit a dead end.
      onReply(to, E::NOTSUPPORTED, false);
    } else {
      nodeset_accessor_->onShardAccessed(
          to, {StorageSetAccessor::Result::TRANSIENT_ERROR, status});
    }
  }
}

bool IsLogEmptyRequest::haveNonEmptyCopyset() const {
  return failure_domain_->canReplicate(node_non_empty_filter);
}

bool IsLogEmptyRequest::haveEmptyFMajority() const {
  auto fmajority_result = failure_domain_->isFmajority(node_empty_filter);
  return fmajority_result != FmajorityResult::NONE &&
      fmajority_result != FmajorityResult::NON_AUTHORITATIVE;
}

bool IsLogEmptyRequest::haveDeadEnd() const {
  // If we have an f-majority of responses but neither an empty f-majority, or
  // non-empty copyset, this function is used to see if there's enough
  // non-underreplicated and available nodes we're still waiting for answers
  // from -- in other words, if there's any point in continuing to wait.
  auto potential_response_filter = [](shard_status_t val) {
    return ~val & SHARD_HAS_RESULT && ~val & SHARD_IS_REBUILDING &&
        ~val & SHARD_HAS_ERROR;
  };

  // Check if we could possibly get a non-empty copyset
  if (failure_domain_->canReplicate([&](shard_status_t val) {
        return this->node_non_empty_filter(val) ||
            potential_response_filter(val);
      })) {
    return false;
  }

  // Check if we could possibly get an empty f-majority
  auto fmajority_result = failure_domain_->isFmajority([&](shard_status_t val) {
    return this->node_empty_filter(val) || potential_response_filter(val);
  });
  return fmajority_result == FmajorityResult::NONE ||
      fmajority_result == FmajorityResult::NON_AUTHORITATIVE;
}

bool IsLogEmptyRequest::haveFmajorityOfResponses() const {
  // In case we hit a dead end on a change of shard authoritative status, this
  // is used to determine whether we have enough responses to consider it a
  // sufficient result and return E::OK, or whether to return E::PARTIAL.
  // Shards that are authoritative empty are already ignored, so there's no
  // need to account for them.
  auto fmajority_result = failure_domain_->isFmajority(
      [](shard_status_t val) { return val & SHARD_HAS_RESULT; });
  return fmajority_result != FmajorityResult::NONE &&
      fmajority_result != FmajorityResult::NON_AUTHORITATIVE;
}

bool IsLogEmptyRequest::haveOnlyRebuildingFailures() const {
  // Count number of non-rebuilding failures
  return failure_domain_->countShards(
             [](shard_status_t val) { return val & SHARD_HAS_ERROR; }) == 0;
}

void IsLogEmptyRequest::onReply(ShardID from, Status status, bool is_empty) {
  ld_debug("Received IS_LOG_EMPTY_REPLY[%lu] from %s, status=%s, result=%s",
           log_id_.val(),
           from.toString().c_str(),
           error_name(status),
           is_empty ? "TRUE" : "FALSE");

  shard_status_t shard_st;
  int rv = failure_domain_->getShardAttribute(from, &shard_st);

  if (rv != 0) {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    10,
                    "Shard %s not known, ignoring",
                    from.toString().c_str());
    ld_check(!failure_domain_->containsShard(from));
    return;
  }

  if (shard_st & SHARD_HAS_RESULT && status != E::OK) {
    ld_debug("Got reply from %s with status %s, but we already have a healthy "
             "result for it! Ignoring.",
             from.toString().c_str(),
             error_name(status));
    return;
  }

  auto res = StorageSetAccessor::Result::SUCCESS;

  switch (status) {
    case E::ACCESS:
      finalize(E::ACCESS, false);
      return;

    case E::OK:
      shard_st |= SHARD_HAS_RESULT;
      shard_st =
          is_empty ? shard_st | SHARD_IS_EMPTY : shard_st & ~SHARD_IS_EMPTY;
      failure_domain_->setShardAttribute(from, shard_st);

      // If we have a non-empty copyset, go ahead and finish since the
      // NodeSetAccessor won't check if we can finish before we have responses
      // from an f-majority of the nodes. That's a waste of time in this case.
      if (!is_empty && haveNonEmptyCopyset()) {
        finalize(E::OK, false);
        return;
      }
      // Whether we have an empty f-majority is checked in completion_cond.
      break;

    case E::REBUILDING:
      RATELIMIT_DEBUG(std::chrono::seconds(1),
                      10,
                      "Shard %s is rebuilding.",
                      from.toString().c_str());

      // Even if this is mini-rebuilding, treat it the same, i.e. don't count
      // on it to finish within the duration of this request.
      failure_domain_->setShardAttribute(from, shard_st | SHARD_IS_REBUILDING);
      res = StorageSetAccessor::Result::PERMANENT_ERROR;
      break;

    case E::AGAIN:
      res = StorageSetAccessor::Result::TRANSIENT_ERROR;
      break;

    case E::SHUTDOWN:
    case E::NOTSTORAGE:
    case E::FAILED:
    case E::NOTSUPPORTED:
    case E::NOTSUPPORTEDLOG:
      res = StorageSetAccessor::Result::PERMANENT_ERROR;
      // Note that we shouldn't expect a response from this node; no need to
      // keep any previous shard status such as underreplication.
      failure_domain_->setShardAttribute(from, SHARD_HAS_ERROR);
      break;

    default:
      RATELIMIT_ERROR(std::chrono::seconds(10),
                      2,
                      "Received IS_LOG_EMPTY_REPLY message from %s with "
                      "unexpected status %s",
                      from.toString().c_str(),
                      error_description(status));
      res = StorageSetAccessor::Result::PERMANENT_ERROR;
      break;
  }

  if (!haveFmajorityOfResponses() && haveDeadEnd()) {
    // We've hit a dead end, before even hearing from an f-majority of the
    // nodes. Since the NodeSetAccessor only understands responses, but doesn't
    // understand our additional requirements for reaching consensus, it can
    // miss this case under certain conditions.
    completion(E::FAILED); // this will correctly decide the final status
  } else {
    nodeset_accessor_->onShardAccessed(from, {res, status});
  }
}

bool IsLogEmptyRequest::haveShardAuthoritativeStatusDifferences(
    const AuthoritativeStatusMap* fd_auth_statuses_before,
    const AuthoritativeStatusMap* na_auth_statuses_before) const {
  const auto fd_auth_statuses_after =
      failure_domain_->getShardAuthoritativeStatusMap();
  const auto na_auth_statuses_after =
      nodeset_accessor_->getFailureDomainShardAuthoritativeStatusMap();

  auto shard_status_pair_to_str =
      [&](ShardID shard,
          const std::unordered_map<ShardID, AuthoritativeStatus, ShardID::Hash>*
              map) -> std::string {
    if (map == nullptr) {
      return "?";
    }
    auto pair = map->find(shard);
    return pair == map->cend() ? "?" : logdevice::toShortString(pair->second);
  };

  bool have_difference = false;

  auto print_shard_status_difference = [&](ShardID shard) {
    have_difference = true;
    RATELIMIT_ERROR(
        std::chrono::seconds(60),
        60,
        "Failure domains differ: request for log %lu says shard %s [%s -> %s], "
        "NodeSetAccessor says [%s -> %s]",
        log_id_.val(),
        shard.toString().c_str(),
        shard_status_pair_to_str(shard, fd_auth_statuses_before).c_str(),
        shard_status_pair_to_str(shard, &fd_auth_statuses_after).c_str(),
        shard_status_pair_to_str(shard, na_auth_statuses_before).c_str(),
        shard_status_pair_to_str(shard, &na_auth_statuses_after).c_str());
  };

  // Look through all shards known by this request's FailureDomain, and look
  // for any inconsistencies w.r.t. the NodeSetAccessor's FailureDomain.
  for (const auto fd_pair : fd_auth_statuses_after) {
    const auto na_pair = na_auth_statuses_after.find(fd_pair.first);
    if (na_pair == na_auth_statuses_after.cend() ||
        fd_pair.second != na_pair->second) {
      print_shard_status_difference(fd_pair.first);
    }
  }
  // Now, all that possibly remains is some shards that NodeSetAccessor know
  // about that this request doesn't.
  for (const auto na_pair : na_auth_statuses_after) {
    if (fd_auth_statuses_after.find(na_pair.first) ==
        fd_auth_statuses_after.cend()) {
      print_shard_status_difference(na_pair.first);
    }
  }

  return have_difference;
}

std::string IsLogEmptyRequest::getHumanReadableShardStatuses() const {
  std::vector<std::string> result_statuses;

  for (const ShardID shard : shards_) {
    const auto st = failure_domain_->getShardAuthoritativeStatus(shard);

    shard_status_t st_attr_status = 0;
    int rv = failure_domain_->getShardAttribute(shard, &st_attr_status);
    if (rv != 0) {
      RATELIMIT_ERROR(std::chrono::seconds(10),
                      10,
                      "INTERNAL ERROR: state for shard %s not found!",
                      shard.toString().c_str());
      ld_check(false);
      return "<error>";
    }

    // Print status of nodes that did not give us a result
    if (~st_attr_status & SHARD_HAS_RESULT) {
      result_statuses.push_back(shard.toString() + " = " +
                                logdevice::toShortString(st).c_str() + "(" +
                                getShardState(st_attr_status).c_str() + ")");
    }
  }

  return folly::join("; ", result_statuses);
}

std::string IsLogEmptyRequest::getShardState(shard_status_t s) {
  std::vector<std::string> results;

  if (s & SHARD_HAS_ERROR) {
    results.push_back("HAS_ERROR");
  }
  if (s & SHARD_HAS_RESULT) {
    results.push_back(s & SHARD_IS_EMPTY ? "RESULT=EMPTY" : "RESULT=NON_EMPTY");
  }
  if (s & SHARD_IS_REBUILDING) {
    results.push_back("IS_REBUILDING");
  }

  return results.empty() ? "-" : folly::join("|", results);
}

void IsLogEmptyRequest::deleteThis() {
  Worker* worker = Worker::onThisThread();

  auto& map = worker->runningIsLogEmpty().map;
  auto it = map.find(id_);
  ld_check(it != map.end());

  map.erase(it); // destroys unique_ptr which owns this
}

void IsLogEmptyRequest::completion(Status st) {
  ld_check_in(st, ({E::OK, E::TIMEDOUT, E::FAILED, E::ACCESS}));

  if (nodeset_accessor_) {
    if (started_) {
      ld_check(!haveShardAuthoritativeStatusDifferences());
    }
  }

  if (st == E::TIMEDOUT) {
    // Hit the client timeout before seeing an f-majority of responses.
    RATELIMIT_WARNING(std::chrono::seconds(1),
                      10,
                      "timed out (%ld ms) waiting for storage nodes, "
                      "assuming that log %lu is not empty. Shard states "
                      "according to NA: [%s], FD: [%s]",
                      client_timeout_.count(),
                      log_id_.val(),
                      nodeset_accessor_
                          ? nodeset_accessor_->describeState().c_str()
                          : "<not initialized>",
                      getHumanReadableShardStatuses().c_str());
    ld_check(!completion_cond_called_);
    finalize(E::TIMEDOUT, false);
  } else if (st == E::OK) {
    // We'll get here if an f-majority of the nodes responded, and either
    // 1) we hit the client or grace period timeout before the result was
    //    clear, or
    // 2) completion_cond allowed the NodeSetAccessor to terminate because
    //    we've got an empty f-majority, or
    // 3) completion_cond allowed the NodeSetAccessor to terminate because
    //    we've reached a dead end.
    ld_check(completion_cond_called_);

    if (!completion_cond_allowed_termination_) {
      RATELIMIT_INFO(std::chrono::seconds(10),
                     10,
                     "Saw an f-majority of responses but got no consensus "
                     "despite waiting for a %lu ms grace period or "
                     "the %lu ms client timeout; considering log %lu "
                     "non-empty (partial result). Shard states according to "
                     "NA: [%s], FD: [%s]. Non-empty shards: %s",
                     grace_period_.count(),
                     client_timeout_.count(),
                     log_id_.val(),
                     nodeset_accessor_->describeState().c_str(),
                     getHumanReadableShardStatuses().c_str(),
                     getNonEmptyShardsList().c_str());

      finalize(E::PARTIAL, false);
    } else if (haveEmptyFMajority()) {
      finalize(E::OK, true);
    } else {
      ld_check(haveDeadEnd());
      finalize(E::PARTIAL, false);
    }
  } else if (st == E::ACCESS) {
    finalize(E::ACCESS, false);
  } else {
    // We'll get here if
    // 1) Too many nodes couldn't answer, and NodeSetAccessor decided that this
    //    was a fitting final status.
    // 2) We hit a dead end before even hearing from an f-majority -- either
    //    due to a shard's authoritative status changing, or getting an
    //    E::REBUILDING response.
    // In either case, use FAILED unless all the failures were just rebuilding;
    // then use PARTIAL.
    ld_check_eq(st, E::FAILED);
    finalize(haveOnlyRebuildingFailures() ? E::PARTIAL : E::FAILED, false);
  }
}

std::string IsLogEmptyRequest::getNonEmptyShardsList() const {
  std::string result = "[";
  bool first = true;
  bool some_not_found = false;

  for (ShardID shard : shards_) {
    shard_status_t status;
    int rv = failure_domain_->getShardAttribute(shard, &status);
    ld_check_eq(rv, 0);
    if (rv != 0) {
      some_not_found = true;
    } else if (node_non_empty_filter(status)) {
      if (first) {
        first = false;
      } else {
        result += ", ";
      }
      result += shard.toString();
    }
  }

  result += "]";

  if (some_not_found) {
    // This should be impossible, but just in case.
    result += " (some shards not found due to an internal error)";
  }

  return result;
}

}} // namespace facebook::logdevice
