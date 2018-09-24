/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "EventLogUtils.h"

#include <signal.h>

#include <unordered_map>
#include <unordered_set>

#include "logdevice/common/FailureDomainNodeSet.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/ReaderImpl.h"
#include "logdevice/common/ThreadID.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/configuration/InternalLogs.h"
#include "logdevice/common/event_log/EventLogStateMachine.h"
#include "logdevice/common/replicated_state_machine/TrimRSMRequest.h"
#include "logdevice/common/replicated_state_machine/ReplicatedStateMachine-enum.h"
#include "logdevice/lib/ClientImpl.h"
#include "logdevice/lib/ClientSettingsImpl.h"

namespace facebook { namespace logdevice { namespace EventLogUtils {

bool aborted = false;

int getRebuildingSet(Client& client, EventLogRebuildingSet& set, bool tail) {
  ClientImpl* client_impl = dynamic_cast<ClientImpl*>(&client);
  ld_check(client_impl);

  ClientSettingsImpl* client_settings =
      dynamic_cast<ClientSettingsImpl*>(&client_impl->settings());
  ld_check(client_settings);

  ld_check(!ThreadID::isWorker());

  auto event_log =
      std::make_unique<EventLogStateMachine>(client_settings->getSettings());

  if (!tail) {
    event_log->stopAtTail();
  } else {
    // The user can stop reading by sending SIGTERM or SIGINT
    struct sigaction sa;
    sa.sa_handler = [](int /* sig */) { aborted = true; };
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    int rv = sigaction(SIGTERM, &sa, nullptr);
    ld_check(rv == 0);
    rv = sigaction(SIGINT, &sa, nullptr);
    ld_check(rv == 0);
  }

  std::unique_ptr<Request> rq = std::make_unique<
      StartReplicatedStateMachineRequest<EventLogStateMachine>>(
      event_log.get());
  client_impl->getProcessor().postWithRetrying(rq);

  bool sent_abort = false;
  while (true) {
    if (event_log->wait(client_impl->getTimeout())) {
      // EventLogStateMachine reached the tail or was successfully aborted or
      // timed out.
      break;
    } else if ((aborted || (err == E::TIMEDOUT && !tail)) && !sent_abort) {
      // if we are tailing and the user aborted with a signal or we are not
      // tailing and wait() timed out, then send a stop request and give up.
      std::unique_ptr<Request> req = std::make_unique<
          StopReplicatedStateMachineRequest<EventLogStateMachine>>(
          event_log.get());
      client_impl->getProcessor().postWithRetrying(req);
      sent_abort = true;
    }
  }

  set = event_log->getCurrentRebuildingSet();
  if (err == E::TIMEDOUT) {
    RATELIMIT_ERROR(
        std::chrono::seconds(1),
        1,
        "Timed out(%lums) while fetching EventLog rebuilding set: %s",
        client_impl->getTimeout().count(),
        set.toString().c_str());
  }

  Semaphore destroy_sem;
  std::unique_ptr<Request> req = std::make_unique<
      DestroyReplicatedStateMachineRequest<EventLogStateMachine>>(
      event_log.release(), [&destroy_sem]() { destroy_sem.post(); });
  client_impl->getProcessor().postWithRetrying(req);
  destroy_sem.wait();
  // if we are not tailing, and got a timed out, we return an error to
  // the caller. any other case is considered a success.
  return (sent_abort && !tail) ? -1 : 0;
}

int getShardAuthoritativeStatusMap(Client& client,
                                   ShardAuthoritativeStatusMap& map) {
  EventLogRebuildingSet set;
  const int rv = getRebuildingSet(client, set);
  if (rv != 0) {
    // err set by getRebuildingSet.
    return rv;
  }

  // Hold a shared_ptr reference to the current ServerConfig so it cannot
  // change while we update the map.
  std::shared_ptr<ServerConfig> server_config =
      dynamic_cast<ClientImpl*>(&client)->getConfig()->get()->serverConfig();
  map = set.toShardStatusMap(server_config->getNodes());
  return 0;
}

bool canWipeShardsWithoutCausingDataLoss(
    Client& client,
    const ShardAuthoritativeStatusMap& map,
    std::vector<std::pair<node_index_t, uint32_t>> shards,
    ReplicationProperty min_replication) {
  ClientImpl* client_impl = dynamic_cast<ClientImpl*>(&client);
  ld_check(client_impl);

  auto config = client_impl->getConfig()->get();

  std::unordered_map<uint32_t, std::unordered_set<node_index_t>> shard_map;
  for (auto p : shards) {
    shard_map[p.second].insert(p.first);
  }

  for (const auto& p : shard_map) {
    uint32_t shard = p.first;
    const auto& nodes_to_wipe_in_that_shard = p.second;
    std::vector<node_index_t> other_nodes_already_underreplicated;

    std::vector<ShardID> storage_set;
    for (const auto& k : config->serverConfig()->getNodes()) {
      if (shard < k.second.getNumShards()) {
        storage_set.push_back(ShardID(k.first, shard));
      }
    }

    ld_info("Checking if there would be data loss in shard %u...", shard);

    FailureDomainNodeSet<bool> failure_domain_info(
        storage_set, config->serverConfig(), min_replication);

    for (ShardID s : storage_set) {
      AuthoritativeStatus status = map.getShardStatus(s.node(), s.shard());
      bool is_underreplicated =
          status == AuthoritativeStatus::UNDERREPLICATION ||
          status == AuthoritativeStatus::UNAVAILABLE;
      failure_domain_info.setShardAttribute(s, is_underreplicated);
      if (is_underreplicated) {
        if (nodes_to_wipe_in_that_shard.count(s.node())) {
          ld_warning("%s is already underreplicated", s.toString().c_str());
        } else {
          other_nodes_already_underreplicated.push_back(s.node());
        }
      }
    }

    for (node_index_t node_id : nodes_to_wipe_in_that_shard) {
      failure_domain_info.setShardAttribute(ShardID(node_id, shard), true);
    }

    // If we can replicate on the set of nodes known as underreplicated
    // according to replication factor and failure domain properties, this means
    // wiping (nid, shard) would cause data loss.
    const bool wipe_would_cause_dataloss =
        failure_domain_info.canReplicate(true);

    if (wipe_would_cause_dataloss) {
      ld_error("Wiping shard %u of nodes %s would cause data loss considering "
               "the following other nodes already have their shard %u "
               "underreplicated: %s",
               shard,
               toString(nodes_to_wipe_in_that_shard).c_str(),
               shard,
               toString(other_nodes_already_underreplicated).c_str());
      return false;
    }
  }

  return true;
}

Status trim(Client& client, std::chrono::milliseconds timestamp) {
  Semaphore sem;
  Status res;

  auto cb = [&](Status st) {
    res = st;
    sem.post();
  };

  logid_t delta_log_id = configuration::InternalLogs::EVENT_LOG_DELTAS;
  logid_t snapshot_log_id = configuration::InternalLogs::EVENT_LOG_SNAPSHOTS;

  ClientImpl* client_impl = dynamic_cast<ClientImpl*>(&client);
  ld_check(client_impl);

  std::unique_ptr<Request> rq =
      std::make_unique<TrimRSMRequest>(delta_log_id,
                                       snapshot_log_id,
                                       timestamp,
                                       cb,
                                       worker_id_t{0},
                                       WorkerType::GENERAL,
                                       RSMType::EVENT_LOG_STATE_MACHINE,
                                       false, /* trim_everthing = false */
                                       client_impl->getTimeout(),
                                       client_impl->getTimeout());

  client_impl->getProcessor().postWithRetrying(rq);

  sem.wait();
  return res;
}

}}} // namespace facebook::logdevice::EventLogUtils
