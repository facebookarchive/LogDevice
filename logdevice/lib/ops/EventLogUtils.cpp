/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/lib/ops/EventLogUtils.h"

#include <signal.h>
#include <unordered_map>
#include <unordered_set>

#include "logdevice/common/FailureDomainNodeSet.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/ReaderImpl.h"
#include "logdevice/common/ThreadID.h"
#include "logdevice/common/configuration/InternalLogs.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/event_log/EventLogStateMachine.h"
#include "logdevice/common/replicated_state_machine/ReplicatedStateMachine-enum.h"
#include "logdevice/common/replicated_state_machine/TrimRSMRequest.h"
#include "logdevice/lib/ClientImpl.h"
#include "logdevice/lib/ClientSettingsImpl.h"

namespace facebook { namespace logdevice { namespace EventLogUtils {

std::atomic<bool> stop_requested_by_signal{false};

int tailEventLog(
    Client& client,
    EventLogRebuildingSet* set,
    std::function<
        bool(const EventLogRebuildingSet&, const EventLogRecord*, lsn_t)> cb,
    std::chrono::milliseconds timeout,
    bool stop_at_tail,
    bool stop_on_signal) {
  ClientImpl* client_impl = dynamic_cast<ClientImpl*>(&client);
  ld_check(client_impl);

  ClientSettingsImpl* client_settings =
      dynamic_cast<ClientSettingsImpl*>(&client_impl->settings());
  ld_check(client_settings);

  ld_check(!ThreadID::isWorker());

  auto event_log =
      std::make_unique<EventLogStateMachine>(client_settings->getSettings());

  if (stop_at_tail) {
    event_log->stopAtTail();
  }
  if (stop_on_signal) {
    // The user can stop reading by sending SIGTERM or SIGINT
    struct sigaction sa;
    sa.sa_handler = [](int /* sig */) { stop_requested_by_signal.store(true); };
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    int rv = sigaction(SIGTERM, &sa, nullptr);
    ld_check(rv == 0);
    rv = sigaction(SIGINT, &sa, nullptr);
    ld_check(rv == 0);
  }

  std::atomic<bool> stop_requested_by_cb{false};
  auto subscription = event_log->subscribe([&](const EventLogRebuildingSet& s,
                                               const EventLogRecord* delta,
                                               lsn_t version) {
    if (!cb(s, delta, version)) {
      // Ask the main thread to post a StopReplicatedStateMachineRequest.
      // Note that we can't directly call stop() here (because ClientReadStream
      // doesn't like it), and can't directly post
      // StopReplicatedStateMachineRequest here (because main thread also posts
      // the request when getting a signal, and duplicate
      // StopReplicatedStateMachineRequest-s are not allowed).
      stop_requested_by_cb.store(true);
    }
  });

  std::unique_ptr<Request> rq = std::make_unique<
      StartReplicatedStateMachineRequest<EventLogStateMachine>>(
      event_log.get());
  client_impl->getProcessor().postWithRetrying(rq);

  bool timed_out = false;
  bool stopped = false;
  SteadyTimestamp start_time = SteadyTimestamp::now();
  while (true) {
    if (event_log->wait(std::chrono::milliseconds(10))) {
      // EventLogStateMachine reached the tail.
      stopped = true;
      break;
    }

    if (stop_requested_by_signal.load() || stop_requested_by_cb.load()) {
      break;
    }

    if (std::chrono::duration_cast<std::chrono::milliseconds>(
            SteadyTimestamp::now() - start_time) >= timeout) {
      timed_out = true;
      break;
    }
  }

  if (!stopped) {
    // If timeout expired or reading was aborted by callback or signal, send
    // a stop request.
    std::unique_ptr<Request> req = std::make_unique<
        StopReplicatedStateMachineRequest<EventLogStateMachine>>(
        event_log.get());
    client_impl->getProcessor().postWithRetrying(req);
    // Wait for the stop request to come through.
    bool rv = event_log->wait(std::chrono::milliseconds::max());
    ld_check(rv);
  }
  subscription.reset();

  if (set != nullptr) {
    *set = event_log->getCurrentRebuildingSet();
  }
  if (timed_out) {
    RATELIMIT_ERROR(
        std::chrono::seconds(1),
        1,
        "Timed out(%lums) while fetching EventLog rebuilding set: %s",
        timeout.count(),
        event_log->getCurrentRebuildingSet().toString().c_str());
  }

  Semaphore destroy_sem;
  std::unique_ptr<Request> req = std::make_unique<
      DestroyReplicatedStateMachineRequest<EventLogStateMachine>>(
      event_log.release(), [&destroy_sem]() { destroy_sem.post(); });
  client_impl->getProcessor().postWithRetrying(req);
  destroy_sem.wait();
  if (timed_out) {
    err = E::TIMEDOUT;
    return -1;
  }
  return 0;
}

int getRebuildingSet(Client& client, EventLogRebuildingSet& set) {
  ClientImpl* client_impl = dynamic_cast<ClientImpl*>(&client);
  ld_check(client_impl);
  return tailEventLog(
      client,
      &set,
      [](const EventLogRebuildingSet&, const EventLogRecord*, lsn_t) {
        return true;
      },
      client_impl->getTimeout(),
      /* stop_at_tail */ true);
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
  map = set.toShardStatusMap(
      *server_config->getNodesConfigurationFromServerConfigSource());
  return 0;
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
