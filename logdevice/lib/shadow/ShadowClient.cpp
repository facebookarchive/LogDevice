/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/lib/shadow/ShadowClient.h"

#include <functional>

#include "logdevice/common/AppendRequest.h"
#include "logdevice/common/ThreadID.h"
#include "logdevice/common/util.h"
#include "logdevice/include/ClientSettings.h"
#include "logdevice/lib/ClientImpl.h"

using namespace std::chrono_literals;

namespace facebook { namespace logdevice {

ShadowClientFactory::ShadowClientFactory(
    std::string origin_name,
    StatsHolder* stats,
    UpdateableSettings<Settings> client_settings)
    : origin_name_(origin_name),
      stats_(stats),
      client_settings_(client_settings) {}

ShadowClientFactory::~ShadowClientFactory() {
  shutdown();
}

void ShadowClientFactory::start(std::chrono::milliseconds client_timeout) {
  client_timeout_ = client_timeout;
  client_init_thread_ =
      std::thread(&ShadowClientFactory::clientInitThreadMain, this);
}

void ShadowClientFactory::shutdown() {
  if (client_init_thread_.joinable()) {
    ld_debug(LD_SHADOW_PREFIX "Shutting down client initialization thread");
    std::unique_lock<Mutex> client_init_lock(client_init_mutex_);
    shutdown_ = true;
    client_init_lock.unlock();
    client_init_cv_.notify_one();
    client_init_thread_.join();
  }
}

std::shared_ptr<ShadowClient>
ShadowClientFactory::get(const std::string& destination) const {
  // std::lock_guard<Mutex> client_lock(client_mutex_);
  auto locked_map = client_map_.rlock();
  auto found = locked_map->find(destination);
  if (found != locked_map->end()) {
    return found->second;
  } else {
    return nullptr;
  }
}

int ShadowClientFactory::createAsync(const Shadow::Attrs& attrs,
                                     bool is_a_retry) {
  std::unique_lock<Mutex> client_init_lock(client_init_mutex_);
  if (is_a_retry) {
    // If an append failed because we had failed to create the shadow
    // client then it enqueues a request to retry the creation. The
    // request is kept in a map for shadow_client_creation_retry_interval
    // seconds and then retried.
    // All duplicate requests in this interval are deduped by the map.
    // The request is thrown away after one retry so that only actively
    // failing shadow attempts are retried and not (somehow) stale entries.
    if ((retry_map_.find(attrs->destination()) == retry_map_.end())) {
      retry_map_[attrs->destination()] = attrs;
    }
  } else {
    client_init_queue_.push(attrs);
    client_init_lock.unlock();
    client_init_cv_.notify_one();
  }
  return 0;
}

void ShadowClientFactory::reset() {
  shutdown();

  std::unique_lock<Mutex> client_init_lock(client_init_mutex_);
  // std::queue<Shadow::Attrs>().swap(client_init_queue_);
  client_init_queue_ = std::queue<Shadow::Attrs>{};
  client_init_retry_queue_ = std::queue<Shadow::Attrs>{};
}

void ShadowClientFactory::clientInitThreadMain() {
  ThreadID::set(ThreadID::UTILITY, "shadow:U0");
  ld_debug(LD_SHADOW_PREFIX "Started client initialization thread");
  SteadyTimestamp lastRetry = SteadyTimestamp::now();
  while (true) {
    auto retryPeriod = client_settings_->shadow_client_creation_retry_interval;
    std::unique_lock<Mutex> client_init_lock(client_init_mutex_);
    while (client_init_queue_.empty() && client_init_retry_queue_.empty() &&
           !shutdown_) {
      client_init_cv_.wait_for(client_init_lock, retryPeriod);

      // If it's time to retry then move the retry entries to the main queue
      auto waiting = std::chrono::duration_cast<std::chrono::seconds>(
          SteadyTimestamp::now() - lastRetry);
      if (waiting >= retryPeriod) {
        lastRetry = SteadyTimestamp::now();
        std::unordered_map<std::string, Shadow::Attrs>::iterator it =
            retry_map_.begin();
        while (it != retry_map_.end()) {
          client_init_retry_queue_.push(it->second);
          it = retry_map_.erase(it);
          STAT_INCR(stats_, client.shadow_client_load_retry);
        }
      }
    }
    if (shutdown_) {
      break;
    }

    Shadow::Attrs attrs;
    bool retry = false;
    if (!client_init_queue_.empty()) {
      attrs = client_init_queue_.front();
      client_init_queue_.pop();
    } else {
      attrs = client_init_retry_queue_.front();
      client_init_retry_queue_.pop();
      retry = true;
    }
    client_init_lock.unlock();

    // Only one thread will be writing to the map
    const std::string& destination = attrs->destination();
    bool found = client_map_.withRLock(
        [&](const auto& map) { return map.find(destination) != map.end(); });

    // A small retry period implies a test. In the test mode we
    // only allow the retry path to create the client.
    if (found || (!retry && retryPeriod.count() == 1)) {
      continue;
    }

    ld_debug(LD_SHADOW_PREFIX "Initializing client for '%s'",
             attrs->destination().c_str());
    ld_check(client_timeout_.count() > 0);
    std::shared_ptr<ShadowClient> shadow_client =
        ShadowClient::create(origin_name_, attrs, client_timeout_, stats_);
    if (shadow_client == nullptr) {
      // TODO scuba detailed stats T20416930 about which shadow and error code
      if (retry) {
        STAT_INCR(stats_, client.shadow_client_init_retry_failed);
      } else {
        STAT_INCR(stats_, client.shadow_client_init_failed);
      }
      ld_error(LD_SHADOW_PREFIX
               "Failed to initialize shadow client for '%s' with error '%s'",
               destination.c_str(),
               error_description(err));
      continue;
    }
    ld_debug(
        LD_SHADOW_PREFIX "Client for '%s' initialized", destination.c_str());
    client_map_.withWLock([&](auto& map) { map[destination] = shadow_client; });
    if (retry) {
      STAT_INCR(stats_, client.shadow_client_init_retry_success);
    }
  }
  ld_debug(LD_SHADOW_PREFIX "Client initialization thread finished");
}

std::shared_ptr<ShadowClient>
ShadowClient::create(const std::string& origin_name,
                     const Shadow::Attrs& attrs,
                     std::chrono::milliseconds timeout,
                     StatsHolder* stats) {
  std::string shadow_name(origin_name + ".shadow:" + attrs->destination());
  std::shared_ptr<Client> client =
      ClientFactory()
          .setSetting("shadow-client", "true")
          // In case the default is changed in the future
          .setSetting("on-demand-logs-config", "false")
          // Epoch metadata cache is used for reading, not necessary here
          .setSetting("client-epoch-metadata-cache-size", "0")
          // Don't want to pollute traces with shadow data - TODO not sure about
          // this
          .setSetting("disable-trace-logger", "true")
          .setClusterName(shadow_name)
          .setTimeout(timeout)
          .create(attrs->destination());
  if (client == nullptr) {
    return nullptr;
  }

  return std::shared_ptr<ShadowClient>{new ShadowClient(client, attrs, stats)};
}

ShadowClient::ShadowClient(std::shared_ptr<Client> client,
                           const Shadow::Attrs& attrs,
                           StatsHolder* stats)
    : client_(std::move(client)), shadow_attrs_(attrs), stats_(stats) {}

ShadowClient::~ShadowClient() {}

int ShadowClient::append(logid_t logid,
                         const Payload& payload,
                         AppendAttributes attrs,
                         bool buffered_writer_blob) noexcept {
  auto callback = [&](auto a, const auto& b) { this->appendCallback(a, b); };

  // Need to copy payload, since it is technically owned by the client
  // This will likely be a performance impact, so care should be taken
  // to keep the ratio low and only enable shadowing on clients that
  // can handle the impact (TODO better alternative t19772899)
  Payload payload_copy;
  try {
    payload_copy = payload.dup();
  } catch (const std::bad_alloc& e) {
    // TODO scuba detailed stats T20416930 about which origin and shadow
    STAT_INCR(stats_, client.shadow_payload_alloc_failed);
    ld_warning(LD_SHADOW_PREFIX
               "Failed to allocate memory for duplicating shadow payload");
    err = E::NOMEM;
    return -1;
  }

  ld_spew(LD_SHADOW_PREFIX "Shadowing payload of size %zu to shadow '%s'",
          payload_copy.size(),
          shadow_attrs_->destination().c_str()); // TODO replace with stats

  // Downcast client in order to use lower level API. The reason is we need
  // to be able to alter append request flags to match those of the original
  // request. In particular, we need to propage the BUFFERED_WRITER_BLOB
  // flag so readers can detect buffered writer batches and unpack them.
  ClientImpl* client_impl = checked_downcast<ClientImpl*>(client_.get());
  int rv = -1;
  auto req = client_impl->prepareRequest(
      logid, payload_copy, callback, attrs, worker_id_t{-1}, nullptr);
  if (req) {
    if (buffered_writer_blob) {
      req->setBufferedWriterBlobFlag();
    }
    rv = client_impl->postAppend(std::move(req));
  }

  if (rv == -1) {
    // Payload was created via Payload.dup() which uses malloc()
    free(const_cast<void*>(payload_copy.data()));
    RATELIMIT_WARNING(1s,
                      1,
                      LD_SHADOW_PREFIX "Shadow append failed with '%s'",
                      error_description(err));
  }
  return rv;
}

void ShadowClient::appendCallback(Status status, const DataRecord& record) {
  ld_spew(LD_SHADOW_PREFIX "Shadow append finished with lsn=%s",
          lsn_to_string(record.attrs.lsn).c_str());
  if (status == E::OK) {
    // TODO detailed scuba stats T20416930
    STAT_INCR(stats_, client.shadow_append_success);
  } else {
    // TODO detailed scuba stats T20416930 including error code
    STAT_INCR(stats_, client.shadow_append_failed);
    RATELIMIT_WARNING(1s,
                      1,
                      LD_SHADOW_PREFIX
                      "Shadow append to logid %lu failed with '%s'",
                      record.logid.val(),
                      error_description(status));
  }

  // Payload was created via Payload.dup() which uses malloc()
  free(const_cast<void*>(record.payload.data()));
}

}} // namespace facebook::logdevice
