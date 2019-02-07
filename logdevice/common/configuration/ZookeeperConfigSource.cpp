/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/configuration/ZookeeperConfigSource.h"

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <queue>
#include <thread>

#include <boost/multi_index/composite_key.hpp>
#include <boost/multi_index/hashed_index.hpp>
#include <boost/multi_index/identity.hpp>
#include <boost/multi_index/mem_fun.hpp>
#include <boost/multi_index/ordered_index.hpp>
#include <boost/multi_index_container.hpp>
#include <folly/MapUtil.h>
#include <folly/Random.h>

#include "logdevice/common/ConstructorFailed.h"
#include "logdevice/common/ThreadID.h"
#include "logdevice/common/ZookeeperClient.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/TextConfigUpdater.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/plugin/ZookeeperClientFactory.h"
#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice {

using std::chrono::steady_clock;

class ZookeeperConfigSource::BackgroundThread {
 public:
  explicit BackgroundThread(std::chrono::milliseconds max_delay)
      : max_delay_(max_delay) {
    thread_ = std::thread(&BackgroundThread::run, this);
  }

  ~BackgroundThread() {
    stop();
  }

  void requestWithDelay(std::unique_ptr<const RequestContext> context) {
    std::chrono::milliseconds delay(
        folly::Random::rand64(0.75 * max_delay_.count(), max_delay_.count()));
    ld_debug(context->with_data ? "Will fetch %s in %ld ms"
                                : "Will check %s for changes in %ld ms",
             context->path.c_str(),
             delay.count());
    {
      std::unique_lock<std::mutex> lock(bg_mutex_);
      auto insert_result =
          tasks_.insert(Task{steady_clock::now() + delay, std::move(context)});
      if (!insert_result.second) {
        // There was already a pending request for this quorum path, didn't add
        // another.
        return;
      }
    }
    cv_.notify_one();
  }

  void stop() {
    if (!stop_.exchange(true)) {
      {
        std::unique_lock<std::mutex> lock(bg_mutex_);
      }
      cv_.notify_one();
      thread_.join();
    }
  }

 private:
  std::chrono::milliseconds max_delay_;

  // Protects tasks_. ZookeeperConfigSource::mutex_ and
  // BackgroundThread::bg_mutex_ are never held together by the same thread.
  // The general rule is that methods in this file are called with
  // no mutex held.
  std::mutex bg_mutex_;

  std::condition_variable cv_;
  struct Task {
    steady_clock::time_point when;
    std::unique_ptr<const RequestContext> context;
    bool operator<(const Task& task) const {
      return when < task.when;
    }
    std::string getQuorum() const {
      return context->quorum;
    }
    std::string getPath() const {
      return context->path;
    }
    bool withData() const {
      return context->with_data;
    }
  };
  // A container for Tasks ordered by their scheduled time. It ensures that no
  // two tasks with the same quorum, path, and with_data are in the container
  // at the same time.
  struct OrderedIndex {};
  boost::multi_index::multi_index_container<
      Task,
      boost::multi_index::indexed_by<
          boost::multi_index::ordered_non_unique<
              boost::multi_index::tag<OrderedIndex>,
              boost::multi_index::identity<Task>>,
          boost::multi_index::hashed_unique<boost::multi_index::composite_key<
              Task,
              boost::multi_index::
                  const_mem_fun<Task, std::string, &Task::getQuorum>,
              boost::multi_index::
                  const_mem_fun<Task, std::string, &Task::getPath>,
              boost::multi_index::const_mem_fun<Task, bool, &Task::withData>>>>>
      tasks_;
  std::atomic<bool> stop_{false};
  std::thread thread_;

  void run() {
    ThreadID::set(ThreadID::Type::UTILITY, "ld:zookeeper");
    std::unique_lock<std::mutex> lock(bg_mutex_);
    while (!stop_.load()) {
      auto& index = tasks_.get<OrderedIndex>();
      auto first_ready = [&]() {
        return !index.empty() && index.begin()->when <= steady_clock::now();
      };
      if (tasks_.empty()) {
        cv_.wait(lock);
      } else if (!first_ready()) {
        cv_.wait_until(lock, index.begin()->when);
      }

      while (!stop_.load() && first_ready()) {
        std::unique_ptr<const RequestContext> context =
            index.begin()->context->clone();
        index.erase(index.begin());
        // Avoid calling requestZnode() with locked bg_mutex_.
        // requestZnode() may call requestWithDelay() which locks bg_mutex_.
        lock.unlock();
        context->parent->requestZnode(
            context->quorum, context->path, context->with_data);
        lock.lock();
      }
    }
  }
};

ZookeeperConfigSource::ZookeeperConfigSource(
    std::chrono::milliseconds polling_delay,
    std::shared_ptr<ZookeeperClientFactory> zookeeper_client_factory,
    std::string uri_scheme)
    : polling_delay_(polling_delay),
      zookeeper_client_factory_(std::move(zookeeper_client_factory)),
      uri_scheme_(uri_scheme) {}

Status ZookeeperConfigSource::getConfig(const std::string& quorum_path,
                                        Output* /* out */) {
  size_t pos = quorum_path.find('/');
  if (pos == std::string::npos) {
    ld_error("ZookeeperConfigSource path parameter must include quorum and "
             "path, e.g. \"%s\".  Received \"%s\"",
             "1.2.3.4:2181,5.6.7.8:2181,9.10.11.12:2181/path/to/node",
             quorum_path.c_str());
    return E::INVALID_PARAM;
  }
  std::string quorum = quorum_path.substr(0, pos);
  std::string path = quorum_path.substr(pos);

  int rv = requestZnode(quorum, path, /* with_data */ true);
  return rv == 0 ? E::NOTREADY : err;
}

int ZookeeperConfigSource::requestZnode(const std::string& quorum,
                                        const std::string& path,
                                        const bool with_data) {
  ZookeeperClientBase* zkclient = getClient(quorum);
  if (zkclient == nullptr) {
    return -1;
  }

  std::unique_ptr<RequestContext> ctx(
      new RequestContext(this, quorum, path, with_data));
  {
    std::lock_guard<std::mutex> guard(mutex_);
    requests_in_flight_.push_back(*ctx);
  }

  // Callbacks bind to `this'; destructor disarms all callbacks so this is
  // safe.
  if (with_data) {
    zkclient->getData(
        path, [ctx = ctx.release()](int rc, std::string value, zk::Stat stat) {
          struct ::Stat st;
          st.version = stat.version_;
          dataCompletionCallback(rc, value.c_str(), value.length(), &st, ctx);
        });

  } else {
    zkclient->exists(path, [ctx = ctx.release()](int rc, zk::Stat stat) {
      struct ::Stat st;
      st.version = stat.version_;
      statCompletionCallback(rc, &st, ctx);
    });
  }

  return 0;
}

ZookeeperClientBase*
ZookeeperConfigSource::getClient(const std::string& quorum) {
  ZookeeperConfig zkcfg(
      quorum, uri_scheme_, std::chrono::seconds(ZK_SESSION_TIMEOUT_SEC));

  std::lock_guard<std::mutex> guard(mutex_);
  auto it = zkclients_.find(quorum);
  if (it == zkclients_.end()) {
    try {
      auto insert_result = zkclients_.emplace(
          quorum, zookeeper_client_factory_->getClient(zkcfg));
      it = insert_result.first;
    } catch (const ConstructorFailed&) {
      ld_info("ZookeeperClient constructor failed with %s", error_name(err));
      return nullptr;
    }
  }
  return it->second.get();
}

void ZookeeperConfigSource::dataCompletionCallback(int rc,
                                                   const char* value,
                                                   int value_len,
                                                   const struct ::Stat* stat,
                                                   const void* context_void) {
  std::unique_ptr<RequestContext> context(
      static_cast<RequestContext*>(const_cast<void*>(context_void)));
  ld_debug("path=%s rc=%d version=%d len=%d",
           context->path.c_str(),
           rc,
           stat != nullptr ? stat->version : -1,
           value_len);

  ZookeeperConfigSource* self = context->parent;
  {
    std::lock_guard<std::mutex> guard(self->mutex_);
    context->list_hook.unlink(); // from `requests_in_flight_'
  };

  if (rc == ZBADARGUMENTS) {
    err = E::INVALID_PARAM;
    ld_critical(
        "getData() call to Zookeeper failed: %s", error_description(err));
    Output out;
    self->async_cb_->onAsyncGet(self, context->quorum, err, out);
    return;
  }

  if (rc != 0) {
    RATELIMIT_ERROR(
        std::chrono::seconds(1),
        1,
        "zoo_aget(%s) asynchronously failed with error code %d.  Scheduling "
        "retry.",
        context->path.c_str(),
        rc);
    // Retry later
    ld_check(context->with_data);
    self->bgThread().requestWithDelay(std::move(context));
    return;
  }

  {
    std::lock_guard<std::mutex> guard(self->mutex_);
    // Record the delivered version for polling
    self->delivered_versions_[context->quorum + context->path] = stat->version;
  }
  Output out;
  out.contents = std::string(value, value_len);
  // Zookeeper's mtime is in milliseconds
  out.mtime = std::chrono::milliseconds(stat->mtime);
  // `out.hash' stays clear, we don't have a good hash to supply
  self->async_cb_->onAsyncGet(
      self, context->quorum + context->path, E::OK, std::move(out));
  // Kick off polling (schedule a light version check)
  context->with_data = false;
  self->bgThread().requestWithDelay(std::move(context));
}

void ZookeeperConfigSource::statCompletionCallback(int rc,
                                                   const struct ::Stat* stat,
                                                   const void* context_void) {
  std::unique_ptr<RequestContext> context(
      static_cast<RequestContext*>(const_cast<void*>(context_void)));
  ld_debug("path=%s rc=%d version=%d",
           context->path.c_str(),
           rc,
           stat != nullptr ? stat->version : -1);

  ZookeeperConfigSource* self = context->parent;
  int64_t delivered_version;
  {
    std::lock_guard<std::mutex> guard(self->mutex_);
    context->list_hook.unlink(); // from `requests_in_flight_'
    delivered_version = folly::get_default(
        self->delivered_versions_, context->quorum + context->path, -1);
  };

  if (rc == ZBADARGUMENTS) {
    err = E::INVALID_PARAM;
    ld_critical(
        "exists() call to Zookeeper failed: %s", error_description(err));
    return;
  }

  if (rc != 0) {
    RATELIMIT_ERROR(
        std::chrono::seconds(1),
        1,
        "zoo_aexists(%s) asynchronously failed with error code %d.  Scheduling "
        "retry.",
        context->path.c_str(),
        rc);
    // Retry later
    ld_check(!context->with_data);
    self->bgThread().requestWithDelay(std::move(context));
    return;
  }

  if (stat->version != delivered_version) {
    ld_info("Polling detected change to znode %s (new version %d,"
            " last delivered %ld), requesting data",
            context->path.c_str(),
            stat->version,
            delivered_version);
    self->requestZnode(context->quorum, context->path, /* with_data */ true);
  } else {
    ld_debug("Already delivered version %ld", delivered_version);
    // Continue polling
    ld_check(!context->with_data);
    self->bgThread().requestWithDelay(std::move(context));
  }
}

ZookeeperConfigSource::~ZookeeperConfigSource() {
  if (bg_thread_) {
    bg_thread_->stop();
  }

  // Tear down the ZK clients to ensure callbacks are disarmed before starting
  // to tear down members
  zkclients_.clear();

  requests_in_flight_.clear_and_dispose(std::default_delete<RequestContext>());
}

ZookeeperConfigSource::BackgroundThread& ZookeeperConfigSource::bgThread() {
  std::lock_guard<std::mutex> guard(mutex_);
  if (!bg_thread_) {
    ld_debug("Initializing BackgroundThread");
    bg_thread_ = std::make_unique<BackgroundThread>(polling_delay_);
  }
  return *bg_thread_;
}

}} // namespace facebook::logdevice
