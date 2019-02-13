/**
 * Copyright (c) 2018-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <iomanip>
#include <iostream>
#include <memory>
#include <mutex>
#include <pthread.h>
#include <sstream>
#include <vector>

#include <folly/IntrusiveList.h>
#include <logdevice/common/Timestamp.h>
#include <logdevice/common/debug.h>
#include <logdevice/common/stats/Stats.h>
#include <logdevice/common/util.h>

/**
 * @file A general purpose scheduler for request based and byte based
 * scheduling. It can be used for both networks and disks.
 *
 * This scheduler implements a version of Deficit Round Robin scheduler. I used
 * DRR because it is an efficient O(1) scheduler.
 *
 * A proportional scheduler is useful for weight-based scheduling bwtween the
 * various principals which allows scheduling without starvation. This can help
 * to keep all the queues moving, preventing timeouts and retries. By tuning the
 * weights, and chosing appropriate 'quanta' the actual cost of disk IOS can be
 * reduced. The weights help to prioritize important priciples.
 *
 * The schduler can also be helpful in meeting SLA's around latency and
 * throughput which effectively helps to implement isolation or 'Quality Of
 * Service'.
 *
 * Another example on the networking side is that besides getting proportional
 * scheduling between different streams or Principals, this can be effective in
 * making sure that small requests (like acks and replies) are not inordinately
 * delayed behind large IO requests. So we get better defined latencies.
 *
 * Each IO principal gets a queue along with a share. In the request based
 * version the queues are scheduled proportional to their share, assuming that
 * each request is of size 1. In the byte-based version the size of the request
 * is taken into account and we get proportionally different throughput per
 * queue.
 *
 * In addition, there is the concept of 'credit'. In the byte-based scheduling
 * version a request may not actually use all the bytes it was charged. So
 * the credit() interface allows us to credit back the unused bytes. But the
 * cost of disk IO requests are not completely byte based -- there is fixed seek
 * cost. See comments on top of the 'credit' interface.
 *
 * The actual number of requests in flight is determined by the number of
 * threads processing the queues. In general, there is better control on
 * scheduling if there is a limited amount of IO in flight. Ideally, only as
 * many IOs in flight as is necessry to max out the underlying resource (n/w or
 * disk).
 *
 * The underlying queue uses the folly::IntrusiveList. Since no memory
 * allocation occurs  during enq/deq, the scheduler overhead should be very low.
 *
 * The shares of the queues, or the mode of operation (req based or byte based)
 * can be manually changed from the settings file.
 *
 */
namespace facebook { namespace logdevice {

struct DRRPrincipal {
  std::string name;
  uint64_t share;
};

class DRRStats {
 public:
  DRRStats(DRRPrincipal p)
      : principal(p), reqsProcessed(0), bytesProcessed(0) {}

  void resetStats() {
    reqsProcessed = 0;
    bytesProcessed = 0;
  }

  DRRPrincipal principal;
  uint64_t reqsProcessed;
  uint64_t bytesProcessed;
};

class DRRStatsSnapshot {
 public:
  /*
   * returns a formatted string of stats and reset the stats.
   */
  std::string toString() {
    std::ostringstream s;
    s << std::endl;
    double secs = duration.count();
    if (secs) {
      double totalReqs = 0;
      double totalBytes = 0;
      for (const auto& stat : perfStats) {
        totalReqs += stat.reqsProcessed;
        totalBytes += stat.bytesProcessed;
        double reqsPerSec = stat.reqsProcessed / secs;
        double bytesPerSec = stat.bytesProcessed / secs;
        double normReqs = reqsPerSec / stat.principal.share;
        double normBytes = bytesPerSec / stat.principal.share;
        s << "name= " << std::setw(15) << stat.principal.name
          << ": share= " << std::setw(2) << stat.principal.share
          << ", secs= " << secs << ", reqs/sec= " << std::setw(7)
          << std::setprecision(2) << reqsPerSec
          << ", bytes/sec= " << std::setw(7) << std::setprecision(2)
          << bytesPerSec << ", norm= " << std::setw(7) << std::setprecision(2)
          << normReqs << ",  " << std::setw(7) << std::setprecision(2)
          << normBytes << std::endl;
      }
      s << "total-reqs/sec: " << totalReqs / secs
        << "total-bytes/sec: " << totalBytes / secs << std::endl;
    }
    return s.str();
  }

  std::chrono::seconds duration;
  std::vector<DRRStats> perfStats;
};

template <class T, folly::IntrusiveListHook T::*Link>
class DRRScheduler {
 public:
  DRRScheduler() {}

  ~DRRScheduler() {}

  class DRRQueue {
   public:
    DRRQueue(DRRPrincipal p, uint64_t quanta)
        : stats_(p), deficit_(p.share * quanta), numReqs_(0) {}

    ~DRRQueue() {}

    DRRStats stats_;
    uint64_t deficit_;
    uint64_t numReqs_;

    // Actual queue of requests.
    folly::IntrusiveList<T, Link> q_;
  };

  /*
   * Interface for initializing the per-principal shares for the scheduler.
   */
  void initShares(std::string name,
                  uint64_t quanta,
                  std::vector<DRRPrincipal> principals) {
    name_ = name;
    // Initialize all the internal DRR queues
    quanta_ = quanta;
    next_ = 0;
    numReqs_ = 0;
    statsStart_ = SteadyTimestamp::now();

    for (const auto& p : principals) {
      ld_check(p.share);
      queues_.emplace_back(std::make_shared<DRRQueue>(p, quanta));
    }
  }

  /*
   * Interface for resetting shares and quanta.
   */
  void setShares(uint64_t quanta, std::vector<DRRPrincipal> principals) {
    if (!quanta) {
      ld_info("failed to reset quanta from %ld to %ld", quanta_, quanta);
      return;
    }
    std::unique_lock<std::mutex> lock(mutex_);
    if (quanta_ != quanta) {
      ld_info("resetting quanta from %ld to %ld", quanta_, quanta);
    }
    quanta_ = quanta;

    uint64_t ind = 0;
    for (const auto& p : principals) {
      DRRQueue* q = queues_[ind].get();
      ld_check(p.name.compare(q->stats_.principal.name) == 0);
      if (p.share) {
        if (q->stats_.principal.share != p.share) {
          ld_info("resetting share for principal %s from %ld to %ld",
                  p.name.c_str(),
                  q->stats_.principal.share,
                  p.share);
        }
        q->stats_.principal.share = p.share;
      } else {
        ld_info("failed to reset share for principal %s from %ld to %ld",
                p.name.c_str(),
                q->stats_.principal.share,
                p.share);
      }
      ind++;
    }
  }

  /*
   * Return current stats and reset the local values.
   */
  void getAndResetStats(DRRStatsSnapshot& stats) {
    stats.perfStats.clear();
    stats.perfStats.reserve(queues_.size());

    std::unique_lock<std::mutex> lock(mutex_);
    stats.duration = std::chrono::duration_cast<std::chrono::seconds>(
        SteadyTimestamp::now() - statsStart_);
    for (auto& q : queues_) {
      stats.perfStats.emplace_back(q->stats_);
      q->stats_.resetStats();
    }

    statsStart_ = SteadyTimestamp::now();
  }

  /*
   * The scheduler permits the caller to update the IO
   * cost with its principal if not all of the initial
   * cost was utilized. This occurs, e.g., if the queued
   * task meant to perform say 1MB of IO but only managed
   * to do 64K. But for disks, this interface cannot be
   * naively used at the byte level. The cost of a 32K IO
   * to HDD is not that different from a 64K IO. In fact
   * two independent 32K IOs are more expensive than one
   * 64K IO. In other words, both IOPS and request size matter.
   * See an example of using this in the StorageThreadpool.cpp.
   *
   * The simplest way to use the schduler is to do request
   * based scheduling, without using the credit iterface..
   */
  void returnCredit(uint64_t bytesUnused, uint64_t principal) {
    std::unique_lock<std::mutex> lock(mutex_);
    if (queues_[principal]->numReqs_) {
      queues_[principal]->deficit_ += bytesUnused;
    }
  }

  /*
   * Non-blocking interface for enqueing the request.
   */
  void enqueue(T* req, uint64_t principal) {
    std::unique_lock<std::mutex> lock(mutex_);
    DRRQueue* queue = queues_[principal].get();
    queue->q_.push_back(*req);
    numReqs_++;
    queue->numReqs_++;
    cond_.notify_one();
  }

  /*
   * Blocking interface for dequeueing the next
   * eligible request. The interface dequeues the
   * request based on its share and turn.
   */
  T* blockingDequeue() {
    std::unique_lock<std::mutex> lock(mutex_);
    while (numReqs_ == 0) {
      cond_.wait(lock);
    }
    return dequeueInternal();
  }

  /*
   * Non-blocking interface for dequeueing the next
   * eligible request. Returns NULL if all the
   * queues are empty.
   *
   */
  T* dequeue() {
    std::unique_lock<std::mutex> lock(mutex_);
    if (numReqs_ == 0) {
      return NULL;
    }
    // We are guaranteed to dequeue something
    return dequeueInternal();
  }

  T* dequeueInternal() {
    T* req;
    while (1) {
      DRRQueue* queue = queues_[next_].get();
      if (queue->numReqs_) {
        req = &queue->q_.front();
        uint64_t size = req->reqSize();
        ld_check(size);
        if (queue->deficit_ >= size) {
          // We are eligible to dequeue
          queue->q_.pop_front();
          numReqs_--;
          queue->numReqs_--;
          queue->deficit_ -= size;
          queue->stats_.reqsProcessed += 1;
          queue->stats_.bytesProcessed += size;
          return req;
        } else {
          queue->deficit_ += (queue->stats_.principal.share * quanta_);
        }
      } else {
        queue->deficit_ = 0;
      }

      next_++;
      if (next_ == queues_.size()) {
        next_ = 0;
      }
    }
  }

  ssize_t size() {
    std::unique_lock<std::mutex> lock(mutex_);
    return numReqs_;
  }

  /*
   * This function introspects contents of the queue
   * and calls cb() on every element.
   */
  void introspect_contents(std::function<void(const T*)> cb) {
    std::unique_lock<std::mutex> lock(mutex_);
    for (const auto& q : queues_) {
      for (const auto& req : q->q_) {
        cb(&req);
      }
    }
  }

 private:
  /* Scheduler name*/
  std::string name_;

  /* Min processing per turn */
  uint64_t quanta_;

  /* Per Q stats */
  std::vector<std::shared_ptr<DRRQueue>> queues_;
  uint64_t next_;

  /* Total reqs in the scheduler */
  uint64_t numReqs_;
  std::mutex mutex_;
  std::condition_variable cond_;
  SteadyTimestamp statsStart_;
};

}} // namespace facebook::logdevice
