/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/server/GOSSIP_onReceived.h"

#include "logdevice/common/Request.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/stats/Histogram.h"
#include "logdevice/common/stats/ServerHistograms.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/server/FailureDetector.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/ServerWorker.h"

namespace facebook { namespace logdevice {

namespace {
// Request used to route the GOSSIP message to the worker which runs the failure
// detector.
class GossipRequest : public Request {
 public:
  explicit GossipRequest(std::unique_ptr<GOSSIP_Message> message)
      : Request(RequestType::GOSSIP), message_(std::move(message)) {}

  Execution execute() override {
    auto now = std::chrono::steady_clock::now();
    auto failure_detector =
        ServerWorker::onThisThread()->processor_->failure_detector_.get();
    ld_check(failure_detector != nullptr);

    const int64_t queue_latency_us =
        std::chrono::duration_cast<std::chrono::microseconds>(now -
                                                              enqueue_time_)
            .count();
    HISTOGRAM_ADD(Worker::stats(), gossip_queue_latency, queue_latency_us);

    if (queue_latency_us / 1000 >=
        std::min(
            std::chrono::milliseconds(1000),
            failure_detector->getGossipSettings()->gossip_time_skew_threshold)
            .count()) {
      RATELIMIT_WARNING(std::chrono::seconds(1),
                        10,
                        "Gossip message took %lums to dequeue.",
                        queue_latency_us / 1000);
      STAT_INCR(Worker::stats(), gossips_delayed_pipe);
    }

    failure_detector->onGossipReceived(*message_);
    return Execution::COMPLETE;
  }

  WorkerType getWorkerTypeAffinity() override {
    return WorkerType::FAILURE_DETECTOR;
  }

 private:
  std::unique_ptr<GOSSIP_Message> message_;
};
} // namespace

Message::Disposition GOSSIP_onReceived(GOSSIP_Message* m,
                                       const Address& /* unused */) {
  ServerWorker* w = ServerWorker::onThisThread();

  auto failure_detector = w->processor_->failure_detector_.get();
  if (!failure_detector) {
    RATELIMIT_WARNING(std::chrono::seconds(10),
                      1,
                      "Received a GOSSIP message, but failure_detector is not "
                      "set. Ignoring.");
    return Message::Disposition::NORMAL;
  }

  if (w->worker_type_ != WorkerType::FAILURE_DETECTOR) {
    // message needs to be routed to gossip thread
    std::unique_ptr<GOSSIP_Message> msg(m);
    std::unique_ptr<Request> rq =
        std::make_unique<GossipRequest>(std::move(msg));
    int rv = w->processor_->postRequest(rq);
    if (rv) {
      RATELIMIT_ERROR(std::chrono::seconds(10),
                      10,
                      "Failed to propagate GOSSIP message to gossip thread: %s",
                      error_description(err));
    }

    STAT_INCR(Worker::stats(), gossips_received_on_worker_thread);
    return Message::Disposition::KEEP;
  }

  STAT_INCR(Worker::stats(), gossips_received_on_gossip_thread);
  failure_detector->onGossipReceived(*m);
  return Message::Disposition::NORMAL;
}
}} // namespace facebook::logdevice
