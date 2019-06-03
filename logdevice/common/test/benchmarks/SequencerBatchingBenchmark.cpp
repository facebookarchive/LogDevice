/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

#include <folly/Benchmark.h>
#include <folly/ScopeGuard.h>
#include <folly/Singleton.h>
#include <folly/container/F14Map.h>

#include "logdevice/common/Appender.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Semaphore.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Sequencer.h"
#include "logdevice/common/SequencerBatching.h"
#include "logdevice/common/Timer.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/test/TestUtil.h"

using namespace facebook::logdevice;
using namespace std::chrono_literals;
using namespace std::chrono;

namespace {

class SequencerBatchingBenchmark {
 public:
  struct Params {
    size_t num_appends;
    size_t nworkers{64};
    size_t nwriters{32};
    size_t append_size{140};
    // by defautl time trigger is not used
    std::chrono::milliseconds time_trigger{10000};
    // by default batch 50 records
    ssize_t size_trigger{7000};
    Compression compression{Compression::NONE};

    // how many appends can be in-flight for each writer
    size_t writer_max_in_flight{1000};
  };

  struct TestStats {
    std::atomic<size_t> append_started{0};
    std::atomic<size_t> append_received{0};
    std::atomic<size_t> append_success{0};
    std::atomic<size_t> append_failed{0};
  };

  explicit SequencerBatchingBenchmark(Params params);
  ~SequencerBatchingBenchmark();

  // block until the num_appends finished appending
  void run();
  bool shouldStop() const {
    const size_t num_success = test_stats_.append_success.load();
    return num_success > 0 && num_success >= params_.num_appends;
  }

  void shutdown();

  std::unique_ptr<Appender> createAppender(size_t writer_id, logid_t logid);
  TestStats test_stats_;

  const Params params_;
  struct WriterState {
    explicit WriterState(size_t writer_max_in_flight)
        : tickets(writer_max_in_flight) {}

    std::atomic<size_t> appends_finished{0};
    Semaphore tickets;
  };

  const std::string payload_;
  // settings used to create the processor and sequencer batching
  Settings settings_;
  std::shared_ptr<UpdateableConfig> updateable_config_;
  std::vector<std::thread> writer_threads_;
  std::unordered_map<size_t, WriterState> writer_states_;
  //  StatsHolder stats_;
  std::shared_ptr<Processor> processor_;
  void writerThread(size_t writer_id);
  PayloadHolder genPayload();
};

class MockSequencerBatching : public SequencerBatching {
 public:
  using MockSender = SenderTestProxy<MockSequencerBatching>;
  MockSequencerBatching(SequencerBatchingBenchmark* test, Processor* processor)
      : SequencerBatching(processor), test_(test) {
    sender_ = std::make_unique<MockSender>(this);
    ld_check(test != nullptr);
  }

  folly::Optional<APPENDED_Header>
  runBufferedAppend(logid_t,
                    AppendAttributes,
                    const Payload&,
                    InternalAppendRequest::Callback callback,
                    APPEND_flags_t,
                    int,
                    uint32_t,
                    uint32_t) override {
    Worker::onThisThread()->addWithPriority(
        [callback]() {
          callback(Status::OK, lsn_t(1), NodeID(), RecordTimestamp::zero());
        },
        folly::Executor::HI_PRI);
    // append is accepted;
    return folly::none;
  }

  int sendMessageImpl(std::unique_ptr<Message>&& msg,
                      const Address&,
                      BWAvailableCallback*,
                      SocketCallback*) {
    ld_check(msg->type_ == MessageType::APPENDED);

    APPENDED_Message* reply = dynamic_cast<APPENDED_Message*>(msg.get());
    ld_check(reply != nullptr);

    const auto writer_id = reply->header_.rqid.val();
    ld_check(writer_id < test_->params_.nwriters);
    auto& w_st = test_->writer_states_.at(writer_id);
    if (reply->header_.status == Status::OK) {
      ++test_->test_stats_.append_success;
      ++w_st.appends_finished;
    } else {
      ++test_->test_stats_.append_failed;
    }

    w_st.tickets.post();
    return 0;
  }

  bool canSendToImpl(const Address&, TrafficClass, BWAvailableCallback&) {
    return true;
  }

 private:
  SequencerBatchingBenchmark* const test_;
};

class TestAppenderRequest : public Request {
 public:
  explicit TestAppenderRequest(SequencerBatchingBenchmark* test,
                               size_t writer_id,
                               logid_t log_id)
      : Request(RequestType::TEST_APPENDER_REQUEST),
        test_(test),
        writer_id_(writer_id),
        log_id_(log_id) {}

  Request::Execution execute() override {
    auto& w_st = test_->writer_states_.at(writer_id_);
    if (test_->shouldStop()) {
      ++test_->test_stats_.append_failed;
      w_st.tickets.post();
      return Execution::COMPLETE;
    }

    auto appender = test_->createAppender(writer_id_, log_id_);
    auto res = test_->processor_->sequencerBatching().buffer(log_id_, appender);
    if (res) {
      ++test_->test_stats_.append_received;
    } else {
      RATELIMIT_INFO(std::chrono::seconds(2),
                     2,
                     "Appender for log %lu from writer %lu is not bufferd.",
                     log_id_.val(),
                     writer_id_);
      ++test_->test_stats_.append_failed;
      w_st.tickets.post();
    }

    return Execution::COMPLETE;
  }

 private:
  SequencerBatchingBenchmark* const test_;
  const size_t writer_id_;
  const logid_t log_id_;
};

SequencerBatchingBenchmark::SequencerBatchingBenchmark(Params params)
    : params_(std::move(params)),
      payload_('c', params_.append_size),
      settings_(create_default_settings<Settings>()) {
  ld_check(params_.num_appends > 0);
  ld_check(params_.nwriters > 0);

  // change settings to reflect the benchmark params
  settings_.server = true;
  settings_.num_workers = params_.nworkers;
  settings_.sequencer_batching = true;
  settings_.sequencer_batching_time_trigger = params_.time_trigger;
  settings_.sequencer_batching_size_trigger = params_.size_trigger;

  updateable_config_ = std::make_shared<UpdateableConfig>(
      Configuration::fromJsonFile(TEST_CONFIG_FILE("sequencer_test.conf")));
  processor_ = make_test_processor(
      settings_, updateable_config_, /*stats=*/nullptr, NodeID(1, 1));
  ld_check(processor_ != nullptr);

  processor_->setSequencerBatching(
      std::make_unique<MockSequencerBatching>(this, processor_.get()));

  // pre-allocate writer states
  for (size_t i = 0; i < params_.nwriters; ++i) {
    auto res = writer_states_.emplace(
        std::piecewise_construct,
        std::forward_as_tuple(i),
        std::forward_as_tuple(params_.writer_max_in_flight));
    ld_check(res.second);
  }
}

PayloadHolder SequencerBatchingBenchmark::genPayload() {
  return PayloadHolder(
      Payload(payload_.data(), payload_.size()), PayloadHolder::UNOWNED);
}

std::unique_ptr<Appender>
SequencerBatchingBenchmark::createAppender(size_t writer_id, logid_t log_id) {
  return std::make_unique<Appender>(nullptr,
                                    nullptr,
                                    std::chrono::milliseconds(1000),
                                    request_id_t(writer_id),
                                    STORE_flags_t(0),
                                    log_id,
                                    AppendAttributes(),
                                    genPayload(),
                                    ClientID(),
                                    EPOCH_MIN,
                                    params_.append_size,
                                    LSN_INVALID);
}

void SequencerBatchingBenchmark::writerThread(size_t writer_id) {
  auto& w_st = writer_states_.at(writer_id);
  while (!shouldStop()) {
    // get an inflight ticket
    w_st.tickets.wait();
    ++test_stats_.append_started;

    // pick a logid from [1, 100]
    logid_t log_id = logid_t(folly::Random::rand32() % 100 + 1);
    std::unique_ptr<Request> rq =
        std::make_unique<TestAppenderRequest>(this, writer_id, log_id);
    int rv = processor_->postRequest(rq);
    if (rv != 0) {
      ++test_stats_.append_failed;
      w_st.tickets.post();
    }
  }
  ld_info("Writer %lu finished processing of %lu appends successfuly.",
          writer_id,
          w_st.appends_finished.load());
}

SequencerBatchingBenchmark::~SequencerBatchingBenchmark() {}

void SequencerBatchingBenchmark::run() {
  for (size_t i = 0; i < params_.nwriters; i++) {
    writer_threads_.emplace_back(std::thread([i, this]() { writerThread(i); }));
  }

  for (std::thread& t : writer_threads_) {
    t.join();
  }
}

void SequencerBatchingBenchmark::shutdown() {
  gracefully_shutdown_processor(processor_.get());
  processor_.reset();

  // print benchmark run stats
  ld_info("\nAppend started: %lu, append received: %lu.\n"
          "Append success: %lu, append failed: %lu.\n",
          test_stats_.append_started.load(),
          test_stats_.append_received.load(),
          test_stats_.append_success.load(),
          test_stats_.append_failed.load());
}

BENCHMARK_MULTI(LDSequencerBatchingBenchmark, n) {
  SequencerBatchingBenchmark::Params params{n};
  std::unique_ptr<SequencerBatchingBenchmark> b;
  BENCHMARK_SUSPEND {
    b = std::make_unique<SequencerBatchingBenchmark>(params);
    ld_info("n appends = %u", n);
  }

  b->run();
  BENCHMARK_SUSPEND {
    b->shutdown();
  }
  return b->test_stats_.append_success.load();
}

} // namespace

#ifndef BENCHMARK_BUNDLE

int main(int argc, char** argv) {
  dbg::currentLevel = dbg::Level::ERROR;
  folly::SingletonVault::singleton()->registrationComplete();
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  folly::runBenchmarks();
  return 0;
}
#endif
