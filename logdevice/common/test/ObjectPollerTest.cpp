/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/ObjectPoller.h"

#include <queue>

#include <folly/Memory.h>
#include <gtest/gtest.h>

#include "logdevice/common/Timer.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/test/MockBackoffTimer.h"
#include "logdevice/common/test/MockTimer.h"
#include "logdevice/common/util.h"

using namespace facebook::logdevice;

namespace {

// Object and Response are all integers
using NodeObjectPoller = ObjectPoller<int, int, node_index_t>;
using SourceSet = NodeObjectPoller::SourceSet;
using RoundID = NodeObjectPoller::RoundID;
using RequestResult = NodeObjectPoller::RequestResult;
using ObjectCallback = NodeObjectPoller::ObjectCallback;

class ObjectPollerTest : public ::testing::Test {
 public:
  SourceSet candidates_{1, 2, 3, 4, 5, 6, 7};
  NodeObjectPoller::SourceSelectionFunc source_selector_;
  NodeObjectPoller::SourceRequestFunc request_func_;
  NodeObjectPoller::ObjectAggrFunc aggr_;
  NodeObjectPoller::ObjectCallback callback_;
  NodeObjectPoller::Options options_;

  // nodes to be selected as the next result of
  // source_selector_ invocation
  SourceSet next_wave_nodes_;

  // filled by source_selector_ on each invocation
  struct SelectionParams {
    SourceSet candidates;
    SourceSet existing;
    SourceSet blacklist;
    SourceSet graylist;
    size_t num_required;
    size_t num_extras;
  };

  SelectionParams selection_param_;

  std::vector<std::pair<RoundID, node_index_t>> requests_sent_;
  std::unordered_map<node_index_t, RequestResult> send_result_;

  struct RoundResult {
    Status status;
    RoundID round;
    folly::Optional<int> object;
  };

  folly::Optional<RoundResult> result_;

  explicit ObjectPollerTest();
  std::unique_ptr<NodeObjectPoller> create();
};

class MockNodeObjectPoller : public NodeObjectPoller {
 public:
  MockNodeObjectPoller(ObjectPollerTest* test,
                       SourceSet candidates,
                       SourceSelectionFunc source_selector,
                       SourceRequestFunc request_func,
                       ObjectAggrFunc aggr,
                       ObjectCallback callback,
                       Options options)
      : NodeObjectPoller(std::move(candidates),
                         std::move(source_selector),
                         std::move(request_func),
                         std::move(aggr),
                         std::move(callback),
                         std::move(options)),
        test_(test) {
    ld_check(test_ != nullptr);
  }

  std::unique_ptr<BackoffTimer>
  createBackoffTimer(const chrono_expbackoff_t<std::chrono::milliseconds>&,
                     std::function<void()> callback) override {
    auto timer = std::make_unique<MockBackoffTimer>();
    if (callback) {
      timer->setCallback(std::move(callback));
    }
    return std::move(timer);
  }

  std::unique_ptr<Timer> createTimer(std::function<void()> cb) override {
    auto timer = std::make_unique<MockTimer>();
    timer->setCallback(std::move(cb));
    return std::move(timer);
  }

 private:
  ObjectPollerTest* const test_;
};

ObjectPollerTest::ObjectPollerTest() {
  aggr_ = [](const int* current, int response) {
    if (current && *current >= response) {
      return folly::Optional<int>();
    }
    return folly::Optional<int>(response);
  };

  source_selector_ = [this](const SourceSet& candidates,
                            const SourceSet& existing,
                            const SourceSet& blacklist,
                            const SourceSet& graylist,
                            size_t num_required,
                            size_t num_extras) {
    selection_param_.candidates = candidates;
    selection_param_.existing = existing;
    selection_param_.blacklist = blacklist;
    selection_param_.graylist = graylist;
    selection_param_.num_required = num_required;
    selection_param_.num_extras = num_extras;
    return next_wave_nodes_;
  };

  request_func_ = [this](RoundID r, node_index_t n) {
    requests_sent_.push_back(std::make_pair(r, n));
    return send_result_.count(n) > 0 ? send_result_[n] : RequestResult::OK;
  };

  callback_ = [this](Status st, RoundID r, folly::Optional<int> o) {
    result_.assign({st, r, std::move(o)});
  };
}

#define CHECK_REQUESTS_SENT(round, ...)                                       \
  do {                                                                        \
    std::set<node_index_t> nodes({__VA_ARGS__});                              \
    ASSERT_EQ(nodes.size(), requests_sent_.size());                           \
    ASSERT_TRUE(std::all_of(nodes.begin(), nodes.end(), [&](node_index_t n) { \
      return std::find(requests_sent_.begin(),                                \
                       requests_sent_.end(),                                  \
                       std::make_pair((RoundID)round, n)) !=                  \
          requests_sent_.end();                                               \
    }));                                                                      \
    requests_sent_.clear();                                                   \
  } while (0)

#define CHECK_SELECTION_PARAMS(_c, _e, _b, _g, _r, _x)     \
  do {                                                     \
    ASSERT_EQ(SourceSet(_c), selection_param_.candidates); \
    ASSERT_EQ(SourceSet(_e), selection_param_.existing);   \
    ASSERT_EQ(SourceSet(_b), selection_param_.blacklist);  \
    ASSERT_EQ(SourceSet(_g), selection_param_.graylist);   \
    ASSERT_EQ((_r), selection_param_.num_required);        \
    ASSERT_EQ((_x), selection_param_.num_extras);          \
  } while (0)

#define CHECK_ROUND_RESULT(_r, _s, _i)                 \
  do {                                                 \
    ASSERT_TRUE(result_.hasValue());                   \
    ASSERT_EQ((_r), result_.value().round);            \
    ASSERT_EQ((_s), result_.value().status);           \
    if (result_.value().status == Status::OK ||        \
        result_.value().status == Status::PARTIAL) {   \
      ASSERT_TRUE(result_.value().object.hasValue());  \
      ASSERT_EQ((_i), result_.value().object.value()); \
    } else {                                           \
      ASSERT_FALSE(result_.value().object.hasValue()); \
    }                                                  \
  } while (0)

std::unique_ptr<NodeObjectPoller> ObjectPollerTest::create() {
  return std::make_unique<MockNodeObjectPoller>(this,
                                                candidates_,
                                                std::move(source_selector_),
                                                std::move(request_func_),
                                                std::move(aggr_),
                                                std::move(callback_),
                                                options_);
}

TEST_F(ObjectPollerTest, Basic) {
  // need 2 response per-round
  options_.num_responses_required_round = 2;
  auto poller = create();

  next_wave_nodes_ = {1, 2, 3};
  poller->start();

  // check selection parameters for the first wave
  CHECK_SELECTION_PARAMS(candidates_, {}, {}, {}, 1, 3);
  CHECK_REQUESTS_SENT(/*round*/ 1, 1, 2, 3);

  // wave timer and round timer should be active
  ASSERT_TRUE(poller->getWaveTimer()->isActive());
  ASSERT_TRUE(poller->getRoundTimer()->isActive());
  ASSERT_EQ(nullptr, poller->getRoundIntervalTimer());

  // node 2 replied with success, 3 replied with failure-blacklisting, 1 didn't
  // reply before wave timeout
  poller->onSourceReply(/*round*/ 1, 2, RequestResult::OK, 223);
  poller->onSourceReply(/*round*/ 1, 3, RequestResult::FAILURE_BLACKLIST, {});
  // poller shouldn't finish round 1 yet
  ASSERT_FALSE(result_.hasValue());
  ASSERT_EQ(nullptr, poller->getRoundResultTimer());

  next_wave_nodes_ = {4, 5, 6};
  ((MockBackoffTimer*)poller->getWaveTimer())->trigger();
  CHECK_SELECTION_PARAMS(candidates_,
                         /*existing*/ {2},
                         /*blacklist*/ {3},
                         /*graylist*/ {1},
                         /*required*/ 1,
                         /*extras*/ 2);
  CHECK_REQUESTS_SENT(/*round*/ 1, 4, 5, 6);
  poller->onSourceReply(/*round*/ 1, 4, RequestResult::FAILURE_GRAYLIST, {});
  poller->onSourceReply(/*round*/ 1, 5, RequestResult::OK, 237);
  ASSERT_FALSE(result_.hasValue());
  // round should complete after 5 replies
  ASSERT_TRUE(poller->getRoundResultTimer()->isActive());
  ((MockTimer*)poller->getRoundResultTimer())->trigger();
  CHECK_ROUND_RESULT(1, Status::OK, 237);

  // round interval timer must be started for the next round
  ASSERT_TRUE(poller->getRoundIntervalTimer()->isActive());
  ((MockTimer*)poller->getRoundIntervalTimer())->trigger();
  // round 2 should get started, blacklist and graylist from last round should
  // be remembered
  SourceSet expect_graylist{1, 4};
  CHECK_SELECTION_PARAMS(candidates_,
                         /*existing*/ {},
                         /*blacklist*/ {3},
                         /*graylist*/ expect_graylist,
                         /*required*/ 1,
                         /*extras*/ 3);
  CHECK_REQUESTS_SENT(/*round*/ 2, 4, 5, 6);
}

TEST_F(ObjectPollerTest, BlacklistGraylistTTL) {
  options_.num_responses_required_round = 2;
  options_.extras_request_each_wave = 1;
  options_.graylist_effective_rounds = 1;
  options_.blacklist_effective_rounds = 2;
  auto poller = create();

  next_wave_nodes_ = {1, 2, 3};
  // node 1 will syncrhonously return failure-graylist
  send_result_[node_index_t(1)] = RequestResult::FAILURE_GRAYLIST;
  poller->start();

  // check selection parameters for the first wave
  CHECK_SELECTION_PARAMS(candidates_, {}, {}, {}, 1, 2);
  CHECK_REQUESTS_SENT(/*round*/ 1, 1, 2, 3);

  // node 3 replied with failure-blacklist
  poller->onSourceReply(/*round*/ 1, 3, RequestResult::FAILURE_BLACKLIST, {});
  // trigger the round timeout
  ((MockTimer*)poller->getRoundTimer())->trigger();
  ASSERT_TRUE(poller->getRoundResultTimer()->isActive());
  ((MockTimer*)poller->getRoundResultTimer())->trigger();
  CHECK_ROUND_RESULT(1, Status::FAILED, /*ignored*/ 0);
  ASSERT_TRUE(poller->getRoundIntervalTimer()->isActive());
  next_wave_nodes_ = {4, 5, 6};
  ((MockTimer*)poller->getRoundIntervalTimer())->trigger();
  CHECK_SELECTION_PARAMS(candidates_,
                         /*existing*/ {},
                         /*blacklist*/ {3},
                         /*graylist*/ {1},
                         /*required*/ 1,
                         /*extras*/ 2);
  CHECK_REQUESTS_SENT(/*round*/ 2, 4, 5, 6);
  // round 2 timed out
  ((MockTimer*)poller->getRoundTimer())->trigger();
  ASSERT_TRUE(poller->getRoundResultTimer()->isActive());
  ((MockTimer*)poller->getRoundResultTimer())->trigger();
  CHECK_ROUND_RESULT(2, Status::FAILED, /*ignored*/ 0);
  ASSERT_TRUE(poller->getRoundIntervalTimer()->isActive());
  ((MockTimer*)poller->getRoundIntervalTimer())->trigger();
  // node 1's graylist status should be expired but node 3's
  // blacklist status should remain
  CHECK_SELECTION_PARAMS(candidates_,
                         /*existing*/ {},
                         /*blacklist*/ {3},
                         /*graylist*/ {},
                         /*required*/ 1,
                         /*extras*/ 2);
  CHECK_REQUESTS_SENT(/*round*/ 3, 4, 5, 6);
  poller->onSourceReply(/*round*/ 3, 5, RequestResult::OK, 10);
  // round 3 timed out
  ((MockTimer*)poller->getRoundTimer())->trigger();
  ASSERT_TRUE(poller->getRoundResultTimer()->isActive());
  ((MockTimer*)poller->getRoundResultTimer())->trigger();
  CHECK_ROUND_RESULT(3, Status::PARTIAL, 10);
  // node 3's blacklist should be cleared at round 4
  ASSERT_TRUE(poller->getRoundIntervalTimer()->isActive());
  next_wave_nodes_ = {1, 2, 3};
  ((MockTimer*)poller->getRoundIntervalTimer())->trigger();
  CHECK_SELECTION_PARAMS(candidates_,
                         /*existing*/ {},
                         /*blacklist*/ {},
                         /*graylist*/ {},
                         /*required*/ 1,
                         /*extras*/ 2);
  CHECK_REQUESTS_SENT(/*round*/ 4, 1, 2, 3);
}

TEST_F(ObjectPollerTest, StopAndResume) {
  // need 2 response per-round
  options_.extras_request_each_wave = 1;
  options_.num_responses_required_round = 2;
  auto poller = create();

  next_wave_nodes_ = {1, 2, 3};
  poller->start();

  // check selection parameters for the first wave
  CHECK_SELECTION_PARAMS(candidates_, {}, {}, {}, 1, 2);
  CHECK_REQUESTS_SENT(/*round*/ 1, 1, 2, 3);

  // wave timer and round timer should be active
  ASSERT_TRUE(poller->getWaveTimer()->isActive());
  ASSERT_TRUE(poller->getRoundTimer()->isActive());
  ASSERT_EQ(nullptr, poller->getRoundIntervalTimer());

  // node 2 replied with success, 3 replied with failure-blacklisting
  poller->onSourceReply(/*round*/ 1, 2, RequestResult::OK, 223);
  poller->onSourceReply(/*round*/ 1, 3, RequestResult::FAILURE_BLACKLIST, {});
  // poller shouldn't finish round 1 yet
  ASSERT_FALSE(result_.hasValue());
  ASSERT_EQ(nullptr, poller->getRoundResultTimer());

  // stop the poller in the middle of round 1
  poller->stop();

  // callbacks should be suppressed
  ASSERT_EQ(nullptr, poller->getRoundResultTimer());
  ASSERT_FALSE(result_.hasValue());
  ASSERT_EQ(nullptr, poller->getWaveTimer());
  ASSERT_EQ(nullptr, poller->getRoundTimer());

  // node 1's reply won't have any affect
  poller->onSourceReply(/*round*/ 1, 1, RequestResult::OK, 225);
  ASSERT_EQ(nullptr, poller->getRoundResultTimer());
  ASSERT_FALSE(result_.hasValue());

  // restart poller and it should start a new round
  next_wave_nodes_ = {4, 5, 6};
  poller->start();

  // existing blacklist and graylist should be cleared
  CHECK_SELECTION_PARAMS(candidates_, {}, {}, {}, 1, 2);
  CHECK_REQUESTS_SENT(/*round*/ 2, 4, 5, 6);
}

TEST_F(ObjectPollerTest, FAILURE_TRANSIENT) {
  // need 2 response per-round
  options_.extras_request_each_wave = 1;
  options_.num_responses_required_round = 2;
  auto poller = create();

  next_wave_nodes_ = {1, 2, 3};
  poller->start();

  // check selection parameters for the first wave
  CHECK_SELECTION_PARAMS(candidates_, {}, {}, {}, 1, 2);
  CHECK_REQUESTS_SENT(/*round*/ 1, 1, 2, 3);

  // wave timer and round timer should be active
  ASSERT_TRUE(poller->getWaveTimer()->isActive());
  ASSERT_TRUE(poller->getRoundTimer()->isActive());
  ASSERT_EQ(nullptr, poller->getRoundIntervalTimer());

  // node 2 replied with success, 3 replied with failure-transient
  poller->onSourceReply(/*round*/ 1, 2, RequestResult::OK, 223);
  poller->onSourceReply(/*round*/ 1, 3, RequestResult::FAILURE_TRANSIENT, {});
  next_wave_nodes_ = {3, 4};

  // trigger a wave timeout
  ((MockBackoffTimer*)poller->getWaveTimer())->trigger();
  // check only node 1 should be in the graylist but not node 3
  CHECK_SELECTION_PARAMS(candidates_,
                         /*existing*/ {2},
                         /*blacklist*/ {},
                         /*graylist*/ {1},
                         /*required*/ 1,
                         /*extras*/ 1);
  CHECK_REQUESTS_SENT(/*round*/ 1, 3, 4);
}

} // namespace
