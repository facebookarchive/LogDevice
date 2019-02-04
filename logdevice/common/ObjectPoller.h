/**
 * Copyright (c) 2019-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>
#include <set>
#include <unordered_map>

#include <folly/Function.h>
#include <folly/Optional.h>
#include <folly/container/F14Set.h>

#include "logdevice/common/BackoffTimer.h"
#include "logdevice/common/ExponentialBackoffTimer.h"
#include "logdevice/common/Timer.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/common/util.h"
#include "logdevice/include/Err.h"

/**
 * @file  ObjectPoller is a utility class that handles the common pattern
 *        of polling an object from a collection of candidate sources
 *        (e.g., cluster nodes), either one-time or periodically.
 *
 *        ObjectPoller operates in rounds and starts a new round for each
 *        polling internal. Within each round, it supports sending poll requests
 *        to sources in waves with a pluggable source selection policy. The
 *        result `Object' is computed by aggregating the `Response' from
 *        successful replies in each round.
 *
 *        The utility is very flexible, agnostic to how to get response from
 *        sources, and handles common problems such as retries, timeouts,
 *        transient/permanent failure handling, graylisting/blacklisting of
 *        sources based on their responses.
 *
 *        Note 1: ObjectPoller doesn't support `strict wave' pattern that
 *        requires all successful responses are from the save wave. Strict
 *        wave is often used to satify quorum like property with storage
 *        nodes, use `StorageSetAccessor' class for such use case.
 *
 *        Note 2: ObjectPoller is not a thread-safe class, all its methods,
 *        including construction, destruction and all function pointers and
 *        callbacks must be invoked on the same thread.
 *
 *        Note 3: It's generally not safe to access the ObjectPoller object
 *        (i.e., call its member functions) or destroy the object within the
 *        context of callbacks provided to ObjectPoller, e.g.,
 *        SourceSelectionFunc, SourceRequestFunc, ObjectAggrFunc. The only
 *        exception is the ObjectCallback, in which it's safe to call
 *        ObjectPoller::stop() or destroy the ObjectPoller object.
 */

namespace facebook { namespace logdevice {

template <typename Object,
          typename Response,
          typename SourceID,
          typename SourceIDHash = std::hash<SourceID>>
class ObjectPoller {
 public:
  // a collection of sources, each can be identified by its unique ID
  using SourceSet = folly::F14FastSet<SourceID>;
  // used to identify each polling rounds
  using RoundID = uint64_t;

  /**
   * Selection policy for selecting a wave of sources to contact for a round
   * @param candidates    list of sources to choose from
   *
   * @param existing      list of sources which succeed in previous waves
   *                      and should not be picked. Can be empty. May be used
   *                      by implementations for establishing preference of
   *                      remaining candidates.
   * @param blacklist     list of sources which had a previous failure and
   *                      should not be picked. Can be empty.
   * @param graylist      list of sources which are less preferrable to be
   *                      selected. In other words, they are selected only
   *                      if not other candidate source is available. Can be
   *                      empty.
   * @param num_required  required number of sources in the result, if
   *                      cannot be satisfied, the selection fails
   * @param num_extras    number of sources to select in addition to the
   *                      required number. They are not mandatory but
   *                      best-effort.
   * @return              if success, returns the selection result as a
   *                      non-empty SourceSet with at least `num_required'
   *                      elements.
   *                      on failure, an empty SourceSet is returned.
   */
  using SourceSelectionFunc =
      folly::Function<SourceSet(const SourceSet& candidates,
                                const SourceSet& existing,
                                const SourceSet& blacklist,
                                const SourceSet& graylist,
                                size_t num_required,
                                size_t num_extras)>;

  /**
   * Represent the result of a request to one source. Can be one of:
   *   OK                    the request has been successfully sent or executed
   *   FAILURE_TRANSIENT     the request has failed but it is OK to retry the
   *                         same source in the next wave/round
   *   FAILURE_GRAYLIST      the request has failed and the source will be
   *                         graylisted, making it to be less preferrable to be
   *                         selected again during the graylist TTL
   *   FAILURE_BLACKLIST     the request has failed and the source will be
   *                         blacklisted, ObjectPoller guarantees not to pick
   *                         the source during the blacklist TTL
   */
  enum class RequestResult {
    OK,
    FAILURE_TRANSIENT,
    FAILURE_GRAYLIST,
    FAILURE_BLACKLIST
  };

  /**
   * Provided by the upperlayer, this is the function that initiates the request
   * that polls from a particular source. Invoked by ObjectPoller when each wave
   * starts.
   *
   * @return  RequestResult indicates that if the request has been successfully
   *          sent (initiated). Note that this doesn't mean that the request
   *          has been fulfilled or not. use onSourceReply for the latter.
   *
   */
  using SourceRequestFunc = folly::Function<RequestResult(RoundID, SourceID)>;

  /**
   * Aggregation function that takes input from 1) an optional aggregated Object
   * and 2) a valid Response received, and produces a new Object as the
   * aggregation outcome. ObjectPoller consecutively applies this function to
   * all successful Responses received from a single round and compute the
   * aggregated object for the RoundCallback.
   *
   * @return  result Object of the aggregation. Can be optionally empty to
   *          indicate that the response is ignored and the original Object
   *          should be kept
   *
   * Note: this function is currently synchronously invoked, so it shouldn't be
   * too costly, especially if the ObjectPoller is running on a high-pri or
   * latency sensitive event loop.
   */
  using ObjectAggrFunc =
      folly::Function<folly::Optional<Object>(const Object*, Response)>;

  /**
   * Callback used by ObjectPoller to inform the result of its polling
   * operation. Invoked upon the completion of each round. If the ObjectPoller
   * is running in ONE_TIME mode, the callback gets invoked after the first
   * (and only) round. Otherwise the callback is invoked periodically.
   *
   * @param  Status of the polling round, can be one of
   *          OK          enough success responses are received in this round
   *                      and the aggregated result object should be return
   *          PARTIAL     number of responses received is within
   *                      (0, num_responses_required_round), the callback should
   *                      return the partially aggregated result object.
   *          FAILED      one of:
   *                      1) cannot select a single source to send request;
   *                      2) no successful response received before the round
   *                      timeout;
   *                      3) all candidate sources returned a failure result
   */
  using ObjectCallback =
      folly::Function<void(Status, RoundID, folly::Optional<Object>)>;

  enum Mode { ONE_TIME, CONTINUOUS };

  struct Options {
    // mode of operation, can be a one timepoll or continuous periodical polling
    Mode mode{Mode::CONTINUOUS};

    // number of successful responses for one round (across all waves) required
    // for considering the round success (i.e., callback returns Status::OK).
    size_t num_responses_required_round{1};

    // timeout of each round
    std::chrono::milliseconds round_timeout{5000};

    // time to wait between each round. The maximum time between starting of
    // two rounds is `round_timeout + round_interval`.
    std::chrono::milliseconds round_interval{5000};

    // For each round, ObjectPoller tries to send at least
    // (num_responses_required_round - successful_replies_received_this_round)
    // requests to ensure a round can succeed with one fully successful wave.
    // This option specifies the _additional_ number of requests to send
    // for each wave besides the required number above.
    size_t extras_request_each_wave{2};

    // specify the exponential backoff behavior of waves of retries within
    // each round
    chrono_expbackoff_t<std::chrono::milliseconds> wave_timeout{
        std::chrono::milliseconds(500),
        std::chrono::milliseconds(2000)};

    // TTL (in rounds) of nodes in the graylist and blacklist
    size_t graylist_effective_rounds{3};
    size_t blacklist_effective_rounds{3};
  };

  ObjectPoller(SourceSet candidates,
               SourceSelectionFunc source_selector,
               SourceRequestFunc request_func,
               ObjectAggrFunc aggr,
               ObjectCallback callback,
               Options options = Options());

  // note: destruction of ObjectPoller guarantees to suppress all its
  // function invocations, including SourceSelectionFunc, ObjectAggrFunc
  // and ObjectCallback
  virtual ~ObjectPoller() {}

  // start the object poller or resume its execution when it has been stopped.
  void start();

  // stop the object poller if it has been started. Note that it will
  // immediately cancel the current round. Graylist and blacklist will
  // also be cleared.
  void stop();

  /**
   * Notify ObjectPoller about the outcome of a request. Called by the
   * upperlayer when the response of the poll request to source for one round
   * has been received.
   *
   * Note: ObjectCallback may happen in the context of this function.
   *
   * @param round          RoundID of request to which the response corresponds
   * @param source         ID of the source that sent the response
   * @param result         Result in the response, see RequestResult above
   * @param object         if the result is RequestResult::OK, this has to
   *                       provide the Response for result aggregation
   */
  void onSourceReply(RoundID round,
                     SourceID source,
                     RequestResult result,
                     folly::Optional<Response> object);

  // Utilities for changing the parameter and optioins for the ObjectPoller.
  // All parameters and options of ObjectPoller can be changed dynamically
  // while the ObjectPoller is running, however, the changes will only be
  // effective since the _next_ round.
  void setSourceCandidates(SourceSet candidates) {
    candidates_ = std::make_shared<SourceSet>(std::move(candidates));
  }

  void setSourceSelectionFunc(SourceSelectionFunc source_selector) {
    source_selector_ =
        std::make_shared<SourceSelectionFunc>(std::move(source_selector));
  }

  void setSourceRequestFunc(SourceRequestFunc request_func) {
    request_func_ =
        std::make_shared<SourceRequestFunc>(std::move(request_func));
  }

  void setObjectAggrFunc(ObjectAggrFunc aggr) {
    aggr_ = std::make_shared<ObjectAggrFunc>(std::move(aggr));
  }

  void setObjectCallback(ObjectCallback callback) {
    callback_ = std::make_shared<ObjectCallback>(std::move(callback));
  }

  void setOptions(Options options) {
    options_ = std::make_shared<Options>(std::move(options));
  }

  const Options& getOptions() const {
    ld_check(options_ != nullptr);
    return *options_;
  }

  RoundID getLastStartedRoundID() const {
    return last_round_started_;
  }

 protected:
  // the following methods can be overridden by tests
  virtual std::unique_ptr<Timer> createTimer(std::function<void()> callback);
  virtual std::unique_ptr<BackoffTimer> createBackoffTimer(
      const chrono_expbackoff_t<std::chrono::milliseconds>& backoff,
      std::function<void()> callback);

  // called when a round finalizes. Asyncrhonously defer publishing the round
  // result to the next event loop execution
  virtual void postRoundResult(RoundID round,
                               std::shared_ptr<ObjectCallback> callback,
                               Status status,
                               folly::Optional<Object> result);

 public:
  // expose timers for testing purpose, should only be used in tests
  Timer* getRoundIntervalTimer() {
    return round_interval_timer_.get();
  }
  Timer* getRoundResultTimer() {
    return round_result_timer_.get();
  }

  Timer* getRoundTimer() {
    return current_round_ ? current_round_->getRoundTimer() : nullptr;
  }
  BackoffTimer* getWaveTimer() {
    return current_round_ ? current_round_->getWaveTimer() : nullptr;
  }

 private:
  /////////// Parameters /////////////

  std::shared_ptr<SourceSet> candidates_;
  std::shared_ptr<SourceSelectionFunc> source_selector_;
  std::shared_ptr<SourceRequestFunc> request_func_;
  std::shared_ptr<ObjectAggrFunc> aggr_;
  std::shared_ptr<ObjectCallback> callback_;
  std::shared_ptr<Options> options_;

  ////////// Round management //////////

  enum class PollerState {
    // the poller is stopped and no round will be started
    STOPPED = 0,
    // poller is started but not executing a round
    // (e.g., at polling interval)
    STARTED,
    // poller is actively executing a round
    ROUND_EXECUTION
  };

  PollerState state_{PollerState::STOPPED};
  RoundID last_round_started_{0};
  // used by postRoundResult() for asynchronous deferring of
  // round result
  std::unique_ptr<Timer> round_result_timer_;

  class Round {
    /**
     * Workflow of a single polling Round.
     *
     * - start() kicks off the first wave, and it also starts 1) an
     *   exponential backoff timer that will start subsequent waves if
     *   the round has not finalized; and 2) a round timer that would
     *   forcefully finalize the round on expire.
     *
     * - conditions for finalizing a round:
     *     - enough successful replies from sources are received
     *     - round timer expired
     *     - (TODO) no progress can be made in the round due to no available
     *       candidates can be chosen
     *
     * - workflow for a single wave:
     *     - given candidates, existing successful replied sources,
     *       current blacklist and graylist, use source_selector_ to pick
     *       a list of sources
     *     - send requests to the list of sources using request_func_.
     *       depending on the returned result, it may blacklist or graylist
     *       sources that failed to send request to.
     *     - on receiving a reply from source, if the reply indicates a
     *       successful response, perform aggregation using the `aggr_' functor,
     *       and checks if the round can be finalized. Otherwise, put the
     *       source into blacklist or graylist depending on the failure result.
     *     - on wave timeout, graylist all sources that have not yet responded.
     */
   public:
    Round(RoundID round_id,
          std::shared_ptr<SourceSet> candidates,
          std::shared_ptr<SourceSelectionFunc> source_selector,
          std::shared_ptr<SourceRequestFunc> request_func,
          std::shared_ptr<ObjectAggrFunc> aggr,
          std::shared_ptr<Options> options,
          std::shared_ptr<ObjectCallback> callback,
          ObjectPoller* parent);

    void start();

    RoundID getRoundID() const {
      return round_id_;
    }

    void onSourceReply(SourceID source,
                       RequestResult result,
                       folly::Optional<Response> response);

    //// used for testing
    Timer* getRoundTimer() {
      return round_timer_.get();
    }
    BackoffTimer* getWaveTimer() {
      return wave_timer_.get();
    }

   private:
    const RoundID round_id_;
    // parent ObjectPoller should always outlive the Round so it's safe to keep
    // a raw ptr
    ObjectPoller* const parent_;
    // private copy of parameters
    const std::shared_ptr<SourceSet> candidates_;
    const std::shared_ptr<SourceSelectionFunc> source_selector_;
    const std::shared_ptr<SourceRequestFunc> request_func_;
    const std::shared_ptr<ObjectAggrFunc> aggr_;
    const std::shared_ptr<Options> options_;

    // results to be passed to the poller on completion
    std::shared_ptr<ObjectCallback> callback_;
    std::unique_ptr<Object> result_obj_;

    bool started_{false};
    bool completed_{false};

    std::unique_ptr<Timer> round_timer_;
    std::unique_ptr<BackoffTimer> wave_timer_;

    // set of sources that have successfully replied, the round is
    // considered success if success_sources_.size() >=
    // options.num_responses_required_round
    SourceSet success_sources_;
    // current wave number, mainly used for logging and debugging
    size_t current_wave_{0};
    // sources selected to be contacted in the current wave
    SourceSet current_wave_sources_;
    // record the status of reply received in the current wave.
    // sources not in the map means not replied; cleared upon each wave starts;
    // used to determine which nodes to graylist on wave timeout
    std::unordered_map<SourceID, RequestResult> current_replies_;

    const Options& getOptions() const {
      ld_check(options_ != nullptr);
      return *options_;
    }

    size_t repliesNeeded() const {
      return getOptions().num_responses_required_round > success_sources_.size()
          ? getOptions().num_responses_required_round - success_sources_.size()
          : 0;
    }

    bool enoughSuccessReplies() const {
      return repliesNeeded() == 0;
    }

    void sendWave();
    void onWaveTimeout();
    void onRoundTimeout();
    void finalize();
    bool finalizeIfDone();
    void sendRequestsToSources();

    void graylistSource(SourceID source) {
      parent_->graylistSource(
          source, round_id_ + getOptions().graylist_effective_rounds);
    }

    void blacklistSource(SourceID source) {
      parent_->blacklistSource(
          source, round_id_ + getOptions().blacklist_effective_rounds);
    }
  };

  // must not be nullptr if state_ == ROUND_EXECUTION, otherwise nullptr.
  // contains context for the currently executing round
  std::unique_ptr<Round> current_round_;

  // controls the polling interval between rounds
  std::unique_ptr<Timer> round_interval_timer_;

  ///////// blacklisting and graylisting ////////

  struct SourceState {
    enum class State { GRAY_LISTED, BLACK_LISTED };
    State state;
    RoundID effective_until;
  };

  std::unordered_map<SourceID, SourceState, SourceIDHash> source_info_;

  void refreshSourceState() {
    RoundID current_round = getLastStartedRoundID();
    for (auto it = source_info_.begin(); it != source_info_.end();) {
      if (it->second.effective_until < current_round) {
        // source expired in graylist or blacklist, removing...
        it = source_info_.erase(it);
      } else {
        ++it;
      }
    }
  }

  // rules:
  // 1) blacklist a graylisted source will make the source blacklisted with
  //    the blacklisted ttl;
  // 2) graylist an already blacklisted source is a no-op
  void blacklistSource(SourceID source, RoundID effective_until) {
    source_info_[source] = {SourceState::State::BLACK_LISTED, effective_until};
  }

  void graylistSource(SourceID source, RoundID effective_until) {
    const auto value =
        SourceState{SourceState::State::GRAY_LISTED, effective_until};
    auto res = source_info_.insert(std::make_pair(source, value));
    if (!res.second) {
      if (res.first->second.state == SourceState::State::GRAY_LISTED) {
        res.first->second = value;
      }
    }
  }

  void clearBlackgraylist() {
    source_info_.clear();
  }

  std::pair<SourceSet, SourceSet> getBlackAndGraylist() const {
    SourceSet blacklist;
    SourceSet graylist;
    for (const auto& kv : source_info_) {
      switch (kv.second.state) {
        case SourceState::State::BLACK_LISTED:
          blacklist.insert(kv.first);
          break;
        case SourceState::State::GRAY_LISTED:
          graylist.insert(kv.first);
          break;
      }
    }
    return std::make_pair(std::move(blacklist), std::move(graylist));
  }

  void onRoundCompleted(RoundID round,
                        std::shared_ptr<ObjectCallback> callback,
                        Status status,
                        folly::Optional<Object> result);

  void startNewRound();
  void onRoundIntervalTimedout();
  void startRoundIntervalTimer();

  template <typename TimerPtr>
  bool timerActive(const TimerPtr& timer) {
    return timer != nullptr && timer->isActive();
  }

  template <typename TimerPtr>
  void cancelTimer(TimerPtr& timer) {
    if (timer) {
      timer->cancel();
    }
  }
};

}} // namespace facebook::logdevice

#define LOGDEVICE_OBJECT_POLLER_H_
#include "logdevice/common/ObjectPoller-inl.h"
#undef LOGDEVICE_OBJECT_POLLER_H_
