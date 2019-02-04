/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#ifndef LOGDEVICE_OBJECT_POLLER_H_
#error "This should only be included by ObjectPoller.h"
#endif

namespace facebook { namespace logdevice {

template <typename Object,
          typename Response,
          typename SourceID,
          typename SourceIDHash>
ObjectPoller<Object, Response, SourceID, SourceIDHash>::ObjectPoller(
    SourceSet candidates,
    SourceSelectionFunc source_selector,
    SourceRequestFunc request_func,
    ObjectAggrFunc aggr,
    ObjectCallback callback,
    Options options)
    : candidates_(std::make_shared<SourceSet>(std::move(candidates))),
      source_selector_(
          std::make_shared<SourceSelectionFunc>(std::move(source_selector))),
      request_func_(
          std::make_shared<SourceRequestFunc>(std::move(request_func))),
      aggr_(std::make_shared<ObjectAggrFunc>(std::move(aggr))),
      callback_(std::make_shared<ObjectCallback>(std::move(callback))),
      options_(std::make_shared<Options>(std::move(options))) {
  // all function pointers and callbacks should not be null
  ld_check(*source_selector_ != nullptr);
  ld_check(*request_func_ != nullptr);
  ld_check(*aggr_ != nullptr);
  ld_check(*callback_ != nullptr);
}

template <typename Object,
          typename Response,
          typename SourceID,
          typename SourceIDHash>
void ObjectPoller<Object, Response, SourceID, SourceIDHash>::
    startRoundIntervalTimer() {
  if (round_interval_timer_ == nullptr) {
    round_interval_timer_ =
        createTimer([this]() { onRoundIntervalTimedout(); });
  }
  round_interval_timer_->activate(getOptions().round_interval);
}

template <typename Object,
          typename Response,
          typename SourceID,
          typename SourceIDHash>
void ObjectPoller<Object, Response, SourceID, SourceIDHash>::start() {
  if (state_ != PollerState::STOPPED) {
    // no-op if poller is in STARTED or ROUND_EXECUTION
    return;
  }

  ld_check(current_round_ == nullptr);
  ld_check(!timerActive(round_interval_timer_));
  state_ = PollerState::STARTED;
  startNewRound();
}

template <typename Object,
          typename Response,
          typename SourceID,
          typename SourceIDHash>
void ObjectPoller<Object, Response, SourceID, SourceIDHash>::stop() {
  switch (state_) {
    case PollerState::STOPPED:
      ld_check(current_round_ == nullptr);
      ld_check(!timerActive(round_interval_timer_));
      break;
    case PollerState::STARTED:
      ld_check(current_round_ == nullptr);
      break;
    case PollerState::ROUND_EXECUTION:
      ld_check(current_round_ != nullptr);
      ld_check(!timerActive(round_interval_timer_));
      break;
  }

  cancelTimer(round_interval_timer_);
  current_round_.reset();
  clearBlackgraylist();
  state_ = PollerState::STOPPED;
}

template <typename Object,
          typename Response,
          typename SourceID,
          typename SourceIDHash>
void ObjectPoller<Object, Response, SourceID, SourceIDHash>::
    onRoundIntervalTimedout() {
  // state cannot be STOPPED or ROUND_EXECUTION as the interval
  // timer would have been stopped or not activated
  ld_check(state_ == PollerState::STARTED);
  ld_check(current_round_ == nullptr);
  startNewRound();
}

template <typename Object,
          typename Response,
          typename SourceID,
          typename SourceIDHash>
void ObjectPoller<Object, Response, SourceID, SourceIDHash>::startNewRound() {
  ld_check(state_ == PollerState::STARTED);
  ld_check(current_round_ == nullptr);
  ld_check(!timerActive(round_interval_timer_));
  RoundID round_id = ++last_round_started_;
  // recompute blacklist and graylist based on their TTL
  refreshSourceState();
  current_round_ = std::make_unique<Round>(round_id,
                                           candidates_,
                                           source_selector_,
                                           request_func_,
                                           aggr_,
                                           options_,
                                           callback_,
                                           this);
  state_ = PollerState::ROUND_EXECUTION;
  current_round_->start();
}

template <typename Object,
          typename Response,
          typename SourceID,
          typename SourceIDHash>
void ObjectPoller<Object, Response, SourceID, SourceIDHash>::onRoundCompleted(
    RoundID round,
    std::shared_ptr<ObjectCallback> callback,
    Status status,
    folly::Optional<Object> result) {
  ld_check(callback != nullptr);
  if (state_ != PollerState::ROUND_EXECUTION) {
    // stale completion
    return;
  }

  ld_check(current_round_ != nullptr);
  if (current_round_->getRoundID() != round) {
    ld_assert(current_round_->getRoundID() > round);
    // stale completion
    return;
  }

  // everything needed for delivering the result of the round has been
  // passed as function argument, it's safe to destroy the Round
  current_round_.reset();
  switch (getOptions().mode) {
    case Mode::ONE_TIME:
      state_ = PollerState::STOPPED;
      break;
    case Mode::CONTINUOUS:
      state_ = PollerState::STARTED;
      // schedule the next periodical polling
      startRoundIntervalTimer();
      break;
  }

  (*callback)(status, round, std::move(result));
  // `*this' may get destroyed after the callback
}

template <typename Object,
          typename Response,
          typename SourceID,
          typename SourceIDHash>
void ObjectPoller<Object, Response, SourceID, SourceIDHash>::onSourceReply(
    RoundID round,
    SourceID source,
    RequestResult result,
    folly::Optional<Response> response) {
  if (state_ != PollerState::ROUND_EXECUTION) {
    // stale reply
    return;
  }

  ld_check(current_round_ != nullptr);
  if (current_round_->getRoundID() != round) {
    // _round_ may come from other sources, use dd_assert instead
    dd_assert(current_round_->getRoundID() > round,
              "Invalid round ID %lu with current round %lu.",
              round,
              current_round_->getRoundID());
    // stale reply
    return;
  }

  current_round_->onSourceReply(source, result, std::move(response));
}

//////////// Single Round Execution ///////////////

template <typename Object,
          typename Response,
          typename SourceID,
          typename SourceIDHash>
ObjectPoller<Object, Response, SourceID, SourceIDHash>::Round::Round(
    RoundID round_id,
    std::shared_ptr<SourceSet> candidates,
    std::shared_ptr<SourceSelectionFunc> source_selector,
    std::shared_ptr<SourceRequestFunc> request_func,
    std::shared_ptr<ObjectAggrFunc> aggr,
    std::shared_ptr<Options> options,
    std::shared_ptr<ObjectCallback> callback,
    ObjectPoller* parent)
    : round_id_(round_id),
      parent_(parent),
      candidates_(std::move(candidates)),
      source_selector_(std::move(source_selector)),
      request_func_(std::move(request_func)),
      aggr_(std::move(aggr)),
      options_(std::move(options)),
      callback_(std::move(callback)) {
  ld_check(parent_ != nullptr);
  ld_check(callback_ != nullptr);
}

template <typename Object,
          typename Response,
          typename SourceID,
          typename SourceIDHash>
void ObjectPoller<Object, Response, SourceID, SourceIDHash>::Round::start() {
  // start() should only be called once
  ld_check(!started_);
  ld_check(round_timer_ == nullptr);

  started_ = true;
  // enforce the timeout for the entire round
  round_timer_ = parent_->createTimer([this]() { onRoundTimeout(); });
  round_timer_->activate(getOptions().round_timeout);

  sendWave();
}

template <typename Object,
          typename Response,
          typename SourceID,
          typename SourceIDHash>
void ObjectPoller<Object, Response, SourceID, SourceIDHash>::Round::sendWave() {
  ld_check(started_);
  ld_check(!completed_);

  ++current_wave_;
  current_replies_.clear();
  SourceSet blacklist;
  SourceSet graylist;
  std::tie(blacklist, graylist) = parent_->getBlackAndGraylist();

  // otherwise the round should already finished
  ld_assert(repliesNeeded() > 0);
  size_t replies_needed = repliesNeeded();

  // we are OK to only have just one source selected this wave in the worst
  // case, as more progress can be made in subsequent waves
  size_t sources_required_this_wave = 1;
  size_t extra_sources_this_wave = replies_needed +
      getOptions().extras_request_each_wave - sources_required_this_wave;

  current_wave_sources_ = (*source_selector_)(*candidates_,
                                              /*existing=*/success_sources_,
                                              blacklist,
                                              graylist,
                                              sources_required_this_wave,
                                              extra_sources_this_wave);
  if (current_wave_sources_.empty()) {
    // source selection failed
    RATELIMIT_WARNING(
        std::chrono::seconds(10),
        2,
        "Failed to pick a source set in round %lu, wave %lu, candidates size: "
        "%lu, existing: %lu, blacklist: %lu, graylist: %lu, required: %lu, "
        "extras: %lu.",
        round_id_,
        current_wave_,
        candidates_->size(),
        success_sources_.size(),
        blacklist.size(),
        graylist.size(),
        sources_required_this_wave,
        extra_sources_this_wave);
  } else {
    sendRequestsToSources();
    if (finalizeIfDone()) {
      // in case that no candidate can ever be selected in subsequent waves,
      // we can conclude the round early
      return;
    }
  }

  // start wave timer for the next wave
  if (wave_timer_ == nullptr) {
    wave_timer_ = parent_->createBackoffTimer(
        getOptions().wave_timeout, [this]() { onWaveTimeout(); });
  }
  wave_timer_->activate();
}

template <typename Object,
          typename Response,
          typename SourceID,
          typename SourceIDHash>
void ObjectPoller<Object, Response, SourceID, SourceIDHash>::Round::
    sendRequestsToSources() {
  ld_check(started_);
  ld_check(!completed_);

  ld_check(!current_wave_sources_.empty());

  for (SourceID s : current_wave_sources_) {
    RequestResult res = (*request_func_)(round_id_, s);
    switch (res) {
      case RequestResult::OK:
        break;
      case RequestResult::FAILURE_TRANSIENT:
        break;
      case RequestResult::FAILURE_GRAYLIST:
        graylistSource(s);
        break;
      case RequestResult::FAILURE_BLACKLIST:
        blacklistSource(s);
        break;
    }
  }
}

template <typename Object,
          typename Response,
          typename SourceID,
          typename SourceIDHash>
void ObjectPoller<Object, Response, SourceID, SourceIDHash>::Round::
    onWaveTimeout() {
  ld_check(started_);
  ld_check(!completed_);

  // for all sources in the previous wave that has not yet responded,
  // put them into graylist.
  for (SourceID s : current_wave_sources_) {
    if (current_replies_.count(s) == 0 && success_sources_.count(s) == 0) {
      // note: since we do not track wave numbers in replies, current_replies_
      // map can be populated by replies from previous waves. The worst case is
      // that the Poller does not graylist some sources on wave timeout because
      // of a FAILURE_TRANSIENT reply from previous waves, which is acceptable.
      graylistSource(s);
    }
  }

  // send the next wave
  sendWave();
}

template <typename Object,
          typename Response,
          typename SourceID,
          typename SourceIDHash>
void ObjectPoller<Object, Response, SourceID, SourceIDHash>::Round::
    onRoundTimeout() {
  ld_check(started_);
  ld_check(!completed_);
  finalize();
}

template <typename Object,
          typename Response,
          typename SourceID,
          typename SourceIDHash>
void ObjectPoller<Object, Response, SourceID, SourceIDHash>::Round::
    onSourceReply(SourceID source,
                  RequestResult result,
                  folly::Optional<Response> response) {
  if (!started_ || completed_) {
    return;
  }

  if (success_sources_.count(source) > 0) {
    // the reply has already been processed for this round, ignoring it
    return;
  }

  // record the reply of the source in the current wave window
  current_replies_[source] = result;

  switch (result) {
    case RequestResult::OK: {
      // caller should provide a valid response
      ld_check(response.hasValue());
      success_sources_.insert(source);

      auto aggr_result = (*aggr_)(result_obj_.get(), response.value());
      if (aggr_result.hasValue()) {
        result_obj_ = std::make_unique<Object>(std::move(aggr_result.value()));
      } // else no changes to the aggregation result is needed

    } break;
    case RequestResult::FAILURE_TRANSIENT: {
    } break;
    case RequestResult::FAILURE_GRAYLIST: {
      graylistSource(source);
    } break;
    case RequestResult::FAILURE_BLACKLIST: {
      blacklistSource(source);
    } break;
  }

  // TODO: in case we have received all in-flight requests for the round
  // but there is not enough success replies, we can start a new wave
  // early instead of wait for the wave timeout

  finalizeIfDone();
}

template <typename Object,
          typename Response,
          typename SourceID,
          typename SourceIDHash>
bool ObjectPoller<Object, Response, SourceID, SourceIDHash>::Round::
    finalizeIfDone() {
  // 1) enough (i.e., num_responses_required_round_) response has been received;
  if (enoughSuccessReplies()) {
    finalize();
    return true;
  }

  // 2) TODO: there are no viable candidate to choose from (i.e., candidates are
  //    either successfully replied or blacklisted)
  //

  // Otherwise the round is not concluded
  return false;
}

template <typename Object,
          typename Response,
          typename SourceID,
          typename SourceIDHash>
void ObjectPoller<Object, Response, SourceID, SourceIDHash>::Round::finalize() {
  ld_check(started_);
  ld_check(!completed_);

  // cancel all timers and suppress callbacks before completion
  round_timer_.reset();
  wave_timer_.reset();
  completed_ = true;

  Status status = enoughSuccessReplies()
      ? Status::OK
      : (result_obj_ != nullptr ? Status::PARTIAL : Status::FAILED);

  folly::Optional<Object> result;
  if (result_obj_ != nullptr) {
    result.assign(std::move(*result_obj_));
  }

  parent_->postRoundResult(
      round_id_, std::move(callback_), status, std::move(result));
}

////////////// virtual functions /////////////////

template <typename Object,
          typename Response,
          typename SourceID,
          typename SourceIDHash>
std::unique_ptr<Timer>
ObjectPoller<Object, Response, SourceID, SourceIDHash>::createTimer(
    std::function<void()> callback) {
  return std::make_unique<Timer>(callback);
}

template <typename Object,
          typename Response,
          typename SourceID,
          typename SourceIDHash>
std::unique_ptr<BackoffTimer>
ObjectPoller<Object, Response, SourceID, SourceIDHash>::createBackoffTimer(
    const chrono_expbackoff_t<std::chrono::milliseconds>& backoff,
    std::function<void()> callback) {
  auto timer =
      std::make_unique<ExponentialBackoffTimer>(std::move(callback), backoff);
  return std::move(timer);
}

// called when a round finalizes. Asyncrhonously defer publishing the round
// result to the next event loop execution
template <typename Object,
          typename Response,
          typename SourceID,
          typename SourceIDHash>
void ObjectPoller<Object, Response, SourceID, SourceIDHash>::postRoundResult(
    RoundID round,
    std::shared_ptr<ObjectCallback> callback,
    Status status,
    folly::Optional<Object> result) {
  if (round_result_timer_ == nullptr) {
    round_result_timer_ = createTimer({});
  }

  round_result_timer_->setCallback([this,
                                    round,
                                    status,
                                    obj_cb = std::move(callback),
                                    obj_result = std::move(result)]() mutable {
    onRoundCompleted(round, std::move(obj_cb), status, std::move(obj_result));
  });
  round_result_timer_->activate(std::chrono::milliseconds::zero());
}

}} // namespace facebook::logdevice
