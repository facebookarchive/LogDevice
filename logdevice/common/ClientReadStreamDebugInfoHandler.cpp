/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/ClientReadStreamDebugInfoHandler.h"

#include "logdevice/common/configuration/if/gen-cpp2/AllReadStreamsDebugConfig_constants.h"

namespace {

std::chrono::seconds currentTimeInSeconds() {
  return std::chrono::duration_cast<std::chrono::seconds>(
      std::chrono::system_clock::now().time_since_epoch());
}

constexpr folly::StringPiece kMatchAll =
    facebook::logdevice::configuration::all_read_streams_debug_config::thrift::
        AllReadStreamsDebugConfig_constants::kMatchAll();
} // namespace

namespace facebook::logdevice {

ClientReadStreamDebugInfoHandler::ClientReadStreamDebugInfoHandler(
    const std::string& csid,
    folly::EventBase* eb,
    TimeoutType sampling_interval,
    std::shared_ptr<PluginRegistry> plugin_regsitry,
    const std::string& config_path,
    AllClientReadStreams& allClientReadStreams)
    : csid_{csid},
      eb_{eb},
      sampling_interval_{std::move(sampling_interval)},
      config_{std::move(plugin_regsitry), config_path},
      allClientReadStreams_{allClientReadStreams} {
  sampleDebugInfoTimer_ =
      folly::AsyncTimeout::make(*eb_, [this]() noexcept { sampleDebugInfo(); });

  eb_->runInEventBaseThread([this]() {
    AllClientReadStreams::Subscriber subscription{
        .onStreamAdd = [this](const auto& stream) { addStream(stream); },
        .onStreamRemoved =
            [this](const auto& stream) { removeStream(stream); }};

    allClientReadStreams_.subscribe(std::move(subscription));
  });

  config_.setUpdateCallback([this](const auto& config) {
    eb_->runInEventBaseThread([this, config]() { buildIndex(config); });
  });
}

ClientReadStreamDebugInfoHandler::~ClientReadStreamDebugInfoHandler() {
  eb_->runImmediatelyOrRunInEventBaseThreadAndWait([this]() {
    config_.unsetUpdateCallback();
    sampleDebugInfoTimer_->cancelTimeout();
    streams_.clear();
    rules_.clear();
    allClientReadStreams_.unsubscribe();
  });
}

void ClientReadStreamDebugInfoHandler::schedule() {
  if (!streams_.empty() && !sampleDebugInfoTimer_->isScheduled()) {
    sampleDebugInfoTimer_->scheduleTimeout(sampling_interval_);
  }
}

void ClientReadStreamDebugInfoHandler::buildIndex(const Config& config) {
  ld_check(eb_->inRunningEventBaseThread());

  const auto timestamp = currentTimeInSeconds();
  rules_.clear();
  for (const auto& rule : *config.configs_ref()) {
    if (timestamp.count() > *rule.deadline_ref()) {
      continue;
    }
    if (*rule.csid_ref() == csid_ || *rule.csid_ref() == kMatchAll) {
      const auto deadline = std::chrono::seconds(*rule.deadline_ref());
      const auto& reader_name = *rule.reader_name_ref();
      rules_[reader_name] = std::max(deadline, rules_[reader_name]);
    }
  }

  streams_.clear();
  allClientReadStreams_.forEachStream([this](auto&& stream) {
    registerStream(stream.getID(), stream.getReaderName());
  });

  schedule();
}

void ClientReadStreamDebugInfoHandler::registerStream(
    const read_stream_id_t& id,
    const std::string& reader_name) {
  const auto timestamp = currentTimeInSeconds();
  const auto matchAllDeadline =
      rules_.contains(kMatchAll) ? rules_[kMatchAll] : timestamp;
  const auto matchExactDeadline =
      rules_.contains(reader_name) ? rules_[reader_name] : timestamp;
  const auto deadline = std::max(matchAllDeadline, matchExactDeadline);
  if (deadline > timestamp) {
    streams_[id] = deadline;
  }
}

void ClientReadStreamDebugInfoHandler::addStream(
    const ClientReadStream& stream) {
  ld_check(eb_->inRunningEventBaseThread());
  registerStream(stream.getID(), stream.getReaderName());
  schedule();
}

void ClientReadStreamDebugInfoHandler::removeStream(
    const ClientReadStream& stream) {
  ld_check(eb_->inRunningEventBaseThread());
  streams_.erase(stream.getID());
}

void ClientReadStreamDebugInfoHandler::eraseExpired() noexcept {
  ld_check(eb_->inRunningEventBaseThread());
  auto timestamp = currentTimeInSeconds();
  folly::erase_if(
      streams_, [&timestamp](const auto& kv) { return timestamp > kv.second; });
  folly::erase_if(
      rules_, [&timestamp](const auto& kv) { return timestamp > kv.second; });
}

void ClientReadStreamDebugInfoHandler::sampleDebugInfo() noexcept {
  eraseExpired();
  for (const auto& [rsid, _] : streams_) {
    auto stream = allClientReadStreams_.getStream(rsid);
    if (stream) {
      stream->sampleDebugInfo();
    }
  }

  schedule();
}

} // namespace facebook::logdevice
