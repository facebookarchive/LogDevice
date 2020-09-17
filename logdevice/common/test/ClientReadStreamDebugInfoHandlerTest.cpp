/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/ClientReadStreamDebugInfoHandler.h"

#include <chrono>
#include <optional>

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <folly/Random.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include <folly/futures/Promise.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "logdevice/common/ReadStreamDebugInfoSamplingConfig.h"
#include "logdevice/common/ThriftCodec.h"
#include "logdevice/common/client_read_stream/ClientReadStreamBufferFactory.h"
#include "logdevice/common/plugin/PluginRegistry.h"
#include "logdevice/common/test/TestUtil.h"

using namespace facebook::logdevice;
using namespace ::testing;
using namespace configuration::all_read_streams_debug_config::thrift;

using namespace std::literals::chrono_literals;

namespace {

class MockClientReadStreamDebugInfoHandler
    : public ClientReadStreamDebugInfoHandler {
 public:
  MockClientReadStreamDebugInfoHandler(
      const std::string& csid,
      folly::EventBase* eb,
      TimeoutType sampling_interval,
      std::shared_ptr<PluginRegistry> plugin_regsitry,
      const std::string& config_path,
      AllClientReadStreams& allClientReadStreams)
      : ClientReadStreamDebugInfoHandler{csid,
                                         eb,
                                         sampling_interval,
                                         plugin_regsitry,
                                         config_path,
                                         allClientReadStreams},
        eb_{eb} {}

  void add(const ClientReadStream& stream) {
    eb_->runImmediatelyOrRunInEventBaseThreadAndWait(
        [this, &stream]() { addStream(stream); });
  }

  void remove(const ClientReadStream& stream) {
    eb_->runImmediatelyOrRunInEventBaseThreadAndWait(
        [this, &stream]() { removeStream(stream); });
  }

  std::vector<read_stream_id_t>
  waitForNextCall(std::chrono::microseconds timeout = 1s) {
    return folly::makeSemiFuture()
        .via(eb_)
        .thenValue([this](auto&&) { return nextCall_.getSemiFuture(); })
        .via(eb_)
        .get(timeout);
  }

 protected:
  void sampleDebugInfo() noexcept override {
    ClientReadStreamDebugInfoHandler::sampleDebugInfo();

    std::vector<read_stream_id_t> streamIds;
    streamIds.reserve(streamIds.size());
    for (const auto& [rsid, _] : streams()) {
      streamIds.push_back(rsid);
    }

    nextCall_.setValue(std::move(streamIds));
    nextCall_ = {};
  }

  folly::EventBase* eb_;
  folly::Promise<std::vector<read_stream_id_t>> nextCall_;
};

struct ClientReadStreamDebugInfoHandlerTest : public ::testing::Test {
  std::string uuid() {
    return boost::uuids::to_string(boost::uuids::random_generator()());
  }

  int64_t adjustanceTimestamp(int64_t shift = 50000) {
    return std::chrono::duration_cast<std::chrono::seconds>(
               std::chrono::system_clock::now().time_since_epoch())
               .count() +
        shift;
  }

  std::unique_ptr<ClientReadStream> getClientReadStream(
      std::string reader_name,
      std::string csid,
      read_stream_id_t id = read_stream_id_t{folly::Random::rand64()}) {
    constexpr logid_t log_id{1};
    auto dependencies = std::make_unique<ClientReadStreamDependencies>(
        id,
        log_id,
        csid,
        [](auto&&...) { return false; },
        [](auto&&...) { return false; },
        [](auto&&...) {},
        nullptr,
        [](auto&&...) {});
    dependencies->setReaderName(reader_name);
    return std::make_unique<ClientReadStream>(
        id,
        log_id,
        compose_lsn(EPOCH_MIN, ESN_MIN),
        LSN_MAX,
        0.5,
        ClientReadStreamBufferType::CIRCULAR,
        1,
        std::move(dependencies),
        std::make_shared<UpdateableConfig>(),
        nullptr,
        nullptr);
  }

  AllReadStreamsDebugConfig
  buildConfig(const std::string& csid,
              int64_t deadline,
              std::optional<std::string> reader_name = std::nullopt) {
    AllReadStreamsDebugConfig config;
    config.csid_ref() = csid;
    config.deadline_ref() = deadline;
    config.reader_name_ref() = reader_name.value_or("*");
    return config;
  }

  std::unique_ptr<MockClientReadStreamDebugInfoHandler>
  getClientReadStreamDebugInfoHandler(
      std::string csid,
      AllReadStreamsDebugConfigs config,
      AllClientReadStreams& allClientReadStreams,
      std::chrono::milliseconds sampling_rate = 100ms) {
    return std::make_unique<MockClientReadStreamDebugInfoHandler>(
        std::move(csid),
        ioExecutor->getEventBase(),
        sampling_rate,
        make_test_plugin_registry(),
        "data: " +
            ThriftCodec::serialize<apache::thrift::SimpleJSONSerializer>(
                config),
        allClientReadStreams);
  }

  std::string csid = uuid();
  AllReadStreamsDebugConfigs config;
  AllClientReadStreams allClientReadStreams;
  std::unique_ptr<folly::IOThreadPoolExecutor> ioExecutor =
      std::make_unique<folly::IOThreadPoolExecutor>(1);
};

TEST_F(ClientReadStreamDebugInfoHandlerTest, CsidNotSampled) {
  config.configs_ref() = {buildConfig(uuid(), adjustanceTimestamp())};
  auto handler =
      getClientReadStreamDebugInfoHandler(csid, config, allClientReadStreams);

  for (int i = 0; i < 20; ++i) {
    handler->add(*getClientReadStream(uuid(), csid));
  }

  EXPECT_THROW(handler->waitForNextCall(), folly::FutureTimeout);
}

TEST_F(ClientReadStreamDebugInfoHandlerTest, SpecificReaderNameSample) {
  config.configs_ref() = {
      buildConfig(csid, adjustanceTimestamp(), "test-reader-name-1")};
  auto handler =
      getClientReadStreamDebugInfoHandler(csid, config, allClientReadStreams);

  for (int i = 0; i < 10; ++i) {
    auto clientReadStream = getClientReadStream(uuid(), csid);
    handler->add(*getClientReadStream(uuid(), csid));
  }

  std::vector<read_stream_id_t> expectedStreams;
  for (int i = 0; i < 10; ++i) {
    auto clientReadStream = getClientReadStream("test-reader-name-1", csid);
    expectedStreams.push_back(clientReadStream->getID());
    handler->add(*clientReadStream);
  }

  EXPECT_THAT(
      handler->waitForNextCall(), UnorderedElementsAreArray(expectedStreams));
}

TEST_F(ClientReadStreamDebugInfoHandlerTest, AddDeleteAndSampleProperly) {
  std::string reader_name = "test-reader-name-1";
  config.configs_ref() = {
      buildConfig(csid, adjustanceTimestamp(), reader_name)};
  auto handler = getClientReadStreamDebugInfoHandler(
      csid, config, allClientReadStreams, 5s);

  std::vector<read_stream_id_t> remove_ids;
  std::vector<read_stream_id_t> expectedStreams;
  for (int i = 0; i < 20; ++i) {
    auto clientReadStream = getClientReadStream(reader_name, csid);
    if (i < 10) {
      remove_ids.push_back(clientReadStream->getID());
    } else {
      expectedStreams.push_back(clientReadStream->getID());
    }
    handler->add(*clientReadStream);
  }

  for (const auto& id : remove_ids) {
    handler->remove(*getClientReadStream(reader_name, csid, id));
  }

  EXPECT_THAT(handler->waitForNextCall(10s),
              UnorderedElementsAreArray(expectedStreams));
}

TEST_F(ClientReadStreamDebugInfoHandlerTest, MatchAll) {
  config.configs_ref() = {buildConfig("*", adjustanceTimestamp(), "*")};

  std::unordered_map<std::string, std::vector<read_stream_id_t>> streams;
  std::unordered_map<std::string,
                     std::unique_ptr<MockClientReadStreamDebugInfoHandler>>
      handlers;

  for (int i = 0; i < 10; ++i) {
    std::string csid = uuid();
    auto handler =
        getClientReadStreamDebugInfoHandler(csid, config, allClientReadStreams);

    for (int j = 0; j < 10; ++j) {
      auto clientReadStream = getClientReadStream(uuid(), csid);
      streams[csid].push_back(clientReadStream->getID());
      handler->add(*clientReadStream);
    }
    handlers[csid] = std::move(handler);
  }

  for (auto&& [csid, expectedStreams] : streams) {
    EXPECT_THAT(handlers[csid]->waitForNextCall(),
                UnorderedElementsAreArray(expectedStreams));
  }
}

TEST_F(ClientReadStreamDebugInfoHandlerTest, MultipleRules) {
  config.configs_ref() = {
      buildConfig(csid, adjustanceTimestamp(), "test-reader-name-0"),
      buildConfig("*", adjustanceTimestamp(), "test-reader-name-1"),
      buildConfig(uuid(), adjustanceTimestamp(), "test-reader-name-2"),
      buildConfig(csid, 1, "test-reader-name-3"),
      buildConfig("*", 1, "test-reader-name-4"),
      buildConfig("*", 1, "*"),
      buildConfig(uuid(), 1, "test-reader-name-5"),
  };
  auto handler =
      getClientReadStreamDebugInfoHandler(csid, config, allClientReadStreams);

  for (int i = 0; i < 10; ++i) {
    handler->add(*getClientReadStream(uuid(), csid));
  }

  std::vector<read_stream_id_t> expectedStreams;
  for (int nameIdx = 0; nameIdx < 6; ++nameIdx) {
    auto name = "test-reader-name-" + std::to_string(nameIdx);
    for (int i = 0; i < 10; ++i) {
      auto clientReadStream = getClientReadStream(name, csid);
      if (nameIdx <= 1) {
        expectedStreams.push_back(clientReadStream->getID());
      }
      handler->add(*clientReadStream);
    }
  }

  EXPECT_THAT(
      handler->waitForNextCall(), UnorderedElementsAreArray(expectedStreams));
}

} // namespace
