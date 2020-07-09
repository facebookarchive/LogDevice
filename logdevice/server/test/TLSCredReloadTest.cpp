/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <boost/filesystem.hpp>
#include <gtest/gtest.h>

#include "logdevice/common/test/TestUtil.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/test/TestUtil.h"

using namespace facebook::logdevice;
namespace fs = boost::filesystem;

class TLSCredReloadTest : public ::testing::Test {
 public:
  void touch(const std::string& fileName) {
    auto previous = fs::last_write_time(fileName);
    auto newTime = std::chrono::system_clock::to_time_t(
        std::chrono::system_clock::from_time_t(previous) +
        std::chrono::seconds(10));
    fs::last_write_time(fileName, newTime);
  }

  void SetUp() override {
    StatsParams params;
    params.setIsServer(true);
    stats_ = std::make_unique<StatsHolder>(std::move(params));
    initProcessor();
  }

  void TearDown() override {
    shutdown_test_server(processor);
  }

 private:
  std::unique_ptr<StatsHolder> stats_;

  std::string copyToTempFile(std::string path) {
    auto temp = fs::temp_directory_path() / fs::unique_path();
    fs::copy_file(path, temp);
    return temp.native();
  }

  void initProcessor() {
    Settings settings = create_default_settings<Settings>();
    settings.num_workers = 1;
    settings,
        settings.ssl_cert_path = copyToTempFile(
            TEST_SSL_FILE("logdevice_test_server_identity.cert"));
    settings.ssl_key_path = copyToTempFile(TEST_SSL_FILE("logdevice_test.key"));
    settings.ssl_ca_path =
        copyToTempFile(TEST_SSL_FILE("logdevice_test_valid_ca.cert"));
    settings.ssl_cert_refresh_interval = std::chrono::seconds(1);

    ServerSettings server_settings = create_default_settings<ServerSettings>();
    server_settings.tls_ticket_seeds_path =
        copyToTempFile(TEST_SSL_FILE("ssl_ticket_seeds.json"));
    server_settings.use_tls_ticket_seeds = true;
    auto processor_builder = TestServerProcessorBuilder{settings}
                                 .setServerSettings(server_settings)
                                 .setStatsHolder(stats_.get())
                                 .setMyNodeID(NodeID(0, 1));
    processor = std::move(processor_builder).build();
  }

 public:
  std::shared_ptr<ServerProcessor> processor;
};

TEST_F(TLSCredReloadTest, ReloadCerts) {
  wait_until([&]() {
    return processor->stats_->aggregate().ssl_context_created ==
        processor->getAllWorkersCount();
  });
  EXPECT_EQ(processor->getAllWorkersCount(),
            processor->stats_->aggregate().ssl_context_created);
  touch(processor->settings()->ssl_ca_path);
  wait_until([&]() {
    return processor->stats_->aggregate().ssl_context_created ==
        2 * processor->getAllWorkersCount();
  });
  EXPECT_EQ(2 * processor->getAllWorkersCount(),
            processor->stats_->aggregate().ssl_context_created);
}

TEST_F(TLSCredReloadTest, ReloadTLSTickets) {
  wait_until([&]() {
    return processor->stats_->aggregate().tls_ticket_seeds_reloaded ==
        processor->getAllWorkersCount();
  });
  EXPECT_EQ(processor->getAllWorkersCount(),
            processor->stats_->aggregate().tls_ticket_seeds_reloaded);
  touch(processor->updateableServerSettings().get()->tls_ticket_seeds_path);
  wait_until([&]() {
    return processor->stats_->aggregate().tls_ticket_seeds_reloaded ==
        2 * processor->getAllWorkersCount();
  });
  EXPECT_EQ(2 * processor->getAllWorkersCount(),
            processor->stats_->aggregate().tls_ticket_seeds_reloaded);
}
