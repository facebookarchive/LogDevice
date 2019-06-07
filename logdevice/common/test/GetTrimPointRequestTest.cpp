/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/GetTrimPointRequest.h"

#include <functional>

#include <folly/Memory.h>
#include <gtest/gtest.h>

#include "logdevice/common/debug.h"
#include "logdevice/common/test/MockBackoffTimer.h"
#include "logdevice/common/test/MockNodeSetAccessor.h"
#include "logdevice/common/test/MockNodeSetFinder.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/include/types.h"

using namespace facebook::logdevice;

namespace {

class MockGetTrimPointRequest : public GetTrimPointRequest {
 public:
  MockGetTrimPointRequest(int storage_set_size, ReplicationProperty replication)
      : GetTrimPointRequest(logid_t(1), std::chrono::seconds(1)),
        replication_(replication) {
    Configuration::NodesConfig nodes_config =
        createSimpleNodesConfig(storage_set_size);
    Configuration::MetaDataLogsConfig meta_config =
        createMetaDataLogsConfig(nodes_config,
                                 nodes_config.getNodes().size(),
                                 replication.getReplicationFactor());
    config_ = ServerConfig::fromDataTest(
        __FILE__, std::move(nodes_config), std::move(meta_config));

    storage_set_.reserve(storage_set_size);
    for (node_index_t nid = 0; nid < storage_set_size; ++nid) {
      storage_set_.emplace_back(ShardID(nid, 1));
    }
  }

 protected: // mock stuff that communicates externally
  void sendTo(ShardID) override {}

  std::shared_ptr<const configuration::nodes::NodesConfiguration>
  getNodesConfiguration() const override {
    return config_->getNodesConfigurationFromServerConfigSource();
  }

  void updateTrimPoint(Status status, lsn_t trim_point) const override {
    if (status == E::OK && trim_point > trim_point_) {
      trim_point_ = trim_point;
    }
  }

 public:
  mutable lsn_t trim_point_ = LSN_INVALID;

 private:
  StorageSet storage_set_;
  ReplicationProperty replication_;
  std::shared_ptr<ServerConfig> config_;
};

static ShardID node(node_index_t index) {
  return ShardID(index, 1);
}

TEST(GetTrimPointRequestTest, Normal) {
  MockGetTrimPointRequest req(
      5, ReplicationProperty({{NodeLocationScope::NODE, 3}}));
  req.onReply(node(1), E::OK, 10);
  ASSERT_EQ(10, req.trim_point_);
  req.onReply(node(1), E::OK, 8);
  ASSERT_EQ(10, req.trim_point_);
  req.onReply(node(1), E::OK, 20);
  ASSERT_EQ(20, req.trim_point_);
}

TEST(GetTrimPointRequestTest, Fail) {
  MockGetTrimPointRequest req(
      5, ReplicationProperty({{NodeLocationScope::NODE, 3}}));
  req.onReply(node(1), E::SHUTDOWN, 10);
  ASSERT_EQ(LSN_INVALID, req.trim_point_);
  req.onReply(node(1), E::OK, 8);
  ASSERT_EQ(8, req.trim_point_);
  req.onReply(node(1), E::NOTSUPPORTED, 20);
  ASSERT_EQ(8, req.trim_point_);
}

} // namespace
