/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/GetHeadAttributesRequest.h"

#include <functional>

#include <folly/Memory.h>
#include <gtest/gtest.h>

#include "logdevice/common/debug.h"
#include "logdevice/common/test/MockNodeSetAccessor.h"
#include "logdevice/common/test/MockNodeSetFinder.h"
#include "logdevice/common/test/MockSequencerRouter.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/types.h"

using namespace facebook::logdevice;

namespace {

class Callback {
 public:
  void operator()(Status status, std::unique_ptr<LogHeadAttributes> attr) {
    called_ = true;
    attr_ = std::move(attr);
    status_ = status;
  }

  void assertNotCalled() {
    ASSERT_FALSE(called_);
  }

  void assertCalled(Status expected_status, LogHeadAttributes expected_attr) {
    ASSERT_TRUE(called_);
    ASSERT_EQ(expected_attr.trim_point, attr_->trim_point);
    ASSERT_EQ(expected_attr.trim_point_timestamp, attr_->trim_point_timestamp);
    ASSERT_EQ(expected_status, status_);
  }

 private:
  bool called_ = false;
  std::unique_ptr<LogHeadAttributes> attr_;
  Status status_;
};

class MockGetHeadAttributesRequest : public GetHeadAttributesRequest {
 public:
  MockGetHeadAttributesRequest(int storage_set_size,
                               ReplicationProperty replication,
                               Callback& callback)
      : GetHeadAttributesRequest(logid_t(1),
                                 std::chrono::seconds(1),
                                 std::ref(callback)),
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

    initNodeSetFinder();
  }

 protected:
  void deleteThis() override {}

  StorageSetAccessor::SendResult sendTo(ShardID) override {
    return {StorageSetAccessor::Result::SUCCESS, Status::OK};
  }

  std::unique_ptr<StorageSetAccessor> makeStorageSetAccessor(
      const std::shared_ptr<const configuration::nodes::NodesConfiguration>&
          nodes_configuration,
      StorageSet shards,
      ReplicationProperty minRep,
      StorageSetAccessor::ShardAccessFunc shard_access,
      StorageSetAccessor::CompletionFunc completion) override {
    return std::make_unique<MockStorageSetAccessor>(
        logid_t(1),
        shards,
        nodes_configuration,
        minRep,
        shard_access,
        completion,
        StorageSetAccessor::Property::FMAJORITY,
        std::chrono::seconds(1));
  }

  std::unique_ptr<NodeSetFinder> makeNodeSetFinder() override {
    return std::make_unique<MockNodeSetFinder>(
        storage_set_, replication_, [this](Status status) {
          this->start(status);
        });
  }

  std::shared_ptr<const configuration::nodes::NodesConfiguration>
  getNodesConfiguration() const override {
    return config_->getNodesConfigurationFromServerConfigSource();
  }

  void onShardStatusChanged() override {}

 private:
  StorageSet storage_set_;
  ReplicationProperty replication_;
  std::shared_ptr<ServerConfig> config_;
};

ShardID node(node_index_t index) {
  return ShardID(index, 1);
}

} // namespace

TEST(GetHeadAttributesRequestTest, Basic) {
  Callback cb;
  MockGetHeadAttributesRequest req(
      5, ReplicationProperty({{NodeLocationScope::NODE, 3}}), cb);
  req.onReply(
      node(0),
      E::OK,
      {compose_lsn(epoch_t(1), esn_t(10)), std::chrono::milliseconds(100)});
  cb.assertNotCalled();
  req.onReply(
      node(1),
      E::OK,
      {compose_lsn(epoch_t(3), esn_t(5)), std::chrono::milliseconds(130)});
  cb.assertNotCalled();
  req.onReply(
      node(2),
      E::OK,
      {compose_lsn(epoch_t(2), esn_t(10)), std::chrono::milliseconds::max()});
  // Expected to get trim point for node 3 as a highest value and
  // trim point timestamp from note 0 as a lowest value.
  cb.assertCalled(
      E::OK,
      {compose_lsn(epoch_t(3), esn_t(5)), std::chrono::milliseconds(100)});
}

TEST(GetHeadAttributesRequestTest, ClientTimeout) {
  Callback cb;
  MockGetHeadAttributesRequest req(
      5, ReplicationProperty({{NodeLocationScope::NODE, 3}}), cb);
  req.onReply(node(0), E::FAILED, LogHeadAttributes());
  cb.assertNotCalled();
  req.onClientTimeout();
  cb.assertCalled(E::TIMEDOUT, LogHeadAttributes());
}

TEST(GetHeadAttributesRequestTest, Failed) {
  Callback cb;
  MockGetHeadAttributesRequest req(
      5, ReplicationProperty({{NodeLocationScope::NODE, 3}}), cb);
  req.onReply(node(0), E::FAILED, LogHeadAttributes());
  cb.assertNotCalled();
  req.onReply(node(1), E::FAILED, LogHeadAttributes());
  cb.assertNotCalled();
  req.onReply(node(2), E::FAILED, LogHeadAttributes());
  cb.assertCalled(E::FAILED, LogHeadAttributes());
}
