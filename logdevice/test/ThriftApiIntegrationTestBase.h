/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <gtest/gtest.h>

#include "common/fb303/if/gen-cpp2/FacebookService.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/if/gen-cpp2/LogDeviceAPIAsyncClient.h"
#include "logdevice/common/request_util.h"
#include "logdevice/common/thrift/ThriftRouter.h"
#include "logdevice/include/Client.h"
#include "logdevice/lib/ClientImpl.h"
#include "logdevice/test/utils/IntegrationTestBase.h"
#include "logdevice/test/utils/IntegrationTestUtils.h"

using namespace ::testing;
using facebook::fb303::cpp2::fb_status;
using facebook::logdevice::IntegrationTestUtils::ClusterFactory;

namespace facebook { namespace logdevice { namespace test {

class ThriftApiIntegrationTestBase : public IntegrationTestBase {
 public:
  virtual void customizeClusterFactory(ClusterFactory& factory) {}

  void SetUp() override {
    IntegrationTestBase::SetUp();
    auto factory = ClusterFactory();
    customizeClusterFactory(factory);
    cluster_ = factory.create(1);
    int res = cluster_->getNode(0).waitUntilAvailable();
    ASSERT_EQ(0, res);
    client_ = cluster_->createClient();
  }

 protected:
  // This test checks that Thirft server starts and we are able to make an RPC
  // request to it from within Worker
  void checkSingleRpcCall() {
    auto status = runWithClient([](thrift::LogDeviceAPIAsyncClient& client) {
      return client.semifuture_getStatus().get();
    });
    ASSERT_EQ(fb_status::ALIVE, status);
  }

 private:
  // Runs arbitrary code on client's worker thread and provides Thrift client
  // created on this worker
  template <typename Func>
  typename std::result_of<Func(thrift::LogDeviceAPIAsyncClient&)>::type
  runWithClient(Func cb) {
    ClientImpl* impl = checked_downcast<ClientImpl*>(client_.get());
    return run_on_worker(&(impl->getProcessor()), 0, [&]() {
      auto worker = Worker::onThisThread();
      node_index_t nid{0};
      auto thrift_client = worker->getThriftRouter()->getApiClient(nid);
      return cb(*thrift_client);
    });
  }

  std::unique_ptr<IntegrationTestUtils::Cluster> cluster_;
  std::shared_ptr<Client> client_;
};

}}} // namespace facebook::logdevice::test
