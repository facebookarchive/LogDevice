/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <gtest/gtest.h>

#include "common/fb303/if/gen-cpp2/FacebookService.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/if/gen-cpp2/LogDeviceAPIAsyncClient.h"
#include "logdevice/common/request_util.h"
#include "logdevice/common/thrift/ThriftRouter.h"
#include "logdevice/include/Client.h"
#include "logdevice/lib/ClientImpl.h"
#include "logdevice/test/utils/IntegrationTestBase.h"
#include "logdevice/test/utils/IntegrationTestUtils.h"

using namespace ::testing;
using apache::thrift::ClientReceiveState;
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
  using ThriftClient = thrift::LogDeviceAPIAsyncClient;
  using ThriftClientPtr = std::unique_ptr<thrift::LogDeviceAPIAsyncClient>;
  // This test checks that Thirft server starts and we are able to make an RPC
  // request to it from within Worker
  void checkSingleRpcCall() {
    auto status = runWithClient([](ThriftClientPtr client) {
      auto cb = std::make_unique<StatusCb>(std::move(client));
      auto client_ptr = cb->client_ptr.get();
      auto future = cb->responsePromise.getSemiFuture();
      client_ptr->getStatus(std::move(cb));
      return future;
    });
    ASSERT_EQ(fb_status::ALIVE, std::move(status).get());
  }

  class StatusCb : public apache::thrift::SendRecvRequestCallback {
   public:
    StatusCb(ThriftClientPtr client)
        : client_ptr(std::move(client)),
          original_worker(Worker::onThisThread()){};

    bool ensureWorker() {
      if (Worker::onThisThread(false) != original_worker) {
        responsePromise.setException(
            std::runtime_error("Callback is called either not in Worker "
                               "context or by wrong worker"));
        return false;
      }
      return true;
    }

    void send(folly::exception_wrapper&& ex) override {
      if (!ensureWorker()) {
        return;
      }
      if (ex) {
        responsePromise.setException(std::move(ex));
      }
    }

    void recv(ClientReceiveState&& state) override {
      if (!ensureWorker()) {
        return;
      }
      if (state.isException()) {
        responsePromise.setException(std::move(state.exception()));
      } else {
        fb_status response;
        auto exception = ThriftClient::recv_wrapped_getStatus(response, state);
        if (exception) {
          responsePromise.setException(std::move(exception));
        } else {
          responsePromise.setValue(response);
        }
      }
    }

    ThriftClientPtr client_ptr;
    folly::Promise<fb_status> responsePromise;
    Worker* original_worker;
  };

 private:
  // Runs arbitrary code on client's worker thread and provides Thrift client
  // created on this worker
  template <typename Func>
  typename std::result_of<Func(ThriftClientPtr)>::type runWithClient(Func cb) {
    ClientImpl* impl = checked_downcast<ClientImpl*>(client_.get());
    return run_on_worker(&(impl->getProcessor()), 0, [&]() {
      auto worker = Worker::onThisThread();
      node_index_t nid{0};
      auto thrift_client = worker->getThriftRouter()->getApiClient(nid);
      return cb(std::move(thrift_client));
    });
  }

  std::unique_ptr<IntegrationTestUtils::Cluster> cluster_;
  std::shared_ptr<Client> client_;
};

}}} // namespace facebook::logdevice::test
