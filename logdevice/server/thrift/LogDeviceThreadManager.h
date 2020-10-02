// Copyright 2004-present Facebook. All Rights Reserved.

#pragma once

#include <thrift/lib/cpp/concurrency/Thread.h>
#include <thrift/lib/cpp/concurrency/ThreadManager.h>

#include "logdevice/common/EventLoop.h"
#include "logdevice/common/RequestExecutor.h"

namespace facebook { namespace logdevice {

class RequestExecutor;
using apache::thrift::concurrency::Runnable;
using apache::thrift::concurrency::ThreadFactory;
using apache::thrift::concurrency::ThreadManager;

class LogDeviceThreadManager : public ThreadManager {
 public:
  explicit LogDeviceThreadManager(RequestExecutor work_scheduler);

  void join() override {}
  void start() override {}
  void stop() override {}
  STATE state() const override {
    return STARTED;
  }
  std::shared_ptr<ThreadFactory> threadFactory() const override {
    return nullptr;
  }
  void threadFactory(std::shared_ptr<ThreadFactory> /*value*/) override {}
  std::string getNamePrefix() const override {
    return "LogdeviceTM";
  }
  void setNamePrefix(const std::string& /*name*/) override {}
  void addWorker(size_t /*value*/ = 1) override {}
  void removeWorker(size_t /*value*/ = 1) override {}

  size_t idleWorkerCount() const override {
    return 0;
  }
  size_t workerCount() const override {
    return 0;
  }
  size_t pendingUpstreamTaskCount() const override {
    return 0;
  }
  size_t pendingTaskCount() const override {
    return 0;
  }
  size_t totalTaskCount() const override {
    return 0;
  }
  size_t expiredTaskCount() override {
    return 0;
  }

  void add(std::shared_ptr<apache::thrift::concurrency::Runnable> task,
           int64_t /*timeout*/ = 0,
           int64_t /*expiration*/ = 0,
           bool /*cancellable*/ = false) noexcept override;

  void add(folly::Func f) override;

  void remove(std::shared_ptr<apache::thrift::concurrency::Runnable> /*task*/)
      override {}
  std::shared_ptr<apache::thrift::concurrency::Runnable>
  removeNextPending() override {
    return nullptr;
  }
  void clearPending() override {}

  void setExpireCallback(ExpireCallback /*expireCallback*/) override {}
  void setCodelCallback(ExpireCallback /*expireCallback*/) override {}
  void setThreadInitCallback(InitCallback /*initCallback*/) override {}
  void enableCodel(bool) override {}
  folly::Codel* getCodel() override {
    return nullptr;
  }

 private:
  void postRequest(std::function<void()>&& func);

  RequestExecutor work_scheduler_;
};

}} // namespace facebook::logdevice
