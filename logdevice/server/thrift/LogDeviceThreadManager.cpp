// Copyright 2004-present Facebook. All Rights Reserved.

#include "logdevice/server/thrift/LogDeviceThreadManager.h"

#include <folly/container/Array.h>
#include <thrift/lib/cpp/concurrency/FunctionRunner.h>

#include "logdevice/common/Worker.h"
#include "logdevice/common/request_util.h"
#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice {

LogDeviceThreadManager::LogDeviceThreadManager(RequestExecutor work_scheduler)
    : work_scheduler_(work_scheduler) {}

void LogDeviceThreadManager::add(std::shared_ptr<Runnable> task,
                                 int64_t /* unused */,
                                 int64_t /* unused */,
                                 bool /* unused */) noexcept {
  std::function<void()> func = [=]() { task->run(); };
  postRequest(std::move(func));
}

void LogDeviceThreadManager::add(folly::Func f) {
  std::function<void()> func = [f = std::move(f).asStdFunction()]() { f(); };
  postRequest(std::move(func));
}

void LogDeviceThreadManager::postRequest(std::function<void()>&& func) {
  auto request = FuncRequest::make(
      worker_id_t(-1), WorkerType::GENERAL, RequestType::MISC, std::move(func));

  auto rv = work_scheduler_.postRequest(request, WorkerType::GENERAL, -1);
  if (rv != 0) {
    RATELIMIT_WARNING(std::chrono::seconds(1),
                      10,
                      "Posting request from LogDeviceThreadManager failed: %lu",
                      request->id_.val());
  }
}

}} // namespace facebook::logdevice
