/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <vector>
#include "common/fb303/cpp/FacebookBase2.h"
#include "logdevice/include/Err.h"
#include "logdevice/lib/test/copyloadtestapp/server/gen-cpp2/CopyLoaderTest.h"

#include <memory>
#include <unistd.h>
#include <cstdlib>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <iostream>
#include <mutex>
#include <random>
#include <signal.h>
#include <thread>
#include <unordered_map>

#include "common/init/Init.h"
#include <folly/Memory.h>
#include <folly/Optional.h>
#include <folly/io/async/EventBase.h>
#include <thrift/lib/cpp/async/TAsyncSocket.h>
#include <thrift/lib/cpp2/async/HeaderClientChannel.h>

#include "logdevice/common/debug.h"

#include "logdevice/common/Checksum.h"
#include "logdevice/common/EpochMetaData.h"
#include "logdevice/common/ReaderImpl.h"
#include "logdevice/common/Semaphore.h"
#include "logdevice/common/util.h"
#include "logdevice/lib/ClientSettingsImpl.h"
#include "logdevice/include/Client.h"
#include "logdevice/include/Record.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice { namespace copyloadtestapp {

struct CommandLineSettings {
  bool no_payload;
  bool find_time_approximate;
  size_t total_buffer_size = 2000000;
  size_t parallel_findtime_batch = 2000;
  size_t tasks_queue_size_limit = 2000;
  size_t tasks_in_flight_size_limit = 2000;
  std::chrono::milliseconds findtime_timeout{10000};
  std::chrono::milliseconds since{24 * 60 * 60 * 1000};
  std::chrono::milliseconds chunk_duration{15 * 60 * 1000};
  std::chrono::milliseconds execution_confirmation_timeout{120 * 1000};
  std::string config_path;
  std::vector<logid_t> log_ids;
  uint64_t logs_limit = std::numeric_limits<uint64_t>::max();
  uint32_t port;
};

/**
 * Service handler for CopyLoaderTest thrift interface.
 */
class CopyLoadTestServiceHandler : virtual public thrift::CopyLoaderTestSvIf,
                                   public facebook::fb303::FacebookBase2 {
 public:
  explicit CopyLoadTestServiceHandler(std::shared_ptr<Client> client,
                                      CommandLineSettings settings);
  void getTask(std::vector<thrift::Task>& task,
               int64_t client_id,
               std::unique_ptr<std::string> logging_info) override;
  void informTaskFinished(int64_t task_id,
                          int64_t client_id,
                          std::unique_ptr<std::string> logging_info,
                          int64_t load_data_size) override;
  bool confirmOngoingTaskExecution(int64_t task_id, int64_t client_id) override;

 private:
  struct ExecutionInfo {
    uint64_t client_id;
    std::chrono::time_point<std::chrono::steady_clock> last_update_time;
  };
  class TaskAllocator {
   public:
    TaskAllocator(std::shared_ptr<Client> client, CommandLineSettings settings);

    folly::Optional<thrift::Task> getNextTask(int64_t client_id);
    void releaseTask(int64_t task_id,
                     int64_t client_id,
                     std::unique_ptr<std::string> logging_info);
    void cleanStuckTasks();
    bool updateTaskLastTimestamp(int64_t task_id, int64_t client_id);

   private:
    std::thread task_cteation_;
    std::deque<thrift::Task> tasks_to_process_;
    std::unordered_map<int64_t, std::pair<thrift::Task, ExecutionInfo>>
        tasks_in_flight_;
    CommandLineSettings settings_;
  };

  TaskAllocator task_allocator_;
  std::atomic<uint64_t> all_data_loaded_size_{0};

  static std::string taskToString(thrift::Task);
};

}}} // namespace facebook::logdevice::copyloadtestapp
