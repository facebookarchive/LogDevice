/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/lib/test/copyloadtestapp/server/CopyLoadTestServiceHandler.h"

#include "logdevice/common/Timer.h"

namespace facebook { namespace logdevice { namespace copyloadtestapp {

CopyLoadTestServiceHandler::CopyLoadTestServiceHandler(
    std::shared_ptr<Client> client,
    CommandLineSettings settings)
    : facebook::fb303::FacebookBase2("CopyLoaderTest Service"),
      task_allocator_(client, settings) {}

void CopyLoadTestServiceHandler::getTask(
    std::vector<thrift::Task>& tasks,
    int64_t client_id,
    std::unique_ptr<std::string> logging_info) {
  // In current implementation send only one task per client request
  if (auto task = task_allocator_.getNextTask(client_id)) {
    tasks.push_back(*task);
    ld_info("Tasks (%s) sent to client for processing. Client info: %s",
            taskToString(*task).c_str(),
            logging_info.get()->c_str());
  } else {
    task_allocator_.cleanStuckTasks();
  }
}

void CopyLoadTestServiceHandler::informTaskFinished(
    int64_t task_id,
    int64_t client_id,
    std::unique_ptr<std::string> logging_info,
    int64_t load_data_size) {
  all_data_loaded_size_ += load_data_size;
  ld_info("Amount of data loaded so far: %ld", all_data_loaded_size_.load());
  task_allocator_.releaseTask(task_id, client_id, std::move(logging_info));
}

bool CopyLoadTestServiceHandler::confirmOngoingTaskExecution(
    int64_t task_id,
    int64_t client_id) {
  return task_allocator_.updateTaskLastTimestamp(task_id, client_id);
}

// Multy thread mutex to make find time callbacks and thrift thread safe
std::mutex mutex;

void task_creation_thread(std::shared_ptr<Client> client,
                          std::deque<thrift::Task>& tasks_to_process,
                          CommandLineSettings settings) {
  auto cur_timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::high_resolution_clock::now().time_since_epoch());
  auto diff =
      std::max(cur_timestamp - settings.since, std::chrono::milliseconds{0});

  std::map<logid_t, lsn_t> last_check_points;
  for (int i = 0; i < settings.log_ids.size(); ++i) {
    last_check_points[settings.log_ids[i]] = LSN_INVALID;
  }

  std::map<logid_t, std::chrono::milliseconds> last_timestamps;
  for (int i = 0; i < settings.log_ids.size(); ++i) {
    last_timestamps[settings.log_ids[i]] = diff;
  }

  Semaphore sem(settings.parallel_findtime_batch);
  bool aborted = false;
  uint64_t tasks_count = 0;
  std::queue<logid_t> logs_to_query;
  for (int i = 0; i < settings.log_ids.size(); ++i) {
    logs_to_query.push(settings.log_ids[i]);
  }
  auto time_begin = std::chrono::steady_clock::now();
  int next_task_id = 0;
  std::set<logid_t> invalid_logs;
  int invalid_results = 0;
  while (1) {
    if (logs_to_query.empty()) {
      if (sem.value() == settings.parallel_findtime_batch) {
        // all done
        auto time_elapsed = std::chrono::steady_clock::now() - time_begin;
        ld_info("All tasks finished. Time (seconds): %ld, Tasks created: %ld"
                " invalid_logs: %ld, invalid_results: %d, all logs size: %ld",
                std::chrono::duration_cast<std::chrono::seconds>(time_elapsed)
                    .count(),
                tasks_count,
                invalid_logs.size(),
                invalid_results,
                settings.log_ids.size());
        break;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      continue;
    }
    sem.wait(); // wait until there is an inflight slot available
    logid_t log_id = logs_to_query.front();
    logs_to_query.pop();
    auto timestamp = last_timestamps[log_id];
    auto cb = [log_id,
               &sem,
               &aborted,
               &last_check_points,
               timestamp,
               &cur_timestamp,
               &invalid_logs,
               &invalid_results,
               &tasks_to_process,
               &next_task_id,
               &logs_to_query,
               &tasks_count](Status /*st*/, lsn_t result) {
      std::lock_guard<std::mutex> guard(mutex);
      ld_info("FindTime task finished for logid %ld"
              " with result %ld timestamp %ld ",
              uint64_t(log_id),
              uint64_t(result),
              timestamp.count());
      if (aborted) {
        return;
      }
      if (result == LSN_INVALID) {
        invalid_logs.insert(log_id);
        sem.post();
        return;
      }
      // create task if had last check point
      // and if this task is not 0 records size
      if (last_check_points[log_id] != LSN_INVALID &&
          last_check_points[log_id] != result) {
        if (result < last_check_points[log_id]) {
          invalid_results++;
        } else {
          thrift::Task task;
          task.log_id = uint64_t(log_id);
          task.start_lsn.lsn_hi = lsn_to_epoch(last_check_points[log_id]).val_;
          task.start_lsn.lsn_lo = lsn_to_esn(last_check_points[log_id]).val_;
          task.end_lsn.lsn_hi = lsn_to_epoch(result - 1).val_;
          task.end_lsn.lsn_lo = lsn_to_esn(result - 1).val_;
          task.id = next_task_id;
          next_task_id++;
          tasks_to_process.push_back(task);
          tasks_count++;
          ld_info("Task created. Lsn start: %ld, lsn end: %ld,"
                  " logid %ld with timestamp %ld",
                  uint64_t(last_check_points[log_id]),
                  uint64_t(result),
                  uint64_t(log_id),
                  timestamp.count());
        }
      }
      last_check_points[log_id] = std::max(result, last_check_points[log_id]);
      // continue if it is not last time chunk
      if (timestamp < cur_timestamp) {
        logs_to_query.push(log_id);
      }
      sem.post();
    };
    client->findTime(log_id,
                     timestamp,
                     cb,
                     settings.find_time_approximate
                         ? FindKeyAccuracy::APPROXIMATE
                         : FindKeyAccuracy::STRICT);
    last_timestamps[log_id] = last_timestamps[log_id] + settings.chunk_duration;
  }
}

CopyLoadTestServiceHandler::TaskAllocator::TaskAllocator(
    std::shared_ptr<Client> client,
    CommandLineSettings settings)
    : task_cteation_(task_creation_thread,
                     client,
                     std::ref(tasks_to_process_),
                     settings),
      settings_(settings) {}

folly::Optional<thrift::Task>
CopyLoadTestServiceHandler::TaskAllocator::getNextTask(int64_t client_id) {
  std::lock_guard<std::mutex> lock(mutex);
  if (tasks_in_flight_.size() < settings_.tasks_in_flight_size_limit &&
      !tasks_to_process_.empty()) {
    auto task = tasks_to_process_.front();
    tasks_to_process_.pop_front();
    ExecutionInfo info;
    info.client_id = client_id;
    info.last_update_time = std::chrono::steady_clock::now();
    tasks_in_flight_[task.id] = std::make_pair(task, info);
    return task;
  }
  return folly::Optional<thrift::Task>();
}

void CopyLoadTestServiceHandler::TaskAllocator::releaseTask(
    int64_t task_id,
    int64_t client_id,
    std::unique_ptr<std::string> logging_info) {
  std::lock_guard<std::mutex> lock(mutex);
  // Ckeck if task already finished or expired. It can happens if some client
  // wake up after some time of freezing but other client already took a task
  if (tasks_in_flight_.find(task_id) == tasks_in_flight_.end() ||
      tasks_in_flight_[task_id].second.client_id != client_id) {
    return;
  }
  ld_info("Task (%s) finished. Client info: %s",
          taskToString(tasks_in_flight_[task_id].first).c_str(),
          logging_info.get()->c_str());
  tasks_in_flight_.erase(task_id);
}

void CopyLoadTestServiceHandler::TaskAllocator::cleanStuckTasks() {
  std::lock_guard<std::mutex> lock(mutex);
  auto cur_time = std::chrono::steady_clock::now();
  for (auto it = tasks_in_flight_.cbegin(); it != tasks_in_flight_.cend();) {
    auto time_elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        cur_time - it->second.second.last_update_time);
    if (time_elapsed > settings_.execution_confirmation_timeout) {
      ld_error("Last client %ld task update was more than %ld ms ago."
               "Task (%s) is going to be removed from client",
               it->second.second.client_id,
               settings_.execution_confirmation_timeout.count(),
               taskToString(it->second.first).c_str());
      tasks_to_process_.push_front(it->second.first);
      it = tasks_in_flight_.erase(it);
      ;
    } else {
      ++it;
    }
  }
}

bool CopyLoadTestServiceHandler::TaskAllocator::updateTaskLastTimestamp(
    int64_t task_id,
    int64_t client_id) {
  std::lock_guard<std::mutex> lock(mutex);
  if (tasks_in_flight_.find(task_id) == tasks_in_flight_.end() ||
      tasks_in_flight_[task_id].second.client_id != client_id) {
    return false;
  }
  tasks_in_flight_[task_id].second.last_update_time =
      std::chrono::steady_clock::now();
  return true;
}

std::string CopyLoadTestServiceHandler::taskToString(thrift::Task task) {
  lsn_t lsn_start =
      compose_lsn(epoch_t(task.start_lsn.lsn_hi), esn_t(task.start_lsn.lsn_lo));
  lsn_t lsn_end =
      compose_lsn(epoch_t(task.end_lsn.lsn_hi), esn_t(task.end_lsn.lsn_lo));
  std::string result;
  result.append("id :")
      .append(std::to_string(task.id))
      .append(", lsn start: ")
      .append(lsn_to_string(lsn_start))
      .append(", lsn end: ")
      .append(lsn_to_string(lsn_end))
      .append(", log id: ")
      .append(std::to_string(task.log_id));
  return result;
}

}}} // namespace facebook::logdevice::copyloadtestapp
