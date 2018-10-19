/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/Request.h"
#include "logdevice/common/Semaphore.h"
#include "logdevice/common/SingleEvent.h"
#include "logdevice/common/Timestamp.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/debug.h"
#include "logdevice/include/Client.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

/**
 * @file IsLogEmptyBatchRunner is a utility that can be used to run IsLogEmpty
 * on a list of logs.
 */

class IsLogEmptyBatchRunner {
 public:
  struct Result {
    Status status;
    bool empty;
  };

  using Results = std::unordered_map<logid_t, Result, logid_t::Hash>;
  using Callback = std::function<void(Results)>;

  /**
   * Create a IsLogEmptyBatchRunner.
   *
   * @param client      A client object
   * @param logs        Set of logs to process
   * @param cb          Callback to call once all logs are processed.
   *                    May be called from a different thread than the thread
   *                    that started this IsLogEmptyBatchRunner
   */
  IsLogEmptyBatchRunner(std::shared_ptr<Client> client,
                        std::vector<logid_t> logs,
                        Callback cb);
  ~IsLogEmptyBatchRunner();

  /**
   * @param max How many logs should be processed concurrently;
   */
  void setMaxInFlight(size_t max) {
    max_in_flight_ = max;
  }

  /**
   * Start the state machine.
   */
  void start();

 private:
  std::shared_ptr<Client> client_;
  std::list<logid_t> logs_;

  Results results_;
  size_t count_{0};
  size_t in_flight_{0};
  Callback cb_;
  size_t max_in_flight_ = 1;

  void complete(logid_t logid, Status status, bool empty);
  void scheduleMore();
  void scheduleForLog(logid_t logid);

  friend class StartIsLogEmptyBatchRunnerRequest;
};

}} // namespace facebook::logdevice
