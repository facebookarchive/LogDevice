/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
/**
 * Thrift service to simulate copyloader planner
*/

include "common/fb303/if/fb303.thrift"

namespace cpp2 facebook.logdevice.thrift
namespace java com.facebook.logdevice.thrift
namespace py  logdevice.CopyLoadTestService

typedef i64 logid_t // Log id type


struct Lsn {
  1: i64 lsn_hi;
  2: i64 lsn_lo;
}

struct Task {
  1: required logid_t log_id;
  2: required Lsn start_lsn;
  3: required Lsn end_lsn;
  4: required i64 id;
}

service CopyLoaderTest extends fb303.FacebookService {

  list<Task> getTask(1:i64 client_id, 2:string logging_info);

  void informTaskFinished(1:i64 task_id,
                          2:i64 client_id,
                          3:string logging_info,
                          4:i64 load_data_size);

  // Confirm to the server that client is working on a task
  // server did not expect client to execute this task if false returned
  bool confirmOngoingTaskExecution(1:i64 task_id, 2:i64 client_id);
}
