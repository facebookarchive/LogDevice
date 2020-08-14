/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/test/utils/AdminAPITestUtils.h"

#include <chrono>

#include "logdevice/common/ThriftCodec.h"
#include "logdevice/lib/ClientImpl.h"

namespace facebook { namespace logdevice {

void retry_until_ready(int32_t attempts,
                       std::chrono::seconds delay,
                       folly::Function<void()> operation) {
  do {
    // let's keep trying until the server hits that version
    try {
      operation();
      // No exception thrown.
      return;
    } catch (thrift::NodeNotReady& e) {
      ld_info("Got NodeNotReady exception, attempts left: %i", attempts);
      // retry
      std::this_thread::sleep_for(delay);
      attempts--;
    }
  } while (attempts > 0);
}

lsn_t write_to_maintenance_log(Client& client,
                               maintenance::MaintenanceDelta& delta) {
  logid_t maintenance_log_id =
      configuration::InternalLogs::MAINTENANCE_LOG_DELTAS;

  // Retry for at most 30s to avoid test failures due to transient failures
  // writing to the maintenance log.
  std::chrono::steady_clock::time_point deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds{30};

  std::string serializedData =
      ThriftCodec::serialize<apache::thrift::BinarySerializer>(delta);

  lsn_t lsn = LSN_INVALID;
  auto clientImpl = dynamic_cast<ClientImpl*>(&client);
  clientImpl->allowWriteInternalLog();
  auto rv = wait_until("writes to the maintenance log succeed",
                       [&]() {
                         lsn = clientImpl->appendSync(
                             maintenance_log_id, serializedData);
                         return lsn != LSN_INVALID;
                       },
                       deadline);

  if (rv != 0) {
    ld_check(lsn == LSN_INVALID);
    ld_error("Could not write delta in maintenance log(%lu): %s(%s)",
             maintenance_log_id.val_,
             error_name(err),
             error_description(err));
    return lsn; /* LSN_INVALID in this case */
  }

  ld_info(
      "Wrote maintenance log delta with lsn %s", lsn_to_string(lsn).c_str());
  return lsn;
}
}} // namespace facebook::logdevice
