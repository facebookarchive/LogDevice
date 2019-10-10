/**
 * Copyright (c) 2019-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <gmock/gmock.h>

#include "logdevice/include/CheckpointStore.h"

namespace facebook { namespace logdevice {

class MockCheckpointStore : public CheckpointStore {
 public:
  MOCK_CONST_METHOD3(getLSNSync,
                     Status(const std::string& customer_id,
                            logid_t log_id,
                            lsn_t* value_out));

  MOCK_CONST_METHOD3(getLSN,
                     void(const std::string& customer_id,
                          logid_t log_id,
                          GetCallback cb));

  MOCK_METHOD3(updateLSNSync,
               Status(const std::string& customer_id,
                      logid_t log_id,
                      lsn_t lsn));

  MOCK_METHOD2(updateLSNSync,
               Status(const std::string& customer_id,
                      const std::map<logid_t, lsn_t>& checkpoints));

  MOCK_METHOD4(updateLSN,
               void(const std::string& customer_id,
                    logid_t log_id,
                    lsn_t lsn,
                    UpdateCallback cb));

  MOCK_METHOD3(updateLSN,
               void(const std::string& customer_id,
                    const std::map<logid_t, lsn_t>& checkpoints,
                    UpdateCallback cb));

  MOCK_METHOD3(removeCheckpoints,
               void(const std::string& customer_id,
                    const std::vector<logid_t>& checkpoints,
                    UpdateCallback cb));

  MOCK_METHOD2(removeAllCheckpoints,
               void(const std::string& customer_id, UpdateCallback cb));

  MOCK_METHOD2(removeCheckpointsSync,
               Status(const std::string& customer_id,
                      const std::vector<logid_t>& checkpoints));

  MOCK_METHOD1(removeAllCheckpointsSync,
               Status(const std::string& customer_id));
};
}} // namespace facebook::logdevice
