/**
 * Copyright (c) 2019-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <gmock/gmock.h>

#include "logdevice/include/AsyncReader.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

class MockAsyncReader : public AsyncReader {
 public:
  MOCK_METHOD4(startReading,
               int(logid_t log_id,
                   lsn_t from,
                   lsn_t until,
                   const ReadStreamAttributes* attrs));

  MOCK_METHOD1(setRecordCallback,
               void(std::function<bool(std::unique_ptr<DataRecord>&)>));

  MOCK_METHOD1(setGapCallback, void(std::function<bool(const GapRecord&)>));

  MOCK_METHOD1(setDoneCallback, void(std::function<void(logid_t)>));

  MOCK_METHOD1(setHealthChangeCallback,
               void(std::function<void(logid_t, HealthChangeType)>));

  MOCK_METHOD2(stopReading,
               int(logid_t log_id, std::function<void()> callback));

  MOCK_METHOD1(resumeReading, int(logid_t log_id));

  MOCK_METHOD1(setMonitoringTier, void(MonitoringTier));

  MOCK_METHOD0(withoutPayload, void());

  MOCK_METHOD0(forceNoSingleCopyDelivery, void());

  MOCK_METHOD0(includeByteOffset, void());

  MOCK_METHOD0(doNotSkipPartiallyTrimmedSections, void());

  MOCK_CONST_METHOD1(isConnectionHealthy, int(logid_t));

  MOCK_METHOD0(doNotDecodeBufferedWrites, void());

  MOCK_METHOD1(getBytesBuffered, void(std::function<void(size_t)> callback));
};
}} // namespace facebook::logdevice
