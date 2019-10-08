/**
 * Copyright (c) 2019-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <gmock/gmock.h>

#include "logdevice/include/Reader.h"

namespace facebook { namespace logdevice {

class MockReader : public Reader {
 public:
  MOCK_METHOD4(startReading,
               int(logid_t log_id,
                   lsn_t from,
                   lsn_t until,
                   const ReadStreamAttributes* attrs));

  MOCK_METHOD1(stopReading, int(logid_t log_id));

  MOCK_CONST_METHOD1(isReading, bool(logid_t log_id));

  MOCK_CONST_METHOD0(isReadingAny, bool());

  MOCK_METHOD1(setTimeout, int(std::chrono::milliseconds timeout));

  MOCK_METHOD3(read,
               ssize_t(size_t nrecords,
                       std::vector<std::unique_ptr<DataRecord>>* data_out,
                       GapRecord* gap_out));

  MOCK_METHOD0(waitOnlyWhenNoData, void());

  MOCK_METHOD0(withoutPayload, void());

  MOCK_METHOD0(forceNoSingleCopyDelivery, void());

  MOCK_METHOD0(includeByteOffset, void());

  MOCK_METHOD0(doNotSkipPartiallyTrimmedSections, void());

  MOCK_CONST_METHOD1(isConnectionHealthy, int(logid_t));

  MOCK_METHOD0(doNotDecodeBufferedWrites, void());
};
}} // namespace facebook::logdevice
