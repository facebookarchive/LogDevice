/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>

#include <boost/make_shared.hpp>

#include "logdevice/include/Client.h"
#include "logdevice/clients/c/ld_c_reader.h"

using namespace facebook::logdevice;

using ReaderSPtr = std::shared_ptr<facebook::logdevice::Reader>;

#define DEREF_READER(PSP) (*(ReaderSPtr*)PSP)

ld_err ldr_reader_next(PReaderSPtr preader_sp,
                      PDataRecord* ppdata_record,
                      PGapRecord* ppgap_record) {
  auto record = std::vector<std::unique_ptr<DataRecord>>();
  GapRecord* pgap = new GapRecord;

  ssize_t n = 0;
  {
    // gil_release_and_guard guard;
    n = DEREF_READER(preader_sp)->read(1, &record, pgap);
  }

  if (n < 0) {
    if (err == E::GAP) {
      *ppdata_record = NULL;
      *ppgap_record = (PGapRecord)pgap;
      return LD_ERR_OK;
    }

    return LD_ERR_UNKNOWN;
  }

  if (n > 0) {
    *ppdata_record = (PDataRecord)(record[0].release());
    *ppgap_record = NULL;
    delete pgap;
  }

  return LD_ERR_EMPTY;

}

ld_err ldr_reader_start_reading(PReaderSPtr reader,
                         ldc_logid_t logid,
                         ldc_lsn_t from,
                         ldc_lsn_t until) {
  if (DEREF_READER(reader)->startReading(
        (logid_t)logid, (lsn_t)from, (lsn_t)until) == 0) {
    return LD_ERR_OK;
  } else {
    return LD_ERR_UNKNOWN;
  }
}

ld_err ldr_reader_stop_reading(PReaderSPtr preader_sp, ldc_logid_t logid) {
  if (DEREF_READER(preader_sp)->stopReading((logid_t)logid) == 0) {
    return LD_ERR_OK;
  } else {
    return LD_ERR_UNKNOWN;
  }
}

ld_err ldr_reader_is_connection_healthy(PReaderSPtr preader_sp, ldc_logid_t logid) {
    switch (DEREF_READER(preader_sp)->isConnectionHealthy((logid_t)logid)) {
      case 1:
        return LD_ERR_OK;
      case 0:
        return LD_ERR_CONNFAILED;
      default:
        return LD_ERR_UNKNOWN;
    };
  }

ld_err ldr_reader_without_payload(PReaderSPtr preader_sp) {
  DEREF_READER(preader_sp)->withoutPayload();
  return LD_ERR_OK;
}

