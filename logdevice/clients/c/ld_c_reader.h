/**
 * Copyright (c) 2019-present, CCBFT, Inc. and its affiliates.
 * All rights reserved.
 */

#pragma once

#include "ld_c_common.h"

#ifdef __cplusplus
extern "C" {
#endif


ld_err ldr_reader_next(PReaderSPtr preader_sp,
                       PDataRecord data_record,
                       PGapRecord gap_record);

ld_err ldr_reader_start_reading(PReaderSPtr preader_sp,
                              ldc_logid_t logid,
                              ldc_lsn_t from,
                              ldc_lsn_t until);

ld_err ldr_reader_stop_reading(PReaderSPtr preader_sp, ldc_logid_t logid);

ld_err ldr_reader_is_connection_healthy(PReaderSPtr preader_sp, ldc_logid_t logid);

ld_err ldr_reader_without_payload(PReaderSPtr preader_sp);


#ifdef __cplusplus
}
#endif
