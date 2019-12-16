/**
 * Copyright (c) 2019-present, CCBFT, Inc. and its affiliates.
 * All rights reserved.
 */

#pragma once

#include "ld_c_common.h"

#ifdef __cplusplus
extern "C" {
#endif


ld_err ldc_make_client(const char* name,
                       const char* config,
                       double timeout_seconds,
                       PStr2IntOrStrMap settings,
                       const char* credentials,
                       const char* csid,
                       PClientSPtr* ppclient_sp);

ld_err ldc_free_client(PClientSPtr pclient_sp);

ld_err ldc_set_timeout(PClientSPtr pclient_sp, double timeout_seconds);

ld_err ldc_get_directory_delimiter(PClientSPtr pclient_sp,
                                   int len_limit,
                                   char* delimiter,
                                   int* len);

// tuple ldc_get_log_range_by_name(PClientSPtr pclient_sp);
ld_err ldc_get_log_range_by_name(PClientSPtr pclient_sp,
                                 ldc_logid_t* from,
                                 ldc_logid_t* to);

ld_err ldc_get_tail_lsn(PClientSPtr pclient_sp, ldc_logid_t logid, ldc_lsn_t* plsn);

ld_err ldc_get_tail_attributes(PClientSPtr pclient_sp,
                               ldc_logid_t logid,
                               ldc_lsn_t* p_last_released_real_lsn,
                               uint64_t* p_last_timestamp_count,
                               uint64_t* p_offsets_counter);

ld_err ldc_get_head_attributes(PClientSPtr pclient_sp,
                               ldc_logid_t logid,
                               ldc_lsn_t* trim_point,
                               uint64_t* trim_point_timestamp_count);

ld_err ldc_append(PClientSPtr pclient_sp,
                  ldc_logid_t logid,
                  const char* data,
                  uint64_t len,
                  ldc_lsn_t* plsn);

ld_err ldc_find_time(PClientSPtr pclient_sp,
                     ldc_logid_t logid,
                     double seconds,
                     ldc_lsn_t* plsn);

ld_err ldc_find_key(PClientSPtr pclient_sp,
                    ldc_logid_t logid,
                    const char* key,
                    uint64_t key_len,
                    ldc_lsn_t* lo,
                    ldc_lsn_t* hi);

ld_err ldc_trim(PClientSPtr pclient_sp,
                ldc_logid_t logid,
                ldc_lsn_t lsn);

ld_err ldc_is_log_empty(PClientSPtr pclient_sp, ldc_logid_t logid, bool* pempty);

ld_err ldc_data_size(PClientSPtr pclient_sp,
                     ldc_logid_t logid,
                     double start_sec,
                     double end_sec1,
                     size_t* result_size);

ld_err ldc_get_max_payload_size(PClientSPtr pclient_sp,
                                size_t* max_size);

// ld_err timestr_to_seconds(const char* timestring, uint64_t len, int64_t* seconds);


// ldc_lsn_t smoke_test_for_log(PClientSPtr client,
//                              const ldc_logid_t* logid,
//                              int count);

// PUInt2UIntHashMap smoke_test_write(PClientSPtr client,
//                                    PUIntList log_ids,
//                                    int count);

// ld_err smoke_test_read(PClientSPtr client,
//                        PUInt2UIntMap starting_points,
//                        int count);

// ld_err smoke_test(PClientSPtr client, PUIntList log_ids, int count);

ld_err ldc_get_setting(PClientSettingsSPtr pp_clnt_settings_sp,
                       const char* key,
                       char* setting,
                       uint64_t max_len);

// Only support setting a str val
// For integral type, convert it to string first
ld_err ldc_set_setting(PClientSettingsSPtr pp_clnt_settings_sp,
                      const char* key,
                      const char* value);

ld_err ldc_make_reader(PClientSPtr pclient_sp,
                       size_t max_logs,
                       PReaderSPtr* ppreader_sp);
ld_err ldc_free_reader(PReaderSPtr preader_sp);


#ifdef __cplusplus
}
#endif
