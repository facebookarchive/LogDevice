/**
 * Copyright (c) 2019-present, CCBFT, Inc. and its affiliates.
 * All rights reserved.
 */


#pragma once

#ifdef __cplusplus
extern "C" {
#endif

enum ld_err {
LD_ERR_OK = 0,
LD_ERR_CONN_ERROR,
LD_ERR_BUFFER_NOT_LARGE_ENOUGH,
LD_ERR_NO_MORE_DATA,
LD_ERR_INVALID_ARG,
LD_ERR_UNKNOWN = 1023,
LD_ERR_MAX = 1024
};

#ifdef __cplusplus
}
#endif
