/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/TailRecord.h"

namespace facebook { namespace logdevice {

/**
 * @file Utility functions for manipulating last clean epoch stored in
 *       the epoch store.
 */

namespace EpochStoreLastCleanEpochFormat {

/**
 * Attempts to parse the given buffer to extract last clean epoch and the
 * tail record.
 *
 * @param buf           linear buffer to be read, not nul-terminated
 * @param size          length of @buf in bytes
 *
 * @return              0 on success, -1 on failure
 */
int fromLinearBuffer(const char* buf,
                     size_t buf_len,
                     logid_t log_id,
                     epoch_t* epoch_out,
                     TailRecord* tail_out);

/**
 * Attempt to write the content of LCE and tail record to the
 * given linear buffer.
 *
 * @param buf           linear buffer to be written
 * @param size          length of @buf in bytes
 *
 * @return              on success, return size written in the buffer
 *                      on failure, -1 is returned
 */
int toLinearBuffer(char* buf,
                   size_t buf_len,
                   epoch_t lce,
                   const TailRecord& tail);

/**
 * Calculate the size of the object as if it is written to a linear buffer
 * using toLinearBuffer().
 *
 * @return             on success, return the size in the buffer
 *                     on failure, -1 is returned
 */
int sizeInLinearBuffer(epoch_t lce, const TailRecord& tail);

} // namespace EpochStoreLastCleanEpochFormat

}} // namespace facebook::logdevice
