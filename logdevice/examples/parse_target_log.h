/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <string>

#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {
class Client;
}} // namespace facebook::logdevice

/**
 * Parses a log parameter from the command line.  Input can be in one of the
 * following formats:
 *
 * - "123" (numeric log ID)
 * - "/mylog" (a log range name from the config, containing a single log)
 * - "/mylogs[0]" (log range name from the config and an offset into the range)
 */
facebook::logdevice::logid_t
parse_target_log(const std::string& input, facebook::logdevice::Client& client);

/**
 * Parses a log parameter from the command line.  Input can be in one of the
 * following formats:
 *
 * - "123" (numeric log ID)
 * - "/mylog" (a log range name from the config)
 * - "/mylogs[0]" (log range name from the config and an offset into the range)
 *
 * Returns (LOGID_INVALID, LOGID_INVALID) if the range cannot be parsed.
 */
facebook::logdevice::logid_range_t
parse_target_log_range(const std::string& input,
                       facebook::logdevice::Client& client);
