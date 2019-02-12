/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <functional>
#include <memory>

#include <folly/Optional.h>

#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/LocalLogsConfig.h"
#include "logdevice/common/configuration/SecurityConfig.h"
#include "logdevice/common/configuration/logs/LogsConfigTree.h"
#include "logdevice/include/Err.h"
#include "logdevice/include/LogAttributes.h"

namespace folly {
struct dynamic;
}

namespace facebook { namespace logdevice { namespace configuration {
namespace parser {

using LoadFileCallback = std::function<Status(const char*, std::string*)>;
using LogAttributes = facebook::logdevice::logsconfig::LogAttributes;
using LogGroupNode = facebook::logdevice::logsconfig::LogGroupNode;

bool parseLogs(
    const folly::dynamic&,
    std::shared_ptr<facebook::logdevice::configuration::LocalLogsConfig>&,
    const SecurityConfig& securityConfig,
    LoadFileCallback loadFileCallback,
    const std::string& ns_delimiter,
    const ConfigParserOptions& options);

bool parseLogInterval(const folly::dynamic& logMap,
                      std::string& interval_string,
                      interval_t& interval_raw,
                      bool limit_logid_range);

folly::Optional<LogAttributes>
parseAttributes(const folly::dynamic& attrs,
                const std::string& interval_string,
                bool allow_permissions,
                bool metadata_logs = false);

}}}} // namespace facebook::logdevice::configuration::parser
