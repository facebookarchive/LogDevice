/**
 * Copyright (c) 2020-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/server/thrift/LogDeviceThriftHandler.h"

#include "logdevice/common/BuildInfo.h"

namespace facebook { namespace logdevice {

LogDeviceThriftHandler::LogDeviceThriftHandler(const std::string& service_name,
                                               Processor* processor)
    : facebook::fb303::FacebookBase2(service_name), ld_processor_(processor) {}

facebook::fb303::cpp2::fb_status LogDeviceThriftHandler::getStatus() {
  if (ld_processor_->isShuttingDown()) {
    return facebook::fb303::cpp2::fb_status::STOPPING;
  } else if (!ld_processor_->isLogsConfigLoaded()) {
    return facebook::fb303::cpp2::fb_status::STARTING;
  } else {
    return facebook::fb303::cpp2::fb_status::ALIVE;
  }
}

void LogDeviceThriftHandler::getVersion(std::string& _return) {
  auto build_info =
      ld_processor_->getPluginRegistry()->getSinglePlugin<BuildInfo>(
          PluginType::BUILD_INFO);
  _return = build_info->version();
}

}} // namespace facebook::logdevice
