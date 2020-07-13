/**
 * Copyright (c) 2020-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "common/fb303/cpp/FacebookBase2.h"
#include "logdevice/common/Processor.h"

namespace facebook { namespace logdevice {

/**
 * LogDevice-specific implementation of FacebookBase2.
 */
class LogDeviceThriftHandler : public facebook::fb303::FacebookBase2 {
 public:
  LogDeviceThriftHandler(const std::string& service_name, Processor* processor);

  facebook::fb303::cpp2::fb_status getStatus() override;
  void getVersion(std::string& _return) override;

 private:
  Processor* ld_processor_;
};

}} // namespace facebook::logdevice
