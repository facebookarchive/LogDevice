/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <map>

#include "logdevice/include/Client.h"
#include "logdevice/include/Record.h"

namespace facebook { namespace logdevice {

// Interface for data handling (appending and reading).
class DataSourceWriter {
 public:
  // The destructor must be virtual in order to work correctly.
  virtual ~DataSourceWriter(){};

  virtual int append(logid_t logid,
                     std::string payload,
                     append_callback_t cb,
                     AppendAttributes attrs = AppendAttributes()) = 0;
};
}} // namespace facebook::logdevice
