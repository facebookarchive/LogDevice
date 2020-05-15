/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once


#include <map>
#include <vector>

#include "../Context.h"
#include "../Table.h"

namespace facebook::logdevice::logsconfig {
class LogAttributes;
}

namespace facebook::logdevice::ldquery::tables {

class LogTreeBase {
public:
  static TableColumns getAttributeColumns();
  static void populateAttributeData(TableData&, const facebook::logdevice::logsconfig::LogAttributes& attrs);
};



} // namespace facebook::logdevice::ldquery::tables
