/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <exception>

namespace facebook { namespace logdevice { namespace ldquery {

struct LDQueryError : public std::exception {
  explicit LDQueryError(const std::string& message) : message(message) {}
  const char* what() const throw() override {
    return message.c_str();
  }
  const std::string message;
};

struct StatementError : public LDQueryError {
  explicit StatementError(const std::string& message) : LDQueryError(message) {}
};
}}} // namespace facebook::logdevice::ldquery
