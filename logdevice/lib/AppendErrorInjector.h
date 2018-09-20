/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>
#include <unordered_map>
#include <vector>

#include "logdevice/include/Err.h"
#include "logdevice/include/types.h"

/**
 * @file AppendErrorInjector is used in the ClientImpl to be able to simulate
 * append errors without having to modify anything on nodes. To be used in
 * tests.
 */
namespace facebook { namespace logdevice {

class AppendRequest;

class AppendErrorInjector {
 public:
  AppendErrorInjector(Status error_type, logid_t log, double fail_ratio)
      : AppendErrorInjector(error_type, {{log, fail_ratio}}) {}
  AppendErrorInjector(Status error_type,
                      std::unordered_map<logid_t, double> fail_ratios);
  /**
   * @returns The status that the failed append should have
   */
  Status getErrorType() const;
  /**
   * @param log The log that is currently appended to
   * @returns   true if this append should fail, false otherwise
   */
  bool next(logid_t log) const;

  /**
   * If an append is supposed to fail, the request will be replaced with an
   * AppendRequest that is sure to fail with the error type of the error
   * injector, otherwise it will return the original request.
   *
   * @param req The original AppendRequest that might be replaced
   * @returns   If the append is supposed to fail, an AppendRequest that will
   *            certainly fail with the error type is returned. Otherwise the
   *            request given as argument is returned
   */
  std::unique_ptr<AppendRequest>
  maybeReplaceRequest(std::unique_ptr<AppendRequest> req) const;

 private:
  Status error_type_;

  std::unordered_map<logid_t, double> fail_ratios_;
};
}} // namespace facebook::logdevice
