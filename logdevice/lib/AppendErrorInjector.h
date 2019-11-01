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

#include <folly/Optional.h>

#include "logdevice/common/NodeID.h"
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
  // `cb` will be called right before sending an APPEND message to a sequencer
  // node. If `cb` returns non-folly::none, we won't send the APPEND and will
  // complete the AppendRequest with the given status instead. E::OK is allowed,
  // but keep in mind that the append callback will be called with LSN_OLDEST
  // instead of a real LSN.
  explicit AppendErrorInjector(
      std::function<folly::Optional<Status>(logid_t, node_index_t)> cb)
      : cb_(cb) {}
  AppendErrorInjector(Status error_type,
                      std::unordered_map<logid_t, double> fail_ratios);

  std::function<folly::Optional<Status>(logid_t, node_index_t)>
  getCallback() const;

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
  std::function<folly::Optional<Status>(logid_t, node_index_t)> cb_;
};
}} // namespace facebook::logdevice
