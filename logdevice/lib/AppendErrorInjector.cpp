/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/lib/AppendErrorInjector.h"

#include <folly/Random.h>

#include "logdevice/common/AppendRequest.h"

namespace facebook { namespace logdevice {

namespace {
/**
 * an append request that simply destroys itself with the set status as soon
 * as it's executed
 */
class DestroyWithStatusAppendRequest : public AppendRequest {
 public:
  // move from the request, and then let the unique_ptr go out of scope and
  // delete the original
  DestroyWithStatusAppendRequest(std::unique_ptr<AppendRequest> original,
                                 Status status)
      : AppendRequest(std::move(*original)), destroy_status_(status) {}

 private:
  /**
   * The usual flow for an AppendRequest:
   * execute -> find sequencer -> sendAppendMessage
   * -> onReplyReceived -> destroy
   *
   *  Simply skip the sending and receiving part and just destroy with the
   *  given status right away
   */
  void sendAppendMessage() override {
    destroyWithStatus(destroy_status_);
  }

  Status destroy_status_;
};
} // namespace
AppendErrorInjector::AppendErrorInjector(
    Status error_type,
    std::unordered_map<logid_t, double> fail_ratios)
    : error_type_(error_type), fail_ratios_(std::move(fail_ratios)) {}

Status AppendErrorInjector::getErrorType() const {
  return error_type_;
}

bool AppendErrorInjector::next(logid_t log) const {
  auto it = fail_ratios_.find(log);
  if (it == fail_ratios_.end()) {
    return false;
  }

  return folly::Random::randDouble01() < it->second;
}

std::unique_ptr<AppendRequest> AppendErrorInjector::maybeReplaceRequest(
    std::unique_ptr<AppendRequest> req) const {
  if (req && next(req->getRecordLogID())) {
    std::unique_ptr<AppendRequest> new_req =
        std::make_unique<DestroyWithStatusAppendRequest>(
            std::move(req), error_type_);

    return new_req;
  }

  return req;
}
}} // namespace facebook::logdevice
