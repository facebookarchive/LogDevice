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
class AppendRequestWrapper : public AppendRequest {
 public:
  // move from the request, and then let the unique_ptr go out of scope and
  // delete the original
  AppendRequestWrapper(
      std::unique_ptr<AppendRequest> original,
      std::function<folly::Optional<Status>(logid_t, node_index_t)> cb)
      : AppendRequest(std::move(*original)), cb_(cb) {}

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
    folly::Optional<Status> st = cb_(getRecordLogID(), sequencer_node_.index());
    if (st.hasValue()) {
      if (st.value() == E::OK) {
        // If we're injecting a success rather than an error, assign a fake
        // non-INVALID LSN to avoid tripping an assert in ClientImpl.
        record_.attrs.lsn = LSN_OLDEST;
      }

      destroyWithStatus(st.value());
    } else {
      AppendRequest::sendAppendMessage();
    }
  }

  std::function<folly::Optional<Status>(logid_t, node_index_t)> cb_;
};
} // namespace
AppendErrorInjector::AppendErrorInjector(
    Status error_type,
    std::unordered_map<logid_t, double> fail_ratios)
    : cb_([error_type, fail_ratios](logid_t log,
                                    node_index_t) -> folly::Optional<Status> {
        auto it = fail_ratios.find(log);
        if (it == fail_ratios.end() ||
            folly::Random::randDouble01() >= it->second) {
          return folly::none;
        }
        return error_type;
      }) {}

std::function<folly::Optional<Status>(logid_t, node_index_t)>
AppendErrorInjector::getCallback() const {
  return cb_;
}

std::unique_ptr<AppendRequest> AppendErrorInjector::maybeReplaceRequest(
    std::unique_ptr<AppendRequest> req) const {
  return req ? std::make_unique<AppendRequestWrapper>(std::move(req), cb_)
             : nullptr;
}
}} // namespace facebook::logdevice
