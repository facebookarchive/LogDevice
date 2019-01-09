/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/FireAndForgetRequest.h"

#include "logdevice/common/Worker.h"
#include "logdevice/common/debug.h"

namespace facebook { namespace logdevice {

Request::Execution FireAndForgetRequest::execute() {
  registerRequest();
  executionBody();
  // should always return Execution::CONTINUE since the request is now owned
  // by the FireAndForgetRequestMap in Worker.
  return Execution::CONTINUE;
}

void FireAndForgetRequest::destroy() {
  auto& rqmap = Worker::onThisThread()->runningFireAndForgets().map;
  auto it = rqmap.find(id_);
  // shouldn't be called for a request not exectued yet
  (void)ld_catch(it != rqmap.end(),
                 "INTERNAL ERROR: destroy() call on request %s not found in "
                 "runningFireAndForgetRequests map!",
                 describe().c_str());

  // `this' is destroyed
  rqmap.erase(it);
}

void FireAndForgetRequest::registerRequest() {
  auto& rqmap = Worker::onThisThread()->runningFireAndForgets().map;
  auto result = rqmap.insert(
      std::make_pair(id_, std::unique_ptr<FireAndForgetRequest>(this)));
  (void)ld_catch(result.second,
                 "INTERNAL ERROR: request %s is already registered in "
                 "runningFireAndForgetRequests map!",
                 describe().c_str());
}

}} // namespace facebook::logdevice
