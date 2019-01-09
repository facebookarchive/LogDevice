/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/AppendRequestBase.h"

#include "logdevice/common/Worker.h"

namespace facebook { namespace logdevice {

void AppendRequestBase::registerRequest() {
  auto res = Worker::onThisThread()->runningAppends().map.insert(
      std::make_pair(id_, std::unique_ptr<AppendRequestBase>(this)));
  ld_check(res.second);
}

void AppendRequestBase::destroy() {
  auto& runningAppends = Worker::onThisThread()->runningAppends().map;
  auto it = runningAppends.find(id_);

  ld_check(it != runningAppends.end());
  runningAppends.erase(it);
}

}} // namespace facebook::logdevice
