/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/ConfigurationFetchRequest.h"

#include <folly/Memory.h>

#include "logdevice/common/Sender.h"
#include "logdevice/common/Worker.h"

namespace facebook { namespace logdevice {

Request::Execution ConfigurationFetchRequest::execute() {
  CONFIG_FETCH_Header hdr = {config_type_};
  std::unique_ptr<Message> msg = std::make_unique<CONFIG_FETCH_Message>(hdr);
  int rv =
      Worker::onThisThread()->sender().sendMessage(std::move(msg), node_id_);
  if (rv != 0) {
    ld_error("Unable to fetch config from server %s with error %s",
             node_id_.toString().c_str(),
             error_description(err));
  }
  return Execution::COMPLETE;
}

}} // namespace facebook::logdevice
