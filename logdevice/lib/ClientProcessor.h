/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>
#include <utility>

#include "logdevice/common/Processor.h"

/**
 * @file Subclass of Processor containing state specific to clients, also
 * spawning ClientWorker instances instead of plain Worker.
 */

namespace facebook { namespace logdevice {

class ClientProcessor : public Processor {
 public:
  // Inherit Processor's protected constructor
  using Processor::Processor;

  template <typename... Args>
  static std::shared_ptr<ClientProcessor> create(Args&&... args) {
    auto p = std::make_shared<ClientProcessor>(std::forward<Args>(args)...);
    p->init();
    return p;
  }

  Worker* createWorker(WorkContext::KeepAlive executor,
                       worker_id_t i,
                       WorkerType type) override;
};
}} // namespace facebook::logdevice
