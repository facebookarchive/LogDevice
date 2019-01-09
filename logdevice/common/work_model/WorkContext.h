/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/Executor.h>

namespace facebook { namespace logdevice {

class WorkContext : public folly::Executor {
 public:
  typedef uint64_t work_context_id_t;
  static constexpr work_context_id_t kAnonymousId{0};
  explicit WorkContext(Executor& executor, work_context_id_t id = kAnonymousId);
  WorkContext(const WorkContext&) = delete;
  WorkContext& operator=(const WorkContext&) = delete;
  virtual ~WorkContext() {}

  virtual void add(folly::Func func) override;
  work_context_id_t getId() const;
  bool anonymous() const;

 private:
  Executor& executor_;
  work_context_id_t id_;
};

}} // namespace facebook::logdevice
