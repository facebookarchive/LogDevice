/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/libevent/IEvBase.h"

namespace facebook { namespace logdevice {
thread_local IEvBase* IEvBase::running_base_ = nullptr;

}} // namespace facebook::logdevice
