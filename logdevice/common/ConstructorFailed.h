/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <exception>

namespace facebook { namespace logdevice {

/**
 * @file ConstructorFailed is currently the only exception class defined in
 *       LogDevice code base. Its purpose is to inform callers of constructors
 *       that the object could not be successfully initialized. Constructors
 *       are expected to set the thread-local logdevice::err before throwing
 *       this exception in order to communicate the reason for failure to the
 *       caller.
 */

struct ConstructorFailed : std::exception {};

}} // namespace facebook::logdevice
