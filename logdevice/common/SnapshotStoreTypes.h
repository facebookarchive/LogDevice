/**
 * Copyright (c) 2019-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

namespace facebook { namespace logdevice {
enum class SnapshotStoreType : uint8_t { NONE = 0, LOG, MESSAGE, LOCAL_STORE };
}} // namespace facebook::logdevice
