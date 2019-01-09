/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/configuration/nodes/SequencerConfig.h"

#include "logdevice/common/debug.h"
#include "logdevice/common/types_internal.h"

namespace facebook { namespace logdevice { namespace configuration {
namespace nodes {

template <>
bool SequencerAttributeConfig::attributeSpecificValidate() const {
  return true;
}

template <>
bool SequencerConfig::roleSpecificValidate() const {
  return true;
}

}}}} // namespace facebook::logdevice::configuration::nodes
