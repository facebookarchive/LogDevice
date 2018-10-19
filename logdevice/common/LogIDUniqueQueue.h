/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <boost/multi_index/hashed_index.hpp>
#include <boost/multi_index/identity.hpp>
#include <boost/multi_index/mem_fun.hpp>
#include <boost/multi_index/sequenced_index.hpp>
#include <boost/multi_index_container.hpp>

#include "logdevice/common/types_internal.h"

namespace facebook { namespace logdevice {

// Wrapper instead of typedef to allow forward-declaring in Worker.h
struct LogIDUniqueQueue {
  struct FIFOIndex {};
  boost::multi_index::multi_index_container<
      logid_t,
      boost::multi_index::indexed_by<
          boost::multi_index::sequenced<boost::multi_index::tag<FIFOIndex>>,
          boost::multi_index::hashed_unique<
              boost::multi_index::identity<logid_t>,
              logid_t::Hash>>>
      q;
};
}} // namespace facebook::logdevice
