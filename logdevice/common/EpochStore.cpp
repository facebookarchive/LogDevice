/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/EpochStore.h"

#include "logdevice/common/EpochMetaDataUpdater.h"

namespace facebook { namespace logdevice {

int EpochStore::readMetaData(logid_t logid, CompletionMetaData cf) {
  CompletionMetaData wrapped_cf =
      [cf2 = std::move(cf)](Status st,
                            const logid_t log_id,
                            std::unique_ptr<EpochMetaData> metadata,
                            std::unique_ptr<MetaProperties> meta_props) {
        if (st == E::UPTODATE) {
          st = E::OK;
        }
        cf2(st, log_id, std::move(metadata), std::move(meta_props));
      };
  return createOrUpdateMetaData(logid,
                                std::make_shared<ReadEpochMetaData>(),
                                std::move(wrapped_cf),
                                MetaDataTracer(), // not tracing reads
                                WriteNodeID::NO);
}

}} // namespace facebook::logdevice
