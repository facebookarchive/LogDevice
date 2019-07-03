/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once
#include <folly/container/F14Map.h>

#include "logdevice/common/EpochMetaDataMap.h"
#include "logdevice/common/EpochStore.h"
#include "logdevice/common/MetaDataLog.h"
#include "logdevice/common/MetaDataLogReader.h"
#include "logdevice/common/NodeSetFinder.h"
#include "logdevice/common/Request.h"
#include "logdevice/common/Semaphore.h"
#include "logdevice/common/SingleEvent.h"
#include "logdevice/common/Timestamp.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/debug.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

/**
 * @file LogMetaDataFetcher is a utility that can be used to fetch EpochMetaData
 * for a set of given logs. It can be used to both read the last metadata from
 * the epoch store, or the historical metadata from contacting sequencer or
 * reading the metadata log.
 */

class LogMetaDataFetcher {
 public:
  enum class Type { EPOCH_STORE_ONLY, HISTORICAL_METADATA_ONLY, BOTH };

  struct Result {
    // Only populated if type is != HISTORICAL_METADATA_ONLY
    Status epoch_store_status;
    std::unique_ptr<EpochMetaData> epoch_store_metadata;
    std::unique_ptr<EpochStoreMetaProperties> epoch_store_metadata_meta_props;
    epoch_t lce{EPOCH_INVALID};
    epoch_t metadatalog_lce{EPOCH_INVALID};
    TailRecord tail_record;
    // Only populated if type is != EPOCH_STORE_ONLY
    Status historical_metadata_status;
    std::shared_ptr<const EpochMetaDataMap> historical_metadata;
    // only populated (non-empty) if historical metadata is fetched from
    // metadata log
    NodeSetFinder::MetaDataExtrasMap metadata_extras;
  };

  using Results = folly::F14FastMap<logid_t, Result, logid_t::Hash>;
  using Callback = std::function<void(Results)>;

  /**
   * Create a LogMetaDataFetcher.
   *
   * @param epoch_store EpochStore to use. Can be nullptr if type is
   *                    HISTORICAL_METADATA_ONLY
   * @param logs        Set of logs to process
   * @param cb          Callback to call once all logs are processed.
   *                    May be called from a different thread than the thread
   *                    that started this LogMetaDataFetcher
   * @param             Can have the following values:
   *                    - EPOCH_STORE_ONLY: only read the last epoch metadata
   *                      from the epoch store
   *                    - HISTORICAL_METADATA_ONLY: only read the historical
   *                      epoch metadata from the metadata logs
   *                    - BOTH: read both
   */
  LogMetaDataFetcher(std::shared_ptr<EpochStore> epoch_store,
                     std::vector<logid_t> logs,
                     Callback cb,
                     Type = Type::HISTORICAL_METADATA_ONLY);
  ~LogMetaDataFetcher() {}

  /**
   * @param max How many logs should be processed concurrently;
   */
  void setMaxInFlight(size_t max) {
    max_in_flight_ = max;
  }

  /**
   * Once set, do not attempt to read from sequencers for historical metadata
   */
  void readHistoricalMetaDataOnlyFromMetaDataLog() {
    read_historical_metadata_only_from_metadata_log_ = true;
  }

  /**
   * Set the timeout for reading historical metadata before declaring the
   * operation timed out for each log. The default timeout is 15s.
   */
  void setHistoricalMetaDataTimeout(std::chrono::milliseconds timeout) {
    historical_metadata_timeout_ = timeout;
  }

  /**
   * Start the state machine.
   */
  void start(Processor* processor);

 private:
  std::shared_ptr<EpochStore> epoch_store_;
  std::list<logid_t> logs_;

  std::unordered_map<logid_t, std::unique_ptr<NodeSetFinder>, logid_t::Hash>
      nodeset_finders_;

  Results results_;
  size_t count_{0};
  size_t in_flight_{0};
  Callback cb_;
  Type type_;
  size_t max_in_flight_ = 1;
  bool read_historical_metadata_only_from_metadata_log_ = false;

  std::chrono::milliseconds historical_metadata_timeout_{15000};

  void scheduleMore();
  void scheduleForLog(logid_t logid);

  void getEpochStoreMetaData(logid_t logid);
  void getMetaDataLogLCE(logid_t logid);
  void getLCEAndTailRecord(logid_t logid);
  void readHistoricalMetaData(logid_t logid);
  void complete(logid_t logid);

  friend class StartLogMetaDataFetcherRequest;
};

}} // namespace facebook::logdevice
