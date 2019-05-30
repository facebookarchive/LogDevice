/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/admin/safety/LogMetaDataFetcher.h"

#include "logdevice/common/Processor.h"

namespace facebook { namespace logdevice {

void LogMetaDataFetcher::getEpochStoreMetaData(logid_t logid) {
  int rv = epoch_store_->readMetaData(
      logid,
      [=](Status status,
          logid_t /*log_id*/,
          std::unique_ptr<EpochMetaData> info,
          std::unique_ptr<EpochStoreMetaProperties> meta_props) {
        auto& result = results_[logid];
        if (status != E::OK) {
          ld_error("Could not read metadata from epoch store for log %lu: %s",
                   logid.val_,
                   error_description(status));
          result.epoch_store_status = status;
        } else {
          result.epoch_store_metadata = std::move(info);
          result.epoch_store_metadata_meta_props = std::move(meta_props);
          result.epoch_store_status = E::OK;
        }
        getMetaDataLogLCE(logid);
      });
  if (rv != 0) {
    results_[logid].epoch_store_status = err;
    ld_error("Could not read metadata from epoch store for log %lu: %s",
             logid.val_,
             error_description(err));
    getMetaDataLogLCE(logid);
  }
}

void LogMetaDataFetcher::getMetaDataLogLCE(logid_t logid) {
  const logid_t metadatalog_id = MetaDataLog::metaDataLogID(logid);
  int rv = epoch_store_->getLastCleanEpoch(
      metadatalog_id,
      [=](Status status, logid_t /*log_id*/, epoch_t epoch, TailRecord) {
        auto& result = results_[logid];
        if (status != E::OK) {
          result.epoch_store_status = status;
          ld_error("Could not get LCE for metadata log %lu: %s",
                   metadatalog_id.val_,
                   error_description(status));
        } else {
          result.metadatalog_lce = epoch;
        }
        getLCEAndTailRecord(logid);
      });
  if (rv != 0) {
    results_[logid].epoch_store_status = err;
    ld_error("Could not get LCE for metadata log %lu: %s",
             metadatalog_id.val_,
             error_description(err));
    getLCEAndTailRecord(logid);
  }
}

void LogMetaDataFetcher::getLCEAndTailRecord(logid_t logid) {
  int rv = epoch_store_->getLastCleanEpoch(
      logid,
      [=](Status status,
          logid_t /*log_id*/,
          epoch_t epoch,
          TailRecord tail_record) {
        auto& result = results_[logid];
        if (status != E::OK) {
          result.epoch_store_status = status;
          ld_error("Could not get LCE for log %lu: %s",
                   logid.val_,
                   error_description(status));
        } else {
          result.lce = epoch;
          result.tail_record = std::move(tail_record);
        }
        readHistoricalMetaData(logid);
      });
  if (rv != 0) {
    results_[logid].epoch_store_status = err;
    ld_error("Could not get LCE for log %lu: %s",
             logid.val_,
             error_description(err));
    readHistoricalMetaData(logid);
  }
}

void LogMetaDataFetcher::readHistoricalMetaData(logid_t logid) {
  if (type_ == Type::EPOCH_STORE_ONLY) {
    complete(logid);
    return;
  }

  nodeset_finders_[logid] = std::make_unique<NodeSetFinder>(
      logid,
      historical_metadata_timeout_,
      [this, logid](Status st) {
        auto& r = results_[logid];
        r.historical_metadata_status = st;
        if (st != E::OK) {
          ld_error("Could not read metadata log for log %lu : %s",
                   logid.val_,
                   error_description(st));
          complete(logid);
          return;
        }

        auto& ns_finder = nodeset_finders_[logid];
        // NodeSetFinder object must exist
        ld_check(ns_finder != nullptr);
        r.metadata_extras = ns_finder->getMetaDataExtras();
        r.historical_metadata = ns_finder->getResult();
        complete(logid);
      },
      (read_historical_metadata_only_from_metadata_log_
           ?
           // only from metadata log
           NodeSetFinder::Source::METADATA_LOG
           :
           // first try sequencer then fallback to metadata log if needed
           NodeSetFinder::Source::BOTH));

  nodeset_finders_[logid]->start();
}

void LogMetaDataFetcher::complete(logid_t logid) {
  nodeset_finders_.erase(logid);
  ld_check(in_flight_ > 0);
  --in_flight_;
  scheduleMore();
}

void LogMetaDataFetcher::scheduleMore() {
  while (!logs_.empty() && in_flight_ < max_in_flight_) {
    const logid_t log = logs_.front();
    logs_.pop_front();
    scheduleForLog(log);
  }

  if (logs_.empty() && in_flight_ == 0) {
    cb_(std::move(results_));
    return;
  }
}

void LogMetaDataFetcher::scheduleForLog(logid_t logid) {
  ++in_flight_;

  RATELIMIT_INFO(
      std::chrono::seconds{10}, 1, "%lu/%lu complete", results_.size(), count_);

  if (type_ == Type::HISTORICAL_METADATA_ONLY) {
    readHistoricalMetaData(logid);
  } else {
    getEpochStoreMetaData(logid);
  }
}

LogMetaDataFetcher::LogMetaDataFetcher(std::shared_ptr<EpochStore> epoch_store,
                                       std::vector<logid_t> logs,
                                       Callback cb,
                                       Type type)
    : epoch_store_(std::move(epoch_store)),
      logs_(logs.begin(), logs.end()),
      count_(logs_.size()),
      cb_(cb),
      type_(type) {}

class StartLogMetaDataFetcherRequest : public Request {
 public:
  explicit StartLogMetaDataFetcherRequest(LogMetaDataFetcher* fetcher)
      : Request(RequestType::CHECK_METADATA_LOG) /*TODO*/, fetcher_(fetcher) {}
  Request::Execution execute() override {
    fetcher_->scheduleMore();
    return Execution::COMPLETE;
  }

 private:
  LogMetaDataFetcher* fetcher_;
};

void LogMetaDataFetcher::start(Processor* processor) {
  std::unique_ptr<Request> request =
      std::make_unique<StartLogMetaDataFetcherRequest>(this);
  processor->postImportant(request);
}

}} // namespace facebook::logdevice
