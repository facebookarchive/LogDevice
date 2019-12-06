/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/Random.h>

#include "logdevice/test/ldbench/worker/EventStore.h"

namespace facebook { namespace logdevice { namespace ldbench {

/**
 * Define the EventRecord
 * And eventRecords will be only created in Worker callback functions when
 * we decide to sample events.
 */
struct EventRecord {
  EventRecord(){};

  /**
   * @param
   *   ts -- system_clock timestamp (in ms). Actually, user can also define
   *         their own timestamp
   *   id -- identification of the event, e.g. workerid-logid-lsn-offset
   *   type -- indicating the type of events, e.g. 'wlat' for append latency
   *   value -- the content of the event, e.g. latency value for 'wlat'
   */
  EventRecord(uint64_t ts, std::string id, std::string type, uint64_t value) {
    time_stamp_ = ts;
    event_id_ = id;
    event_type_ = type;
    event_value_ = value;
  }
  uint64_t time_stamp_;
  std::string event_id_; // event identification

  std::string event_type_; // the type of event, e.g. wlat for write latency

  uint64_t event_value_; // the content of the event, e.g. if event type
                         // is wlat, blob will be the value of the latency
};

/**
 * Define the EventRecord and Submit an EventRecord to the EventStore.
 * Callers should use the getSampleRatio() interface to determine
 * how frequently to call doSample().

 */
class BenchTracer {
 public:
  BenchTracer(std::shared_ptr<EventStore> event_store, double sample_ratio)
      : event_store_(event_store), sample_ratio_(sample_ratio) {}

  ~BenchTracer() {}

  // doSample can be called by any threads since logEvent will handle
  // any writes asynchronously.
  void doSample(const EventRecord& event_record) {
    folly::dynamic event_obj = folly::dynamic::object();
    event_obj["timestamp"] = event_record.time_stamp_;
    event_obj["eventid"] = event_record.event_id_;
    event_obj["type"] = event_record.event_type_;
    event_obj["value"] = event_record.event_value_;
    event_store_->logEvent(event_obj);
    return;
  }

  bool isSample() {
    if (sample_ratio_ == 0) {
      return false;
    } else if (sample_ratio_ >= 1) {
      return true;
    }
    return folly::Random::randDouble01() <= sample_ratio_;
  }

 private:
  std::shared_ptr<EventStore> event_store_; // the target to store events
  double sample_ratio_;                     // ratio to sample an event
};

}}} // namespace facebook::logdevice::ldbench
