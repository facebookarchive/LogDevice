/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "folly/container/F14Map.h"
#include "logdevice/common/Connection.h"
#include "logdevice/common/Timer.h"
#include "logdevice/common/configuration/NodesConfig.h"

namespace facebook::logdevice {

class OverloadDetectorDependencies {
 public:
  OverloadDetectorDependencies() = default;
  virtual ~OverloadDetectorDependencies() = default;

  virtual std::chrono::milliseconds getLoopPeriod() const;
  virtual uint32_t getOverloadThreshold() const;
  virtual uint32_t getPercentile() const;
  virtual double getMinBufLengthsRead() const;
  virtual std::shared_ptr<const configuration::nodes::NodesConfiguration>
  getNodesConfiguration();

  Connection* getConnectionFor(node_index_t nid);
  virtual size_t getTcpRecvBufSize(node_index_t nid);
  virtual ssize_t getTcpRecvBufOccupancy(node_index_t nid);
  virtual uint64_t getNumBytesReceived(node_index_t nid);

  const Settings& settings() const;
};

class OverloadDetector {
 public:
  explicit OverloadDetector(std::unique_ptr<OverloadDetectorDependencies> deps);
  ~OverloadDetector();
  void start();
  bool overloaded() const {
    return overloaded_;
  }

  void runOnce();
  void updateSampleFor(node_index_t nid);
  void updateOverloaded();

 private:
  struct Data {
    uint8_t util_pct;
    uint64_t lst_bytes_rcvd;
    uint64_t bytes_rcvd_delta;
    size_t buf_size;
  };

  void issueTimers();
  void cleanupHashMaps();
  void maybeBumpStats();

  folly::F14FastMap<node_index_t, Data> recv_q_data_;
  folly::F14FastMap<node_index_t, Timer> recv_q_timer_;
  std::unique_ptr<Timer> loop_timer_;
  std::unique_ptr<OverloadDetectorDependencies> deps_;
  bool overloaded_{false};
  bool last_reported_state_{false}; // did we report overload before?
};

} // namespace facebook::logdevice
