/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <map>
#include <vector>

#include "../Context.h"
#include "AdminCommandTable.h"

namespace facebook {
  namespace logdevice {
    namespace ldquery {
      namespace tables {

class AppendOutliers : public AdminCommandTable {
 public:
  explicit AppendOutliers(std::shared_ptr<Context> ctx)
      : AdminCommandTable(ctx) {}
  static std::string getName() {
    return "append_outliers";
  }
  std::string getDescription() override {
    return "Provides debugging information for the Sequencer Boycotting "
           "feature, which consists in boycotting sequencer nodes that are "
           "outliers when we compare their append success rate with the rest "
           "of the clusters.  "
           "This table contains the state of a few nodes in the cluster "
           "that are responsible for gathering stats from all other nodes, "
           "aggregate them and decide the list of outliers.";
  }
  TableColumns getFetchableColumns() const override {
    return {
        {"observed_node_id",
         DataType::INTEGER,
         "Node id for which we are measuring stats."},
        {"appends_success",
         DataType::BIGINT,
         "Number of appends that this node completed successfully."},
        {"appends_failed",
         DataType::BIGINT,
         "Number of appends that this node did not complete successfully."},
        {"msec_since",
         DataType::BIGINT,
         "Time in milliseconds since the node has been considered an outlier."},
        {"is_outlier",
         DataType::BOOL,
         "True if this node is considered an outlier."}};
  }
  std::string getCommandToSend(QueryContext& /*ctx*/) const override {
    return std::string("info append_outliers --json\n");
  }
};

}}}} // namespace facebook::logdevice::ldquery::tables
