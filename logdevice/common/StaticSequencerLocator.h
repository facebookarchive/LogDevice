/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/SequencerLocator.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/UpdateableConfig.h"

/**
 * @file  StaticSequencerLocator is an implementation of SequencerLocator which
 *        assumes that the first node in the cluster configuration runs
 *        sequencers for all logs.
 */

namespace facebook { namespace logdevice {

class StaticSequencerLocator : public SequencerLocator {
 public:
  explicit StaticSequencerLocator(std::shared_ptr<UpdateableConfig> config)
      : config_(std::move(config)) {}

  int locateSequencer(logid_t logid,
                      Completion cf,
                      const configuration::SequencersConfig*) override {
    const auto nodes_configuration = config_->getNodesConfiguration();
    const bool has_node_zero_seq =
        nodes_configuration->getSequencerMembership()->hasNode(0);

    if (!has_node_zero_seq) {
      cf(E::NOTFOUND, logid, NodeID());
    }
    cf(E::OK, logid, nodes_configuration->getNodeID(0));
    return 0;
  }

  bool isAllowedToCache() const override {
    return false;
  }

 private:
  std::shared_ptr<UpdateableConfig> config_;
};

}} // namespace facebook::logdevice
