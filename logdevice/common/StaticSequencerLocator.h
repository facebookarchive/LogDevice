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
      : updateable_config_(config) {}

  int locateSequencer(logid_t logid,
                      Completion cf,
                      const configuration::SequencersConfig*) override {
    std::shared_ptr<Configuration> config = updateable_config_->get();
    const Configuration::Node* node =
        config->serverConfig()->getNode(node_index_t(0));
    if (node == nullptr) {
      cf(E::NOTFOUND, logid, NodeID());
    }
    cf(E::OK, logid, NodeID(0, node->generation));
    return 0;
  }

  bool isAllowedToCache() const override {
    return false;
  }

 private:
  std::shared_ptr<UpdateableConfig> updateable_config_;
};

}} // namespace facebook::logdevice
