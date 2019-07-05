/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/ClusterState.h"
#include "logdevice/common/SequencerLocator.h"
#include "logdevice/common/WeakRefHolder.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/include/types.h"

/**
 * @file HashBasedSequencerLocator is an implementation of the SequencerLocator
 *       interface, primarily used by clients to map log ids to sequencer nodes
 *       handling appends for those logs.
 *
 *       This class uses a weighted consistent hash function (the same one
 *       servers use). Clients unable to talk to some of the nodes may call
 *       locateSequencer() multiple times, setting weights of those unavailable
 *       nodes to zero and rehashing.
 */

namespace facebook { namespace logdevice {

class HashBasedSequencerLocator : public SequencerLocator {
 public:
  explicit HashBasedSequencerLocator() : holder_(this) {}

  ~HashBasedSequencerLocator() override {}

  int locateSequencer(
      logid_t log_id,
      Completion cf,
      const ServerConfig::SequencersConfig* sequencers) override;

  // Continuation of the locateSequencer() method after logs config data is
  // received.
  void locateContinuation(logid_t log_id,
                          Completion cf,
                          const ServerConfig::SequencersConfig* sequencers,
                          const logsconfig::LogAttributes* log_attrs);

  // Locates sequencer assuming that all nodes are available.
  // Used by nodeset/copyset selector to make Appenders more likely to send
  // copies to local rack or region, to reduce cross-rack traffic.
  static node_index_t getPrimarySequencerNode(
      logid_t log_id,
      const configuration::nodes::NodesConfiguration& nodes_configuration,
      const logsconfig::LogAttributes* log_attrs);

  // The meat of hash-based sequencer location. Unlike the non-static method,
  // this one is synchronous (and nonblocking) but requires you to already
  // have all the relevant information, like the log's config.
  // Used in tests.
  static int locateSequencer(
      logid_t log_id,
      const configuration::nodes::NodesConfiguration* nodes_configuration,
      const logsconfig::LogAttributes* log_attrs, // if null, no affinity
      ClusterState* cs, // if null, all nodes considered available
      NodeID* out_sequencer,
      const configuration::SequencersConfig* sequencers = nullptr);

  bool isAllowedToCache() const override;

 protected:
  virtual ClusterState* getClusterState() const;
  virtual std::shared_ptr<const Configuration> getConfig() const;
  virtual const Settings& getSettings() const;
  virtual std::shared_ptr<const configuration::nodes::NodesConfiguration>
  getNodesConfiguration() const;

 private:
  WeakRefHolder<HashBasedSequencerLocator> holder_;
};

}} // namespace facebook::logdevice
