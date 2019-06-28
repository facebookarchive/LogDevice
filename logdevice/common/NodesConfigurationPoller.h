/**
 * Copyright (c) 2018-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <atomic>

#include <folly/Optional.h>

#include "logdevice/common/ObjectPoller.h"
#include "logdevice/common/WorkerCallbackHelper.h"
#include "logdevice/common/configuration/nodes/NodesConfiguration.h"
#include "logdevice/common/configuration/nodes/NodesConfigurationStore.h"

namespace facebook { namespace logdevice {

class ClusterState;

// an instantiation of ObjectPoller which can be used to poll NodesConfiguration
// updates from existing server nodes in NodesConfiguration
class NodesConfigurationPoller {
 public:
  struct NodeResponse {
    Status st;
    std::string config_str;
  };

  using Poller =
      ObjectPoller</*object*/ std::string, NodeResponse, node_index_t>;

  using NodeSourceSet = Poller::SourceSet;
  using Callback = Poller::ObjectCallback;
  using Version = configuration::nodes::NodesConfigurationStore::version_t;
  using VersionExtFn =
      configuration::nodes::NodesConfigurationStore::extract_version_fn;

  /**
   * @param conditional_base_version   if set, always use this
   *                                   conditional_base_version
   *                                   as the base version (instead of
   *                                   highest_seen_) to perform conditional
   *                                   polling.
   */
  NodesConfigurationPoller(
      Poller::Options options,
      VersionExtFn version_fn,
      Callback cb,
      folly::Optional<Version> conditional_base_version = {});
  virtual ~NodesConfigurationPoller() {}

  // must be called on the worker thread
  void start();
  void stop();

  /**
   * Called when the worker context of NodesConfigurationPoller has changed to a
   * new NodesConfiguration. Update the candidate node list based on the new
   * configuration, and also update the higest seen config version for
   * conditional polling.
   */
  void onNodesConfigurationChanged();

 protected:
  virtual Poller::RequestResult sendRequestToNode(Poller::RoundID round,
                                                  node_index_t node);

  virtual ClusterState* getClusterState();

  virtual std::shared_ptr<const configuration::nodes::NodesConfiguration>
  getNodesConfiguration() const;

  virtual folly::Optional<node_index_t> getMyNodeID() const;
  // return true if the NCPoller is running in the bootstrapping environment
  // (e.g., NodesConfigurationInit). If so, conditional polling will be disabled
  virtual bool isBootstrapping() const;

 private:
  Poller::Options options_;
  VersionExtFn version_fn_;
  Callback cb_;
  const folly::Optional<Version> conditional_base_version_;
  Version highest_seen_{0};

  std::unique_ptr<Poller> poller_;

  // used to re-route the ConfigurationFetchRequest result callback
  WorkerCallbackHelper<NodesConfigurationPoller> callback_helper_;

  std::unique_ptr<Poller> createPoller();

  folly::Optional<std::string>
  aggregateConfiguration(const std::string* config,
                         NodeResponse response) const;

  void onConfigurationFetchResult(Poller::RoundID round,
                                  node_index_t source,
                                  Status st,
                                  std::string config);

  void onPollerCallback(Status st,
                        Poller::RoundID round,
                        folly::Optional<std::string> config_str);

  folly::Optional<Version> getConditionalPollVersion() const;

  static NodeSourceSet candidatesFromNodesConfiguration(
      const configuration::nodes::NodesConfiguration& config,
      folly::Optional<node_index_t> my_node_id);
};

}} // namespace facebook::logdevice
