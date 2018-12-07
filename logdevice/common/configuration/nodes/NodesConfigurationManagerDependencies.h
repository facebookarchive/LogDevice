/**
 * Copyright (c) 2018-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/Random.h>

#include "logdevice/common/Processor.h"
#include "logdevice/common/Request.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/Node.h"
#include "logdevice/common/configuration/nodes/NodesConfigurationAPI.h"
#include "logdevice/common/configuration/nodes/NodesConfigurationStore.h"
#include "logdevice/common/membership/StorageMembership.h"

namespace facebook { namespace logdevice { namespace configuration {
namespace nodes {

class NodesConfigurationManager;

// This namespace should be considered implementation detail.
namespace ncm {

using NCMWeakPtr = std::weak_ptr<NodesConfigurationManager>;

//////// REQUESTS ////////

// Base class to pin any NCM related request to the correct worker. Derived
// requests should override executeOnNCM.
class NCMRequest : public Request {
 public:
  explicit NCMRequest(WorkerType worker_type,
                      worker_id_t worker_id,
                      NCMWeakPtr ncm)
      : Request(RequestType::NODES_CONFIGURATION_MANAGER),
        worker_type_(worker_type),
        worker_id_(worker_id),
        ncm_(std::move(ncm)) {
    ld_assert(ncm_.lock());
  }

  WorkerType getWorkerTypeAffinity() override {
    return worker_type_;
  }

  int getThreadAffinity(int /*nthreads*/) override {
    return worker_id_.val();
  }

  Request::Execution execute() final override;

 protected:
  // Derived classes may assume executeOnNCM won't be called with nullptr
  virtual Request::Execution
  executeOnNCM(std::shared_ptr<NodesConfigurationManager> ncm) = 0;

  WorkerType worker_type_;
  worker_id_t worker_id_;
  // Should only be accessed on the NCM worker, i.e., in executeOnNCM()
  NCMWeakPtr ncm_;
};

class NewConfigRequest : public NCMRequest {
 public:
  template <typename... Args>
  explicit NewConfigRequest(std::string new_config, Args&&... args)
      : NCMRequest(std::forward<Args>(args)...),
        new_config_(std::move(new_config)) {}

  Request::Execution
      executeOnNCM(std::shared_ptr<NodesConfigurationManager>) override;

 private:
  std::string new_config_;
};

class ProcessingFinishedRequest : public NCMRequest {
 public:
  template <typename... Args>
  explicit ProcessingFinishedRequest(
      std::shared_ptr<const NodesConfiguration> config,
      Args&&... args)
      : NCMRequest(std::forward<Args>(args)...), config_(std::move(config)) {}

  Request::Execution
      executeOnNCM(std::shared_ptr<NodesConfigurationManager>) override;

 private:
  std::shared_ptr<const NodesConfiguration> config_;
};

// External dependencies for the NodesConfigurationManager. Dependencies is
// owned by the state machine, thus it's safe to access it as long as the state
// machine is alive.
class Dependencies {
 public:
  explicit Dependencies(Processor* processor,
                        std::unique_ptr<NodesConfigurationStore> store);

  Dependencies(const Dependencies&) = delete;
  Dependencies& operator=(const Dependencies&) = delete;
  Dependencies(Dependencies&&) = delete;
  Dependencies& operator=(Dependencies&&) = delete;

  virtual ~Dependencies() {}

  // dcheck that we are in the proper NCM work context
  void dcheckOnNCM() const;

 protected:
  static constexpr const auto kConfigKey = "/ncm/config";

 private:
  // Convenience method to reduce boilerplate: only necessary to specify the
  // custom arguments
  template <typename Req, typename... Args>
  std::unique_ptr<Request> makeNCMRequest(Args&&... args) const {
    return std::make_unique<Req>(
        std::forward<Args>(args)..., worker_type_, worker_id_, ncm_);
  }

  class InitRequest : public NCMRequest {
   public:
    using NCMRequest::NCMRequest;
    Request::Execution
        executeOnNCM(std::shared_ptr<NodesConfigurationManager>) override;
  };

  void init(NCMWeakPtr);
  void readFromStoreAndActivateTimer();

  NCMWeakPtr ncm_{};
  // Worker / thread pinning in the current work execution model
  Processor* processor_{nullptr};
  WorkerType worker_type_;
  worker_id_t worker_id_;

  // Dependencies owns the underlying store
  std::unique_ptr<NodesConfigurationStore> store_{nullptr};
  // Timer for periodically polling from store_
  std::unique_ptr<Timer> timer_;

  friend class nodes::NodesConfigurationManager;
};
} // namespace ncm
}}}} // namespace facebook::logdevice::configuration::nodes
