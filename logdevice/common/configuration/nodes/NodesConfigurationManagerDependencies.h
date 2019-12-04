/**
 * Copyright (c) 2018-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/Random.h>
#include <folly/dynamic.h>

#include "logdevice/common/Processor.h"
#include "logdevice/common/Request.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/Node.h"
#include "logdevice/common/configuration/nodes/NodesConfigurationAPI.h"
#include "logdevice/common/configuration/nodes/NodesConfigurationStore.h"
#include "logdevice/common/configuration/nodes/NodesConfigurationTracer.h"
#include "logdevice/common/membership/StorageMembership.h"

namespace facebook { namespace logdevice { namespace configuration {
namespace nodes {

class NodesConfigurationManager;
enum class NCMReportType : uint16_t;

// This namespace should be considered implementation detail.
namespace ncm {

using NCMWeakPtr = std::weak_ptr<NodesConfigurationManager>;

//////// CONTEXTS ////////
// Tracing information that gets thread-through all NCM interactions
//
// Note: in the future, we could inherit from folly::RequestData and integrate
// with folly::RequestContext.
struct NCMRequestData {
  explicit NCMRequestData(std::shared_ptr<TraceLogger> trace_logger)
      : tracer_(std::move(trace_logger)) {}
  virtual ~NCMRequestData() {}
  virtual void onDestruction() = 0;
  void addTimestamp(folly::StringPiece key,
                    SystemTimestamp ts = SystemTimestamp::now()) {
    // We shouldn't overwrite timestamps
    ld_assert(timestamps_.find(key) == timestamps_.items().end());
    timestamps_[key] = ts.toMilliseconds().count();
  }

  NodesConfigurationTracer tracer_;
  Status status_{Status::UNKNOWN};
  folly::dynamic timestamps_ = folly::dynamic::object();
};

struct UpdateRequestData : public NCMRequestData {
  using NCMRequestData::NCMRequestData;
  void onDestruction() override;

  NodesConfiguration::Update update_;
  // proposed NC after NCM applies the updates
  std::shared_ptr<const NodesConfiguration> nc_{nullptr};
};

struct OverwriteRequestData : public NCMRequestData {
  using NCMRequestData::NCMRequestData;
  void onDestruction() override;

  std::shared_ptr<const NodesConfiguration> nc_{nullptr};
  std::string serialized_nc_{};
};

template <typename Data>
class NCMContext {
 public:
  static_assert(std::is_base_of<NCMRequestData, Data>::value,
                "NCMContext must be instantiated with a derived class of "
                "NCMRequestData.");
  template <typename... Args>
  explicit NCMContext(Args&&... args) : data_(std::forward<Args>(args)...) {
    // Automatic timestamp for the start of the request.
    data_.addTimestamp("req_received");
  }

  NCMContext(const NCMContext&) = delete;
  NCMContext& operator=(const NCMContext&) = delete;
  NCMContext(NCMContext&& other) noexcept
      : dismissed_(std::exchange(other.dismissed_, true)),
        data_(std::move(other.data_)) {}
  NCMContext& operator=(NCMContext&& other) noexcept {
    dismissed_ = std::exchange(other.dismissed_, true);
    data_(std::move(other.data_));
  }

  ~NCMContext() {
    if (!dismissed_) {
      ld_assert(data_.status_ != Status::UNKNOWN);
      data_.addTimestamp("req_completed");
      // Automatic timestamp for the end of the request.
      data_.onDestruction();
    }
  }

  void setStatus(Status status) {
    data_.status_ = status;
  }

  void addTimestamp(folly::StringPiece key,
                    SystemTimestamp ts = SystemTimestamp::now()) {
    data_.addTimestamp(key, ts);
  }

 private:
  bool dismissed_{false};

 public:
  Data data_;
};

using UpdateContext = NCMContext<UpdateRequestData>;
using OverwriteContext = NCMContext<OverwriteRequestData>;

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
  explicit NewConfigRequest(std::string serialized_new_config, Args&&... args)
      : NCMRequest(std::forward<Args>(args)...),
        serialized_(true),
        serialized_new_config_(std::move(serialized_new_config)),
        new_config_ptr_(nullptr) {}

  template <typename... Args>
  explicit NewConfigRequest(
      std::shared_ptr<const NodesConfiguration> new_config_ptr,
      Args&&... args)
      : NCMRequest(std::forward<Args>(args)...),
        serialized_(false),
        serialized_new_config_(),
        new_config_ptr_(std::move(new_config_ptr)) {}

  Request::Execution
      executeOnNCM(std::shared_ptr<NodesConfigurationManager>) override;

 private:
  bool serialized_;
  std::string serialized_new_config_;
  std::shared_ptr<const NodesConfiguration> new_config_ptr_;
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

class UpdateRequest : public NCMRequest {
 public:
  template <typename... Args>
  explicit UpdateRequest(UpdateContext ctx,
                         NodesConfigurationAPI::CompletionCb callback,
                         Args&&... args)
      : NCMRequest(std::forward<Args>(args)...),
        ctx_(std::move(ctx)),
        callback_(std::move(callback)) {}

  Request::Execution
      executeOnNCM(std::shared_ptr<NodesConfigurationManager>) override;

 private:
  UpdateContext ctx_;
  NodesConfigurationAPI::CompletionCb callback_;
};

class ReportRequest : public NCMRequest {
 public:
  template <typename... Args>
  explicit ReportRequest(NCMReportType type, Args&&... args)
      : NCMRequest(std::forward<Args>(args)...), type_(type) {}

  Request::Execution
      executeOnNCM(std::shared_ptr<NodesConfigurationManager>) override;

 private:
  NCMReportType type_;
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
  void dcheckNotOnNCM() const;
  void dcheckNotOnProcessor() const;

  void overwrite(OverwriteContext ctx,
                 NodesConfigurationAPI::CompletionCb callback);

 private:
  bool isOnNCM() const;

  std::shared_ptr<TraceLogger> getTraceLogger() const {
    return processor_->getTraceLogger();
  }

  // Convenience method to reduce boilerplate: only necessary to specify the
  // custom arguments
  template <typename Req, typename... Args>
  std::unique_ptr<Request> makeNCMRequest(Args&&... args) const {
    return std::make_unique<Req>(
        std::forward<Args>(args)..., worker_type_, worker_id_, ncm_);
  }

  void postNewConfigRequest(std::string);
  void postNewConfigRequest(std::shared_ptr<const NodesConfiguration>);

  class InitRequest : public NCMRequest {
   public:
    template <typename... Args>
    InitRequest(std::shared_ptr<const NodesConfiguration> init_nc,
                Args&&... args)
        : NCMRequest(std::forward<Args>(args)...),
          init_nc_(std::move(init_nc)) {}
    Request::Execution
        executeOnNCM(std::shared_ptr<NodesConfigurationManager>) override;

   private:
    std::shared_ptr<const NodesConfiguration> init_nc_;
  };

  class ShutdownRequest : public NCMRequest {
   public:
    using NCMRequest::NCMRequest;
    Request::Execution
        executeOnNCM(std::shared_ptr<NodesConfigurationManager>) override;
  };

  void init(NCMWeakPtr, std::shared_ptr<const NodesConfiguration>);
  void shutdown();
  bool shutdownSignaled() const;

  // should be called in NCM context
  void cancelTimer();
  void readFromStore(bool should_do_consistent_config_fetch);

  void scheduleHeartBeat();

  StatsHolder* getStats();
  void
  reportPropagationLatency(const std::shared_ptr<const NodesConfiguration>&);
  // Only worker threads have access to stats
  void reportEvent(NCMReportType type);

  // Compare the ServerConfig NC against the NCM NC and report the divergence
  // to the stats.
  void checkAndReportConsistency();

  NCMWeakPtr ncm_{};
  // Worker / thread pinning in the current work execution model
  Processor* processor_{nullptr};
  WorkerType worker_type_;
  worker_id_t worker_id_;

  // Dependencies owns the underlying store
  std::unique_ptr<NodesConfigurationStore> store_{nullptr};
  // Timer for periodically polling from store_
  std::unique_ptr<Timer> timer_;
  std::atomic<bool> shutdown_signaled_{false};
  NodesConfigurationTracer tracer_;

  friend class nodes::NodesConfigurationManager;
  friend class ReportRequest;
};
} // namespace ncm
}}}} // namespace facebook::logdevice::configuration::nodes
