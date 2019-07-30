/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_set>
#include <utility>

#include <folly/SharedMutex.h>
#include <opentracing/tracer.h>

#include "logdevice/common/ClientBridge.h"
#include "logdevice/common/buffered_writer/BufferedWriterImpl.h"
#include "logdevice/common/configuration/LogsConfig.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/settings/UpdateableSettings.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/Client.h"
#include "logdevice/include/types.h"
#include "logdevice/lib/AppendErrorInjector.h"

namespace facebook { namespace logdevice {

namespace configuration {
class NodesConfigurationAPI;
namespace nodes {
class NodesConfigurationStore;
class NodesConfigurationManager;
} // namespace nodes
} // namespace configuration

class AppendRequest;
class ClientAPIHitsTracer;
class ClientBridgeImpl;
class ClientEventTracer;
class ClientProcessor;
class ClientSettings;
class ClientSettingsImpl;
class ConfigSubscriptionHandle;
class EpochMetaDataCache;
class EpochMetaDataMap;
class PluginRegistry;
class Processor;
struct Settings;
class Shadow;
class StatsCollectionThread;
class StatsHolder;
class TailRecord;
class TraceLogger;

class ClientImpl : public Client,
                   public std::enable_shared_from_this<ClientImpl>,
                   public BufferedWriterAppendSink {
 public:
  using SteadyClock = std::chrono::steady_clock;
  using StatsSet = StatsParams::StatsSet;

  /**
   * This constructor should only be called by create() and the object
   * immediately wrapped in a shared_ptr, since certain methods of this class
   * call shared_from_this() which requires the object to already be managed
   * by a shared_ptr. See Client::create() for a description of
   * arguments. credentials are currently unused.
   */
  ClientImpl(std::string cluster_name,
             std::shared_ptr<UpdateableConfig> config,
             std::string credentials,
             std::string csid,
             std::chrono::milliseconds timeout,
             std::unique_ptr<ClientSettings>&& settings,
             std::shared_ptr<PluginRegistry> plugin_registry);

  virtual ~ClientImpl() override;

  // return value and error codes are the same as defined in appendSync()
  // in include/Client.h. One special case of BADPAYLOAD is when
  // allow_write_metadata_log_ is true (i.e., user called
  // allowWriteMetaDataLog()) and _logid_ is a metadata log id. The BADPAYLOAD
  // error code can be caused by the payload not containing valid epoch metadata
  lsn_t
  appendSync(logid_t logid,
             const Payload& payload,
             AppendAttributes attrs = AppendAttributes(),
             std::chrono::milliseconds* timestamp = nullptr) noexcept override;

  lsn_t
  appendSync(logid_t logid,
             std::string payload,
             AppendAttributes attrs = AppendAttributes(),
             std::chrono::milliseconds* timestamp = nullptr) noexcept override;

  int append(logid_t logid,
             std::string payload,
             append_callback_t cb,
             AppendAttributes attrs = AppendAttributes()) noexcept override;

  int append(logid_t logid,
             const Payload& payload,
             append_callback_t cb,
             AppendAttributes attrs = AppendAttributes()) noexcept override;

  int append(logid_t logid,
             std::string payload,
             append_callback_t cb,
             AppendAttributes attrs,
             worker_id_t target_worker,
             std::unique_ptr<std::string> per_request_token);

  int append(logid_t logid,
             const Payload& payload,
             append_callback_t cb,
             AppendAttributes attrs,
             worker_id_t target_worker,
             std::unique_ptr<std::string> per_request_token);

  // Variant of append() for use by BufferedWriter.  Differences:
  // - Forces the append to go to a specific Worker.
  // - Uses a slightly higher limit on the max payload size to allow for
  //   serialization overhead.
  std::pair<Status, NodeID>
  appendBuffered(logid_t logid,
                 const BufferedWriter::AppendCallback::ContextSet&,
                 AppendAttributes attrs,
                 const Payload& payload,
                 BufferedWriterAppendSink::AppendRequestCallback cb,
                 worker_id_t target_worker,
                 int checksum_bits) override;

  std::unique_ptr<Reader>
  createReader(size_t max_logs, ssize_t buffer_size = -1) noexcept override;

  std::unique_ptr<AsyncReader>
  createAsyncReader(ssize_t buffer_size) noexcept override;

  // normally I would just make timeout_ public, but here setTimeout() is
  // to be called by Client only (for binary compatibility), and getTimeout()
  // is only used by Impl classes, so I gave timeout_ a setter and getter to
  // simplify finding callsites. --march

  void setTimeout(std::chrono::milliseconds timeout) noexcept override {
    timeout_ = timeout;
  }

  int trimSync(
      logid_t logid,
      lsn_t lsn,
      std::unique_ptr<std::string> per_request_token = nullptr) noexcept;

  int trimSync(logid_t logid, lsn_t lsn) noexcept override;

  // Non blocking version of trim
  int trim(logid_t logid,
           lsn_t lsn,
           std::unique_ptr<std::string> per_request_token,
           trim_callback_t cb) noexcept;

  int trim(logid_t logid, lsn_t lsn, trim_callback_t cb) noexcept override;

  void addWriteToken(std::string token) noexcept override {
    folly::SharedMutex::WriteHolder guard(write_tokens_mutex_);
    write_tokens_.insert(token);
  }

  lsn_t findTimeSync(logid_t logid,
                     std::chrono::milliseconds timestamp,
                     Status* status_out,
                     FindKeyAccuracy accuracy) noexcept override;

  int findTime(logid_t logid,
               std::chrono::milliseconds timestamp,
               find_time_callback_t cb,
               FindKeyAccuracy accuracy) noexcept override;

  FindKeyResult findKeySync(logid_t logid,
                            std::string key,
                            FindKeyAccuracy accuracy) noexcept override;
  int findKey(logid_t logid,
              std::string key,
              find_key_callback_t cb,
              FindKeyAccuracy accuracy) noexcept override;

  int isLogEmptySync(logid_t logid, bool* empty) noexcept override;

  int isLogEmpty(logid_t logid, is_empty_callback_t cb) noexcept override;

  int isLogEmptyV2Sync(logid_t logid, bool* empty) noexcept override;

  int isLogEmptyV2(logid_t logid, is_empty_callback_t cb) noexcept override;

  int dataSizeSync(logid_t logid,
                   std::chrono::milliseconds start,
                   std::chrono::milliseconds end,
                   DataSizeAccuracy accuracy,
                   size_t* size) noexcept override;
  int dataSize(logid_t logid,
               std::chrono::milliseconds start,
               std::chrono::milliseconds end,
               DataSizeAccuracy accuracy,
               data_size_callback_t cb) noexcept override;

  lsn_t getTailLSNSync(logid_t logid) noexcept override;

  int getTailLSN(logid_t logid, get_tail_lsn_callback_t cb) noexcept override;

  std::unique_ptr<LogTailAttributes>
  getTailAttributesSync(logid_t logid) noexcept override;

  int getTailAttributes(logid_t logid,
                        get_tail_attributes_callback_t cb) noexcept override;

  std::unique_ptr<LogHeadAttributes>
  getHeadAttributesSync(logid_t logid) noexcept override;

  int getHeadAttributes(logid_t logid,
                        get_head_attributes_callback_t cb) noexcept override;

  std::chrono::milliseconds getTimeout() const {
    return timeout_;
  }

  // Note: the config should not be used after ClientImpl is destroyed
  const std::shared_ptr<UpdateableConfig>& getConfig() const {
    return config_;
  }

  logid_range_t getLogRangeByName(const std::string& name) noexcept override;

  void getLogRangeByName(const std::string& name,
                         get_log_range_by_name_callback_t cb) noexcept override;

  std::string getLogNamespaceDelimiter() noexcept override;

  LogsConfig::NamespaceRangeLookupMap
  getLogRangesByNamespace(const std::string& ns) noexcept override;

  void getLogRangesByNamespace(
      const std::string& ns,
      get_log_ranges_by_namespace_callback_t cb) noexcept override;

  std::unique_ptr<client::LogGroup>
  getLogGroupSync(const std::string& name) noexcept override;

  void getLogGroup(const std::string& name,
                   get_log_group_callback_t cb) noexcept override;

  std::unique_ptr<client::LogGroup>
  getLogGroupByIdSync(const logid_t logid) noexcept override;

  void getLogGroupById(const logid_t logid,
                       get_log_group_callback_t cb) noexcept override;

  int makeDirectory(const std::string& path,
                    bool mk_intermediate_dirs,
                    const client::LogAttributes& attrs,
                    make_directory_callback_t cb) noexcept override;

  std::unique_ptr<client::Directory>
  makeDirectorySync(const std::string& path,
                    bool mk_intermediate_dirs,
                    const client::LogAttributes& attrs,
                    std::string* failure_reason) noexcept override;

  int removeDirectory(const std::string& path,
                      bool recursive,
                      logsconfig_status_callback_t cb) noexcept override;

  bool removeDirectorySync(const std::string& path,
                           bool recursive,
                           uint64_t* version) noexcept override;

  int removeLogGroup(const std::string& path,
                     logsconfig_status_callback_t cb) noexcept override;

  bool removeLogGroupSync(const std::string& path,
                          uint64_t* version) noexcept override;

  int rename(const std::string& from_path,
             const std::string& to_path,
             logsconfig_status_callback_t cb) noexcept override;

  bool renameSync(const std::string& from_path,
                  const std::string& to_path,
                  uint64_t* version,
                  std::string* failure_reason) noexcept override;

  int makeLogGroup(const std::string& path,
                   const logid_range_t& range,
                   const client::LogAttributes& attrs,
                   bool mk_intermediate_dirs,
                   make_log_group_callback_t cb) noexcept override;

  std::unique_ptr<client::LogGroup>
  makeLogGroupSync(const std::string& path,
                   const logid_range_t& range,
                   const client::LogAttributes& attrs,
                   bool mk_intermediate_dirs,
                   std::string* failure_reason) noexcept override;

  int setAttributes(const std::string& path,
                    const client::LogAttributes& attrs,
                    logsconfig_status_callback_t cb) noexcept override;

  bool setAttributesSync(const std::string& path,
                         const client::LogAttributes& attrs,
                         uint64_t* version,
                         std::string* failure_reason) noexcept override;

  int setLogGroupRange(const std::string& path,
                       const logid_range_t& range,
                       logsconfig_status_callback_t) noexcept override;

  bool setLogGroupRangeSync(const std::string& path,
                            const logid_range_t& range,
                            uint64_t* version,
                            std::string* failure_reason) noexcept override;

  int getDirectory(const std::string& path,
                   get_directory_callback_t cb) noexcept override;

  std::unique_ptr<client::Directory>
  getDirectorySync(const std::string& path) noexcept override;

  bool syncLogsConfigVersion(uint64_t version) noexcept override;

  ConfigSubscriptionHandle
  notifyOnLogsConfigVersion(uint64_t version,
                            std::function<void()> cb) noexcept override;

  std::unique_ptr<ClusterAttributes> getClusterAttributes() noexcept override;

  ConfigSubscriptionHandle
      subscribeToConfigUpdates(config_update_callback_t) noexcept override;

  // historical metadata functions are not exposed to Client.h as public API
  using historical_metadata_callback_t =
      std::function<void(Status status,
                         std::shared_ptr<const EpochMetaDataMap>)>;

  using tail_record_callback_t =
      std::function<void(Status status, std::shared_ptr<TailRecord>)>;

  using read_tail_callback_t =
      std::function<void(Status status, std::unique_ptr<DataRecord>)>;

  std::shared_ptr<const EpochMetaDataMap>
  getHistoricalMetaDataSync(logid_t logid) noexcept;

  std::shared_ptr<TailRecord> getTailRecordSync(logid_t logid) noexcept;

  std::unique_ptr<DataRecord> readLogTailSync(logid_t logid) noexcept;

  int getHistoricalMetaData(logid_t logid,
                            historical_metadata_callback_t cb) noexcept;

  int getTailRecord(logid_t logid, tail_record_callback_t cb) noexcept;

  int readLogTail(logid_t logid, read_tail_callback_t cb) noexcept;

  ClientSettings& settings() override;

  std::string getAllReadStreamsDebugInfo() noexcept override;

  void publishEvent(Severity,
                    std::string name_space,
                    std::string type,
                    std::string data = "",
                    std::string context = "") noexcept override;

  ClientProcessor& getClientProcessor() const {
    return *processor_;
  }

  Processor& getProcessor() const;

  std::shared_ptr<ClientProcessor> getProcessorPtr() {
    return processor_;
  }

  StatsHolder* stats() const {
    return stats_.get();
  }

  // Registers custom stats to be published along with regular client stats.
  // As a proxy for stats being enabled, if stats_ isn't initialized, these
  // custom stats will be ignored.
  // Custom stats mustn't be destroyed before the client itself.
  void registerCustomStats(StatsHolder* custom_stats);

  configuration::NodesConfigurationAPI* getNodesConfigurationAPI();
  configuration::nodes::NodesConfigurationManager*
  getNodesConfigurationManager();

  read_stream_id_t issueReadStreamID();

  // Once called, the ClientImpl object can write to metadata log. Noted
  // that this function is not exposed to logdevice users in (Client.h) to
  // prevent them from accidentally writing to metadata logs.
  void allowWriteMetaDataLog();

  // Once called, the ClientImpl object can write to internal log. Noted
  // that this function is not exposed to logdevice users in (Client.h) to
  // prevent them from accidentally writing to internal logs.
  void allowWriteInternalLog();

  size_t getMaxPayloadSize() noexcept override;

  // return a raw pointer to the epoch metadata cache
  EpochMetaDataCache* getEpochMetaDataCache() {
    return epoch_metadata_cache_.get();
  }

  // verifies that arguments to append() are valid; sets err
  bool checkAppend(logid_t logid,
                   size_t payload_size,
                   bool allow_extra = false) override;

  bool hasWriteToken(const std::string& required) const {
    folly::SharedMutex::ReadHolder guard(write_tokens_mutex_);
    return write_tokens_.count(required);
  }

  const std::string& getClientSessionID() const {
    return csid_;
  }

  const std::shared_ptr<TraceLogger> getTraceLogger() const {
    return trace_logger_;
  }
  const std::shared_ptr<opentracing::Tracer> getOTTracer() const {
    return ottracer_;
  }

  void addServerConfigHookHandle(UpdateableServerConfig::HookHandle handle);

  void setAppendErrorInjector(folly::Optional<AppendErrorInjector> injector);

  bool shouldE2ETrace();

  template <typename T>
  std::unique_ptr<AppendRequest>
  prepareRequest(logid_t logid,
                 T payload,
                 append_callback_t cb,
                 AppendAttributes attrs,
                 worker_id_t target_worker,
                 std::unique_ptr<std::string> per_request_token);

  // Proxy for Processor::postRequest() with careful error handling
  int postAppend(std::unique_ptr<AppendRequest> req);

 private:
  // Used to validate that the `cluster_name` does not change across updates to
  // the config.
  bool validateServerConfig(ServerConfig& cfg) const;

  // Generates a new seed and stores that in logsconfig_api_random_seed_
  // This seed is used by LogsConfigApiRequest to consistently pick the same
  // server out of a randomized list of nodes for this client instance.
  void initLogsConfigRandomSeed();

  void updateStatsSettings();

  int getLocalDirectory(const std::string& path,
                        get_directory_callback_t cb) noexcept;

  void getLocalLogGroup(const std::string& name,
                        get_log_group_callback_t cb) noexcept;

  void getLocalLogGroupById(const logid_t logid,
                            get_log_group_callback_t cb) noexcept;

  bool hasFullyLoadedLocalLogsConfig() const;

  std::shared_ptr<PluginRegistry> plugin_registry_;

  std::string cluster_name_;

  std::shared_ptr<TraceLogger> trace_logger_;

  std::string credentials_;

  std::string csid_;

  std::chrono::milliseconds timeout_;

  // A random seed used by instances of LogsConfigApiRequest to choose a
  // server to talk to.
  uint64_t logsconfig_api_random_seed_;

  // Order matters.  Settings need to stick around longer than the Processor.
  std::unique_ptr<ClientSettingsImpl> settings_;

  // cache epoch metadata entries read from the metadata log
  std::unique_ptr<EpochMetaDataCache> epoch_metadata_cache_;

  std::shared_ptr<UpdateableConfig> config_;

  std::unique_ptr<StatsHolder> stats_;

  std::unique_ptr<ClientBridgeImpl> bridge_;

  std::unique_ptr<ClientEventTracer> event_tracer_;

  // Set of write tokens added with addWriteToken().  Consulted on every write
  // to see if the write should be allowed.
  std::unordered_set<std::string> write_tokens_;
  folly::SharedMutex write_tokens_mutex_;

  // The pointer to the processor might be shared with RemoteLogsConfig
  std::shared_ptr<ClientProcessor> processor_;

  std::unique_ptr<StatsCollectionThread> stats_thread_;

  // Should be deleted before config and settings
  std::unique_ptr<Shadow> shadow_;

  // see allowWriteMetaDataLog() above
  bool allow_write_metadata_log_{false};

  // see allowWriteInternalLog() above
  bool allow_write_internal_log_{false};

  std::list<UpdateableServerConfig::HookHandle> server_config_hook_handles_;

  UpdateableSettings<Settings>::SubscriptionHandle
      settings_subscription_handle_;

  folly::Optional<AppendErrorInjector> append_error_injector_;

  // OpenTracing tracer for client operations (append)
  std::shared_ptr<opentracing::Tracer> ottracer_;
};

class ClientBridgeImpl : public ClientBridge {
 public:
  explicit ClientBridgeImpl(ClientImpl* parent) : parent_(parent) {}

  const std::shared_ptr<TraceLogger> getTraceLogger() const override {
    return parent_->getTraceLogger();
  }

  const std::shared_ptr<opentracing::Tracer> getOTTracer() const override {
    return parent_->getOTTracer();
  }

  bool hasWriteToken(const std::string& required) const override {
    return parent_->hasWriteToken(required);
  }

  bool shouldE2ETrace() const override {
    return parent_->shouldE2ETrace();
  }

 private:
  ClientImpl* parent_;
};

}} // namespace facebook::logdevice
