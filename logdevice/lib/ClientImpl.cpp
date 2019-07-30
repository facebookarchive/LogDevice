/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/lib/ClientImpl.h"

#include <chrono>
#include <unordered_map>

#include <boost/algorithm/string.hpp>
#include <folly/Memory.h>
#include <folly/Random.h>

#include "logdevice/common/AppendRequest.h"
#include "logdevice/common/ClientAPIHitsTracer.h"
#include "logdevice/common/ClientBridge.h"
#include "logdevice/common/ClientEventTracer.h"
#include "logdevice/common/DataRecordFromTailRecord.h"
#include "logdevice/common/DataSizeRequest.h"
#include "logdevice/common/E2ETracer.h"
#include "logdevice/common/EpochMetaDataCache.h"
#include "logdevice/common/EpochMetaDataMap.h"
#include "logdevice/common/FindKeyRequest.h"
#include "logdevice/common/GetHeadAttributesRequest.h"
#include "logdevice/common/IsLogEmptyRequest.h"
#include "logdevice/common/LogsConfigApiRequest.h"
#include "logdevice/common/NoopTraceLogger.h"
#include "logdevice/common/PrincipalParser.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/ReaderImpl.h"
#include "logdevice/common/Semaphore.h"
#include "logdevice/common/StatsCollectionThread.h"
#include "logdevice/common/SyncSequencerRequest.h"
#include "logdevice/common/TailRecord.h"
#include "logdevice/common/ThreadID.h"
#include "logdevice/common/TrimRequest.h"
#include "logdevice/common/client_read_stream/AllClientReadStreams.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/TextConfigUpdater.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/configuration/logs/LogsConfigDeltaTypes.h"
#include "logdevice/common/configuration/logs/LogsConfigManager.h"
#include "logdevice/common/configuration/logs/LogsConfigStateMachine.h"
#include "logdevice/common/configuration/logs/LogsConfigTree.h"
#include "logdevice/common/configuration/nodes/NodesConfigurationManagerFactory.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/plugin/TraceLoggerFactory.h"
#include "logdevice/common/plugin/ZookeeperClientFactory.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/settings/UpdateableSettings.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/common/util.h"
#include "logdevice/include/ClientSettings.h"
#include "logdevice/include/Err.h"
#include "logdevice/include/Record.h"
#include "logdevice/lib/AsyncReaderImpl.h"
#include "logdevice/lib/ClientProcessor.h"
#include "logdevice/lib/ClientSettingsImpl.h"
#include "logdevice/lib/ClusterAttributesImpl.h"
#include "logdevice/lib/LogsConfigTypesImpl.h"
#include "logdevice/lib/shadow/Shadow.h"

using facebook::logdevice::logsconfig::FBuffersLogsConfigCodec;
using std::chrono::duration;
using std::chrono::steady_clock;

namespace facebook { namespace logdevice {

using NodesConfigurationManager =
    configuration::nodes::NodesConfigurationManager;
using NCSType = configuration::nodes::NodesConfigurationStoreFactory::NCSType;

// Implementing the member function of Client inside ClientImpl.cpp sounds
// confusing, but this is not an error. This is for the purpose of moving
// the indirection of calling ClientImpl::create in Client::create.
// Instead, users who call create will always call the Client::create.
std::shared_ptr<Client> Client::create(std::string cluster_name,
                                       std::string config_url,
                                       std::string credentials,
                                       std::chrono::milliseconds timeout,
                                       std::unique_ptr<ClientSettings> settings,
                                       std::string csid) noexcept {
  return ClientFactory()
      .setClusterName(std::move(cluster_name))
      .setCredentials(std::move(credentials))
      .setTimeout(timeout)
      .setClientSettings(std::move(settings))
      .setCSID(std::move(csid))
      .create(std::move(config_url));
}

bool ClientImpl::validateServerConfig(ServerConfig& cfg) const {
  ld_check(config_);
  ld_check(config_->get());
  if (config_->get()->serverConfig()->getClusterName() ==
      cfg.getClusterName()) {
    return true;
  } else {
    ld_error("Refusing to load config because cluster name has changed from "
             "%s to %s",
             config_->get()->serverConfig()->getClusterName().c_str(),
             cfg.getClusterName().c_str());
    return false;
  }
}

ClientImpl::ClientImpl(std::string cluster_name,
                       std::shared_ptr<UpdateableConfig> config,
                       std::string credentials,
                       std::string csid,
                       std::chrono::milliseconds timeout,
                       std::unique_ptr<ClientSettings>&& client_settings,
                       std::shared_ptr<PluginRegistry> plugin_registry)
    : plugin_registry_(std::move(plugin_registry)),
      cluster_name_(cluster_name),
      credentials_(std::move(credentials)),
      csid_(std::move(csid)),
      timeout_(timeout),
      config_(std::move(config)),
      bridge_(new ClientBridgeImpl(this)) {
  addServerConfigHookHandle(
      config_->updateableServerConfig()->addHook(std::bind(
          &ClientImpl::validateServerConfig, this, std::placeholders::_1)));

  // CSID generation: Duplicating subset of client::create functionality.
  // Reason: 'client::create' should be the only one calling this constructor,
  // but this is not always the case.
  if (csid_.empty()) {
    boost::uuids::uuid gen_csid = boost::uuids::random_generator()();
    csid_ = boost::uuids::to_string(gen_csid);
  }

  initLogsConfigRandomSeed();
  ClientSettings* raw_client_settings =
      client_settings ? client_settings.release() : ClientSettings::create();
  settings_.reset(static_cast<ClientSettingsImpl*>(raw_client_settings));
  UpdateableSettings<Settings> settings = settings_->getSettings();

  const size_t metadata_cache_size = settings->client_epoch_metadata_cache_size;
  if (metadata_cache_size > 0) {
    epoch_metadata_cache_ =
        std::make_unique<EpochMetaDataCache>(metadata_cache_size);
  }

  if (settings->stats_collection_interval.count() > 0 ||
      settings->client_test_force_stats) {
    auto params =
        StatsParams()
            .setIsServer(false)
            .addAdditionalEntitySuffix(".client")
            .setNodeStatsRetentionTimeOnClients(
                settings->sequencer_boycotting.node_stats_send_period);
    // Only create StatsHolder when we're going to collect stats, primarily to
    // avoid instantianting thread-local Stats unnecessarily
    stats_ = std::make_unique<StatsHolder>(std::move(params));
  }

  std::shared_ptr<ServerConfig> server_cfg = config_->get()->serverConfig();

  std::shared_ptr<TraceLoggerFactory> trace_logger_factory =
      plugin_registry_->getSinglePlugin<TraceLoggerFactory>(
          PluginType::TRACE_LOGGER_FACTORY);
  if (!trace_logger_factory || settings->trace_logger_disabled) {
    trace_logger_ = std::make_shared<NoopTraceLogger>(
        config_, /* my_node_id */ folly::none);
  } else {
    trace_logger_ =
        (*trace_logger_factory)(config_, /* my_node_id */ folly::none);
  }

  event_tracer_ =
      std::make_unique<ClientEventTracer>(trace_logger_, stats_.get());

  processor_ = ClientProcessor::create(config_,
                                       trace_logger_,
                                       settings,
                                       stats_.get(),
                                       plugin_registry_,
                                       credentials_,
                                       csid_);

  if (settings->enable_nodes_configuration_manager) {
    // create and initialize NodesConfigurationManager (NCM) and attach it to
    // the Processor

    // TODO: get NCS from NodesConfigurationInit instead
    auto zk_client_factory =
        plugin_registry_->getSinglePlugin<ZookeeperClientFactory>(
            PluginType::ZOOKEEPER_CLIENT_FACTORY);
    auto ncm = configuration::nodes::NodesConfigurationManagerFactory::create(
        processor_.get(),
        /*store=*/nullptr,
        /*server_roles*/ folly::none,
        std::move(zk_client_factory));
    if (ncm == nullptr) {
      ld_critical(
          "Unable to create NodesConfigurationManager during Client creation!");
      throw ConstructorFailed();
    }

    auto initial_nc = processor_->config_->getNodesConfigurationFromNCMSource();
    if (!initial_nc) {
      // Currently this should only happen in tests as our boostrapping
      // workflow should always ensure the Processor has a valid
      // NodesConfiguration before initializing NCM. In the future we will
      // require a valid NC for Processor construction and will turn this into
      // a ld_check.
      ld_warning("NodesConfigurationManager initialized without a valid "
                 "NodesConfiguration in its Processor context. This should "
                 "only happen in tests.");
      initial_nc = std::make_shared<const NodesConfiguration>();
    }
    if (!ncm->init(std::move(initial_nc))) {
      ld_critical(
          "Processing initial NodesConfiguration did not finish in time.");
      throw ConstructorFailed();
    }
  }

  if (!LogsConfigManager::createAndAttach(
          *processor_, false /* is_writable */)) {
    err = E::INVALID_CONFIG;
    ld_critical("Internal LogsConfig Manager could not be started in Client. "
                "LogsConfig will not be available!");
    throw ConstructorFailed();
  }

  stats_thread_ =
      StatsCollectionThread::maybeCreate(settings_->getSettings(),
                                         config_->get()->serverConfig(),
                                         plugin_registry_,
                                         /* num_shards */ 0,
                                         stats_.get());

  if (!config_->getLogsConfig() || !config_->getLogsConfig()->isFullyLoaded()) {
    Semaphore sem;

    auto start_time = std::chrono::steady_clock::now();
    auto updateable_logs_config = config_->updateableLogsConfig();
    // We always wait until we have a fully loaded config.
    auto updates_cb = [&]() {
      if (updateable_logs_config->get()->isFullyLoaded()) {
        auto end_time = std::chrono::steady_clock::now();
        ld_info("LogsConfig has been loaded in %.3f seconds...",
                std::chrono::duration_cast<std::chrono::duration<double>>(
                    end_time - start_time)
                    .count());
        sem.post();
      }
    };
    auto subscription = updateable_logs_config->subscribeToUpdates(updates_cb);
    ld_info("Waiting to load the LogsConfig...");

    const auto timeout_for_logconfig =
        settings_->getSettings()->logsconfig_timeout.value_or(timeout_);

    int rv = sem.timedwait(timeout_for_logconfig);
    if (rv != 0) {
      STAT_INCR(stats_.get(), client.logsconfig_start_timeout);
      ld_critical("Timeout waiting on LogsConfig to become fully loaded "
                  "after %.3f seconds",
                  timeout_for_logconfig.count() / 1e3);
      err = E::TIMEDOUT;
      throw ConstructorFailed();
    }
  } else {
    ld_info("Internal LogsConfig Manager is DISABLED.");
    if (!config_->getLogsConfig()) {
      ld_critical("Could not load the LogsConfig from the config file!");
      err = E::INVALID_CONFIG;
      throw ConstructorFailed();
    }
  }

  ld_check(config_->getLogsConfig() != nullptr);

  // Initialize traffic shadowing
  if (!settings->shadow_client) {
    shadow_ = std::make_unique<Shadow>(
        cluster_name_, config_, settings_->getSettings(), stats_.get());
  }

  settings_subscription_handle_ =
      settings.subscribeToUpdates([this] { this->updateStatsSettings(); });
}

ClientImpl::~ClientImpl() {
  auto start_time = std::chrono::steady_clock::now();
  ld_info("Destroying Client. Cluster name: %s", cluster_name_.c_str());

  server_config_hook_handles_.clear();
  processor_->shutdown();

  auto end_time = std::chrono::steady_clock::now();
  ld_info("Destroyed Client in %.3f seconds. Cluster name: %s",
          std::chrono::duration_cast<std::chrono::duration<double>>(end_time -
                                                                    start_time)
              .count(),
          cluster_name_.c_str());
}

int ClientImpl::append(logid_t logid,
                       const Payload& payload,
                       append_callback_t cb,
                       AppendAttributes attrs,
                       worker_id_t target_worker,
                       std::unique_ptr<std::string> per_request_token) {
  auto req = prepareRequest(logid,
                            payload,
                            cb,
                            std::move(attrs),
                            target_worker,
                            std::move(per_request_token));
  if (!req) {
    return -1;
  }
  return postAppend(std::move(req));
}

int ClientImpl::append(logid_t logid,
                       std::string payload,
                       append_callback_t cb,
                       AppendAttributes attrs,
                       worker_id_t target_worker,
                       std::unique_ptr<std::string> per_request_token) {
  auto req = prepareRequest(logid,
                            std::move(payload),
                            cb,
                            std::move(attrs),
                            target_worker,
                            std::move(per_request_token));
  if (!req) {
    return -1;
  }
  return postAppend(std::move(req));
}

std::pair<Status, NodeID> ClientImpl::appendBuffered(
    logid_t logid,
    const BufferedWriter::AppendCallback::ContextSet&,
    AppendAttributes attrs,
    const Payload& payload,
    BufferedWriterAppendSink::AppendRequestCallback buffered_writer_cb,
    worker_id_t target_worker,
    int checksum_bits) {
  ld_check(target_worker.val_ >= 0);
  ld_check(target_worker.val_ <
           processor_->getWorkerCount(WorkerType::GENERAL));
  // We don't expect BufferedWriter to prepend the checksum in a client
  // context.  Instead it will be prepended by
  // APPEND_Message::serialize() as with normal appends.
  ld_check(checksum_bits == 0);

  if (!checkAppend(logid, payload.size(), true)) {
    return std::make_pair(E::OK, NodeID());
  }

  // BufferedWriter's AppendRequestCallback takes the redirect NodeID for
  // internal use on servers but doesn't need that in a client context.
  auto wrapped_cb =
      [buffered_writer_cb /* TODO cb = std::move(buffered_writer_cb) */](
          Status status, const DataRecord& record) {
        buffered_writer_cb(status, record, NodeID());
      };

  auto req = std::make_unique<AppendRequest>(
      bridge_.get(),
      logid,
      std::move(attrs),
      payload,
      settings_->getSettings()->append_timeout.value_or(timeout_),
      std::move(wrapped_cb));

  req->setTargetWorker(target_worker);
  req->setBufferedWriterBlobFlag();
  int rv = postAppend(std::move(req));
  return std::make_pair(rv == 0 ? E::OK : err, NodeID());
}

int ClientImpl::postAppend(std::unique_ptr<AppendRequest> req_append) {
  if (append_error_injector_) {
    // with the set probability of the error injector, maybe replace the request
    // with and append request that is sure to fail
    req_append =
        append_error_injector_->maybeReplaceRequest(std::move(req_append));
  }
  ld_check(req_append != nullptr);

  req_append->setAppendProbeController(&processor_->appendProbeController());

  // First perform shadowing. since postRequest can invalidate pointer
  if (shadow_ != nullptr) { // Is only null for shadow clients
    shadow_->appendShadow(*req_append.get());
  }

  // Then post original client request
  std::unique_ptr<Request> req(std::move(req_append));
  int rv = processor_->postRequest(req);
  if (rv != 0) {
    // Instruct ~AppendRequest not to invoke the callback
    static_cast<AppendRequest*>(req.get())->setFailedToPost();

    AppendRequest::bumpStatForOutcome(stats_.get(), err);
  }

  return rv;
}

/**
 * AppendGate is a synchronization functor for appendSync().
 */
class AppendGate {
 public:
  AppendGate() : lsn_(LSN_INVALID), status_(E::OK) {}

  // called by AppendRequest when request processing completes or
  // timeout expires
  void operator()(Status st, const DataRecord& r) {
    lsn_ = r.attrs.lsn;
    timestamp_ = r.attrs.timestamp;
    status_ = st;

    validate();
    sem_.post();
  }

  void wait() {
    sem_.wait();
  }

  void validate() {
    if (lsn_ == LSN_INVALID) {
      ld_check(status_ != E::OK);
    } else {
      ld_check(status_ == E::OK);
    }
  }

  lsn_t getLSN() {
    return lsn_;
  }
  std::chrono::milliseconds getTimestamp() {
    return timestamp_;
  }
  Status getStatus() {
    return status_;
  }

 private:
  Semaphore sem_;
  lsn_t lsn_;
  std::chrono::milliseconds timestamp_;
  Status status_;
};

template <typename T>
static lsn_t append_sync_helper(ClientImpl* client,
                                logid_t logid,
                                T&& payload,
                                AppendAttributes attrs,
                                std::chrono::milliseconds* timestamp) {
  AppendGate gate; // request-reply synchronization
  int rv;

  rv = client->append(logid,
                      std::forward<T>(payload),
                      std::ref(gate),
                      std::move(attrs),
                      worker_id_t{-1},
                      nullptr);
  if (rv != 0) {
    // err was set by append()
    return LSN_INVALID;
  }

  gate.wait();
  gate.validate();

  if (gate.getStatus() != E::OK) {
    err = gate.getStatus();
    return LSN_INVALID;
  }

  if (timestamp) {
    *timestamp = gate.getTimestamp();
  }
  return gate.getLSN();
}

int ClientImpl::append(logid_t logid,
                       std::string payload,
                       append_callback_t cb,
                       AppendAttributes attrs) noexcept {
  return append(logid,
                std::move(payload),
                std::move(cb),
                std::move(attrs),
                worker_id_t{-1},
                nullptr);
}

int ClientImpl::append(logid_t logid,
                       const Payload& payload,
                       append_callback_t cb,
                       AppendAttributes attrs) noexcept {
  return append(logid,
                payload,
                std::move(cb),
                std::move(attrs),
                worker_id_t{-1},
                nullptr);
}

lsn_t ClientImpl::appendSync(logid_t logid,
                             const Payload& payload,
                             AppendAttributes attrs,
                             std::chrono::milliseconds* ts) noexcept {
  return append_sync_helper(this, logid, payload, std::move(attrs), ts);
}

lsn_t ClientImpl::appendSync(logid_t logid,
                             std::string payload,
                             AppendAttributes attrs,
                             std::chrono::milliseconds* ts) noexcept {
  return append_sync_helper(
      this, logid, std::move(payload), std::move(attrs), ts);
}

std::unique_ptr<Reader> ClientImpl::createReader(size_t max_logs,
                                                 ssize_t buffer_size) noexcept {
  return std::make_unique<ReaderImpl>(max_logs,
                                      buffer_size,
                                      processor_.get(),
                                      getEpochMetaDataCache(),
                                      shared_from_this());
}

std::unique_ptr<AsyncReader>
ClientImpl::createAsyncReader(ssize_t buffer_size) noexcept {
  return std::make_unique<AsyncReaderImpl>(shared_from_this(), buffer_size);
}

static std::string get_full_name(const std::string& name,
                                 const Configuration& config,
                                 const ClientSettingsImpl& settings) {
  // If the name starts with a delimiter, we don't use the default namespace
  std::string delim = config.logsConfig()->getNamespaceDelimiter();
  if (name.size() > 0 && name.compare(0, delim.size(), delim) == 0) {
    return name;
  } else {
    return config.logsConfig()->getNamespacePrefixedLogRangeName(
        settings.getSettings()->default_log_namespace, name);
  }
}

logid_range_t ClientImpl::getLogRangeByName(const std::string& name) noexcept {
  if (ThreadID::isWorker()) {
    ld_error("Synchronous methods for fetching log configuration should not "
             "be called from worker threads on the client.");
    ld_check(false);
    err = E::FAILED;
    return logid_range_t(logid_t(0), logid_t(0));
  }

  std::string full_name = get_full_name(name, *config_->get(), *settings_);
  return config_->get()->logsConfig()->getLogRangeByName(full_name);
}

void ClientImpl::getLogRangeByName(
    const std::string& name,
    get_log_range_by_name_callback_t cb) noexcept {
  std::string full_name = get_full_name(name, *config_->get(), *settings_);
  config_->get()->logsConfig()->getLogRangeByNameAsync(full_name, cb);
}

std::string ClientImpl::getLogNamespaceDelimiter() noexcept {
  return config_->get()->logsConfig()->getNamespaceDelimiter();
}

LogsConfig::NamespaceRangeLookupMap
ClientImpl::getLogRangesByNamespace(const std::string& ns) noexcept {
  if (ThreadID::isWorker()) {
    ld_error("Synchronous methods for fetching log configuration should not "
             "be called from worker threads on the client.");
    ld_check(false);
    err = E::FAILED;
    return {};
  }

  auto full_ns = get_full_name(ns, *config_->get(), *settings_);
  return config_->get()->logsConfig()->getLogRangesByNamespace(full_ns);
}

void ClientImpl::getLogRangesByNamespace(
    const std::string& ns,
    get_log_ranges_by_namespace_callback_t cb) noexcept {
  auto full_ns = get_full_name(ns, *config_->get(), *settings_);
  config_->get()->logsConfig()->getLogRangesByNamespaceAsync(full_ns, cb);
}

std::unique_ptr<client::LogGroup>
ClientImpl::getLogGroupSync(const std::string& name) noexcept {
  auto full_ns = get_full_name(name, *config_->get(), *settings_);
  if (ThreadID::isWorker()) {
    ld_error("Synchronous methods for fetching log configuration should not "
             "be called from worker threads on the client.");
    ld_check(false);
    err = E::FAILED;
    return nullptr;
  }
  Status status = E::OK;
  std::unique_ptr<client::LogGroup> lg;
  Semaphore sem;
  auto cb = [&](Status st, std::unique_ptr<client::LogGroup> result) {
    lg = std::move(result);
    status = st;
    sem.post();
  };

  getLogGroup(full_ns, cb);

  sem.wait();
  if (status != E::OK) {
    err = status;
    return nullptr;
  }
  return lg;
}

bool ClientImpl::hasFullyLoadedLocalLogsConfig() const {
  auto logs_config = config_->getLogsConfig();
  return logs_config != nullptr && logs_config->isLocal() &&
      logs_config->isFullyLoaded();
}

void ClientImpl::getLocalLogGroup(const std::string& path,
                                  get_log_group_callback_t cb) noexcept {
  ld_assert(hasFullyLoadedLocalLogsConfig());
  auto logs_config = config_->getLocalLogsConfig();

  auto log_group = logs_config->getLogGroup(path);
  if (log_group == nullptr) {
    cb(Status::NOTFOUND, nullptr);
    return;
  }
  std::unique_ptr<client::LogGroupImpl> lg =
      std::make_unique<client::LogGroupImpl>(
          std::move(log_group), path, logs_config->getVersion());
  cb(Status::OK, std::move(lg));
}

void ClientImpl::getLogGroup(const std::string& path,
                             get_log_group_callback_t cb) noexcept {
  auto full_ns = get_full_name(path, *config_->get(), *settings_);
  if (hasFullyLoadedLocalLogsConfig()) {
    getLocalLogGroup(full_ns, std::move(cb));
    return;
  }
  std::string delimiter =
      config_->get()->serverConfig()->getNamespaceDelimiter();
  auto callback = [cb, full_ns, delimiter](
                      Status st, uint64_t config_version, std::string payload) {
    if (st == E::OK) {
      ld_check(payload.size() > 0);
      // deserialize the payload
      std::shared_ptr<logsconfig::LogGroupNode> recovered_lg =
          LogsConfigStateMachine::deserializeLogGroup(payload, delimiter);
      ld_check(recovered_lg != nullptr);
      if (recovered_lg == nullptr) {
        // deserialization failed!
        st = E::FAILED;
        cb(st, nullptr);
        return;
      }
      std::unique_ptr<client::LogGroupImpl> lg =
          std::make_unique<client::LogGroupImpl>(
              std::move(recovered_lg), full_ns, config_version);
      cb(st, std::move(lg));
    } else {
      cb(st, nullptr);
    }
  };

  std::unique_ptr<Request> req = std::make_unique<LogsConfigApiRequest>(
      LOGS_CONFIG_API_Header::Type::GET_LOG_GROUP_BY_NAME,
      full_ns,
      settings_->getSettings()->logsconfig_timeout.value_or(timeout_),
      LogsConfigApiRequest::MAX_ERRORS,
      logsconfig_api_random_seed_,
      callback);
  processor_->postRequest(req);
}

std::unique_ptr<client::LogGroup>
ClientImpl::getLogGroupByIdSync(const logid_t logid) noexcept {
  if (ThreadID::isWorker()) {
    ld_error("Synchronous methods for fetching log configuration should not "
             "be called from worker threads on the client.");
    ld_check(false);
    err = E::FAILED;
    return nullptr;
  }
  Status status = E::OK;
  std::unique_ptr<client::LogGroup> lg;
  Semaphore sem;
  auto cb = [&](Status st, std::unique_ptr<client::LogGroup> result) {
    lg = std::move(result);
    status = st;
    sem.post();
  };

  getLogGroupById(logid, cb);

  sem.wait();
  if (status != E::OK) {
    err = status;
    return nullptr;
  }
  return lg;
}

void ClientImpl::getLocalLogGroupById(const logid_t logid,
                                      get_log_group_callback_t cb) noexcept {
  ld_assert(hasFullyLoadedLocalLogsConfig());
  auto logs_config = config_->getLocalLogsConfig();

  auto lid = logs_config->getLogGroupInDirectoryByIDRaw(logid);
  if (lid == nullptr) {
    cb(Status::NOTFOUND, nullptr);
    return;
  }

  std::string full_ns = lid->getFullyQualifiedName();
  std::unique_ptr<client::LogGroupImpl> lg =
      std::make_unique<client::LogGroupImpl>(
          lid->log_group, std::move(full_ns), logs_config->getVersion());
  cb(Status::OK, std::move(lg));
}

void ClientImpl::getLogGroupById(const logid_t logid,
                                 get_log_group_callback_t cb) noexcept {
  if (MetaDataLog::isMetaDataLog(logid) ||
      configuration::InternalLogs::isInternal(logid)) {
    cb(Status::NOTFOUND, nullptr);
    return;
  }
  if (hasFullyLoadedLocalLogsConfig()) {
    getLocalLogGroupById(logid, std::move(cb));
    return;
  }
  std::string delimiter =
      config_->get()->serverConfig()->getNamespaceDelimiter();
  auto callback = [cb, delimiter](
                      Status st, uint64_t config_version, std::string payload) {
    if (st == E::OK) {
      ld_check(payload.size() > 0);

      std::unique_ptr<logsconfig::LogGroupWithParentPath> lid =
          logsconfig::FBuffersLogsConfigCodec::deserialize<
              logsconfig::LogGroupWithParentPath>(
              Payload(payload.data(), payload.size()), delimiter);
      ld_check(lid != nullptr);
      if (lid == nullptr) {
        // deserialization failed!
        st = E::FAILED;
        cb(st, nullptr);
        return;
      }

      std::string full_ns = lid->getFullyQualifiedName();
      std::unique_ptr<client::LogGroupImpl> lg =
          std::make_unique<client::LogGroupImpl>(
              std::move(lid->log_group), std::move(full_ns), config_version);
      cb(st, std::move(lg));
    } else {
      cb(st, nullptr);
    }
  };

  std::unique_ptr<Request> req = std::make_unique<LogsConfigApiRequest>(
      LOGS_CONFIG_API_Header::Type::GET_LOG_GROUP_BY_ID,
      std::to_string(logid.val()),
      settings_->getSettings()->logsconfig_timeout.value_or(timeout_),
      LogsConfigApiRequest::MAX_ERRORS,
      logsconfig_api_random_seed_,
      callback);
  processor_->postRequest(req);
}

int ClientImpl::makeDirectory(const std::string& path,
                              bool mk_intermediate_dirs,
                              const client::LogAttributes& attrs,
                              make_directory_callback_t cb) noexcept {
  auto full_ns = get_full_name(path, *config_->get(), *settings_);
  std::string delimiter =
      config_->get()->serverConfig()->getNamespaceDelimiter();
  // create the payload
  logsconfig::DeltaHeader header; // Resolution is Auto by default
  logsconfig::MkDirectoryDelta delta{header, path, mk_intermediate_dirs, attrs};

  auto callback = [cb, full_ns, delimiter](
                      Status st, uint64_t config_version, std::string payload) {
    if (st == E::OK) {
      // deserialize the payload
      std::unique_ptr<logsconfig::DirectoryNode> recovered_dir =
          LogsConfigStateMachine::deserializeDirectory(payload, delimiter);
      ld_check(recovered_dir != nullptr);
      if (recovered_dir == nullptr) {
        st = E::FAILED;
        cb(st, nullptr, "Failed to deserialize payload from server!");
        return;
      }
      std::unique_ptr<client::DirectoryImpl> dir =
          std::make_unique<client::DirectoryImpl>(
              *recovered_dir, nullptr, full_ns, delimiter, config_version);
      cb(st, std::move(dir), "");
    } else {
      // payload has the failure reason
      cb(st, nullptr, payload);
    }
  };

  std::string payload =
      FBuffersLogsConfigCodec::serialize(delta, false /* flatten */).toString();

  std::unique_ptr<Request> req = std::make_unique<LogsConfigApiRequest>(
      LOGS_CONFIG_API_Header::Type::MUTATION_REQUEST,
      payload,
      settings_->getSettings()->logsconfig_timeout.value_or(timeout_),
      LogsConfigApiRequest::MAX_ERRORS,
      logsconfig_api_random_seed_,
      callback);
  int rv = processor_->postRequest(req);
  if (rv != 0) {
    return -1;
  }
  return 0;
}

std::unique_ptr<client::Directory>
ClientImpl::makeDirectorySync(const std::string& path,
                              bool mk_intermediate_dirs,
                              const client::LogAttributes& attrs,
                              std::string* failure_reason) noexcept {
  Status status = E::OK;

  std::unique_ptr<client::Directory> dir;
  Semaphore sem;
  auto cb = [&](Status st,
                std::unique_ptr<client::Directory> result,
                const std::string& failure) {
    dir = std::move(result);
    if (failure_reason) {
      *failure_reason = failure;
    }
    status = st;
    sem.post();
  };

  int rv = makeDirectory(path, mk_intermediate_dirs, attrs, cb);
  if (rv != 0) {
    if (failure_reason) {
      *failure_reason = "Failed to send the request to the server!";
    }
    return nullptr;
  }

  sem.wait();
  if (status != E::OK) {
    err = status;
    return nullptr;
  }
  return dir;
}

int ClientImpl::removeDirectory(const std::string& path,
                                bool recursive,
                                logsconfig_status_callback_t cb) noexcept {
  auto full_ns = get_full_name(path, *config_->get(), *settings_);
  std::string delimiter = config_->get()->logsConfig()->getNamespaceDelimiter();
  // create the payload
  logsconfig::DeltaHeader header; // Resolution is Auto by default
  logsconfig::RemoveDelta delta{
      header, path, true /* is directory */, recursive};

  auto callback = [cb, delimiter](
                      Status st, uint64_t version, std::string failure_reason) {
    cb(st, version, std::move(failure_reason));
  };

  std::string payload =
      FBuffersLogsConfigCodec::serialize(delta, false /* flatten */).toString();

  std::unique_ptr<Request> req = std::make_unique<LogsConfigApiRequest>(
      LOGS_CONFIG_API_Header::Type::MUTATION_REQUEST,
      payload,
      settings_->getSettings()->logsconfig_timeout.value_or(timeout_),
      LogsConfigApiRequest::MAX_ERRORS,
      logsconfig_api_random_seed_,
      callback);
  int rv = processor_->postRequest(req);
  if (rv != 0) {
    return -1;
  }
  return 0;
}

bool ClientImpl::removeDirectorySync(const std::string& path,
                                     bool recursive,
                                     uint64_t* version) noexcept {
  Status status = E::OK;

  Semaphore sem;
  auto cb = [&](Status st, uint64_t cfg_version, std::string /* unused */) {
    status = st;
    if (version != nullptr) {
      *version = cfg_version;
    }
    sem.post();
  };

  int rv = removeDirectory(path, recursive, cb);
  if (rv != 0) {
    return false;
  }

  sem.wait();
  if (status != E::OK) {
    err = status;
    return false;
  }
  return true;
}

bool ClientImpl::removeLogGroupSync(const std::string& path,
                                    uint64_t* version) noexcept {
  Status status = E::OK;

  Semaphore sem;
  auto cb = [&](Status st, uint64_t cfg_version, std::string /* unused */) {
    status = st;
    if (version != nullptr) {
      *version = cfg_version;
    }
    sem.post();
  };

  int rv = removeLogGroup(path, cb);
  if (rv != 0) {
    return false;
  }

  sem.wait();
  if (status != E::OK) {
    err = status;
    return false;
  }
  return true;
}

int ClientImpl::removeLogGroup(const std::string& path,
                               logsconfig_status_callback_t cb) noexcept {
  auto full_ns = get_full_name(path, *config_->get(), *settings_);
  std::string delimiter = config_->get()->logsConfig()->getNamespaceDelimiter();
  // create the payload
  logsconfig::DeltaHeader header; // Resolution is Auto by default
  logsconfig::RemoveDelta delta{
      header, path, false /* not a directory */, false};

  auto callback = [cb, delimiter](
                      Status st, uint64_t version, std::string failure_reason) {
    cb(st, version, failure_reason);
  };

  std::string payload =
      FBuffersLogsConfigCodec::serialize(delta, false /* flatten */).toString();

  std::unique_ptr<Request> req = std::make_unique<LogsConfigApiRequest>(
      LOGS_CONFIG_API_Header::Type::MUTATION_REQUEST,
      payload,
      settings_->getSettings()->logsconfig_timeout.value_or(timeout_),
      LogsConfigApiRequest::MAX_ERRORS,
      logsconfig_api_random_seed_,
      callback);
  int rv = processor_->postRequest(req);
  if (rv != 0) {
    return -1;
  }
  return 0;
}

int ClientImpl::rename(const std::string& from_path,
                       const std::string& to_path,
                       logsconfig_status_callback_t cb) noexcept {
  auto source_full_ns = get_full_name(from_path, *config_->get(), *settings_);
  auto dest_full_ns = get_full_name(to_path, *config_->get(), *settings_);
  std::string delimiter = config_->get()->logsConfig()->getNamespaceDelimiter();
  // create the payload
  logsconfig::DeltaHeader header; // Resolution is Auto by default
  logsconfig::RenameDelta delta{header, source_full_ns, dest_full_ns};

  auto callback = [cb](
                      Status st, uint64_t version, std::string failure_reason) {
    cb(st, version, std::move(failure_reason));
  };

  std::string payload =
      FBuffersLogsConfigCodec::serialize(delta, false /* flatten */).toString();

  std::unique_ptr<Request> req = std::make_unique<LogsConfigApiRequest>(
      LOGS_CONFIG_API_Header::Type::MUTATION_REQUEST,
      payload,
      settings_->getSettings()->logsconfig_timeout.value_or(timeout_),
      LogsConfigApiRequest::MAX_ERRORS,
      logsconfig_api_random_seed_,
      callback);
  int rv = processor_->postRequest(req);
  if (rv != 0) {
    return -1;
  }
  return 0;
}

bool ClientImpl::renameSync(const std::string& from_path,
                            const std::string& to_path,
                            uint64_t* version,
                            std::string* failure_reason) noexcept {
  Status status = E::OK;

  Semaphore sem;
  auto cb = [&](Status st, uint64_t cfg_version, std::string failure) {
    status = st;
    if (version != nullptr) {
      *version = cfg_version;
    }
    if (failure_reason) {
      *failure_reason = failure;
    }
    sem.post();
  };

  int rv = rename(from_path, to_path, cb);
  if (rv != 0) {
    if (failure_reason) {
      *failure_reason = "Failed to send the request to the server!";
    }
    return false;
  }

  sem.wait();
  if (status != E::OK) {
    err = status;
    return false;
  }
  return true;
}

int ClientImpl::makeLogGroup(const std::string& path,
                             const logid_range_t& range,
                             const client::LogAttributes& attrs,
                             bool mk_intermediate_dirs,
                             make_log_group_callback_t cb) noexcept {
  auto full_ns = get_full_name(path, *config_->get(), *settings_);
  std::string delimiter =
      config_->get()->serverConfig()->getNamespaceDelimiter();
  // create the payload
  logsconfig::DeltaHeader header; // Resolution is Auto by default
  logsconfig::MkLogGroupDelta delta{
      header, path, range, mk_intermediate_dirs, attrs};

  auto callback = [cb, full_ns, delimiter](
                      Status st, uint64_t config_version, std::string payload) {
    if (st == E::OK) {
      // deserialize the payload
      ld_check(payload.size() > 0);
      std::unique_ptr<logsconfig::LogGroupNode> recovered_log_group =
          LogsConfigStateMachine::deserializeLogGroup(payload, delimiter);
      ld_check(recovered_log_group != nullptr);
      if (recovered_log_group == nullptr) {
        // deserialization failed in opt builds!
        st = E::FAILED;
        cb(st, nullptr, "Failed to deserialize payload from server!");
        return;
      }
      std::unique_ptr<client::LogGroupImpl> lg =
          std::make_unique<client::LogGroupImpl>(
              std::move(recovered_log_group), full_ns, config_version);
      cb(st, std::move(lg), "");
    } else {
      // payload contains the failure reason.
      cb(st, nullptr, payload);
    }
  };

  std::string payload =
      FBuffersLogsConfigCodec::serialize(delta, false /* flatten */).toString();

  std::unique_ptr<Request> req = std::make_unique<LogsConfigApiRequest>(
      LOGS_CONFIG_API_Header::Type::MUTATION_REQUEST,
      payload,
      settings_->getSettings()->logsconfig_timeout.value_or(timeout_),
      LogsConfigApiRequest::MAX_ERRORS,
      logsconfig_api_random_seed_,
      callback);
  int rv = processor_->postRequest(req);
  if (rv != 0) {
    return -1;
  }
  return 0;
}

std::unique_ptr<client::LogGroup>
ClientImpl::makeLogGroupSync(const std::string& path,
                             const logid_range_t& range,
                             const client::LogAttributes& attrs,
                             bool mk_intermediate_dirs,
                             std::string* failure_reason) noexcept {
  Status status = E::OK;

  std::unique_ptr<client::LogGroup> lg;
  Semaphore sem;
  auto cb = [&](Status st,
                std::unique_ptr<client::LogGroup> result,
                const std::string& failure) {
    lg = std::move(result);
    status = st;
    if (st != E::OK && failure_reason != nullptr) {
      *failure_reason = failure;
    }
    sem.post();
  };

  int rv = makeLogGroup(path, range, attrs, mk_intermediate_dirs, cb);
  if (rv != 0) {
    if (failure_reason) {
      *failure_reason = "Failed to send the request to the server!";
    }
    return nullptr;
  }

  sem.wait();
  if (status != E::OK) {
    err = status;
    return nullptr;
  }
  return lg;
}

int ClientImpl::setAttributes(const std::string& path,
                              const client::LogAttributes& attrs,
                              logsconfig_status_callback_t cb) noexcept {
  auto full_ns = get_full_name(path, *config_->get(), *settings_);
  std::string delimiter = config_->get()->logsConfig()->getNamespaceDelimiter();
  // create the payload
  logsconfig::DeltaHeader header; // Resolution is Auto by default
  logsconfig::SetAttributesDelta delta{header, path, attrs};

  auto callback = [cb, full_ns, delimiter](
                      Status st, uint64_t version, std::string failure_reason) {
    cb(st, version, failure_reason);
  };

  std::string payload =
      FBuffersLogsConfigCodec::serialize(delta, false /* flatten */).toString();

  std::unique_ptr<Request> req = std::make_unique<LogsConfigApiRequest>(
      LOGS_CONFIG_API_Header::Type::MUTATION_REQUEST,
      payload,
      settings_->getSettings()->logsconfig_timeout.value_or(timeout_),
      LogsConfigApiRequest::MAX_ERRORS,
      logsconfig_api_random_seed_,
      callback);
  int rv = processor_->postRequest(req);
  if (rv != 0) {
    return -1;
  }
  return 0;
}

bool ClientImpl::setAttributesSync(const std::string& path,
                                   const client::LogAttributes& attrs,
                                   uint64_t* version,
                                   std::string* failure_reason) noexcept {
  Status status = E::OK;

  Semaphore sem;
  auto cb = [&](Status st, uint64_t cfg_version, std::string failure) {
    status = st;
    if (version != nullptr) {
      *version = cfg_version;
    }
    if (st != E::OK && failure_reason != nullptr) {
      *failure_reason = failure;
    }
    sem.post();
  };

  int rv = setAttributes(path, attrs, cb);
  if (rv != 0) {
    if (failure_reason) {
      *failure_reason = "Failed to send the request to the server!";
    }
    return false;
  }

  sem.wait();
  if (status != E::OK) {
    err = status;
    return false;
  }
  return true;
}

int ClientImpl::setLogGroupRange(const std::string& path,
                                 const logid_range_t& range,
                                 logsconfig_status_callback_t cb) noexcept {
  auto full_ns = get_full_name(path, *config_->get(), *settings_);
  std::string delimiter = config_->get()->logsConfig()->getNamespaceDelimiter();
  // create the payload
  logsconfig::DeltaHeader header; // Resolution is Auto by default
  logsconfig::SetLogRangeDelta delta{header, path, range};

  auto callback = [cb, full_ns, delimiter](
                      Status st, uint64_t version, std::string failure_reason) {
    cb(st, version, std::move(failure_reason));
  };

  std::string payload =
      FBuffersLogsConfigCodec::serialize(delta, false /* flatten */).toString();

  std::unique_ptr<Request> req = std::make_unique<LogsConfigApiRequest>(
      LOGS_CONFIG_API_Header::Type::MUTATION_REQUEST,
      payload,
      settings_->getSettings()->logsconfig_timeout.value_or(timeout_),
      LogsConfigApiRequest::MAX_ERRORS,
      logsconfig_api_random_seed_,
      callback);
  int rv = processor_->postRequest(req);
  if (rv != 0) {
    return -1;
  }
  return 0;
}

bool ClientImpl::setLogGroupRangeSync(const std::string& path,
                                      const logid_range_t& range,
                                      uint64_t* version,
                                      std::string* failure_reason) noexcept {
  Status status = E::OK;

  Semaphore sem;
  auto cb = [&](Status st, uint64_t cfg_version, std::string failure) {
    status = st;
    if (version != nullptr) {
      *version = cfg_version;
    }
    if (st != E::OK && failure_reason != nullptr) {
      *failure_reason = failure;
    }
    sem.post();
  };

  int rv = setLogGroupRange(path, range, cb);
  if (rv != 0) {
    if (failure_reason) {
      *failure_reason = "Failed to send the request to the server!";
    }
    return false;
  }

  sem.wait();
  if (status != E::OK) {
    err = status;
    return false;
  }
  return true;
}

int ClientImpl::getLocalDirectory(const std::string& path,
                                  get_directory_callback_t cb) noexcept {
  ld_assert(hasFullyLoadedLocalLogsConfig());
  auto logs_config = config_->getLocalLogsConfig();
  const auto& tree = logs_config->getLogsConfigTree();
  auto dir = tree.findDirectory(path);

  if (dir == nullptr) {
    cb(Status::NOTFOUND, nullptr);
    return 0;
  }
  auto server_cfg = config_->getServerConfig();
  std::string delimiter = server_cfg->getNamespaceDelimiter();
  std::unique_ptr<client::DirectoryImpl> lg =
      std::make_unique<client::DirectoryImpl>(
          *dir, nullptr, path, delimiter, logs_config->getVersion());
  cb(Status::OK, std::move(lg));
  return 0;
}

int ClientImpl::getDirectory(const std::string& path,
                             get_directory_callback_t cb) noexcept {
  auto full_ns = get_full_name(path, *config_->get(), *settings_);
  if (hasFullyLoadedLocalLogsConfig()) {
    return getLocalDirectory(full_ns, std::move(cb));
  }
  std::string delimiter =
      config_->get()->serverConfig()->getNamespaceDelimiter();
  auto callback = [cb, full_ns, delimiter](
                      Status st, uint64_t config_version, std::string payload) {
    if (st == E::OK) {
      // deserialize the payload
      std::unique_ptr<logsconfig::DirectoryNode> recovered_dir =
          LogsConfigStateMachine::deserializeDirectory(payload, delimiter);
      if (recovered_dir == nullptr) {
        st = E::FAILED;
        cb(st, nullptr);
        return;
      }
      std::unique_ptr<client::DirectoryImpl> dir =
          std::make_unique<client::DirectoryImpl>(
              *recovered_dir, nullptr, full_ns, delimiter, config_version);
      cb(st, std::move(dir));
    } else {
      cb(st, nullptr);
    }
  };

  std::unique_ptr<Request> req = std::make_unique<LogsConfigApiRequest>(
      LOGS_CONFIG_API_Header::Type::GET_DIRECTORY,
      full_ns,
      settings_->getSettings()->logsconfig_timeout.value_or(timeout_),
      LogsConfigApiRequest::MAX_ERRORS,
      logsconfig_api_random_seed_,
      callback);
  int rv = processor_->postRequest(req);
  if (rv != 0) {
    return -1;
  }
  return 0;
}

std::unique_ptr<client::Directory>
ClientImpl::getDirectorySync(const std::string& path) noexcept {
  Status status = E::OK;

  std::unique_ptr<client::Directory> dir;
  Semaphore sem;
  auto cb = [&](Status st, std::unique_ptr<client::Directory> result) {
    dir = std::move(result);
    status = st;
    sem.post();
  };

  int rv = getDirectory(path, cb);
  if (rv != 0) {
    return nullptr;
  }

  sem.wait();
  if (status != E::OK) {
    err = status;
    return nullptr;
  }
  return dir;
}

bool ClientImpl::syncLogsConfigVersion(uint64_t version) noexcept {
  Semaphore sem;
  auto cb = [&]() { sem.post(); };
  auto handle = notifyOnLogsConfigVersion(version, cb);
  int rv = sem.timedwait(
      settings_->getSettings()->logsconfig_timeout.value_or(timeout_));
  if (rv != 0) {
    ld_critical("Timeout waiting on LogsConfig to reach version %lu", version);
    return false;
  }
  return true;
}

ConfigSubscriptionHandle
ClientImpl::notifyOnLogsConfigVersion(uint64_t version,
                                      std::function<void()> cb) noexcept {
  auto start_time = std::chrono::steady_clock::now();
  auto updateable_logs_config = config_->updateableLogsConfig();
  // if enable-logsconfig-manager is set, we need to wait for the RSM to be
  // ready, or timeout.
  configuration::subscription_callback_t updates_cb =
      [=]() -> configuration::SubscriptionStatus {
    auto logs_config = config_->updateableLogsConfig()->get();
    if (logs_config->getVersion() >= version) {
      auto end_time = std::chrono::steady_clock::now();
      ld_info("LogsConfig has been synced to version %lu in %.3f seconds...",
              version,
              std::chrono::duration_cast<std::chrono::duration<double>>(
                  end_time - start_time)
                  .count());
      cb();
      // stop the subscription
      return configuration::SubscriptionStatus::UNSUBSCRIBE;
    }
    return configuration::SubscriptionStatus::KEEP;
  };
  ConfigSubscriptionHandle handle =
      updateable_logs_config->subscribeToUpdates(updates_cb);
  if (updates_cb() == configuration::SubscriptionStatus::UNSUBSCRIBE) {
    // No need to keep the subscription.
    handle.unsubscribe();
  }
  return handle;
}

std::unique_ptr<ClusterAttributes> ClientImpl::getClusterAttributes() noexcept {
  return ClusterAttributesImpl::create(config_->get()->serverConfig());
}

ConfigSubscriptionHandle
ClientImpl::subscribeToConfigUpdates(config_update_callback_t cb) noexcept {
  return config_->subscribeToUpdates(cb);
}

read_stream_id_t ClientImpl::issueReadStreamID() {
  return processor_->issueReadStreamID();
}

struct TrimGate {
  void operator()(Status status) {
    this->status = status;
    sem.post();
  }
  void wait() {
    sem.wait();
  }

  Semaphore sem;
  Status status;
};

int ClientImpl::trimSync(
    logid_t logid,
    lsn_t lsn,
    std::unique_ptr<std::string> per_request_token) noexcept {
  if (logid == LOGID_INVALID || lsn == LSN_INVALID || lsn >= LSN_MAX) {
    err = E::INVALID_PARAM;
    return -1;
  }

  TrimGate gate;

  int rv = trim(logid, lsn, std::move(per_request_token), std::ref(gate));
  if (rv != 0) {
    return -1;
  }

  gate.wait();
  if (gate.status != E::OK) {
    err = gate.status;
    return -1;
  }
  return 0;
}

int ClientImpl::trimSync(logid_t logid, lsn_t lsn) noexcept {
  return trimSync(logid, lsn, nullptr);
}

int ClientImpl::trim(logid_t logid,
                     lsn_t lsn,
                     std::unique_ptr<std::string> per_request_token,
                     trim_callback_t cb) noexcept {
  auto cb_wrapper = [logid, lsn, cb, start = SteadyClock::now()](
                        const TrimRequest& req, Status st) {
    // log response
    Worker* w = Worker::onThisThread();
    if (w) {
      auto& tracer = w->processor_->api_hits_tracer_;
      if (tracer) {
        tracer->traceTrim(
            msec_since(start), logid, lsn, req.getFailedShards(st), st);
      }
    }
    cb(st);
  };
  auto reqImpl = std::make_unique<TrimRequest>(
      bridge_.get(),
      logid,
      lsn,
      settings_->getSettings()->meta_api_timeout.value_or(timeout_),
      cb_wrapper);
  if (per_request_token) {
    reqImpl->setPerRequestToken(std::move(per_request_token));
  }
  std::unique_ptr<Request> req = std::move(reqImpl);
  return processor_->postRequest(req);
}

int ClientImpl::trim(logid_t logid, lsn_t lsn, trim_callback_t cb) noexcept {
  return trim(logid, lsn, {}, cb);
  ;
}

struct FindTimeGate {
  // called by FindKeyRequest when request processing completes or
  // timeout expires
  void operator()(Status status, lsn_t result) {
    ld_debug("FindTimeGate callback called");
    this->lsn = result;
    this->status = status;
    sem.post();
  }

  void wait() {
    sem.wait();
  }

  Semaphore sem;
  lsn_t lsn;
  Status status;
};

struct FindKeyGate {
  // called by FindKeyRequest when request processing completes or
  // timeout expires
  void operator()(FindKeyResult result) {
    ld_debug("FindKeyGate callback called");
    this->result = result;
    sem.post();
  }

  void wait() {
    sem.wait();
  }

  Semaphore sem;
  FindKeyResult result;
};

lsn_t ClientImpl::findTimeSync(logid_t logid,
                               std::chrono::milliseconds timestamp,
                               Status* status_out,
                               FindKeyAccuracy accuracy) noexcept {
  FindTimeGate gate; // request-reply synchronization
  int rv;

  rv = findTime(logid, timestamp, std::ref(gate), accuracy);

  if (rv != 0) {
    if (status_out != nullptr) {
      *status_out = E::FAILED;
    }
    return LSN_INVALID;
  }

  gate.wait();
  if (status_out != nullptr) {
    *status_out = gate.status;
  }
  return gate.lsn;
}

int ClientImpl::findTime(logid_t logid,
                         std::chrono::milliseconds timestamp,
                         find_time_callback_t cb,
                         FindKeyAccuracy accuracy) noexcept {
  auto cb_wrapper =
      [cb, logid, timestamp, accuracy, start = SteadyClock::now()](
          const FindKeyRequest& req, Status st, lsn_t result) {
        Worker* w = Worker::onThisThread();
        if (w) {
          auto& tracer = w->processor_->api_hits_tracer_;
          if (tracer) {
            tracer->traceFindTime(msec_since(start),
                                  logid,
                                  timestamp,
                                  accuracy,
                                  req.getFailedShards(st),
                                  st,
                                  result);
          }
        }
        cb(st, result);
      };

  std::unique_ptr<Request> req = std::make_unique<FindKeyRequest>(
      logid,
      timestamp,
      folly::none,
      settings_->getSettings()->findkey_timeout.value_or(timeout_),
      cb_wrapper,
      find_key_callback_ex_t(),
      accuracy);
  return processor_->postRequest(req);
}

FindKeyResult ClientImpl::findKeySync(logid_t logid,
                                      std::string key,
                                      FindKeyAccuracy accuracy) noexcept {
  FindKeyGate gate; // request-reply synchronization
  int rv;

  rv = findKey(logid, std::move(key), std::ref(gate), accuracy);
  if (rv != 0) {
    FindKeyResult result = {E::FAILED, LSN_INVALID, LSN_INVALID};
    return result;
  }

  gate.wait();
  return gate.result;
}

int ClientImpl::findKey(logid_t logid,
                        std::string key,
                        find_key_callback_t cb,
                        FindKeyAccuracy accuracy) noexcept {
  auto cb_wrapper = [cb, logid, key, accuracy, start = SteadyClock::now()](
                        const FindKeyRequest& req, FindKeyResult result) {
    // log response
    Worker* w = Worker::onThisThread();
    if (w) {
      auto& tracer = w->processor_->api_hits_tracer_;
      if (tracer) {
        tracer->traceFindKey(msec_since(start),
                             logid,
                             key,
                             accuracy,
                             req.getFailedShards(result.status),
                             result.status,
                             result.lo,
                             result.hi);
      }
    }
    cb(result);
  };
  std::unique_ptr<Request> req = std::make_unique<FindKeyRequest>(
      logid,
      std::chrono::milliseconds(0),
      std::move(key),
      settings_->getSettings()->findkey_timeout.value_or(timeout_),
      find_time_callback_ex_t(),
      cb_wrapper,
      accuracy);
  return processor_->postRequest(req);
}

namespace {
struct IsLogEmptyGate {
  void operator()(Status status, bool empty) {
    ld_debug("IsLogEmptyGate callback called");
    this->empty = empty;
    this->status = status;
    sem.post();
  }

  void wait() {
    sem.wait();
  }

  Semaphore sem;
  bool empty{false};
  Status status{E::OK};
};
}; // namespace

int ClientImpl::isLogEmptySync(logid_t logid, bool* empty) noexcept {
  if (settings_->getSettings()->enable_is_log_empty_v2) {
    return isLogEmptyV2Sync(logid, empty);
  }

  IsLogEmptyGate gate; // request-reply synchronization
  int rv;

  ld_check(empty);

  rv = isLogEmpty(logid, std::ref(gate));

  if (rv == 0) {
    gate.wait();
    if (gate.status == E::OK) {
      *empty = gate.empty;
    } else {
      logdevice::err = gate.status;
      rv = -1;
    }
  }

  return rv;
}

int ClientImpl::isLogEmpty(logid_t logid, is_empty_callback_t cb) noexcept {
  if (settings_->getSettings()->enable_is_log_empty_v2) {
    return isLogEmptyV2(logid, cb);
  }

  auto cb_wrapper = [cb, logid, start = SteadyClock::now()](
                        const IsLogEmptyRequest& req, Status st, bool empty) {
    // log response
    Worker* w = Worker::onThisThread();
    if (w) {
      auto& tracer = w->processor_->api_hits_tracer_;
      if (tracer) {
        tracer->traceIsLogEmpty(
            msec_since(start), logid, req.getFailedShards(st), st, empty);
      }
    }
    cb(st, empty);
  };
  std::unique_ptr<Request> req = std::make_unique<IsLogEmptyRequest>(
      logid,
      settings_->getSettings()->meta_api_timeout.value_or(timeout_),
      cb_wrapper,
      settings_->getSettings()->client_is_log_empty_grace_period);
  return processor_->postRequest(req);
}

int ClientImpl::isLogEmptyV2Sync(logid_t logid, bool* empty) noexcept {
  IsLogEmptyGate gate; // request-reply synchronization
  int rv;

  ld_check(empty);

  rv = isLogEmptyV2(logid, std::ref(gate));

  if (rv == 0) {
    gate.wait();
    if (gate.status == E::OK) {
      *empty = gate.empty;
    } else {
      logdevice::err = gate.status;
      rv = -1;
    }
  }

  return rv;
}

int ClientImpl::isLogEmptyV2(logid_t logid, is_empty_callback_t cb) noexcept {
  auto cb_wrapper =
      [logid, cb, start = SteadyClock::now()](
          Status st,
          NodeID /*seq*/,
          lsn_t /*next_lsn*/,
          std::unique_ptr<LogTailAttributes> /*tail_attributes*/,
          std::shared_ptr<const EpochMetaDataMap> /*metadata_map*/,
          std::shared_ptr<TailRecord> /*tail_record*/,
          folly::Optional<bool> is_log_empty) {
        if (!is_log_empty.hasValue()) {
          ld_check_ne(st, E::OK);
          is_log_empty = false;
        }
        // log response
        Worker* w = Worker::onThisThread();
        if (w) {
          auto& tracer = w->processor_->api_hits_tracer_;
          if (tracer) {
            tracer->traceIsLogEmptyV2(
                msec_since(start), logid, st, is_log_empty.value());
          }
        }
        cb(st, is_log_empty.value());
      };
  auto sync_seq_req = std::make_unique<SyncSequencerRequest>(
      logid,
      SyncSequencerRequest::INCLUDE_IS_LOG_EMPTY,
      cb_wrapper,
      GetSeqStateRequest::Context::IS_LOG_EMPTY_V2,
      settings_->getSettings()->meta_api_timeout.value_or(timeout_));
  sync_seq_req->setCompleteIfLogNotFound(true);
  sync_seq_req->setCompleteIfAccessDenied(true);
  sync_seq_req->setPreventMetadataLogs(true);
  std::unique_ptr<Request> req = std::move(sync_seq_req);
  return processor_->postRequest(req);
}

namespace {
struct DataSizeGate {
  void operator()(Status status, size_t size) {
    ld_debug("DataSizeGate callback called");
    this->status = status;
    this->size = size;
    sem.post();
  }

  void wait() {
    sem.wait();
  }

  Semaphore sem;
  size_t size{0};
  Status status{E::OK};
};
}; // namespace

int ClientImpl::dataSizeSync(logid_t logid,
                             std::chrono::milliseconds start,
                             std::chrono::milliseconds end,
                             DataSizeAccuracy accuracy,
                             size_t* size) noexcept {
  DataSizeGate gate; // request-reply synchronization
  ld_check(size);

  int rv = dataSize(logid, start, end, accuracy, std::ref(gate));

  if (rv == 0) {
    gate.wait();
    if (gate.status == E::OK) {
      *size = gate.size;
    } else {
      logdevice::err = gate.status;
      rv = -1;
    }
  }

  return rv;
}

int ClientImpl::dataSize(logid_t logid,
                         std::chrono::milliseconds start,
                         std::chrono::milliseconds end,
                         DataSizeAccuracy accuracy,
                         data_size_callback_t cb) noexcept {
  auto cb_wrapper = [cb,
                     logid,
                     start,
                     end,
                     accuracy,
                     request_start_time = SteadyClock::now()](
                        const DataSizeRequest& req, Status st, size_t size) {
    // log response
    Worker* w = Worker::onThisThread();
    if (w) {
      auto& tracer = w->processor_->api_hits_tracer_;
      if (tracer) {
        tracer->traceDataSize(msec_since(request_start_time),
                              logid,
                              start,
                              end,
                              accuracy,
                              req.getFailedShards(st),
                              st,
                              size);
      }
    }
    cb(st, size);
  };
  std::unique_ptr<Request> req = std::make_unique<DataSizeRequest>(
      logid,
      start,
      end,
      accuracy,
      cb_wrapper,
      settings_->getSettings()->meta_api_timeout.value_or(timeout_));
  return processor_->postRequest(req);
}

lsn_t ClientImpl::getTailLSNSync(logid_t logid) noexcept {
  lsn_t tail_lsn = LSN_INVALID;
  Status status = E::OK;

  Semaphore sem;
  auto cb = [&](Status st, lsn_t tail) {
    tail_lsn = tail;
    status = st;
    sem.post();
  };

  int rv = getTailLSN(logid, cb);
  if (rv != 0) {
    // err set by getTailLSN.
    return LSN_INVALID;
  }

  sem.wait();
  if (status != E::OK) {
    err = status;
    return LSN_INVALID;
  }
  return tail_lsn;
}

int ClientImpl::getTailLSN(logid_t logid, get_tail_lsn_callback_t cb) noexcept {
  auto cb_wrapper =
      [logid, cb, start = SteadyClock::now()](
          Status st,
          NodeID /*seq*/,
          lsn_t next_lsn,
          std::unique_ptr<LogTailAttributes> /*tail_attributes*/,
          std::shared_ptr<const EpochMetaDataMap> /*metadata_map*/,
          std::shared_ptr<TailRecord> /*tail_record*/,
          folly::Optional<bool> /*is_log_empty*/) {
        auto resp = next_lsn <= LSN_OLDEST ? next_lsn : next_lsn - 1;
        // log response
        Worker* w = Worker::onThisThread();
        if (w) {
          auto& tracer = w->processor_->api_hits_tracer_;
          if (tracer) {
            tracer->traceGetTailLSN(msec_since(start), logid, st, resp);
          }
        }
        cb(st, resp);
      };
  std::unique_ptr<Request> req = std::make_unique<SyncSequencerRequest>(
      logid,
      0 /* flags */,
      cb_wrapper,
      GetSeqStateRequest::Context::GET_TAIL_LSN,
      settings_->getSettings()->meta_api_timeout.value_or(timeout_));
  return processor_->postRequest(req);
}

std::unique_ptr<LogTailAttributes>
ClientImpl::getTailAttributesSync(logid_t logid) noexcept {
  std::unique_ptr<LogTailAttributes> tail_attributes;
  Status status = E::OK;

  Semaphore sem;
  auto cb = [&](Status st, std::unique_ptr<LogTailAttributes> attributes) {
    tail_attributes = std::move(attributes);
    status = st;
    sem.post();
  };

  int rv = getTailAttributes(logid, cb);
  if (rv != 0) {
    // err set by getTailAttributes.
    return nullptr;
  }

  sem.wait();
  if (status != E::OK) {
    err = status;
    return nullptr;
  }
  return tail_attributes;
}

int ClientImpl::getTailAttributes(logid_t logid,
                                  get_tail_attributes_callback_t cb) noexcept {
  auto cb_wrapper = [logid, cb, start = SteadyClock::now()](
                        Status st,
                        NodeID /*seq*/,
                        lsn_t /*next_lsn*/,
                        std::unique_ptr<LogTailAttributes> tail_attributes,
                        std::shared_ptr<const EpochMetaDataMap> /*unused*/,
                        std::shared_ptr<TailRecord> /*unused*/,
                        folly::Optional<bool> /*unused*/) {
    // log response
    Worker* w = Worker::onThisThread();
    if (w) {
      auto& tracer = w->processor_->api_hits_tracer_;
      if (tracer) {
        tracer->traceGetTailAttributes(
            msec_since(start), logid, st, tail_attributes.get());
      }
    }
    // Check if recovery is running and sequencer can not provide log tail
    // attributes.
    if (st == E::OK && !tail_attributes) {
      st = E::AGAIN;
    } else if (!tail_attributes) {
      st = E::FAILED;
    }
    cb(st, std::move(tail_attributes));
  };
  std::unique_ptr<Request> req = std::make_unique<SyncSequencerRequest>(
      logid,
      SyncSequencerRequest::INCLUDE_TAIL_ATTRIBUTES,
      cb_wrapper,
      GetSeqStateRequest::Context::GET_TAIL_ATTRIBUTES,
      settings_->getSettings()->meta_api_timeout.value_or(timeout_));
  return processor_->postRequest(req);
}

std::shared_ptr<const EpochMetaDataMap>
ClientImpl::getHistoricalMetaDataSync(logid_t logid) noexcept {
  std::shared_ptr<const EpochMetaDataMap> historical_metadata;
  Status status = E::UNKNOWN;
  Semaphore sem;
  auto cb = [&](Status st, std::shared_ptr<const EpochMetaDataMap> metadata) {
    historical_metadata = std::move(metadata);
    status = st;
    sem.post();
  };

  int rv = getHistoricalMetaData(logid, cb);
  if (rv != 0) {
    // err set by getHistoricalMetaData.
    return nullptr;
  }

  sem.wait();
  if (status != E::OK) {
    err = status;
    return nullptr;
  }
  return historical_metadata;
}

int ClientImpl::getHistoricalMetaData(
    logid_t logid,
    historical_metadata_callback_t cb) noexcept {
  auto cb_wrapper = [cb](Status st,
                         NodeID /*seq*/,
                         lsn_t /*next_lsn*/,
                         std::unique_ptr<LogTailAttributes> /*unused*/,
                         std::shared_ptr<const EpochMetaDataMap> metadata,
                         std::shared_ptr<TailRecord> /*unused*/,
                         folly::Optional<bool> /*unused*/) {
    cb(st, std::move(metadata));
  };
  std::unique_ptr<Request> req = std::make_unique<SyncSequencerRequest>(
      logid,
      SyncSequencerRequest::INCLUDE_HISTORICAL_METADATA,
      cb_wrapper,
      GetSeqStateRequest::Context::HISTORICAL_METADATA,
      settings_->getSettings()->meta_api_timeout.value_or(timeout_));
  return processor_->postRequest(req);
}

std::shared_ptr<TailRecord>
ClientImpl::getTailRecordSync(logid_t logid) noexcept {
  std::shared_ptr<TailRecord> tail_record;
  Status status = E::UNKNOWN;
  Semaphore sem;
  auto cb = [&](Status st, std::shared_ptr<TailRecord> tail) {
    tail_record = std::move(tail);
    status = st;
    sem.post();
  };

  int rv = getTailRecord(logid, cb);
  if (rv != 0) {
    // err set by getTailRecord.
    return nullptr;
  }

  sem.wait();
  if (status != E::OK) {
    err = status;
    return nullptr;
  }
  return tail_record;
}

int ClientImpl::getTailRecord(logid_t logid,
                              tail_record_callback_t cb) noexcept {
  auto cb_wrapper = [cb](Status st,
                         NodeID /*seq*/,
                         lsn_t /*next_lsn*/,
                         std::unique_ptr<LogTailAttributes> /*unused*/,
                         std::shared_ptr<const EpochMetaDataMap> /*unused*/,
                         std::shared_ptr<TailRecord> tail_record,
                         folly::Optional<bool> /*unused*/) {
    cb(st, std::move(tail_record));
  };
  std::unique_ptr<Request> req = std::make_unique<SyncSequencerRequest>(
      logid,
      SyncSequencerRequest::INCLUDE_TAIL_RECORD,
      cb_wrapper,
      GetSeqStateRequest::Context::GET_TAIL_RECORD,
      timeout_);
  return processor_->postRequest(req);
}

std::unique_ptr<DataRecord>
ClientImpl::readLogTailSync(logid_t logid) noexcept {
  std::unique_ptr<DataRecord> record;
  Status status = E::UNKNOWN;
  Semaphore sem;
  auto cb = [&](Status st, std::unique_ptr<DataRecord> r) {
    record = std::move(r);
    status = st;
    sem.post();
  };

  int rv = readLogTail(logid, cb);
  if (rv != 0) {
    // err set by readLogTail
    return nullptr;
  }

  sem.wait();
  if (status != E::OK) {
    err = status;
    return nullptr;
  }
  return record;
}

int ClientImpl::readLogTail(logid_t logid, read_tail_callback_t cb) noexcept {
  auto cb_wrapper = [cb](Status st, std::shared_ptr<TailRecord> tail) {
    if (st != E::OK) {
      cb(st, nullptr);
      return;
    }
    ld_check(tail != nullptr);
    if (!tail->isValid()) {
      cb(E::MALFORMED_RECORD, nullptr);
      return;
    }

    if (tail->header.lsn == LSN_INVALID) {
      // no record released in the log yet
      cb(E::EMPTY, nullptr);
      return;
    }

    if (!tail->hasPayload()) {
      // record doesn't have the payload
      cb(E::NOTSUPPORTEDLOG, nullptr);
      return;
    }

    auto data_record = DataRecordFromTailRecord::create(std::move(tail));
    ld_check(data_record != nullptr);

    if (data_record->checksumFailed()) {
      cb(E::CHECKSUM_MISMATCH, nullptr);
      return;
    }

    cb(E::OK, std::unique_ptr<DataRecord>(std::move(data_record)));
  };

  return getTailRecord(logid, std::move(cb_wrapper));
}

std::unique_ptr<LogHeadAttributes>
ClientImpl::getHeadAttributesSync(logid_t logid) noexcept {
  Semaphore sem;
  std::unique_ptr<LogHeadAttributes> attributes;
  Status status = E::FAILED;

  int rv = getHeadAttributes(
      logid, [&](Status s, std::unique_ptr<LogHeadAttributes> a) {
        status = s;
        attributes = std::move(a);
        sem.post();
      });

  if (rv == 0) {
    sem.wait();
  }

  if (status != E::OK) {
    logdevice::err = status;
    attributes = nullptr;
  }
  return attributes;
}

int ClientImpl::getHeadAttributes(logid_t logid,
                                  get_head_attributes_callback_t cb) noexcept {
  auto cb_wrapper = [logid, cb, start = SteadyClock::now()](
                        const GetHeadAttributesRequest& req,
                        Status st,
                        std::unique_ptr<LogHeadAttributes> head_attributes) {
    // log response
    Worker* w = Worker::onThisThread();
    if (w) {
      auto& tracer = w->processor_->api_hits_tracer_;
      if (tracer) {
        tracer->traceGetHeadAttributes(msec_since(start),
                                       logid,
                                       req.getFailedShards(st),
                                       st,
                                       head_attributes.get());
      }
    }
    cb(st, std::move(head_attributes));
  };
  std::unique_ptr<Request> req = std::make_unique<GetHeadAttributesRequest>(
      logid,
      settings_->getSettings()->meta_api_timeout.value_or(timeout_),
      cb_wrapper);
  return processor_->postRequest(req);
}

bool ClientImpl::checkAppend(logid_t logid,
                             size_t payload_size,
                             bool allow_extra) {
  if (logid == LOGID_INVALID) {
    err = E::INVALID_PARAM;
    return false;
  }

  // disallow writing to metadata or internal log unless the appropriate
  // flag is set
  if ((!allow_write_metadata_log_ && MetaDataLog::isMetaDataLog(logid)) ||
      (!allow_write_internal_log_ &&
       configuration::InternalLogs::isInternal(logid))) {
    err = E::INVALID_PARAM;
    return false;
  }

  return AppendRequest::checkPayloadSize(
      payload_size, getMaxPayloadSize(), allow_extra);
}

void ClientImpl::allowWriteMetaDataLog() {
  allow_write_metadata_log_ = true;
}

void ClientImpl::allowWriteInternalLog() {
  allow_write_internal_log_ = true;
}

ClientSettings& ClientImpl::settings() {
  return *settings_;
}

std::string ClientImpl::getAllReadStreamsDebugInfo() noexcept {
  return AllClientReadStreams::getAllReadStreamsDebugInfo(false, // pretty
                                                          true,  // json
                                                          getProcessor());
}

void ClientImpl::publishEvent(Severity sev,
                              std::string name_space,
                              std::string type,
                              std::string data,
                              std::string context) noexcept {
  event_tracer_->traceEvent(sev,
                            std::move(name_space),
                            std::move(type),
                            std::move(data),
                            std::move(context));
}

Processor& ClientImpl::getProcessor() const {
  return *processor_;
}

configuration::NodesConfigurationAPI* ClientImpl::getNodesConfigurationAPI() {
  return processor_->getNodesConfigurationManager();
}

configuration::nodes::NodesConfigurationManager*
ClientImpl::getNodesConfigurationManager() {
  return processor_->getNodesConfigurationManager();
}

size_t ClientImpl::getMaxPayloadSize() noexcept {
  return settings_->getSettings()->max_payload_size;
}

template <typename T>
std::unique_ptr<AppendRequest>
ClientImpl::prepareRequest(logid_t logid,
                           T payload,
                           append_callback_t cb,
                           AppendAttributes attrs,
                           worker_id_t target_worker,
                           std::unique_ptr<std::string> per_request_token) {
  if (!checkAppend(logid, payload.size())) {
    return nullptr;
  }

  auto req = std::make_unique<AppendRequest>(
      bridge_.get(),
      logid,
      std::move(attrs),
      std::move(payload),
      settings_->getSettings()->append_timeout.value_or(timeout_),
      std::move(cb));

  if (target_worker.val_ > -1) {
    ld_check(target_worker.val_ <
             processor_->getWorkerCount(WorkerType::GENERAL));
    req->setTargetWorker(target_worker);
  }
  if (per_request_token) {
    req->setPerRequestToken(std::move(per_request_token));
  }
  return req;
}
void ClientImpl::initLogsConfigRandomSeed() {
  logsconfig_api_random_seed_ = folly::Random::rand64();
}

void ClientImpl::addServerConfigHookHandle(
    UpdateableServerConfig::HookHandle handle) {
  server_config_hook_handles_.push_back(std::move(handle));
}

void ClientImpl::updateStatsSettings() {
  if (!stats_) {
    return;
  }

  const auto retention_time =
      settings_->getSettings()->sequencer_boycotting.node_stats_send_period;
  const auto shared_params = stats_->params_.get();

  if (shared_params->node_stats_retention_time_on_clients != retention_time) {
    auto params = *shared_params;
    params.node_stats_retention_time_on_clients = retention_time;

    stats_->params_.update(std::make_shared<StatsParams>(std::move(params)));

    stats_->runForEach([&](Stats& stats) {
      for (auto& thread_node_stats :
           stats.synchronizedCopy(&Stats::per_node_stats)) {
        thread_node_stats.second->updateRetentionTime(retention_time);
      }
    });
  }
}

void ClientImpl::setAppendErrorInjector(
    folly::Optional<AppendErrorInjector> injector) {
  append_error_injector_ = injector;
}

bool ClientImpl::shouldE2ETrace() {
  // decide based on configuration/sampling rate if e2e tracing is on
  double sampling_rate = config_->get()
                             ->serverConfig()
                             ->getTracerSamplePercentage(E2E_APPEND_TRACER)
                             .value_or(DEFAULT_E2E_TRACING_RATE);
  // flip the coin
  return folly::Random::randDouble(0, 100) < sampling_rate;
}

void ClientImpl::registerCustomStats(StatsHolder* custom_stats) {
  // Use presence of regular stats as a proxy for stats being enabled; this
  // also means that we won't have to take care of starting the stats
  // collection thread or anything like that.
  if (stats_) {
    stats_thread_->addStatsSource(custom_stats);
  }
}

}} // namespace facebook::logdevice
