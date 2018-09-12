/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/ZookeeperEpochStore.h"

#include <boost/filesystem.hpp>
#include <cstring>

#include <folly/Memory.h>
#include <folly/small_vector.h>

#include "logdevice/common/ConstructorFailed.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/EpochMetaDataZRQ.h"
#include "logdevice/common/GetLastCleanEpochZRQ.h"
#include "logdevice/common/SetLastCleanEpochZRQ.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/ZookeeperClient.h"
#include "logdevice/common/ZookeeperEpochStoreRequest.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/include/Err.h"

namespace fs = boost::filesystem;

namespace facebook { namespace logdevice {

using MultiOpState = ZookeeperEpochStore::MultiOpState;
using CreateRootsState = ZookeeperEpochStore::CreateRootsState;

ZookeeperEpochStore::ZookeeperEpochStore(
    std::string cluster_name,
    Processor* processor,
    const std::shared_ptr<UpdateableServerConfig>& config,
    UpdateableSettings<Settings> settings,
    ZKFactory zkFactory)
    : processor_(processor),
      cluster_name_(cluster_name),
      config_(config),
      settings_(settings),
      shutting_down_(std::make_shared<std::atomic<bool>>(false)),
      zkFactory_(std::move(zkFactory)) {
  ld_check(!cluster_name.empty() &&
           cluster_name.length() <
               configuration::ZookeeperConfig::MAX_CLUSTER_NAME);

  std::shared_ptr<ZookeeperClientBase> zkclient = zkFactory_(*config->get());

  if (!zkclient) {
    throw ConstructorFailed();
  }
  zkclient_.update(std::move(zkclient));

  config_subscription_ = config_->subscribeToUpdates(
      std::bind(&ZookeeperEpochStore::onConfigUpdate, this));
}

ZookeeperEpochStore::~ZookeeperEpochStore() {
  shutting_down_->store(true);
}

std::string ZookeeperEpochStore::identify() const {
  return "zookeeper://" + zkclient_.get()->getQuorum() + rootPath();
}

void ZookeeperEpochStore::postCompletion(
    std::unique_ptr<EpochStore::CompletionMetaDataRequest>&& completion) const {
  int rv;

  ld_check(completion);

  logid_t logid = std::get<0>(completion->params_);

  std::unique_ptr<Request> rq(std::move(completion));

  rv = processor_->postWithRetrying(rq);

  if (rv != 0 && err != E::SHUTDOWN) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    1,
                    "Got an unexpected status "
                    "code %s from Processor::postWithRetrying(), dropping "
                    "request for log %lu",
                    error_name(err),
                    logid.val_);
    ld_check(false);
  }
}
void ZookeeperEpochStore::postCompletion(
    std::unique_ptr<EpochStore::CompletionLCERequest>&& completion) const {
  int rv;

  ld_check(completion);

  logid_t logid = std::get<0>(completion->params_);

  std::unique_ptr<Request> rq(std::move(completion));

  rv = processor_->postWithRetrying(rq);

  if (rv != 0 && err != E::SHUTDOWN) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    1,
                    "Got an unexpected status "
                    "code %s from Processor::postWithRetrying(), dropping "
                    "request for log %lu",
                    error_name(err),
                    logid.val_);
    ld_check(false);
  }
}

Status ZookeeperEpochStore::zkOpStatus(int rc,
                                       logid_t logid,
                                       const char* op) const {
  int zstate; // zookeeper session state

  std::shared_ptr<ZookeeperClientBase> zkclient = zkclient_.get();
  switch (rc) {
    case ZOK:
      return E::OK;
    case ZBADARGUMENTS:
      ld_check(false);
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      1,
                      "%s() reported "
                      "ZBADARGUMENTS. logid was %lu.",
                      op,
                      logid.val_);
      return E::INTERNAL;
    case ZINVALIDSTATE:

      zstate = zkclient->state();
      // ZOO_ constants are C const ints, can't switch()
      if (zstate == ZOO_EXPIRED_SESSION_STATE) {
        return E::NOTCONN;
      } else if (zstate == ZOO_AUTH_FAILED_STATE) {
        return E::ACCESS;
      } else {
        ld_check(false);
        RATELIMIT_ERROR(std::chrono::seconds(1),
                        1,
                        "Unexpected session state %s after ZINVALIDSTATE",
                        ZookeeperClient::stateString(zstate).c_str());
        return E::INTERNAL;
      }
    case ZMARSHALLINGERROR:
      return E::SYSLIMIT;
    default:
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      1,
                      "Got unexpected status code %s from %s() for log %lu",
                      zerror(rc),
                      op,
                      logid.val_);
  }

  return E::INTERNAL;
}

static Status zkCfStatus(int rc, logid_t logid, StatsHolder* stats = nullptr) {
  switch (rc) {
    case ZOK:
      return E::OK;
    case ZNONODE:
      return E::NOTFOUND;
    case ZNOAUTH:
      return E::ACCESS;
    case ZCONNECTIONLOSS:
    case ZOPERATIONTIMEOUT:
    case ZSESSIONEXPIRED:
      return E::CONNFAILED;
    case ZCLOSING:
      return E::SHUTDOWN;
    case ZBADVERSION:
      return E::AGAIN;
    case ZNODEEXISTS:
      return E::EXISTS;
    case ZRUNTIMEINCONSISTENCY: {
      RATELIMIT_CRITICAL(
          std::chrono::seconds(10),
          10,
          "Got status code %s from Zookeeper completion function for log %lu.",
          zerror(rc),
          logid.val_);
      STAT_INCR(stats, zookeeper_epoch_store_internal_inconsistency_error);
      return E::FAILED;
    }
    default:
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      1,
                      "Got unexpected status code %s from Zookeeper completion "
                      "function for log %lu",
                      zerror(rc),
                      logid.val_);
      ld_check(false);
  }

  return E::INTERNAL;
}

std::string ZookeeperEpochStore::znodePathForLog(logid_t logid) const {
  ld_check(logid != LOGID_INVALID);
  return rootPath() + "/" + std::to_string(logid.val_);
}

// state for ZK multi-ops.
class ZookeeperEpochStore::MultiOpState {
 public:
  explicit MultiOpState(
      std::unique_ptr<ZookeeperEpochStoreRequest> rq = nullptr)
      : zrq(std::move(rq)) {}

  // Adds a CREATE operation to the list, copies all inputs
  void addCreateOp(std::string path, const char* content, size_t len) {
    path_buffers_.push_back(std::move(path));
    value_buffers_.emplace_back(content, len);
    operations_.emplace_back();

    zoo_create_op_init(&operations_.back(),
                       path_buffers_.back().c_str(),
                       value_buffers_.back().data(),
                       value_buffers_.back().size(),
                       &ZOO_OPEN_ACL_UNSAFE,
                       0,       // flags
                       nullptr, // path_buffer
                       0);      // path_buffer_len
  }

  // Runs a multi-op contained in this struct on the given client instance.
  // The given completion function will be called with the given `data`
  // argument. Typically `data` will contain a reference to this struct to
  // retrieve the results of individual operations (or retry them).
  // If this returns 0, this instance of MultiOpState should not be destroyed
  // until the supplied completion function is called.
  int runMultiOp(ZookeeperClientBase& zkclient,
                 void_completion_t cf,
                 const void* data) {
    ld_check(!operations_.empty());
    // result buffer size should match the operation count
    op_results_.clear();
    op_results_.resize(operations_.size());

    return zkclient.multiOp(
        operations_.size(), operations_.data(), op_results_.data(), cf, data);
  }

  // request that drove the multi-op (optional)
  std::unique_ptr<ZookeeperEpochStoreRequest> zrq;

  // Returns results of individual sub-operations
  const folly::small_vector<zoo_op_result_t, 4>& getResults() {
    return op_results_;
  }

 private:
  // paths are not copied by zoo_create_op_init(), and will only be copied by
  // the zoo_amulti() call
  folly::small_vector<std::string, 4> path_buffers_;

  // values are not copied by zoo_create_op_init(), and will only be copied by
  // the zoo_amulti() call
  folly::small_vector<std::string, 4> value_buffers_;

  // ZK multi-op structs
  folly::small_vector<zoo_op_t, 4> operations_;

  // individual sub-operations results will be stored here
  folly::small_vector<zoo_op_result_t, 4> op_results_;
};

// State for a series of operations to create root znodes that can be started
// after znode creation operations for logs failed with ZNONODE, indicating the
// parent didn't exist
class ZookeeperEpochStore::CreateRootsState {
 public:
  CreateRootsState(std::unique_ptr<MultiOpState> mos, std::string root_path)
      : deferred_multi_op_state_(std::move(mos)) {
    // Enumerating all the parents that may need to be created
    fs::path path(root_path);
    while (!path.empty() && path.string() != "/") {
      paths_to_create_.push(path.string());
      path = path.parent_path();
    }
  }

  // Takes one path from the list and schedules the creation operation on that.
  // If this method returns 0, the CreateRootsState machine is now self-owned.
  // If it returns anything else, the ownership is still with the caller
  int run() {
    ld_check(!paths_to_create_.empty());
    ZookeeperEpochStore* store = deferred_multi_op_state_->zrq->store_;
    auto client = store->getZookeeperClient();

    // All operations are scheduled one-by-one. but the multi-op API is used
    // in order to minimize the number of ZK APIs used (we already use the
    // multi-op API to create multiple znodes when provisioning a log).
    current_op_ = std::make_unique<MultiOpState>();
    current_op_->addCreateOp(getNextPathToCreate(), "", 0);
    ld_spew("Scheduling creation of %s", getNextPathToCreate().c_str());
    int rc = current_op_->runMultiOp(*client, multiOpCF, this);
    if (rc != 0) {
      // This will output errors to the log
      store->zkOpStatus(rc, LOGID_INVALID, "create_roots");
    }
    return rc;
  }

  // This gets called as the completion function for every parent znode's
  // creation
  static void multiOpCF(int rc, const void* data) {
    std::unique_ptr<CreateRootsState> state(
        reinterpret_cast<CreateRootsState*>(const_cast<void*>(data)));
    auto st = zkCfStatus(rc, LOGID_INVALID);
    ld_check(!state->paths_to_create_.empty());
    if (st == E::OK) {
      ld_info("Created root znode %s successfully",
              state->getNextPathToCreate().c_str());
    } else {
      ld_spew("Creation of root znode %s completed with rv %d, ld error %s",
              state->getNextPathToCreate().c_str(),
              rc,
              error_name(st));
    }
    // If the path already exists or has just been created, continue
    if (st == E::OK || st == E::EXISTS) {
      state->paths_to_create_.pop();
      if (!state->paths_to_create_.empty()) {
        // More paths to create
        rc = state->run();
        if (rc == 0) {
          // The state machine is now self-owned
          state.release();
          return;
        }
        // if we couldn't schedule the next request, run() is supposed to
        // output the error. At this point we can override the error code (to
        // return the one previously received by the original API call) and
        // fall through to createRootZnodesCF()
        rc = ZNONODE;
      }
    }
    ZookeeperEpochStore::createRootZnodesCF(std::move(state), rc);
  }

  const std::string& getNextPathToCreate() {
    return paths_to_create_.top();
  }

  // This is the operation that was deferred until creation of the root znodes
  // is completed. This class doesn't act on it until all the root znodes are
  // created (or something fails).
  std::unique_ptr<MultiOpState> deferred_multi_op_state_;

 private:
  // The list of paths to be created
  std::stack<std::string> paths_to_create_;

  // Currently running operation
  std::unique_ptr<MultiOpState> current_op_;
};

Status ZookeeperEpochStore::provisionLogZnodes(
    std::unique_ptr<ZookeeperEpochStoreRequest>& zrq,
    const char* sequencer_znode_value,
    int sequencer_znode_value_size) {
  ld_check(sequencer_znode_value);
  ld_check(sequencer_znode_value_size > 0);
  const ZookeeperEpochStore* self = zrq->store_;

  logid_t log_id = zrq->logid_;
  std::string logroot = self->znodePathForLog(log_id);

  // State contains results of sub-requests of the multi-op and the ZRQ that
  // drives this
  auto state = std::make_unique<MultiOpState>(std::move(zrq));

  // Creating root znode for this log
  state->addCreateOp(logroot, "", 0);

  // Creating the epoch metadata znode with the supplied znode_value
  state->addCreateOp(logroot + "/" + EpochMetaDataZRQ::znodeName,
                     sequencer_znode_value,
                     sequencer_znode_value_size);

  // Creating empty lce/metadata_lce nodes
  state->addCreateOp(
      logroot + "/" + LastCleanEpochZRQ::znodeNameDataLog, "", 0);
  state->addCreateOp(
      logroot + "/" + LastCleanEpochZRQ::znodeNameMetaDataLog, "", 0);

  std::shared_ptr<ZookeeperClientBase> zkclient = self->zkclient_.get();

  int rv = state->runMultiOp(*zkclient, zkLogMultiCreateCF, state.get());

  Status st = self->zkOpStatus(rv, log_id, "zoo_amulti");
  if (st == E::OK) {
    // zkLogMultiCreateCF() should take care of this
    state.release();
  } else {
    // Failed to start request. Returning zrq to the caller, `state` should
    // self-destroy
    zrq = std::move(state->zrq);
  }
  return st;
}

void ZookeeperEpochStore::zkGetCF(int rc,
                                  const char* value_from_zk,
                                  int value_len_from_zk,
                                  const struct ::Stat* stat,
                                  const void* data) {
  int rv;
  ZookeeperEpochStoreRequest::NextStep next_step;
  bool do_provision = false;
  std::unique_ptr<ZookeeperEpochStoreRequest> zrq{
      reinterpret_cast<ZookeeperEpochStoreRequest*>(const_cast<void*>(data))};
  ld_check(zrq);

  StatsHolder* stats_holder = zrq->store_->processor_->stats_;

  const char* value_for_zrq = value_from_zk;

  Status st = zkCfStatus(rc, zrq->logid_, stats_holder);
  if (st != E::OK && st != E::NOTFOUND) {
    goto err;
  }

  if (st == E::NOTFOUND) {
    // no znode exists, passing nullptr with length 0 to zrq
    value_for_zrq = nullptr;
  }

  next_step = zrq->onGotZnodeValue(value_for_zrq, value_len_from_zk);
  switch (next_step) {
    case ZookeeperEpochStoreRequest::NextStep::PROVISION:
      // continue with creation of new znodes
      do_provision = true;
      break;
    case ZookeeperEpochStoreRequest::NextStep::MODIFY:
      // continue the read-modify-write
      ld_check(do_provision == false);
      break;
    case ZookeeperEpochStoreRequest::NextStep::STOP:
      st = err;
      ld_check(
          (dynamic_cast<GetLastCleanEpochZRQ*>(zrq.get()) && st == E::OK) ||
          (dynamic_cast<EpochMetaDataZRQ*>(zrq.get()) && st == E::UPTODATE));
      goto done;
    case ZookeeperEpochStoreRequest::NextStep::FAILED:
      st = err;
      ld_check(st == E::FAILED || st == E::BADMSG || st == E::NOTFOUND ||
               st == E::EMPTY || st == E::EXISTS || st == E::DISABLED ||
               st == E::TOOBIG ||
               ((st == E::INVALID_PARAM || st == E::ABORTED) &&
                dynamic_cast<EpochMetaDataZRQ*>(zrq.get())) ||
               (st == E::STALE &&
                (dynamic_cast<EpochMetaDataZRQ*>(zrq.get()) ||
                 dynamic_cast<SetLastCleanEpochZRQ*>(zrq.get()))));
      goto done;

      // no default to let compiler to check if we exhaust all NextSteps
  }

  { // Without this scope gcc throws a "goto cross initialization" error.
    // Using a switch() instead of a goto results in a similar error.
    // Using a single-iteration loop is confusing. Perphas we should start
    // using #define BEGIN_SCOPE { to avoid the extra indentation? --march

    char znode_value[ZNODE_VALUE_WRITE_LEN_MAX];
    int znode_value_size =
        zrq->composeZnodeValue(znode_value, sizeof(znode_value));
    if (znode_value_size < 0 || znode_value_size >= sizeof(znode_value)) {
      ld_check(false);
      RATELIMIT_CRITICAL(std::chrono::seconds(1),
                         10,
                         "INTERNAL ERROR: invalid value size %d reported by "
                         "ZookeeperEpochStoreRequest::composeZnodeValue() "
                         "for log %lu",
                         znode_value_size,
                         zrq->logid_.val_);
      st = E::INTERNAL;
      goto err;
    }

    st = E::INTERNAL;
    ZookeeperEpochStore* self = const_cast<ZookeeperEpochStore*>(zrq->store_);
    if (do_provision) {
      st = self->provisionLogZnodes(zrq, znode_value, znode_value_size);
      if (st == E::OK) {
        // zrq should've been captured by provisionLogZnodes() if all went OK
        ld_check(zrq == nullptr);
        return;
      }
    } else {
      std::string znode_path = zrq->getZnodePath();
      // setData() below succeeds only if the current version number of
      // znode at znode_path matches stat->version that the znode had
      // when we read its value. Zookeeper atomically increments the version
      // number of znode on every write to that znode. If the versions do not
      // match zkSetCf() will be called with status ZBADVERSION. This ensures
      // that if our read-modify-write of znode_path succeeds, it was atomic.
      std::shared_ptr<ZookeeperClientBase> zkclient = self->zkclient_.get();
      logid_t log_id = zrq->logid_;
      rv = zkclient->setData(znode_path.c_str(),
                             znode_value,
                             znode_value_size,
                             stat->version,
                             zkSetCF,
                             zrq.get());

      st = self->zkOpStatus(rv, log_id, "zoo_aset");
      if (st == E::OK) {
        zrq.release();
        return;
      }
    }
    ld_check(st != E::OK);
    ld_check(zrq);
    switch (st) {
      case E::NOTCONN:
      case E::SYSLIMIT:
        st = E::CONNFAILED;
        break;
      default:
        break;
    }
  }

err:
  ld_check(st != E::OK);

done:
  if (st != E::SHUTDOWN || !zrq->epoch_store_shutting_down_->load()) {
    zrq->postCompletion(st);
  } else {
    // Do not post a CompletionRequest if Zookeeper client is shutting down and
    // the EpochStore is being destroyed. Note that we can get also get an
    // E::SHUTDOWN code if the ZookeeperClient is being destroyed due to
    // zookeeper quorum change, but the EpochStore is still there.
  }
}

void ZookeeperEpochStore::zkSetCF(int rc,
                                  const struct ::Stat* /*stat*/,
                                  const void* data) {
  std::unique_ptr<ZookeeperEpochStoreRequest> zrq{
      reinterpret_cast<ZookeeperEpochStoreRequest*>(const_cast<void*>(data))};

  StatsHolder* stats_holder = zrq->store_->processor_->stats_;
  Status st = zkCfStatus(rc, zrq->logid_, stats_holder);

  if (st != E::SHUTDOWN || !zrq->epoch_store_shutting_down_->load()) {
    zrq->postCompletion(st);
  } else {
    // Do not post a CompletionRequest if Zookeeper client is shutting down and
    // the EpochStore is being destroyed. Note that we can get also get an
    // E::SHUTDOWN code if the ZookeeperClient is being destroyed due to
    // zookeeper quorum change, but the EpochStore is still there.
  }
}

void ZookeeperEpochStore::zkLogMultiCreateCF(int rc, const void* data) {
  std::unique_ptr<MultiOpState> state{
      reinterpret_cast<MultiOpState*>(const_cast<void*>(data))};
  ld_check(state->zrq);
  ZookeeperEpochStore* self = state->zrq->store_;
  ld_check(self);

  StatsHolder* stats_holder = self->processor_->stats_;
  Status st = zkCfStatus(rc, state->zrq->logid_, stats_holder);
  if (st == E::OK) {
    // If everything worked well, then each individual operation should've went
    // through fine as well
    for (const auto& res : state->getResults()) {
      ld_check(zkCfStatus(res.err, state->zrq->logid_, stats_holder) == E::OK);
    }
  } else if (st == E::NOTFOUND) {
    // znode creation operation failed because the root znode was not found.
    if (self->settings_->zk_create_root_znodes) {
      RATELIMIT_INFO(
          std::chrono::seconds(1), 1, "Root znode doesn't exist, creating it.");

      // Creating root znodes via a series of create operations (since some
      // parent znodes may be present and others may be missing). Passing
      // `state` here since the original operation should be retried after root
      // znodes have been created.
      ZookeeperEpochStore* store = state->zrq->store_;
      store->createRootZnodes(std::move(state));

      // not calling zkSetCF, since the request will be retried and hopefully
      // will succeed afterwards
      return;
    } else {
      RATELIMIT_ERROR(
          std::chrono::seconds(1),
          1,
          "Root znode doesn't exist! It hast to be created by external tooling "
          "if `zk-create-root-znodes` is set to `false`");
    }
  }

  // deferring to zkSetCf to do the actual work
  zkSetCF(rc, nullptr, state->zrq.release());
}

void ZookeeperEpochStore::createRootZnodes(
    std::unique_ptr<MultiOpState> multi_op_state) {
  ld_check(multi_op_state);
  ld_check(multi_op_state->zrq);

  auto create_root_state =
      std::make_unique<CreateRootsState>(std::move(multi_op_state), rootPath());

  int rc = create_root_state->run();
  if (rc != 0) {
    // run() should output the actual error, but here we are calling a CF where
    // the set of expected errors is different from those we expect from running
    // a request. Returning ZNONODE - the original code that the API returned
    // before root znode creation was started.
    createRootZnodesCF(std::move(create_root_state), ZNONODE);
    return;
  }
  // The state machine now owns itself
  create_root_state.release();
}

void ZookeeperEpochStore::createRootZnodesCF(
    std::unique_ptr<CreateRootsState> state,
    int rc) {
  auto st = zkCfStatus(rc, LOGID_INVALID);
  if (st != E::OK && st != E::EXISTS) {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    10,
                    "Unable to create root znode %s: ZK "
                    "error %d, LD error %s",
                    state->getNextPathToCreate().c_str(),
                    rc,
                    error_description(st));
    zkSetCF(rc, nullptr, state->deferred_multi_op_state_->zrq.release());
    return;
  }
  // All root znodes should've been created by now, retrying the original
  // multi-op
  std::unique_ptr<MultiOpState> multi_op_state =
      std::move(state->deferred_multi_op_state_);
  ld_check(multi_op_state);
  ld_check(multi_op_state->zrq);
  logid_t log_id = multi_op_state->zrq->logid_;

  auto store = multi_op_state->zrq->store_;
  ld_check(store);
  auto client = store->getZookeeperClient();
  int rv = multi_op_state->runMultiOp(
      *client, zkLogMultiCreateCF, multi_op_state.get());

  Status op_st = store->zkOpStatus(rv, log_id, "zoo_amulti_after_root_created");
  if (op_st == E::OK) {
    // zkLogMultiCreateCF() should take care of this
    multi_op_state.release();
  } else {
    // Failed to start request. Calling the request's CF with the original error
    // of ZNONODE
    zkSetCF(ZNONODE, nullptr, multi_op_state->zrq.release());
  }
}

int ZookeeperEpochStore::runRequest(
    std::unique_ptr<ZookeeperEpochStoreRequest> zrq) {
  ld_check(zrq);

  std::string znode_path = zrq->getZnodePath();
  const logid_t logid = zrq->logid_;

  std::shared_ptr<ZookeeperClientBase> zkclient = zkclient_.get();
  int rv = zkclient->getData(
      znode_path.c_str(), &ZookeeperEpochStore::zkGetCF, zrq.get());

  Status st = zkOpStatus(rv, logid, "zoo_aget");
  if (st == E::OK) {
    zrq.release(); // now owned by Zookeeper client library
    return 0;
  }
  err = st;
  return -1;
}

void ZookeeperEpochStore::onConfigUpdate() {
  std::shared_ptr<ServerConfig> cfg = config_->get();
  std::shared_ptr<ZookeeperClientBase> cur = zkclient_.get();
  if (cfg->getZookeeperQuorumString() == cur->getQuorum()) {
    // No changes.
    return;
  }

  ld_info("Zookeeper quorum changed, reconnecting: %s",
          cfg->getZookeeperQuorumString().c_str());

  std::shared_ptr<ZookeeperClientBase> zkclient = zkFactory_(*cfg);

  if (!zkclient) {
    ld_error("Zookeeper reconnect failed: %s", error_description(err));
    return;
  }
  zkclient_.update(std::move(zkclient));
}

int ZookeeperEpochStore::getLastCleanEpoch(logid_t logid, CompletionLCE cf) {
  return runRequest(std::unique_ptr<ZookeeperEpochStoreRequest>(
      new GetLastCleanEpochZRQ(logid, EPOCH_INVALID, cf, this)));
}

int ZookeeperEpochStore::setLastCleanEpoch(logid_t logid,
                                           epoch_t lce,
                                           const TailRecord& tail_record,
                                           EpochStore::CompletionLCE cf) {
  if (!tail_record.isValid() || tail_record.containOffsetWithinEpoch()) {
    RATELIMIT_CRITICAL(std::chrono::seconds(5),
                       5,
                       "INTERNAL ERROR: attempting to update LCE with invalid "
                       "tail record! log %lu, lce %u, tail record flags: %u",
                       logid.val_,
                       lce.val_,
                       tail_record.header.flags);
    err = E::INVALID_PARAM;
    ld_check(false);
    return -1;
  }

  return runRequest(std::unique_ptr<ZookeeperEpochStoreRequest>(
      new SetLastCleanEpochZRQ(logid, lce, tail_record, cf, this)));
}

int ZookeeperEpochStore::createOrUpdateMetaData(
    logid_t logid,
    std::shared_ptr<EpochMetaData::Updater> updater,
    CompletionMetaData cf,
    MetaDataTracer tracer,
    WriteNodeID write_node_id) {
  // do not allow calling this function with metadata logids
  if (logid <= LOGID_INVALID || logid > LOGID_MAX) {
    err = E::INVALID_PARAM;
    return -1;
  }

  return runRequest(std::unique_ptr<ZookeeperEpochStoreRequest>(
      new EpochMetaDataZRQ(logid,
                           EPOCH_INVALID,
                           cf,
                           this,
                           std::move(updater),
                           std::move(tracer),
                           write_node_id,
                           config_->get())));
}

NodeID ZookeeperEpochStore::getMyNodeID() const {
  return config_->get()->getMyNodeID();
}

}} // namespace facebook::logdevice
