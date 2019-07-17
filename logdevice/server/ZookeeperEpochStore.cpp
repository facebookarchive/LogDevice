/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/ZookeeperEpochStore.h"

#include <cstring>

#include <boost/filesystem.hpp>
#include <folly/Memory.h>
#include <folly/small_vector.h>

#include "logdevice/common/ConstructorFailed.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/ZookeeperClient.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/include/Err.h"
#include "logdevice/server/EpochMetaDataZRQ.h"
#include "logdevice/server/GetLastCleanEpochZRQ.h"
#include "logdevice/server/SetLastCleanEpochZRQ.h"
#include "logdevice/server/ZookeeperEpochStoreRequest.h"

namespace fs = boost::filesystem;

namespace facebook { namespace logdevice {

ZookeeperEpochStore::ZookeeperEpochStore(
    std::string cluster_name,
    Processor* processor,
    const std::shared_ptr<UpdateableZookeeperConfig>& zk_config,
    const std::shared_ptr<UpdateableNodesConfiguration>& nodes_configuration,
    UpdateableSettings<Settings> settings,
    std::shared_ptr<ZookeeperClientFactory> zkFactory)
    : processor_(processor),
      cluster_name_(cluster_name),
      zk_config_(zk_config),
      nodes_configuration_(nodes_configuration),
      settings_(settings),
      shutting_down_(std::make_shared<std::atomic<bool>>(false)),
      zkFactory_(zkFactory) {
  ld_check(!cluster_name.empty() &&
           cluster_name.length() <
               configuration::ZookeeperConfig::MAX_CLUSTER_NAME);

  auto cfg = zk_config_->get();
  std::shared_ptr<ZookeeperClientBase> zkclient = zkFactory_->getClient(*cfg);

  if (!zkclient) {
    throw ConstructorFailed();
  }
  zkclient_.store(zkclient);

  config_subscription_ = zk_config_->subscribeToUpdates(
      std::bind(&ZookeeperEpochStore::onConfigUpdate, this));
}

ZookeeperEpochStore::~ZookeeperEpochStore() {
  shutting_down_->store(true);
  // close() ensures that no callbacks are invoked after this point. So, we can
  // be sure that no references are held to zkclient_ from any of the callback
  // functions.
  zkclient_.load()->close();
}

std::string ZookeeperEpochStore::identify() const {
  return "zookeeper://" + zkclient_.load()->getQuorum() + rootPath();
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

Status ZookeeperEpochStore::completionStatus(int rc, logid_t logid) {
  std::shared_ptr<ZookeeperClientBase> zkclient = zkclient_.load();
  // Special handling for cases where additional information would be helpful
  if (rc == ZRUNTIMEINCONSISTENCY) {
    RATELIMIT_CRITICAL(
        std::chrono::seconds(10),
        10,
        "Got status code %s from Zookeeper completion function for log %lu.",
        zerror(rc),
        logid.val_);
    STAT_INCR(
        processor_->stats_, zookeeper_epoch_store_internal_inconsistency_error);
    return E::FAILED;
  } else if (rc == ZBADARGUMENTS) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    1,
                    "Zookeeper reported ZBADARGUMENTS. logid was %lu.",
                    logid.val_);
    ld_assert(false);
    return E::INTERNAL;
  } else if (rc == ZINVALIDSTATE) {
    // Note: state() returns the current state of the session and does not
    // necessarily reflect that state at the time of error
    int zstate = zkclient->state();
    // ZOO_ constants are C const ints, can't switch()
    if (zstate == ZOO_EXPIRED_SESSION_STATE) {
      return E::NOTCONN;
    } else if (zstate == ZOO_AUTH_FAILED_STATE) {
      return E::ACCESS;
    } else {
      RATELIMIT_WARNING(
          std::chrono::seconds(10),
          5,
          "Unable to recover session state at time of ZINVALIDSTATE error, "
          "possibly EXPIRED or AUTH_FAILED. But the current session state is "
          "%s, could be due to a session re-establishment.",
          ZookeeperClient::stateString(zstate).c_str());
      return E::FAILED;
    }
  }

  Status status = ZookeeperClientBase::toStatus(rc);
  if (status == E::VERSION_MISMATCH) {
    return E::AGAIN;
  }
  if (status == E::UNKNOWN) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    1,
                    "Got unexpected status code %s from Zookeeper completion "
                    "function for log %lu",
                    zerror(rc),
                    logid.val_);
    ld_check(false);
  }

  return status;
}

std::string ZookeeperEpochStore::znodePathForLog(logid_t logid) const {
  ld_check(logid != LOGID_INVALID);
  return rootPath() + "/" + std::to_string(logid.val_);
}

void ZookeeperEpochStore::provisionLogZnodes(
    std::unique_ptr<ZookeeperEpochStoreRequest> zrq,
    std::string znode_value) {
  ld_check(!znode_value.empty());

  std::string logroot = znodePathForLog(zrq->logid_);

  std::vector<zk::Op> ops;
  // Creating root znode for this log
  ops.emplace_back(ZookeeperClientBase::makeCreateOp(logroot, ""));
  // Creating the epoch metadata znode with the supplied znode_value
  ops.emplace_back(ZookeeperClientBase::makeCreateOp(
      logroot + "/" + EpochMetaDataZRQ::znodeName, znode_value));
  // Creating empty lce/metadata_lce nodes
  ops.emplace_back(ZookeeperClientBase::makeCreateOp(
      logroot + "/" + LastCleanEpochZRQ::znodeNameDataLog, ""));
  ops.emplace_back(ZookeeperClientBase::makeCreateOp(
      logroot + "/" + LastCleanEpochZRQ::znodeNameMetaDataLog, ""));

  auto cb = [this, znode_value = std::move(znode_value), zrq = std::move(zrq)](
                int rc, std::vector<zk::OpResponse> results) mutable {
    Status st = completionStatus(rc, zrq->logid_);
    if (st == E::OK) {
      // If everything worked well, then each individual operation should've
      // went through fine as well
      for (const auto& res : results) {
        ld_check(completionStatus(res.rc_, zrq->logid_) == E::OK);
      }
    } else if (st == E::NOTFOUND) {
      // znode creation operation failed because the root znode was not found.
      if (settings_->zk_create_root_znodes) {
        RATELIMIT_INFO(std::chrono::seconds(1),
                       1,
                       "Root znode doesn't exist, creating it.");

        // Creating root znodes using createWithAncestors and retrying the logs
        // provisioning upon success.
        auto rootcb = [this,
                       znode_value = std::move(znode_value),
                       zrq = std::move(zrq)](
                          int rootrc, std::string rootpath) mutable {
          auto rootst = completionStatus(rootrc, LOGID_INVALID);
          if (rootst == E::OK) {
            ld_info("Created root znode %s successfully", rootpath.c_str());
          } else {
            ld_spew(
                "Creation of root znode %s completed with rv %d, ld error %s",
                rootpath.c_str(),
                rootrc,
                error_name(rootst));
          }
          // If the path already exists or has just been created, continue
          if (rootst == E::OK || rootst == E::EXISTS) {
            // retrying the original multi-op
            provisionLogZnodes(std::move(zrq), std::move(znode_value));
          } else {
            RATELIMIT_ERROR(std::chrono::seconds(10),
                            10,
                            "Unable to create root znode %s: ZK "
                            "error %d, LD error %s",
                            rootpath.c_str(),
                            rootrc,
                            error_description(rootst));
            postRequestCompletion(rootrc, std::move(zrq));
          }
        };
        std::shared_ptr<ZookeeperClientBase> zkclient = zkclient_.load();
        zkclient->createWithAncestors(rootPath(), "", std::move(rootcb));
        // not calling postRequestCompletion, since the request will be retried
        // and hopefully will succeed afterwards
        return;
      } else {
        RATELIMIT_ERROR(
            std::chrono::seconds(1),
            1,
            "Root znode doesn't exist! It has to be created by external "
            "tooling if `zk-create-root-znodes` is set to `false`");
      }
    }

    // post completion to do the actual work
    postRequestCompletion(rc, std::move(zrq));
  };

  std::shared_ptr<ZookeeperClientBase> zkclient = zkclient_.load();
  zkclient->multiOp(std::move(ops), std::move(cb));
}

void ZookeeperEpochStore::onGetZnodeComplete(
    int rc,
    std::string value_from_zk,
    const zk::Stat& stat,
    std::unique_ptr<ZookeeperEpochStoreRequest> zrq) {
  ZookeeperEpochStoreRequest::NextStep next_step;
  bool do_provision = false;
  ld_check(zrq);

  const char* value_for_zrq = value_from_zk.data();

  Status st = completionStatus(rc, zrq->logid_);
  if (st != E::OK && st != E::NOTFOUND) {
    goto err;
  }

  if (st == E::NOTFOUND) {
    // no znode exists, passing nullptr with length 0 to zrq
    value_for_zrq = nullptr;
  }

  next_step = zrq->onGotZnodeValue(value_for_zrq, value_from_zk.size());
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

    std::string znode_value_str(znode_value, znode_value_size);
    if (do_provision) {
      provisionLogZnodes(std::move(zrq), std::move(znode_value_str));
      return;
    } else {
      std::string znode_path = zrq->getZnodePath();
      // setData() below succeeds only if the current version number of
      // znode at znode_path matches the version that the znode had
      // when we read its value. Zookeeper atomically increments the version
      // number of znode on every write to that znode. If the versions do not
      // match zkSetCf() will be called with status ZBADVERSION. This ensures
      // that if our read-modify-write of znode_path succeeds, it was atomic.
      std::shared_ptr<ZookeeperClientBase> zkclient = zkclient_.load();
      auto cb = [this, req = std::move(zrq)](int res, zk::Stat) mutable {
        postRequestCompletion(res, std::move(req));
      };
      zkclient->setData(std::move(znode_path),
                        std::move(znode_value_str),
                        std::move(cb),
                        stat.version_);
      return;
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

void ZookeeperEpochStore::postRequestCompletion(
    int rc,
    std::unique_ptr<ZookeeperEpochStoreRequest> zrq) {
  Status st = completionStatus(rc, zrq->logid_);

  if (st != E::SHUTDOWN || !zrq->epoch_store_shutting_down_->load()) {
    zrq->postCompletion(st);
  } else {
    // Do not post a CompletionRequest if Zookeeper client is shutting down and
    // the EpochStore is being destroyed. Note that we can get also get an
    // E::SHUTDOWN code if the ZookeeperClient is being destroyed due to
    // zookeeper quorum change, but the EpochStore is still there.
  }
}

int ZookeeperEpochStore::runRequest(
    std::unique_ptr<ZookeeperEpochStoreRequest> zrq) {
  ld_check(zrq);

  std::string znode_path = zrq->getZnodePath();
  std::shared_ptr<ZookeeperClientBase> zkclient = zkclient_.load();
  auto cb = [this, req = std::move(zrq)](
                int rc, std::string value, zk::Stat stat) mutable {
    onGetZnodeComplete(rc, std::move(value), stat, std::move(req));
  };
  zkclient->getData(znode_path, std::move(cb));
  return 0;
}

void ZookeeperEpochStore::onConfigUpdate() {
  std::shared_ptr<ZookeeperConfig> cfg = zk_config_->get();
  if (cfg == nullptr) {
    RATELIMIT_ERROR(
        std::chrono::seconds(10),
        1,
        "Zookeeper configuration is empty. Failed to update epoch store.");
    return;
  }

  std::shared_ptr<ZookeeperClientBase> cur = zkclient_.load();
  auto quorum = cfg->getQuorumString();
  if (quorum == cur->getQuorum()) {
    return;
  }

  ld_info("Zookeeper quorum changed, reconnecting: %s", quorum.c_str());

  std::shared_ptr<ZookeeperClientBase> zkclient = zkFactory_->getClient(*cfg);

  if (!zkclient) {
    ld_error("Zookeeper reconnect failed: %s", error_description(err));
    return;
  }
  zkclient_.store(zkclient);
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
                           nodes_configuration_->get(),
                           processor_->getOptionalMyNodeID())));
}

}} // namespace facebook::logdevice
