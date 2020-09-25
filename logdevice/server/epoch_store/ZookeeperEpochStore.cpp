/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/epoch_store/ZookeeperEpochStore.h"

#include <cstring>

#include <boost/filesystem.hpp>
#include <folly/Memory.h>
#include <folly/small_vector.h>

#include "logdevice/common/ConstructorFailed.h"
#include "logdevice/common/MetaDataLog.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/ZookeeperClient.h"
#include "logdevice/common/ZookeeperClientBase.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/include/Err.h"
#include "logdevice/server/epoch_store/EpochMetaDataZRQ.h"
#include "logdevice/server/epoch_store/GetLastCleanEpochZRQ.h"
#include "logdevice/server/epoch_store/LogMetaData.h"
#include "logdevice/server/epoch_store/LogMetaDataCodec.h"
#include "logdevice/server/epoch_store/SetLastCleanEpochZRQ.h"
#include "logdevice/server/epoch_store/ZookeeperEpochStoreRequest.h"

namespace fs = boost::filesystem;

namespace facebook { namespace logdevice {

ZookeeperEpochStore::ZookeeperEpochStore(
    std::string cluster_name,
    RequestExecutor request_executor,
    std::shared_ptr<ZookeeperClientBase> zkclient,
    const std::shared_ptr<UpdateableNodesConfiguration>& nodes_configuration,
    UpdateableSettings<Settings> settings,
    folly::Optional<NodeID> my_node_id,
    StatsHolder* stats)
    : request_executor_(std::move(request_executor)),
      zkclient_(std::move(zkclient)),
      cluster_name_(cluster_name),
      nodes_configuration_(nodes_configuration),
      settings_(settings),
      my_node_id_(std::move(my_node_id)),
      stats_(stats),
      shutting_down_(false) {
  ld_check(!cluster_name.empty() &&
           cluster_name.length() <
               configuration::ZookeeperConfig::MAX_CLUSTER_NAME);
  if (!zkclient_) {
    throw ConstructorFailed();
  }
}

ZookeeperEpochStore::~ZookeeperEpochStore() {
  shutting_down_.store(true);
  // close() ensures that no callbacks are invoked after this point. So, we can
  // be sure that no references are held to zkclient_ from any of the callback
  // functions.
  zkclient_->close();
}

std::string ZookeeperEpochStore::identify() const {
  return "zookeeper://" + zkclient_->getQuorum() + rootPath();
}

Status ZookeeperEpochStore::completionStatus(int rc, logid_t logid) {
  // Special handling for cases where additional information would be helpful
  if (rc == ZRUNTIMEINCONSISTENCY) {
    RATELIMIT_CRITICAL(
        std::chrono::seconds(10),
        10,
        "Got status code %s from Zookeeper completion function for log %lu.",
        zerror(rc),
        logid.val_);
    STAT_INCR(stats_, zookeeper_epoch_store_internal_inconsistency_error);
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
    int zstate = zkclient_->state();
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
  if (MetaDataLog::isMetaDataLog(logid)) {
    logid = MetaDataLog::dataLogID(logid);
  }
  return rootPath() + "/" + std::to_string(logid.val_);
}

void ZookeeperEpochStore::provisionLogZnodes(RequestContext&& context,
                                             std::string znode_value) {
  ld_check(!znode_value.empty());

  std::string logroot = znodePathForLog(context.zrq->logid_);

  std::vector<zk::Op> ops;
  // Creating root znode for this log
  ops.emplace_back(ZookeeperClientBase::makeCreateOp(
      logroot,
      context.settings.double_write
          ? LogMetaDataCodec::serialize(context.log_metadata)
          : ""));
  // Creating the epoch metadata znode with the supplied znode_value
  ops.emplace_back(ZookeeperClientBase::makeCreateOp(
      logroot + "/" + EpochMetaDataZRQ::znodeName, znode_value));
  // Creating empty lce/metadata_lce nodes
  ops.emplace_back(ZookeeperClientBase::makeCreateOp(
      logroot + "/" + LastCleanEpochZRQ::znodeNameDataLog, ""));
  ops.emplace_back(ZookeeperClientBase::makeCreateOp(
      logroot + "/" + LastCleanEpochZRQ::znodeNameMetaDataLog, ""));

  auto cb = [this,
             znode_value = std::move(znode_value),
             context = std::move(context)](
                int rc, std::vector<zk::OpResponse> results) mutable {
    const Status st = completionStatus(rc, context.zrq->logid_);
    if (st == E::OK) {
      // If everything worked well, then each individual operation should've
      // went through fine as well
      for (const auto& res : results) {
        ld_check(completionStatus(res.rc_, context.zrq->logid_) == E::OK);
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
                       context = std::move(context)](
                          int rootrc, std::string rootpath) mutable {
          const auto rootst = completionStatus(rootrc, context.zrq->logid_);
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
            provisionLogZnodes(std::move(context), std::move(znode_value));
          } else {
            RATELIMIT_ERROR(std::chrono::seconds(10),
                            10,
                            "Unable to create root znode %s: ZK "
                            "error %d, LD error %s",
                            rootpath.c_str(),
                            rootrc,
                            error_description(rootst));
            postRequestCompletion(rootst, std::move(context));
          }
        };
        zkclient_->createWithAncestors(rootPath(), "", std::move(rootcb));
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
    postRequestCompletion(st, std::move(context));
  };

  zkclient_->multiOp(std::move(ops), std::move(cb));
}

void ZookeeperEpochStore::writeZnode(
    RequestContext&& context,
    std::string legacy_znode_value,
    zk::version_t legacy_znode_version,
    folly::Optional<zk::version_t> migration_znode_version) {
  if (context.settings.double_write && migration_znode_version.has_value()) {
    doubleWriteZnode(std::move(context),
                     std::move(legacy_znode_value),
                     legacy_znode_version,
                     *migration_znode_version);
  } else {
    legacyWriteZnode(std::move(context),
                     std::move(legacy_znode_value),
                     legacy_znode_version);
  }
}

void ZookeeperEpochStore::legacyWriteZnode(RequestContext&& context,
                                           std::string legacy_znode_value,
                                           zk::version_t legacy_znode_version) {
  std::string znode_path = context.zrq->getZnodePath(rootPath());
  // setData() below succeeds only if the current version number of
  // znode at znode_path matches the version that the znode had
  // when we read its value. Zookeeper atomically increments the version
  // number of znode on every write to that znode. If the versions do not
  // match zkSetCf() will be called with status ZBADVERSION. This ensures
  // that if our read-modify-write of znode_path succeeds, it was atomic.
  auto cb = [this, context = std::move(context)](int res, zk::Stat) mutable {
    auto logid = context.zrq->logid_;
    postRequestCompletion(completionStatus(res, logid), std::move(context));
  };
  zkclient_->setData(std::move(znode_path),
                     std::move(legacy_znode_value),
                     std::move(cb),
                     legacy_znode_version);
}

void ZookeeperEpochStore::doubleWriteZnode(
    RequestContext&& context,
    std::string legacy_znode_value,
    zk::version_t legacy_znode_version,
    zk::version_t migration_znode_version) {
  // For the double writes to succeed both the legacy and migration znode
  // shouldn't have changed since the read. We also need to make sure that
  // either both of them succeed or both of them fail.

  auto migration_znode_path = znodePathForLog(context.zrq->logid_);
  auto serialized_log_metadata =
      LogMetaDataCodec::serialize(context.log_metadata);

  auto legacy_znode_path = context.zrq->getZnodePath(rootPath());

  std::vector<zk::Op> ops{
      ZookeeperClientBase::makeSetOp(std::move(migration_znode_path),
                                     std::move(serialized_log_metadata),
                                     migration_znode_version),
      ZookeeperClientBase::makeSetOp(std::move(legacy_znode_path),
                                     std::move(legacy_znode_value),
                                     legacy_znode_version),
  };

  auto cb = [this, context = std::move(context)](
                int rc, std::vector<zk::OpResponse> /* results */) mutable {
    auto logid = context.zrq->logid_;
    postRequestCompletion(completionStatus(rc, logid), std::move(context));
  };
  zkclient_->multiOp(std::move(ops), std::move(cb));
}

void ZookeeperEpochStore::onGetZnodeComplete(
    RequestContext&& context,
    ZnodeReadResult legacy_znode,
    folly::Optional<ZnodeReadResult> migration_znode) {
  auto& zrq = context.zrq;
  auto& log_metadata = context.log_metadata;
  ZookeeperEpochStoreRequest::NextStep next_step;
  bool do_provision = false;
  ld_check(zrq);

  Status legacy_st = completionStatus(legacy_znode.zk_return_code, zrq->logid_);
  Status st = legacy_st;
  bool value_existed = (legacy_st == E::OK);
  if (legacy_st != E::OK && legacy_st != E::NOTFOUND) {
    goto err;
  }

  if (migration_znode.has_value()) {
    Status migration_st =
        completionStatus(migration_znode->zk_return_code, zrq->logid_);
    if (migration_st != E::OK && migration_st != E::NOTFOUND) {
      goto err;
    }

    // At this point, both legacy and migration znode statuses are either OK
    // or NOTFOUND. Given that we provision them in a transaction they either
    // both exist or don't. So their statuses must be equal.
    if (legacy_st != migration_st) {
      RATELIMIT_ERROR(
          std::chrono::seconds(1),
          1,
          "INTERNAL ERROR: expected status code of legacy znode (%s) to be "
          "equal to migration znode status (%s) but it's not the case "
          "for log %lu",
          error_name(legacy_st),
          error_name(migration_st),
          zrq->logid_.val_);
      st = E::INTERNAL;
      goto err;
    }

    if (migration_st == E::OK) {
      // If migration_znode->value is empty, it means that this log will be
      // migrated as part of this request. Only EpochMetaDataZRQ is allowed to
      // trigger a migration as we can't serialize a LogMetaData with an
      // invalid current_epoch_metadata.
      if (!migration_znode->value.empty()) {
        std::shared_ptr<const LogMetaData> deserialized_log_metadata =
            LogMetaDataCodec::deserialize(migration_znode->value);

        if (!deserialized_log_metadata) {
          RATELIMIT_ERROR(std::chrono::seconds(1),
                          1,
                          "Failed to deserialize log metadata for log %lu",
                          zrq->logid_.val_);
          st = E::BADMSG;
          goto err;
        }

        // This is a copy unfortunatly because the deserialization returns a
        // const object.
        log_metadata = *deserialized_log_metadata;
      } else if (!zrq->allowedToTriggerNewFormatMigration()) {
        // We're not allowed to trigger a migration as part of this request.
        // Let's disable double writing and act as if we didn't read this znode.
        migration_znode.reset();
        context.settings.double_write = false;
      } else {
        // Migration is going to happen as part of this request. Congrats!
      }
    }
  }

  if (value_existed) {
    st = zrq->legacyDeserializeIntoLogMetaData(
        std::move(legacy_znode.value), log_metadata);
    if (st != Status::OK) {
      goto err;
    }
  }

  next_step = zrq->applyChanges(log_metadata, value_existed);
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

  // Increment version and timestamp of log metadata.
  log_metadata.touch();

  { // Without this scope gcc throws a "goto cross initialization" error.
    // Using a switch() instead of a goto results in a similar error.
    // Using a single-iteration loop is confusing. Perphas we should start
    // using #define BEGIN_SCOPE { to avoid the extra indentation? --march

    char znode_value[ZNODE_VALUE_WRITE_LEN_MAX];
    int znode_value_size =
        zrq->composeZnodeValue(log_metadata, znode_value, sizeof(znode_value));
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
      provisionLogZnodes(std::move(context), std::move(znode_value_str));
      return;
    } else {
      writeZnode(std::move(context),
                 std::move(znode_value_str),
                 legacy_znode.stat.version_,
                 migration_znode.has_value()
                     ? migration_znode->stat.version_
                     : folly::Optional<zk::version_t>(folly::none));
      return;
    }
  }

err:
  ld_check(st != E::OK);

done:
  postRequestCompletion(st, std::move(context));
}

void ZookeeperEpochStore::postRequestCompletion(Status st,
                                                RequestContext&& context) {
  if (st != E::SHUTDOWN || !shutting_down_.load()) {
    context.zrq->postCompletion(
        st, std::move(context.log_metadata), request_executor_);
  } else {
    // Do not post a CompletionRequest if Zookeeper client is shutting down and
    // the EpochStore is being destroyed. Note that we can get also get an
    // E::SHUTDOWN code if the ZookeeperClient is being destroyed due to
    // zookeeper quorum change, but the EpochStore is still there.
  }
}

folly::SemiFuture<ZookeeperEpochStore::ZnodeReadResult>
ZookeeperEpochStore::readLegacyZnode(const RequestContext& context) {
  auto [promise, future] = folly::makePromiseContract<ZnodeReadResult>();
  std::string legacy_znode_path = context.zrq->getZnodePath(rootPath());
  auto legacy_znode_cb = [p = std::move(promise)](
                             int rc, std::string value, zk::Stat stat) mutable {
    p.setValue(ZnodeReadResult{rc, std::move(value), std::move(stat)});
  };
  zkclient_->getData(legacy_znode_path, std::move(legacy_znode_cb));
  return std::move(future);
}

folly::SemiFuture<folly::Optional<ZookeeperEpochStore::ZnodeReadResult>>
ZookeeperEpochStore::readMigrationZnode(const RequestContext& context) {
  // The read Request for the new znode only if double writing is enabled in
  // settings. If it's not enabled, we bypass the read completely.
  auto [promise, future] =
      folly::makePromiseContract<folly::Optional<ZnodeReadResult>>();
  if (context.settings.double_write) {
    std::string new_znode_path = znodePathForLog(context.zrq->logid_);
    auto new_znode_cb = [p = std::move(promise)](
                            int rc, std::string value, zk::Stat stat) mutable {
      p.setValue(ZnodeReadResult{rc, std::move(value), std::move(stat)});
    };
    zkclient_->getData(new_znode_path, std::move(new_znode_cb));
  } else {
    promise.setValue(folly::none);
  }
  return std::move(future);
}

int ZookeeperEpochStore::runRequest(
    std::unique_ptr<ZookeeperEpochStoreRequest> zrq) {
  ld_check(zrq);
  RequestContext context{
      std::move(zrq),
      LogMetaData(),
      RequestSettings{
          /* double_write */ settings_
              ->epoch_store_double_write_new_serialization_format,
      },
  };
  auto legacy_znode_fut = readLegacyZnode(context);
  auto migration_znode_fut = readMigrationZnode(context);

  folly::collectAll(legacy_znode_fut, migration_znode_fut)
      .toUnsafeFuture()
      .thenValue([this, context = std::move(context)](
                     std::tuple<folly::Try<ZnodeReadResult>,
                                folly::Try<folly::Optional<ZnodeReadResult>>>
                         results) mutable {
        // We don't throw exceptions so it should be safe to access the Try
        // value directly.
        ld_check(std::get<0>(results).hasValue());
        ld_check(std::get<1>(results).hasValue());
        onGetZnodeComplete(std::move(context),
                           std::move(std::get<0>(results)).value(),
                           std::move(std::get<1>(results)).value());
      });

  return 0;
}

int ZookeeperEpochStore::getLastCleanEpoch(logid_t logid, CompletionLCE cf) {
  return runRequest(std::unique_ptr<ZookeeperEpochStoreRequest>(
      new GetLastCleanEpochZRQ(logid, cf)));
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
      new SetLastCleanEpochZRQ(logid, lce, tail_record, cf)));
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
                           cf,
                           std::move(updater),
                           std::move(tracer),
                           write_node_id,
                           nodes_configuration_->get(),
                           my_node_id_)));
}

}} // namespace facebook::logdevice
