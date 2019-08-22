/**
 * Copyright (c) 2018-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/ZookeeperVersionedConfigStore.h"

#include <chrono>

#include <folly/synchronization/Baton.h>

#include "logdevice/common/debug.h"
#include "logdevice/common/util.h"
#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice {

//////// ZookeeperVersionedConfigStore ////////

void ZookeeperVersionedConfigStore::getConfig(
    std::string key,
    value_callback_t callback,
    folly::Optional<version_t> base_version) const {
  auto locked_ptr = shutdown_completed_.tryRLock();
  if (shutdownSignaled()) {
    callback(E::SHUTDOWN, "");
    return;
  }
  // If shutdown was not signaled, we should not have failed to acquire the lock
  // and shutdown_completed should be false.
  ld_assert(locked_ptr && !*locked_ptr);

  ZookeeperClientBase::data_callback_t completion =
      [this, cb = std::move(callback), base_version, key](
          int rc, std::string value, zk::Stat) mutable {
        Status status = ZookeeperClientBase::toStatus(rc);
        if (status != Status::OK) {
          cb(status, "");
          return;
        }

        if (base_version.hasValue()) {
          auto current_version_opt = extract_fn_(value);
          if (!current_version_opt) {
            RATELIMIT_WARNING(
                std::chrono::seconds(10),
                5,
                "Failed to extract version from value read from "
                "ZookeeperVersionedConfigurationStore. key: \"%s\"",
                key.c_str());
            cb(Status::BADMSG, "");
            return;
          }
          if (current_version_opt.value() <= base_version.value()) {
            // zk's config version is not larger than the base version
            cb(Status::UPTODATE, "");
            return;
          }
        }

        cb(Status::OK, std::move(value));
      };
  zk_->getData(std::move(key), std::move(completion));
}

void ZookeeperVersionedConfigStore::getLatestConfig(
    std::string key,
    value_callback_t callback) const {
  auto locked_ptr = shutdown_completed_.tryRLock();
  if (shutdownSignaled()) {
    callback(E::SHUTDOWN, "");
    return;
  }
  ld_assert(locked_ptr && !*locked_ptr);

  auto sync_cb = [this, cb = std::move(callback), key = std::move(key)](
                     int sync_rc) mutable {
    auto locked_p = this->shutdown_completed_.tryRLock();
    // (1) try acquiring rlock failed || (2) shutdown_completed == true
    if (!locked_p || *locked_p) {
      // (2) should not happen based on our assumption of ZK dtor behavior.
      ld_assert(!*locked_p);
      ld_assert(this->shutdownSignaled());
      cb(E::SHUTDOWN, "");
      return;
    }

    if (sync_rc != ZOK) {
      cb(ZookeeperClientBase::toStatus(sync_rc), "");
      return;
    }
    auto read_cb = [cb = std::move(cb)](
                       int read_rc, std::string value, zk::Stat) mutable {
      Status status = ZookeeperClientBase::toStatus(read_rc);
      cb(status, status == Status::OK ? std::move(value) : "");
    };
    // TODO: we must ensure the ZK session for the sync and that for the read
    // remain the same, i.e., if the zk client reconnects in between, we should
    // error out and retry the operation.
    this->zk_->getData(std::move(key), std::move(read_cb));
  };
  zk_->sync(std::move(sync_cb));
}

void ZookeeperVersionedConfigStore::readModifyWriteConfig(
    std::string key,
    mutation_callback_t mcb,
    write_callback_t cb) {
  auto locked_ptr = shutdown_completed_.tryRLock();
  if (shutdownSignaled()) {
    cb(E::SHUTDOWN, version_t{}, "");
    return;
  }
  ld_assert(locked_ptr && !*locked_ptr);

  // naive implementation of read-modify-write
  ZookeeperClientBase::data_callback_t read_cb =
      [this,
       key,
       mutation_callback = std::move(mcb),
       write_callback = std::move(cb)](
          int rc, std::string current_value, zk::Stat zk_stat) mutable {
        auto locked_p = this->shutdown_completed_.tryRLock();
        // (1) try acquiring rlock failed || (2) shutdown_completed == true
        if (!locked_p || *locked_p) {
          // (2) should not happen based on our assumption of ZK dtor behavior.
          ld_assert(!*locked_p);
          ld_assert(this->shutdownSignaled());
          write_callback(E::SHUTDOWN, version_t{}, "");
          return;
        }

        if (rc != ZNONODE && rc != ZOK) {
          write_callback(ZookeeperClientBase::toStatus(rc), version_t{}, "");
          return;
        }
        folly::Optional<version_t> cur_opt = folly::none;
        if (rc != ZNONODE) {
          cur_opt = extract_fn_(current_value);
          if (!cur_opt) {
            RATELIMIT_WARNING(
                std::chrono::seconds(10),
                5,
                "Failed to extract version from value read from "
                "ZookeeperVersionedConfigurationStore. key: \"%s\"",
                key.c_str());
          }
        }
        auto status_value = mutation_callback(
            (rc == ZNONODE)
                ? folly::none
                : folly::Optional<std::string>(std::move(current_value)));
        auto& write_value = status_value.second;

        if (status_value.first != E::OK) {
          write_callback(
              status_value.first, version_t{}, std::move(write_value));
          return;
        }

        folly::Optional<version_t> opt = folly::none;
        opt = extract_fn_(write_value);
        if (!opt) {
          RATELIMIT_WARNING(std::chrono::seconds(10),
                            5,
                            "Failed to extract version from value provided for "
                            " key: \"%s\"",
                            key.c_str());
          err = E::INVALID_PARAM;
          write_callback(E::INVALID_PARAM, version_t{}, "");
          return;
        }
        version_t new_version = opt.value();

        if (rc != ZNONODE) {
          if (cur_opt) {
            version_t cur_version = cur_opt.value();
            if (new_version.val() <= cur_version.val()) {
              // TODO: Add stricter enforcement of monotonic increment of
              // version.
              RATELIMIT_WARNING(
                  std::chrono::seconds(10),
                  5,
                  "Config value's version is not monitonically increasing"
                  "key: \"%s\". prev version: \"%lu\". version: \"%lu\"",
                  key.c_str(),
                  cur_version.val(),
                  new_version.val());
            }
          }

          ZookeeperClientBase::stat_callback_t completion =
              [new_version, write_callback = std::move(write_callback)](
                  int write_rc, zk::Stat) mutable {
                // In case of a racing write, if we get VERSION_MISMATCH
                // here, we don't have the version or value that prevented the
                // update.
                write_callback(ZookeeperClientBase::toStatus(write_rc),
                               write_rc == ZOK ? new_version : version_t{},
                               "");
              };
          this->zk_->setData(std::move(key),
                             std::move(write_value),
                             std::move(completion),
                             zk_stat.version_);
        } else {
          // For the createWithAncestors recipe, we must keep zk_ alive until
          // create_callback is called / destroyed. We do so by capturing the
          // SharedLockedPtr (i.e., the shared lock) by value in the callback.
          auto keep_alive = this->shutdown_completed_.tryRLock();
          ld_assert(keep_alive);
          ld_assert(!*keep_alive);
          // overwrite() when znode does not exist should rarely happen, so
          // ld_info is OK here.
          ld_info("Creating znode %s with NC version %lu",
                  key.c_str(),
                  new_version.val());
          this->zk_->createWithAncestors(
              std::move(key),
              std::move(write_value),
              [keep_alive = std::move(keep_alive),
               new_version,
               write_callback = std::move(write_callback)](
                  int write_rc, std::string /* unused znode_path */) mutable {
                // In case of a racing write, if we get VERSION_MISMATCH
                // here, we don't have the version or value that prevented the
                // update.
                write_callback(ZookeeperClientBase::toStatus(write_rc),
                               write_rc == ZOK ? new_version : version_t{},
                               "");
              });
        }
      }; // read_cb

  zk_->getData(std::move(key), std::move(read_cb));
}

void ZookeeperVersionedConfigStore::shutdown() {
  shutdown_signaled_.store(true);
  {
    // acquire wlock which will wait for all readers (i.e., in-flight callbacks)
    // to finish, essentially the "join" for ZK-VCS.
    shutdown_completed_.withWLock([this](bool& completed) {
      // let go of the ZookeeperClient instance
      this->zk_ = nullptr;
      completed = true;
    });
  }
}

bool ZookeeperVersionedConfigStore::shutdownSignaled() const {
  return shutdown_signaled_.load();
}

}} // namespace facebook::logdevice
