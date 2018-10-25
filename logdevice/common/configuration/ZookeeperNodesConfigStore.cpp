/**
 * Copyright (c) 2018-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/configuration/ZookeeperNodesConfigStore.h"

#include <chrono>

#include <folly/synchronization/Baton.h>

#include "logdevice/common/debug.h"
#include "logdevice/common/util.h"
#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice { namespace configuration {

//////// ZookeeperNodesConfigStore ////////

int ZookeeperNodesConfigStore::getConfig(std::string key,
                                         value_callback_t callback) const {
  ZookeeperClientBase::data_callback_t completion =
      [cb = std::move(callback)](int rc, std::string value, zk::Stat) mutable {
        Status status = ZookeeperClientBase::toStatus(rc);
        cb(status, status == Status::OK ? std::move(value) : "");
      };
  Status status = ZookeeperClientBase::toStatus(
      zk_->getData(std::move(key), std::move(completion)));
  if (status != Status::OK) {
    err = status;
    return -1;
  }
  return 0;
}

Status ZookeeperNodesConfigStore::getConfigSync(std::string key,
                                                std::string* value_out) const {
  folly::Baton<> b;
  Status ret_status = Status::OK;
  value_callback_t cb = [&b, &ret_status, value_out](
                            Status status, std::string value) {
    set_if_not_null(&ret_status, status);
    if (status == Status::OK) {
      set_if_not_null(value_out, std::move(value));
    }
    b.post();
  };

  int rc = getConfig(std::move(key), std::move(cb));
  if (rc != 0) {
    // err should have been set accordingly already
    ld_assert(err != E::OK);
    return err;
  }

  b.wait();
  return ret_status;
}

int ZookeeperNodesConfigStore::updateConfig(
    std::string key,
    std::string value,
    folly::Optional<version_t> base_version,
    write_callback_t callback) {
  auto opt = (*extract_fn_)(value);
  if (!opt) {
    err = E::INVALID_PARAM;
    return -1;
  }
  version_t new_version = opt.value();

  // naive implementation of read-modify-write
  ZookeeperClientBase::data_callback_t read_cb =
      [extract_fn = extract_fn_,
       zk = zk_,
       key,
       write_value = std::move(value),
       base_version,
       new_version,
       write_callback = std::move(callback)](
          int rc, std::string current_value, zk::Stat zk_stat) mutable {
        if (rc != ZOK) {
          // TODO: handle ZNONODE (create one);
          write_callback(ZookeeperClientBase::toStatus(rc), {}, "");
          return;
        }

        auto current_version_opt = (*extract_fn)(current_value);
        if (!current_version_opt) {
          RATELIMIT_WARNING(std::chrono::seconds(10),
                            5,
                            "Failed to extract version from value read from "
                            "ZookeeperNodesConfigStore. key: \"%s\"",
                            key.c_str());
          write_callback(Status::BADMSG, {}, "");
          return;
        }
        version_t current_version = current_version_opt.value();
        if (base_version.hasValue() && base_version != current_version) {
          // version conditional update failed, invoke the callback with the
          // version and value that are more recent
          write_callback(Status::VERSION_MISMATCH,
                         current_version,
                         std::move(current_value));
          return;
        }

        auto cb_ptr =
            std::make_shared<write_callback_t>(std::move(write_callback));
        ZookeeperClientBase::stat_callback_t completion =
            [new_version, cb_ptr](int write_rc, zk::Stat) mutable {
              Status write_status = ZookeeperClientBase::toStatus(write_rc);
              if (write_status == Status::OK) {
                (*cb_ptr)(write_status, new_version, "");
              } else {
                // TODO: in case of a racing write, if we get VERSION_MISMATCH
                // here, we don't have the version or value that prevented the
                // update.
                (*cb_ptr)(write_status, {}, "");
              }
            };
        int zk_rc = zk->setData(std::move(key),
                                std::move(write_value),
                                std::move(completion),
                                zk_stat.version_);
        Status write_status = ZookeeperClientBase::toStatus(zk_rc);
        if (write_status != Status::OK) {
          RATELIMIT_ERROR(
              std::chrono::seconds(10),
              5,
              "UpdateConfig async call failed with ZK error code: %d",
              zk_rc);

          // TRICKY: since this is a "synchronous" error (i.e., one that
          // prevents us from doing the RPC to ZK), we are not supposed to
          // invoke the write_callback. However, since we are in the read
          // callback already, there is no way of surfacing the error to the
          // caller except to invoke the write_callback. Hopefully this happens
          // rarely enough.
          (*cb_ptr)(write_status, {}, "");
        }
      }; // read_cb

  Status status = ZookeeperClientBase::toStatus(
      zk_->getData(std::move(key), std::move(read_cb)));
  if (status != Status::OK) {
    err = status;
    return -1;
  }
  return 0;
}

Status ZookeeperNodesConfigStore::updateConfigSync(
    std::string key,
    std::string value,
    folly::Optional<version_t> base_version,
    version_t* version_out,
    std::string* value_out) {
  folly::Baton<> b;
  Status ret_status = Status::OK;
  write_callback_t cb =
      [&b, &ret_status, version_out, value_out](
          Status status, version_t current_version, std::string current_value) {
        set_if_not_null(&ret_status, status);
        if (status == Status::OK || status == Status::VERSION_MISMATCH) {
          set_if_not_null(version_out, current_version);
          set_if_not_null(value_out, std::move(current_value));
        }
        b.post();
      };

  int rc = updateConfig(
      std::move(key), std::move(value), base_version, std::move(cb));
  if (rc != 0) {
    // err should have been set accordingly already
    ld_assert(err != E::OK);
    return err;
  }

  b.wait();
  return ret_status;
}

}}} // namespace facebook::logdevice::configuration
