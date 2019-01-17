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

void ZookeeperVersionedConfigStore::getConfig(std::string key,
                                              value_callback_t callback) const {
  ZookeeperClientBase::data_callback_t completion =
      [cb = std::move(callback)](int rc, std::string value, zk::Stat) mutable {
        Status status = ZookeeperClientBase::toStatus(rc);
        cb(status, status == Status::OK ? std::move(value) : "");
      };
  zk_->getData(std::move(key), std::move(completion));
}

void ZookeeperVersionedConfigStore::updateConfig(
    std::string key,
    std::string value,
    folly::Optional<version_t> base_version,
    write_callback_t callback) {
  auto opt = (*extract_fn_)(value);
  if (!opt) {
    err = E::INVALID_PARAM;
    callback(E::INVALID_PARAM, version_t{}, "");
    return;
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
                            "ZookeeperNodesConfigurationStore. key: \"%s\"",
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
                (*cb_ptr)(write_status, version_t{}, "");
              }
            };
        zk->setData(std::move(key),
                    std::move(write_value),
                    std::move(completion),
                    zk_stat.version_);
      }; // read_cb

  zk_->getData(std::move(key), std::move(read_cb));
}

}} // namespace facebook::logdevice
