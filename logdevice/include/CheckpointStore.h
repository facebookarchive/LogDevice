/**
 * Copyright (c) 2019-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/Function.h>

#include "logdevice/include/Err.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {
/*
 * @file  CheckpointStore will be used by the checkpointing library to store
 *        the last read LSNs (called 'checkpoints') of the logs.
 *        This is a key-value storage, where the key is the pair
 *        (customer_id, log_id) and the value is the LSN number.
 *        TODO(T53779383): Change from map((customerID, logID) -> LSN) to
 *        map(customerID -> map(logID -> LSN)).
 */
class CheckpointStore {
 public:
  using Version = vcs_config_version_t;
  using UpdateCallback = folly::Function<void(Status, Version, std::string)>;

  /**
   * Destructor must be virtual in order to work correctly.
   */
  virtual ~CheckpointStore() = default;

  /*
   * Synchronous updateLSN
   *
   * See params for updateLSN()
   *
   * @return status: see the updateConfigSync return value in
   * VersionedConfigStore class, as these are equivalent.
   */
  virtual Status updateLSNSync(const std::string& customer_id,
                               logid_t log_id,
                               lsn_t lsn) = 0;
  /*
   * UpdateLSN does asynchronous update of the LSN for the given log and the
   * customer.
   *
   * @param customer_id: the id of the customer, which updates the LSN.
   * @param log_id:
   *   the id of the log, for which LSN will be updated. Along with
   *   customer_id it creates a key.
   * @param lsn: the new LSN value to be stored
   * @param cb: see the cb parameter of updateConfig function in
   * VersionedConfigStore class.
   *
   */
  virtual void updateLSN(const std::string& customer_id,
                         logid_t log_id,
                         lsn_t lsn,
                         UpdateCallback cb) = 0;
};

}} // namespace facebook::logdevice
