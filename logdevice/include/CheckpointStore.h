/**
 * Copyright (c) 2019-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <map>

#include <folly/Function.h>

#include "logdevice/include/Err.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {
/*
 * @file  CheckpointStore will be used by the checkpointing library to store
 *        the last read LSNs (called 'checkpoints') of the logs.
 *        This is a key-value storage, where the key is the pair
 *        (customer_id, log_id) and the value is the LSN number.
 */
class CheckpointStore {
 public:
  using Version = vcs_config_version_t;
  using GetCallback = folly::Function<void(Status, lsn_t)>;
  using UpdateCallback = folly::Function<void(Status, Version, std::string)>;

  /**
   * Destructor must be virtual in order to work correctly.
   */
  virtual ~CheckpointStore() = default;

  /*
   * GetLSN gets the last written checkpoint for certain customer, log pair.
   *
   * @param customer_id: the id of the customer, which gets the LSN.
   * @param log_id:
   *   the id of the log, for which LSN will be read. Along with
   *   customer_id it creates a key.
   * @param cb: similar to cb parameter of getConfig function in
   *   VersionedConfigStore class but takes the resulting LSN as a parameter
   *   instead of a string value.
   */
  virtual void getLSN(const std::string& customer_id,
                      logid_t log_id,
                      GetCallback cb) const = 0;

  /*
   * Synchronous getLSN
   *
   * See params for getLSN()
   *
   * @return status: see the getConfigSync return value in
   *   VersionedConfigStore class, as these are equivalent.
   */
  virtual Status getLSNSync(const std::string& customer_id,
                            logid_t log_id,
                            lsn_t* value_out) const = 0;

  /*
   * Synchronous updateLSN
   *
   * See params for updateLSN()
   *
   * @return status: see the updateConfigSync return value in
   *   VersionedConfigStore class, as these are equivalent.
   */
  virtual Status updateLSNSync(const std::string& customer_id,
                               logid_t log_id,
                               lsn_t lsn) = 0;

  /*
   * Synchronous updateLSN for many logs at once.
   *
   * See params for updateLSN() for many logs.
   *
   * @return status: see the updateConfigSync return value in
   *   VersionedConfigStore class, as these are equivalent.
   */
  virtual Status updateLSNSync(const std::string& customer_id,
                               const std::map<logid_t, lsn_t>& checkpoints) = 0;

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
   *   VersionedConfigStore class.
   */
  virtual void updateLSN(const std::string& customer_id,
                         logid_t log_id,
                         lsn_t lsn,
                         UpdateCallback cb) = 0;

  /*
   * UpdateLSN does asynchronous update of the LSNs for many logs.
   *
   * @param customer_id: see the customer_id parameter of updateLSN for a single
   *   log.
   * @param checkpoints: a map log_id -> lsn. For each log in this map,
   *   the new LSN will be updated.
   * @param cb: see the cb parameter of updateConfig function in
   *   VersionedConfigStore class.
   */
  virtual void updateLSN(const std::string& customer_id,
                         const std::map<logid_t, lsn_t>& checkpoints,
                         UpdateCallback cb) = 0;

  /*
   * RemoveCheckpoints removes given checkpoints asynchronously.
   *
   * @param customer_id: the id of the customer, which removes the checkpoints.
   * @param checkpoints: a list of logs to remove.
   * @param cb: see the cb parameter of updateConfig function in
   *   VersionedConfigStore class.
   */
  virtual void removeCheckpoints(const std::string& customer_id,
                                 const std::vector<logid_t>& checkpoints,
                                 UpdateCallback cb) = 0;

  /*
   * RemoveAllCheckpoints removes all checkpoints asynchronously.
   *
   * @param customer_id: the id of the customer, which removes the checkpoints.
   * @param cb: see the cb parameter of updateConfig function in
   *   VersionedConfigStore class.
   */
  virtual void removeAllCheckpoints(const std::string& customer_id,
                                    UpdateCallback cb) = 0;

  /*
   * RemoveCheckpointsSync removes given checkpoints synchronously.
   *
   * @param customer_id: the id of the customer, which removes the checkpoints.
   * @param checkpoints: a list of logs to remove.
   * @return status: see the updateConfigSync return value in
   *   VersionedConfigStore class.
   */
  virtual Status
  removeCheckpointsSync(const std::string& customer_id,
                        const std::vector<logid_t>& checkpoints) = 0;

  /*
   * RemoveAllCheckpointsSync removes all checkpoints synchronously.
   *
   * @param customer_id: the id of the customer, which removes the checkpoints.
   * @return status: see the updateConfigSync return value in
   *   VersionedConfigStore class.
   */
  virtual Status removeAllCheckpointsSync(const std::string& customer_id) = 0;
};

}} // namespace facebook::logdevice
