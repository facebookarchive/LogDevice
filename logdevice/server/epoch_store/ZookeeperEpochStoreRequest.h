/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/server/epoch_store/LogMetaData.h"
#include "logdevice/server/epoch_store/ZookeeperEpochStore.h"

namespace facebook { namespace logdevice {

/**
 * @file a ZookeeperEpochStoreRequest represents an outstanding request to
 *       a ZookeeperEpochStore, along with a completion function to call
 *       once the request is finished. Each request executes either a plain
 *       get or a versioned read-modify-write on a single znode. The actual
 *       Zookeeper client API calls are done by ZookeeperEpochStore.
 *       A ZookeeperEpochStoreRequest provides serialization and
 *       deserialization of znode values, determines the znode path to operate
 *       on, and tells ZookeeperEpochStore whether to perform
 *       a read-modify-write, or just a read.
 */

class ZookeeperEpochStoreRequest {
 public:
  explicit ZookeeperEpochStoreRequest(logid_t logid);

  virtual ~ZookeeperEpochStoreRequest() {}

  /**
   * Post an completionRequest to the Worker on which this
   * ZookeeperEpochStoreRequest was constructed. If the object was not
   * constructed on a Worker thread, post the completion request to an
   * arbitrary Worker of the Processor known to store_.
   *
   * When executed, the CompletionRequest will call cf_lce_ or cf_meta_data_
   * with status and other arguments.
   *
   * @param st   set the status argument of the completion function to this
   */
  virtual void postCompletion(Status st,
                              LogMetaData&& log_metadata,
                              RequestExecutor& executor) = 0;

  /**
   * Returns the path to znode on which this request operates.
   */
  virtual std::string getZnodePath(const std::string& rootpath) const = 0;

  /**
   * return value type used in parseZnodeValue(), describe the next step
   * of action for the ZookeeperEpochStoreRequest, specifically:
   *    MODIFY: proceed the read-modify-write operation to update znode value
   *    STOP:   no need to update znode
   *    FAIL:   parsing znode value failed and the operation should abort
   */
  enum class NextStep : uint8_t { MODIFY = 0, PROVISION, STOP, FAILED };

  /**
   * Deserializes the passed znode value into a LogMetaData.
   *
   * @return The Status of the deserialization.
   */
  Status deserializeLogMetaData(std::string value,
                                LogMetaData& log_metadata) const;

  /**
   * Serializes the passed LogMetaData using the LogMetaDataCodec.
   */
  std::string serializeLogMetaData(const LogMetaData& log_metadata) const;

  /**
   * Given the legacy znode value, deserialize it into the provided LogMetaData
   * structure. Returns`Status::OK` on success or the corresponding error on
   * failure.
   * This function won't be called if the znode wasn't found.
   */
  virtual Status
  legacyDeserializeIntoLogMetaData(std::string value,
                                   LogMetaData& log_metadata) const = 0;

  /*
   * Executes the logic of the request itself applying the necessary
   * modifications for the `LogMetaData` structure if needed.
   *
   * @return NextStep::MODIFY if value has been parsed and accepted,
   *         and it needs to be updated. Request processing must continue
   *         the read-modify-write operation.
   *
   *         NextStep::PROVISION if znode does not exist, but this is a valid
   *         state for this type of request, and the log needs to be
   *         provisioned. Request processing must continue to create the
   *         relevant znodes.
   *
   *         NextStep::STOP if there is no need to update and the value
   *         parsed was valid. Processing is complete. err is set to UPTODATE.
   *
   *         NextStep::FAILED if the parsing failed and caller must stop
   *         processing the request and post a completion request. Sets err to:
   *           BADMSG  if value is invalid
   *           STALE   if value indicates that epoch is stale and must not
   *                   be stored
   *           TOOBIG  if the value is too big for this type of request
   */
  virtual NextStep applyChanges(LogMetaData& log_metadata,
                                bool value_existed) = 0;

  /**
   * Composes a string in @param buf of size @param size bytes in the format
   * expected by parseZnodeValue() for this request class. May modify epoch_.
   *
   * @return length of the resulting string, not including the null-terminator
   *         If this is >= @param size, the string was truncated.
   */
  virtual int composeZnodeValue(LogMetaData& log_metadata,
                                char* buf,
                                size_t size) const = 0;

  /**
   * Whether this request is allowed to be the first request that triggers
   * the migration to the new serialization format.
   */
  virtual bool allowedToTriggerNewFormatMigration() const = 0;

  // id of log on whose metadata this request operates, passed to cf_
  const logid_t logid_;

 protected:
  const worker_id_t worker_idx_; // id of Worker on which to execute cf_, or
                                 // -1 if cf_ can be executed on any Worker

  // Type of worker on which to execute cf_. This defines at which worker
  // pool worker lives.
  const WorkerType worker_type_;
};

}} // namespace facebook::logdevice
