/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <functional>
#include <string>

#include "logdevice/common/CompletionRequest.h"
#include "logdevice/common/EpochMetaData.h"
#include "logdevice/common/MetaDataTracer.h"
#include "logdevice/common/TailRecord.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/LogTailAttributes.h"

/**
 * @file  EpochStore is an abstract class that defines the interface of an
 *        epoch store client. An epoch store is a highly reliable and available
 *        data store that contains epoch-related metadata for all the logs
 *        provisioned on this logdevice cluster.
 */

namespace facebook { namespace logdevice {

/**
 * Struct that can get passed to completion functions with some meta
 * information about the data that was read from the EpochStore. Fields may
 * or may not be filled depending on the EpochStore implementation.
 */
struct EpochStoreMetaProperties {
  folly::Optional<NodeID> last_writer_node_id;
};

class EpochStore {
 public:
  using MetaProperties = EpochStoreMetaProperties;
  enum class WriteNodeID { NO = 0, KEEP_LAST, MY };

  /**
   * Completion functions are the types of callback that EpochStore calls when
   * an asynchronous request to the underlying epoch store completes. If the
   * request for which the completion is being called was initiated by a
   * Worker thread, the completion callback is guaranteed to be called
   * on the same Worker. Otherwise the completion callback will be called
   * on an unspecified Worker.
   *
   * The status of the callbacks can be following:
   *                  E::ACCESS      epoch store service denied access
   *                  E::AGAIN       some other logdeviced simultaneously tried
   *                                 to advance next epoch and we lost the race
   *                  E::BADMSG      epoch store record for log has invalid
   *                                 or unsupported format
   *                  E::CONNFAILED  lost connection to epoch store service
   *                                 while waiting for reply, possibly due to
   *                                 timeout
   *                  E::DISABLED    this log is marked as disabled in ts epoch
   *                                 metadata.
   *                                 fully provisioned)
   *                  E::EMPTY       epoch store entry exists but its value is
   *                                 empty (not fully provisioned)
   *
   *                  E::EXISTS      a valid epoch store entry already exists
   *
   *                  E::INTERNAL    an assertion failed and we are in release
   *                                 mode
   *                  E::NOTFOUND    logid is not known to the epoch store or
   *                                 is not correctly provisioned
   *                  E::OK          request completed successfully
   *                  E::STALE       the request was setLastCleanEpoch() and
   *                                 the epoch number was not greater than the
   *                                 one in the EpochStore record for this log
   *                  E::TOOBIG      the request was createOrUpdateMetaData()
   *                                 and the updater returned this error code.
   *                  E::UPTODATE    the request was createOrUpdateMetaData() or
   *                                 and the value in EpochStore is already the
   *                                 latest value, no need to update it.
   */

  /**
   * Completion function for last clean epoch EpochStore request.
   * @param logid             id of the log whose epoch metadata was queried or
   *                          updated
   *
   * @param epoch             epoch value, see methods below for details
   *
   * @param tail_record       the last released record of the log, along with
   *                          its attributes such as timestamp and byte offset
   *
   * @return void
   *
   * Status of request set as err. See docblock above.
   */
  using CompletionLCERequest =
      logdevice::CompletionRequestBase<std::function,
                                       const logid_t, // logid
                                       epoch_t,       // epoch
                                       TailRecord>;   // tail_record
  using CompletionLCE = CompletionLCERequest::CompletionFunction;

  /**
   * Completion function for last clean epoch EpochStore request.
   * @param logid             id of the log whose epoch metadata was queried or
   *                          updated
   *
   * @param EpochMetaData     epoch metadata information for the log.
   *
   * @return void
   *
   * Status of request set as err. See docblock above.
   */
  using CompletionMetaDataRequest = logdevice::CompletionRequestBase<
      std::function,
      const logid_t,                    // logid
      std::unique_ptr<EpochMetaData>,   // metadata
      std::unique_ptr<MetaProperties>>; // meta properties
  using CompletionMetaData = CompletionMetaDataRequest::CompletionFunction;
  static_assert(std::is_same<CompletionMetaData,
                             EpochMetaData::Updater::CompletionFunction>::value,
                "CompletionFunction type mismatch");

  virtual ~EpochStore() {}

  /**
   * Get the value of last clean epoch for log identified by
   * logid. This is the numerically highest epoch number of all epochs
   * that are known to be "clean", that is have no active sequencers writing
   * records in those epochs, and have no log irregularities due to sequencer
   * failures.
   *
   * @return   will return 0 on success, On failure, will return -1 and set err
   *           to one of E::INTERNAL, E::NOTCONN, E::ACCESS, E::SYSLIMIT,
   *           E::INVALID_PARAM, E::NOTFOUND, E::FAILED
   */
  virtual int getLastCleanEpoch(logid_t logid, CompletionLCE cf) = 0;

  /**
   * Increase the value of last clean epoch (LCE) stored in the epoch
   * store for log _logid_ to _lce_. If the epoch store already has a
   * higher LCE value for the log, no change is made and the higher value
   * is reported through the _epoch_ argument of cf with E::STALE passed to
   * cf as the status. Otherwise the _epoch_ argument of cf will be @param lce.
   * See getLastCleanEpoch() above for the definition of a "clean" epoch.
   * In addition, the approximate amount of bytes ever stored to the log up to
   * the the last record of the lce get updated.
   * This two values are getting updated and read together at the same time
   * during sequencer recovery.
   *
   * Along with lce, log tail attributes get updated by the given tail_record.
   * See LogTailAttributes class docblock.
   *
   * @param tail_record   the tail record of the log until the end of epoch
   *                      of lce. This is provided by recovery when it finishes
   *                      recovery LCE and determines the tail that is
   *                      immutable.
   *
   * @return   the same as for getLastCleanEpoch() above, same err codes
   */
  virtual int setLastCleanEpoch(logid_t logid,
                                epoch_t lce,
                                const TailRecord& tail_record,
                                CompletionLCE cf) = 0;

  /**
   * Fetch the most recent EpochMetaData object for the logid.
   * The `epoch` field of the result contains the next epoch,
   * not the current one.
   *
   * @return   the same as for getLastCleanEpoch() above, same err codes
   */
  int readMetaData(logid_t logid, CompletionMetaData cf);

  /**
   * Fetch the most recent EpochMetaData object for the logid,
   * call *updater on the object (or on nullptr if no object exists).
   * The updater may:
   * 1) perform update on the metadata object, or
   * 2) provision a new metadata object if none exists, or
   * 3) decide nothing needs to be changed, or
   * 4) encounter an error.
   * For 1), epochstore will commit the updated metadata to the epochstore, and
   * call cf with E::OK. For 2), epochstore will create the structures necessary
   * (e.g. metadata directories and files) for the log, and provision the log
   * with the generated metadata. For 3), epochstore will not do any update and
   * call cf with E::UPTODATE. For 4), epochstore will call cf with the err
   * returned by updater.
   *
   * @param write_node_id       Defines whether a NodeID of the active sequencer
   *                            node will be written into the epoch store
   *                            alongside the metadata. Can have the following
   *                            values:
   *
   *    WriteNodeID::MY         the writer's node ID will be written alongside
   *                            the metadata. This value should only be used if
   *                            the operation is done by a sequencer node.
   *
   *    WriteNodeID::KEEP_LAST  Keeps the same value of the NodeID as the one
   *                            that was read from the epoch store before
   *                            modification
   *
   *    WriteNodeID::NO         Doesn't write a NodeID to epoch store - if one
   *                            was in the epoch store before, it will be wiped
   *                            if the metadata is updated by the `updater`.
   *
   * @return   the same as for getLastCleanEpoch() above, same err codes
   */
  virtual int
  createOrUpdateMetaData(logid_t logid,
                         std::shared_ptr<EpochMetaData::Updater> updater,
                         CompletionMetaData cf,
                         MetaDataTracer tracer,
                         WriteNodeID write_node_id = WriteNodeID::NO) = 0;

  /**
   * @return  a string, such as a list of ip:ports, identifying the epoch
   *          store service this object talks to. Used in error messages.
   */
  virtual std::string identify() const = 0;
};

}} // namespace facebook::logdevice
