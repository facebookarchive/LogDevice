/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>
#include <unordered_map>

#include <folly/Optional.h>

#include "logdevice/common/types_internal.h"
#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice {

/**
 * @file  A state machine for writing a single record to a metadata log. Running
 *        on a single Worker thread on which it was created. Internally it wraps
 *        a special sequencer for metadata logs. The meta sequencer is created
 *        on-demand and unlike data sequencers, it gets destroyed once the
 *        following conditions are met:
 *        (1) the Appender finishes storing the record, retires, and gets reaped
 *        (2) meta sequencer finishes recovery
 *        (3) RELEASES for the record has successfully sent out to f-majority
 *            of nodes in the metadata nodeset.
 *
 *        Noted that WriteMetaDataRecord can also be started without an
 *        appender, in such case it is considered as "recovery only": it spawns
 *        a meta sequencer only for recovering the metadata log.
 */

class Appender;
enum class RunAppenderStatus;
class MetaDataLogWriter;
class Sequencer;
enum class ReleaseType : uint8_t;

class WriteMetaDataRecord {
 public:
  /**
   * Create the state machine
   *
   * @param log_id    log id of the data log
   * @param epoch     epoch used to write the metadata record
   * @param driver    parent MetaDataLogWriter instance
   */
  WriteMetaDataRecord(logid_t log_id, epoch_t epoch, MetaDataLogWriter* driver);

  /**
   * Start the state machine with an appender
   *
   * @param appender  appender to run. if nullptr, the state machine is used for
   *                  recovery only
   */
  RunAppenderStatus start(Appender* appender);

  /**
   * Called when the appender is retired from the sequencer's sliding window.
   * The appender is guranteed to be reaped as well since there is always at
   * most one appender in flight.
   *
   * @param st   Status of the retired appender
   * @param lsn  lsn of the retired appender
   */
  void onAppenderRetired(Status st, lsn_t lsn);

  /**
   * Called when a release message was successfully sent to a storage node
   */
  void onReleaseSentSuccessful(node_index_t node,
                               lsn_t released_lsn,
                               ReleaseType release_type);

  /**
   * Called when the metadata log sequencer has completed recovery
   * for an epoch
   */
  void onRecoveryCompleted(Status status, epoch_t epoch);

  /**
   * Called when server config has been changed.
   */
  void noteConfigurationChanged();

  /**
   * Cancel the current log recovery process and destroy the state machine
   * can only be called if the state machine is for recovery only
   */
  void abort();

  epoch_t getEpoch() const {
    return epoch_;
  }

  bool isRecoveryOnly() const {
    return recovery_only_;
  }

  MetaDataLogWriter* getDriver() const {
    return driver_;
  }

  void notePreempted(epoch_t epoch, NodeID preempted_by);

  // expose the managed metadata sequencer, must be called on
  // the same thread on which the state machine runs
  std::shared_ptr<Sequencer> getMetaSequencer() const;

  ~WriteMetaDataRecord();

 private:
  enum class State : uint8_t {
    READY = 0, // READY to accept a new metadata log write
    STARTED,   // a metadata record appender is in progress
    STORED,    // metadata record is fully stored, waiting for
               // releases to be sent
  };

  const logid_t log_id_;
  const epoch_t epoch_;
  State state_;

  // target lsn which is used to determine if a release confirmation belongs
  // to the current state machine instance. in specific,
  // if the state machine is used to run an appender, then this is the
  // lsn of the running appender. Otherwise, it is the lng for the new epoch
  // (i.e., compose_lsn(epoch_, 0)).
  lsn_t target_lsn_{LSN_INVALID};

  // indicates that this state machine instance is used for recovering
  // metadata log only and does not run an appender
  bool recovery_only_{false};

  // if set, indicates that recovery has completed with the provided status
  folly::Optional<Status> recovery_status_;

  // track the nodes to which we send releases
  std::unordered_map<node_index_t, bool> release_sent_;
  nodeset_size_t num_releases_sent_{0};

  // the internal metadata log sequencer for storing metadata records
  std::shared_ptr<Sequencer> meta_seq_;

  // parent MetaDataLogWriter object
  MetaDataLogWriter* const driver_;

  worker_id_t created_on_{-1};

  // create a metadata sequencer and activate it to epoch_
  int createAndActivate(bool bypass_recovery);

  // abort log recovery on the worker
  void abortRecovery();

  // finalize the state machine by notifying parent to destroy `this'
  void finalize(Status st);
};

// Wrapper instead of typedef to allow forward-declaring in Worker.h
struct WriteMetaDataRecordMap {
  using RawMap =
      std::unordered_map<logid_t, WriteMetaDataRecord*, logid_t::Hash>;
  RawMap map;

  // propagate config changes to all state machine managed
  void noteConfigurationChanged();
};

}} // namespace facebook::logdevice
