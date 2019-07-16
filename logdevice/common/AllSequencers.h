/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>
#include <functional>
#include <memory>

#include <boost/iterator/iterator_facade.hpp>
#include <folly/SharedMutex.h>
#include <folly/container/F14Map.h>

#include "logdevice/common/EpochStore.h"
#include "logdevice/common/MetaDataLog.h"
#include "logdevice/common/Sequencer.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/settings/UpdateableSettings.h"
#include "logdevice/common/util.h"
#include "logdevice/include/ConfigSubscriptionHandle.h"

namespace facebook { namespace logdevice {

/**
 * @file AllSequencers manages a Processor-wide collection of Sequencer objects.
 *       Its primary responsibilities are:
 *         - managing look-up and on-demand creation of Sequencer objects for
 *           all logs in the config
 *         - orchestrating Sequencer activation workflow, epoch store
 *           interactions and post-activation tasks such as log recovery,
 *           flushing appender buffer, metadata log operations and background
 *           activation
 */

struct Settings;
class StatsHolder;
class Processor;

class AllSequencers {
 public:
  using SequencerMap = folly::F14FastMap<logid_t::raw_type,
                                         std::shared_ptr<Sequencer>,
                                         Hash64<logid_t::raw_type>>;

  // An object used to access the contents of the map of sequencers.
  class Accessor {
   public:
    // Iterator, along with begin() and end() methods, required to
    // support range based for loops on Accessor.
    class Iterator
        : public boost::iterator_facade<Iterator,
                                        Sequencer,
                                        boost::forward_traversal_tag> {
     public:
      explicit Iterator(SequencerMap::iterator iter) : iter_(iter) {}
      bool equal(const Iterator& rhs) const {
        return iter_ == rhs.iter_;
      }
      void increment() {
        ++iter_;
      }
      Sequencer& dereference() const {
        return *iter_->second;
      }

     private:
      SequencerMap::iterator iter_;
    };

    explicit Accessor(AllSequencers* owner);

    Iterator begin();
    Iterator end();

   private:
    AllSequencers* owner_;
    folly::SharedMutex::ReadHolder map_lock_;
  };

  /**
   * @param cluster_config  config used by the Processor that owns this object
   * @param settings        various settings used by the Processor
   */
  AllSequencers(Processor* processor,
                const std::shared_ptr<UpdateableConfig>& updateable_config,
                UpdateableSettings<Settings> settings);

  virtual ~AllSequencers() {}

  /**
   * If no sequencer for logid exists in the map, creates a new
   * Sequencer for logid in UNAVAILABLE state and registers it in the
   * map. If a Sequencer for logid already exists in the map, this call will
   * attempt to reactivate the Sequencer. The new Sequencer will be transitioned
   * to ACTIVATING until an async request to EpochStore initiated
   * by this function in order to get a new epoch number for the Sequencer
   * completes, then the Sequencer will transition to ACTIVE if successful, or
   * transition to the *original* state before activation on transient failures.
   *
   * Note: this call will reactivate a sequencer even if it is already in ACTIVE
   *       state.
   *
   * @param logid  id of the log for which we are creating a sequencer
   *
   * @param pred  Activation predicate of the Sequencer object, will be
   *              evaluated under the internal state mutex of the Sequencer.
   *              see Sequencer::startActivation()
   * @param reason
   *    A human-readable description of why the sequencer is being activated.
   * @param check_metadata_log_before_provisioning
   *              Verify metadata log being empty before provisioning the log
   *              to epoch store if not already there.
   *              Only to be overridden from activateAllSequencers.
   *
   * @param acceptable_activation_epoch
   *    If this is set, sequencer is only allowed to activate into the specified
   *    epoch. If `epoch` in epoch store is already higher than that, don't
   *    update epoch store and abort the activation.
   *    If the new epoch is different, completion function will be called with
   *    E::ABORTED as a transient error
   *
   * @param new_metadata
   *    If not null, update metadata to this value. If null, we'll ask nodeset
   *    selector instead.
   *
   *    Used by background jobs that reconfigure the epoch metadata
   *    (e.g., nodeset) of the log. `new_metadata` is the result of the
   *    reconfiguration. Currently the reconfiguration is coupled with Sequencer
   *    activation so that sequencer can 1) get the next epoch number;
   *    and 2) reconfigure the log epoch metadata to `new_metadata` in a single
   *    epoch store transaction.
   *    Note: must be used together with `acceptable_activation_epoch` to ensure
   *    the reconfiguration transaction is conditional based on the currently
   *    observed epoch metadata. new_metadata->epoch must be equal
   *    to acceptable_activation_epoch.
   *
   * @return  0 if the map now has a Sequencer object for logid in ACTIVATING
   *          state, otherwise -1 with err set to
   *
   *    NOTFOUND   logid is not present in the cluster config
   *    NOBUFS     maximum number of sequencers has been reached
   *
   *    INPROGRESS a sequencer for logid already exists and is in ACTIVATING
   *
   *    ABORTED    pred evaluted false during activation
   *
   *    FAILED     request to EpochStore failed, a retry is needed
   *
   *    TOOMANY    too many sequencer activations have been attempted recently
   *    SYSLIMIT   the existing sequencer for logid is in PERMANENT_ERROR state
   */
  int activateSequencer(
      logid_t logid,
      const std::string& reason,
      Sequencer::ActivationPred pred,
      folly::Optional<epoch_t> acceptable_activation_epoch = folly::none,
      bool check_metadata_log_before_provisioning = true,
      std::shared_ptr<EpochMetaData> new_metadata = nullptr);

  /**
   * Similar to calling activateSequencer() with unconditional predicate,
   * except it does not activate the sequencer if it is already in ACTIVE state.
   *
   * @param logid
   * @param check_metadata_log_before_provisioning
   * @return   same as activateSequencer except it return -1 with err set to
   *           EXISTS if sequencer is already in ACTIVE state
   */
  int activateSequencerIfNotActive(
      logid_t logid,
      const std::string& reason,
      bool check_metadata_log_before_provisioning = true);

  /**
   * Conditionally reactivate the sequencer into a new epoch.
   *
   * @param pred  predicate, evaluated under a lock, which determines whether
   *              to proceed with the reactivation or not
   * @param only_consecutive_epoch
   *    will reactivate only if a reactivated sequencer will end up being in
   *    epoch (e+1) where current sequencer's epoch is (e). If this check fails,
   *    calls completion function with E::ABORTED and sets the sequencer into
   *    a TRANSIENT_ERROR state
   * @return      0 on success (sequencer is in the ACTIVATING state),
   *              -1 otherwise, with err set to one of the error codes of
   *              activateSequencer() or
   *                 NOSEQUENCER   if sequencer does not exist in the map
   */
  int reactivateIf(logid_t logid,
                   const std::string& reason,
                   Sequencer::ActivationPred pred,
                   bool only_consecutive_epoch = false);

  /**
   * Reactivate a Sequencer for a new epoch. The same as calling reactivateIf()
   * with unconditional predicates.
   */
  int reactivateSequencer(logid_t logid, const std::string& reason);

  /**
   * Called when epoch metadata is gotten from epoch store for a previous
   * successful activateSequencer() call.
   *
   * On success moves the Sequencer for logid into ACTIVE state, assigns
   * it an epoch number, and change its metadata (nodeset, replication property)
   * to be consistent with the metadata got from the epochstore. Depending on
   * the outcome of the activation completion (see
   * Sequencer::completeActivationWithMetaData()), it may initiate log recovery
   * procedure after activation.
   *
   * On transient failure moves the Sequencer into the original state when
   * activateSequencer() was called and expects an upper layer to retry.
   *
   * On permanent error moves the Sequencer into PERMANENT_ERROR state.
   *
   * Both state transitions are contingent upon the Sequencer still being
   * in ACTIVATING state. If the Sequencer has moved on to another state,
   * it is left unchanged.
   */
  virtual void
  onEpochMetaDataFromEpochStore(Status st,
                                logid_t logid,
                                const std::string& activation_reason,
                                std::unique_ptr<EpochMetaData> info,
                                std::unique_ptr<EpochStoreMetaProperties>);

  /**
   * When the epoch store was found to be empty, the subsequent request to read
   * the metadata log will return its result to this function.
   *
   * If it's empty, starts a new attempt to bump the epoch in the epoch store,
   * this time allowing it to provision the log if it is empty.
   *
   * If an error was encountered when reading the metadata log, or metadata log
   * was found NOT empty, there is inconsistency between epoch store and
   * metadata log. In that case, fails the sequencer activation.
   */
  virtual void
  onMetadataLogEmptyCheckResult(Status st,
                                logid_t logid,
                                const std::string& activation_reason);

  /**
   * Fails an ongoing sequencer activation on behalf of
   * onEpochMetaDataFromEpochStore or onMetadataLogEmptyCheckResult;
   * see comments for the respective functions.
   */
  virtual void onActivationFailed(logid_t logid,
                                  Status st,
                                  Sequencer* seq,
                                  bool permanent);

  /**
   * Looks for the Sequencer for the given logid in the map. If the logid
   * requested is a metadata logid, looks for the meta sequencer for the logid
   * running on the worker thread, if any.
   *
   * @return   on success a pointer to Sequencer found is returned.
   *           If no Sequencer exists in the map at the time of call, nullptr is
   *           returned and err is set to NOSEQUENCER.
   */
  std::shared_ptr<Sequencer> findSequencer(logid_t logid);

  /**
   * epoch_store_ is initialized after the object is constructed because
   * AllSequencers map is constructed and owned by a Processor and the
   * EpochStore it uses may need a backpointer to that Processor.
   *
   * Also, constructing a ZookeeperEpochStore will require passing a bunch
   * of arguments through Processor and EpochStore constructors. That will
   * just clutter the code and is not needed at all on the client.
   */
  void setEpochStore(std::unique_ptr<EpochStore> epoch_store) {
    ld_check(!epoch_store_);
    epoch_store_ = std::move(epoch_store);
  }

  /**
   * Return a reference to the EpochStore object used by Sequencers. Must be
   * called after setEpochStore().
   */
  EpochStore& getEpochStore() const {
    ld_check(epoch_store_);
    return *epoch_store_;
  }

  /**
   * This method is used for testing only. It tries to activate sequencers
   * for all logs named in cluster_config_. The method blocks until all logs
   * transition out of UNAVAILABLE/ACTIVATING state, or timeout expires.
   *
   * @param  cfg      cluster config of this Processor
   * @param  timeout  unblock and report E::TIMEDOUT if some sequencers are
   *                  still in ACTIVATING/UNAVAILABLE after this
   *                  many milliseconds.
   * @return 0 on succes, -1 if we failed to initiate activation for one
   *         or more sequencers, or if timeout expired before initialization
   *         completed. Sets err to
   *
   *     TIMEDOUT if some sequencers were still in INITIALIZING after timeout
   *     AGAIN    if we could not start activation of some log because of a
   *              transient error
   *     NOBUFS   if the maximum number of logs has been exceeded
   *     INTERNAL if something went terribly wrong
   */
  int activateAllSequencers(std::chrono::milliseconds timeout,
                            const std::string& reason);

  /**
   * This method called after node has spent some time in isolation and
   * we decided to disable all sequencers.
   */
  void disableAllSequencersDueToIsolation();

  /**
   * Returns an accessor that allows iteration over all Sequencer objects.
   */
  Accessor accessAll();

  /**
   * Returns the list of all Sequencer objects.
   */
  std::vector<std::shared_ptr<Sequencer>> getAll();

  Processor* getProcessor() const {
    return processor_;
  }

  /**
   * Called when a sequencer has finished its activation, notify all
   * Workers to process their pending buffered Appenders based on the
   * activation status. If Status is E::OK, it dequeues each buffered
   * Appender from the internal queue and try to start Appenders with
   * Sequencer::runAppender(). For each Appender object, if runAppender()
   * is successful, ownership of the Appender will be released and managed
   * by its sequencer. Otherwise, the Appender object will be
   * destroyed.
   *
   * If Status is not E::OK, there is a permanent error with the Sequencer,
   * clear and destroy all appender items on the sequencer's queue.
   *
   * This function also notifies SequencerBackgroundActivator to potentially
   * schedule reactivation for other logs.
   *
   * Note: This function is also used for processing buffered metadata appends
   * when metadata recovery is finished.
   */
  virtual void notifyWorkerActivationCompletion(logid_t logid, Status st);

  // Called when we the epoch in epoch store is higher than expected
  // (see acceptable_activation_epoch in activateSequencer()).
  // It usually means that a sequencer was activated elsewhere, and our
  // sequencer should move into preempted state.
  void
  notePreemption(logid_t logid,
                 epoch_t preemption_epoch,
                 const EpochStore::MetaProperties* properties_in_epoch_store,
                 Sequencer* seq,
                 const char* context);

  Processor* getProcessor() {
    return processor_;
  }

  /**
   * Performs clean up work during worker and processor shutdown.
   */
  void shutdown();

 protected:
  //// override in tests

  // create a Sequencer object
  virtual std::shared_ptr<Sequencer>
  createSequencer(logid_t logid, UpdateableSettings<Settings> settings);

  // get the metadata sequencer object, which is not stored in the sequencer map
  virtual std::shared_ptr<Sequencer>
  getMetaDataLogSequencer(logid_t datalog_id);

  // inform MetaDatLogWriter on successful activation to perform metadata log
  // related operations.
  virtual void notifyMetaDataLogWriterOnActivation(Sequencer* seq,
                                                   epoch_t epoch,
                                                   bool bypass_recovery);
  /**
   * Request epoch metadata from the epoch store for sequencer activation.
   *
   * @return   0 for success, -1 for failure
   */
  virtual int getEpochMetaData(
      logid_t logid,
      const std::string& activation_reason,
      std::shared_ptr<Configuration> cfg,
      std::shared_ptr<const configuration::nodes::NodesConfiguration>
          nodes_configuration,
      folly::Optional<epoch_t> acceptable_activation_epoch,
      bool check_metadata_log_before_provisioning = true,
      std::shared_ptr<EpochMetaData> new_metadata = nullptr);

  virtual StatsHolder* getStats() const;

 private:
  // Protects map_ and the std::shared_ptr objects within map_.
  //
  // A single instance of std::shared_ptr isn't read/write safe
  // from multiple threads. Since we already use a mutex to serialize
  // concurrent read/write access to map_, it is natural to just continue
  // to hold that mutex after lookup while updating, or instantiating a
  // thread local copy of, the contained std::shared_ptrs.
  //
  // NOTE: If we see contention on this mutex, sharding the map should
  //       be an easy remedy (assuming the contention is not on specific
  //       logs).
  folly::SharedMutex map_mutex_;
  SequencerMap map_;

  // cluster config used by the Processor that owns this object
  std::shared_ptr<UpdateableConfig> updateable_config_;

  // server settings
  UpdateableSettings<Settings> settings_;

  // If sequencer objects managed by this AllSequencers map can perform
  // log recovery, this is the epoch store interface they will use
  // in order to get epoch numbers and epoch metadata during log recovery.
  std::unique_ptr<EpochStore> epoch_store_;

  // Handles for the subscription to various config updates
  ConfigSubscriptionHandle server_config_subscription_;
  ConfigSubscriptionHandle logs_config_subscription_;
  ConfigSubscriptionHandle nodes_configuration_subscription_;

  // pointer to Processor this instance is owned by
  Processor* processor_;

  // Called on an unspecified thread when cluster configuration has been
  // changed. Updates log properties and node weights for all sequencers.
  void noteConfigurationChanged();

  // perform post-activation procedures (e.g., log recovery) upon a successful
  // sequencer activation
  void finalizeActivation(ActivateResult result,
                          Sequencer* seq,
                          epoch_t epoch,
                          bool bypass_recovery);

  virtual void startMetadataLogEmptyCheck(logid_t logid,
                                          const std::string& activation_reason);
  virtual void startEpochStoreNonemptyCheck(logid_t logid);

  static const size_t MAP_SIZE_MAX = (size_t)INT_MAX;
};

}} // namespace facebook::logdevice
