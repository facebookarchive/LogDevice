/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#define __STDC_FORMAT_MACROS
#include "logdevice/common/Appender.h"

#include <algorithm>
#include <alloca.h>
#include <cstdlib>

#include <opentracing/tracer.h>

#include "logdevice/common/Address.h"
#include "logdevice/common/AppendRequest.h"
#include "logdevice/common/AppenderTracer.h"
#include "logdevice/common/Checksum.h"
#include "logdevice/common/ClusterState.h"
#include "logdevice/common/EpochMetaData.h"
#include "logdevice/common/EpochSequencer.h"
#include "logdevice/common/ExponentialBackoffAdaptiveVariable.h"
#include "logdevice/common/LocalLogStoreRecordFormat.h"
#include "logdevice/common/MetaDataLogWriter.h"
#include "logdevice/common/PayloadHolder.h"
#include "logdevice/common/PeriodicReleases.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Sequencer.h"
#include "logdevice/common/Socket.h"
#include "logdevice/common/TailRecord.h"
#include "logdevice/common/TraceLogger.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/WorkerTimeoutStats.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/protocol/APPENDED_Message.h"
#include "logdevice/common/protocol/DELETE_Message.h"
#include "logdevice/common/protocol/RELEASE_Message.h"
#include "logdevice/common/protocol/STORED_Message.h"
#include "logdevice/common/protocol/STORE_Message.h"
#include "logdevice/common/stats/ServerHistograms.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/Err.h"

// Helper macro for checking that the current worker thread is the worker thread
// this Appender was created on. Only do the check when in debug mode.
#define CHECK_WORKER_THREAD() \
  do {                        \
    if (folly::kIsDebug)      \
      checkWorkerThread();    \
  } while (0)

namespace facebook { namespace logdevice {

// If the Appender is above these thresholds, we log more diagnostic output.
static const std::chrono::milliseconds LOG_IF_APPEND_TOOK_LONGER_THAN =
    std::chrono::seconds(20);
static const int LOG_IF_WAVE_ABOVE = 7;

Appender::Appender(Worker* worker,
                   std::shared_ptr<TraceLogger> trace_logger,
                   std::chrono::milliseconds client_timeout,
                   request_id_t append_request_id,
                   STORE_flags_t passthru_flags,
                   logid_t log_id,
                   AppendAttributes attrs,
                   PayloadHolder payload,
                   ClientID return_address,
                   epoch_t seen_epoch,
                   size_t full_appender_size,
                   lsn_t lsn_before_redirect,
                   std::shared_ptr<opentracing::Tracer> e2e_tracer,
                   std::shared_ptr<opentracing::Span> appender_span)
    : sender_(std::make_unique<SenderProxy>()),
      tracer_(std::move(trace_logger)),
      created_on_(worker),
      full_appender_size_(full_appender_size),
      timeout_(),
      seen_(seen_epoch),
      reply_to_(return_address),
      log_id_(log_id),
      attrs_(std::move(attrs)),
      payload_(std::make_shared<PayloadHolder>(std::move(payload))),
      creation_time_(std::chrono::steady_clock::now()),
      client_deadline_(creation_time_ + client_timeout),
      append_request_id_(append_request_id),
      passthru_flags_(passthru_flags),
      release_type_(static_cast<ReleaseTypeRaw>(ReleaseType::GLOBAL)),
      lsn_before_redirect_(lsn_before_redirect),
      e2e_tracer_(std::move(e2e_tracer)),
      appender_span_(std::move(appender_span)) {
  // Increment the total count of Appenders. Note: created_on_ can be nullptr
  // inside tests.
  if (created_on_) {
    created_on_->totalSizeOfAppenders_ += full_appender_size_;
    STAT_INCR(getStats(), num_appenders);
    STAT_ADD(getStats(), total_size_of_appenders, full_appender_size_);
  }
}

Appender::Appender(Worker* worker,
                   std::shared_ptr<TraceLogger> trace_logger,
                   std::chrono::milliseconds client_timeout,
                   request_id_t append_request_id,
                   STORE_flags_t passthru_flags,
                   logid_t log_id,
                   PayloadHolder payload,
                   epoch_t seen_epoch,
                   size_t full_appender_size,
                   std::shared_ptr<opentracing::Tracer> e2e_tracer,
                   std::shared_ptr<opentracing::Span> appender_span)
    : Appender(worker,
               std::move(trace_logger),
               client_timeout,
               append_request_id,
               passthru_flags,
               log_id,
               AppendAttributes(),
               std::move(payload),
               ClientID::INVALID,
               seen_epoch,
               full_appender_size,
               LSN_INVALID,
               std::move(e2e_tracer),
               std::move(appender_span)) {}

Appender::~Appender() {
  if (started()) {
    ld_spew(
        "Deleting Appender for record %s", store_hdr_.rid.toString().c_str());
  } else {
    ld_spew("Deleting Appender that has not been started.");
  }

  // Decrement the total size of Appenders. Note: created_on_ can be nullptr
  // inside tests.
  if (created_on_) {
    size_t total_size = created_on_->totalSizeOfAppenders_.load();
    if (total_size < full_appender_size_ ||
        total_size > std::numeric_limits<size_t>::max() - (1ul << 30)) {
      ld_check(false);
      RATELIMIT_CRITICAL(
          std::chrono::seconds(2),
          2,
          "INTERNAL ERROR while deleting Appender: "
          "Worker::totalSizeOfAppenders_ underflowed (%lu < %lu).",
          total_size,
          full_appender_size_);
    } else {
      created_on_->totalSizeOfAppenders_ -= full_appender_size_;
    }
    STAT_DECR(getStats(), num_appenders);
    STAT_SUB(getStats(), total_size_of_appenders, full_appender_size_);
  }

  if (is_linked()) {
    // We must be on the right Worker thread to unlink from
    // `Worker::activeAppenders()'.  Otherwise (if this Appender is being
    // deleted on a different thread), onComplete() must have alredy unlinked.
    CHECK_WORKER_THREAD();
  } else {
    // appender was never started or removed
    // itself before retirement
  }
}

int Appender::sendSTORE(const StoreChainLink copyset[],
                        copyset_off_t copyset_offset,
                        folly::Optional<lsn_t> block_starting_lsn,
                        STORE_flags_t flags,
                        std::shared_ptr<opentracing::Span> store_span) {
  ShardID dest = copyset[copyset_offset].destination;

  // TODO(e2e tracing): serialize the info and construct store message

  auto store_msg = std::make_unique<STORE_Message>(
      store_hdr_,
      copyset,
      copyset_offset,
      flags,
      extra_,
      attrs_.optional_keys,
      payload_, // attaching to shared_ptr<PayloadHolder>
      true      // appender_context
  );
  if (block_starting_lsn.hasValue()) {
    store_msg->setBlockStartingLSN(block_starting_lsn.value());
  }

  ld_debug("%s-sending a STORE message for record %s (wave %u) to %s. "
           "Copyset is %s.",
           flags & STORE_Header::CHAIN ? "Chain" : "Direct",
           store_hdr_.rid.toString().c_str(),
           store_hdr_.wave,
           dest.toString().c_str(),
           store_msg->printableCopyset().c_str());

  Recipient* r = recipients_.find(dest);
  ld_check(r);

  std::unique_ptr<opentracing::Span> store_message_send_span;

  if (store_span) {
    store_message_send_span = e2e_tracer_->StartSpan(
        "STORE_message_send", {ChildOf(&store_span->context())});
    store_message_send_span->SetTag("dest", dest.asNodeID().toString());
    store_message_send_span->SetTag("wave", store_hdr_.wave);

    // here we know the dest so we now save the store_span in order to finish
    // it when the stored is received
    std::pair<uint32_t, ShardID> current_info(
        store_msg->getHeader().wave, dest);
    std::pair<std::pair<uint32_t, ShardID>, std::shared_ptr<opentracing::Span>>
        current_span(current_info, store_span);
    // save this span. we want to finish it when the reply is received
    all_store_spans_.insert(current_span);
  }

  auto set_status_span_tag = [&store_message_send_span](E send_err) -> void {
    if (store_message_send_span) {
      store_message_send_span->SetTag("status", error_name(send_err));
      store_message_send_span->Finish();
    }
  };

  int rv = sender_->sendMessage(
      std::move(store_msg), dest.asNodeID(), &r->bwAvailCB());
  if (rv == 0) {
    set_status_span_tag(E::OK);
    return 1;
  }

  set_status_span_tag(err);

  // failed to send the message
  switch (err) {
    case E::CBREGISTERED:
      ld_check(store_msg);
      r->bwAvailCB().setMessage(std::move(store_msg));
      // The Receipient's BWAvailableCallback will send the message when
      // resources are available.
      ++deferred_stores_;
      return 1;
    case E::NOTINCONFIG:
      // Worker::sender()'s map of server sockets has not yet been
      // updated to match a new cluster config that copyset_manager_ saw.
      // Skip this destination.
      RATELIMIT_INFO(
          std::chrono::seconds(10),
          2,
          "Can't send record %s wave %u to new destination %s because the "
          "node map has not yet been updated",
          store_hdr_.rid.toString().c_str(),
          store_hdr_.wave,
          dest.toString().c_str());
      return 0;

    case E::NOSSLCONFIG:
      // Misconfiguration.
      RATELIMIT_ERROR(
          std::chrono::seconds(1),
          1,
          "Failed to send record %s wave %u to %s because SSL is required but "
          "recipient is not configured for SSL connections",
          store_hdr_.rid.toString().c_str(),
          store_hdr_.wave,
          dest.toString().c_str());

      setNotAvailableShard(dest,
                           getSettings().unroutable_retry_interval,
                           NodeSetState::NotAvailableReason::UNROUTABLE);

      return 0;

    case E::UNROUTABLE:
      // dest has no route to it
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      1,
                      "No route to storage shard %s",
                      dest.toString().c_str());

      // mark the node as unavailable in the current nodeset with a 60s retry
      // interval
      setNotAvailableShard(dest,
                           getSettings().unroutable_retry_interval,
                           NodeSetState::NotAvailableReason::UNROUTABLE);

      STAT_INCR(getStats(), node_unroutable_received);
      return 0;

    case E::NOBUFS:
      if (bytesPendingLimitReached()) {
        // reached Worker-wide output buffer space limit. Advise the caller
        // to wait for a while before retrying
        return -1;
      } else {
        // reached per-Socket output buffer space limit. It's ok for the
        // caller to send a copy immediately to a different socket.
        return 0;
      }

    case E::SYSLIMIT:
    case E::NOMEM:
      // Out of ephemeral ports or some other system resource.
      err = E::SYSLIMIT;
      return -1;

    case E::SHUTDOWN:
      // Shutdown procedure should wait for appenders before tearing down
      // messaging.
      ld_check(false);

    default:
      // Any other error code (including DISABLED) is an internal error.
      // DISABLED is unexpected because sendSTORE() is always called right after
      // Sender::checkConnection() said that the target node is available.
      RATELIMIT_CRITICAL(std::chrono::seconds(1),
                         10,
                         "INTERNAL ERROR: got an unexpected error code %s "
                         "from Sender::sendMessage() while sending a STORE "
                         " to node %s",
                         error_name(err),
                         dest.toString().c_str());
      ld_check(false);

      err = E::INTERNAL;
      return -1;
  }
}

void Appender::sendDeferredSTORE(std::unique_ptr<STORE_Message> msg,
                                 ShardID dest) {
  ld_check(msg);
  ld_check(deferred_stores_ > 0);

  --deferred_stores_;

  // At least one message should be accepted without traffic shaping when
  // a callback fires, so we do not provide a bandwidth available callback
  // here. If for some reason the message is deferred by traffic shaping,
  // it will be queued on the Sender.
  auto* w = Worker::onThisThread();
  int rv = w->sender().sendMessage(std::move(msg), dest.asNodeID());
  if (rv != 0) {
    ld_check(msg);
    ld_check(err == E::SYSLIMIT || err == E::NOBUFS);
  }

  if ((rv != 0 || deferred_stores_ == 0) && !retryTimerIsActive()) {
    // This check should never fire, because we can get here only from
    // bwAvailCB which cannot be called without shard_ so the copyset has been
    // selected and the store timeout has been set.
    ld_check(timeout_.hasValue());

    activateStoreTimer(timeout_.value());
  }
}

void Appender::onDeferredSTORECancelled(std::unique_ptr<STORE_Message> msg,
                                        ShardID dest,
                                        Status st) {
  ld_check(msg);
  ld_check(deferred_stores_ > 0);
  --deferred_stores_;
  onCopySent(st, dest, msg->getHeader());
}

void Appender::forgetThePreviousWave(const copyset_size_t cfg_synced) {
  deferred_stores_ = 0;
  replies_expected_ = 0;
  store_hdr_.wave++;
  recipients_.replace(nullptr, 0, this);
  synced_replies_remaining_ = cfg_synced;
  held_store_replies_.clear();
}

int Appender::trySendingWavesOfStores(
    const copyset_size_t cfg_synced,
    const copyset_size_t cfg_extras,
    const CopySetManager::AppendContext& append_ctx) {
  // copyset to put in record headers
  StoreChainLink* copyset = nullptr;

  // disable chain-sending for metadata logs as their payload is small and are
  // not written often
  bool chain = (!MetaDataLog::isMetaDataLog(store_hdr_.rid.logid) &&
                !getSettings().disable_chain_sending);

  int attempts = 0;

  do {
    if (attempts >= 10) {
      // If copyset selector keeps picking nodes to which we fail to send,
      // break out of the loop and wait for timeout before retrying.
      ld_info("Failed to send wave for record %s after %d attempts. Will "
              "retry after store timeout. Wave: %u",
              store_hdr_.rid.toString().c_str(),
              attempts,
              store_hdr_.wave);

      // Indicate that the error is transient.
      err = E::NOBUFS;

      return -1;
    }
    ++attempts;

    forgetThePreviousWave(cfg_synced);

    // e2e tracing span for the STORE that is to be sent
    std::shared_ptr<opentracing::Span> store_span;

    if (wave_send_span_) {
      // if we have an wave associated, we continue tracing
      store_span = e2e_tracer_->StartSpan(
          "STORE", {ChildOf(&wave_send_span_->context())});
      // this will be passed to sendStore where it is saved so that we can
      // finish it when the corresponding reply is received
    }

    int ncopies = std::min(recipients_.getReplication() + cfg_extras,
                           static_cast<int>(COPYSET_SIZE_MAX));

    if (wave_send_span_ && !prev_wave_send_span_) {
      // set ncopies tag only for first wave, as all others would have the
      // same property
      wave_send_span_->SetTag("ncopies", ncopies);
    }

    if (!copyset) {
      copyset = (StoreChainLink*)alloca(ncopies * sizeof(StoreChainLink));
    }

    copyset_size_t ndest = 0;
    folly::Optional<lsn_t> block_starting_lsn;
    auto result = copyset_manager_->getCopySet(cfg_extras,
                                               copyset,
                                               &ndest,
                                               &chain,
                                               append_ctx,
                                               block_starting_lsn,
                                               *csm_state_);

    switch (result) {
      case CopySetSelector::Result::PARTIAL:
        ld_check(ndest < ncopies);
        ld_check(ndest >= recipients_.getReplication());
        // We do not have enough available destinations to store r+x copies,
        // but we do have at least r destinations. We should still attempt the
        // write with a reduced number of extra copies. It would be silly to
        // fail just because we cannot send extra copies, which are a
        // latency-saving mechanism.
        ncopies = ndest;

        RATELIMIT_WARNING(std::chrono::seconds(10),
                          1,
                          "There are not enough nodes to store all %d extra "
                          "copies for log %lu (%d nodes selected).",
                          cfg_extras,
                          store_hdr_.rid.logid.val_,
                          ndest);
        break;
      case CopySetSelector::Result::FAILED:
        // Not enough destinations available at the moment. Will start a
        // timer to retry later.
        RATELIMIT_WARNING(std::chrono::seconds(10),
                          1,
                          "There are not enough nodes to store at least r=%d "
                          "copies for log %lu (%d nodes selected), waiting "
                          "for a store timeout to retry later.",
                          recipients_.getReplication(),
                          store_hdr_.rid.logid.val_,
                          ndest);
        STAT_INCR(getStats(), appender_unable_pick_copyset);
        if (MetaDataLog::isMetaDataLog(store_hdr_.rid.logid)) {
          STAT_INCR(getStats(), metadata_log_appender_unable_pick_copyset);
        }
        return 0;
      case CopySetSelector::Result::SUCCESS:
        break;
    }

    ld_check(ncopies > 0);
    ld_check(ndest == ncopies);

    // number of copies to set SYNC flag in. Currently we set it high
    // enough to guarantee that as long as we get 'replication' successful
    // STORED replies 'nsynced_' out of them will be to STORE messages that
    // had its SYNC flag set.
    const int nsync =
        cfg_synced > 0 ? std::min(cfg_synced + cfg_extras, ncopies) : 0;

    timeout_.assign(selectStoreTimeout(copyset, ncopies));
    store_hdr_.copyset_size = ncopies;
    store_hdr_.nsync = nsync;
    store_hdr_.timeout_ms = to_msec(timeout_.value()).count();

    ld_debug("Appender %s is sending a STORE wave #%u of size %d",
             store_hdr_.rid.toString().c_str(),
             store_hdr_.wave,
             ncopies);

    // set the DRAINING flag if the Appender is for draining stores,
    // and the STOREs will ignore soft seals on storage nodes.
    STORE_flags_t store_flags = isDraining() ? STORE_Header::DRAINING : 0;
    if (!attrs_.optional_keys.empty()) {
      store_flags |= STORE_Header::CUSTOM_KEY;
    }

    recipients_.replace(copyset, store_hdr_.copyset_size, this);

    // Decide if we can amend some/all copies.  (This depends on how any
    // previous waves went.)
    std::bitset<COPYSET_SIZE_MAX> amendable_set;
    copyset_size_t first_amendable_offset;
    decideAmends(copyset,
                 store_hdr_.copyset_size,
                 &chain, // may be turned off by the method
                 &amendable_set,
                 &first_amendable_offset);

    if (chain) {
      // try to chain-send this wave of STORE messages
      if (store_span) {
        store_span->SetTag("way of sending", "chain");
      }

      STAT_INCR(getStats(), appender_wave_chain);

      ld_debug(
          "ncopies=%d, copyset size is %hhu", ncopies, store_hdr_.copyset_size);

      // when chain-sending, alway set the SYNC flag unless no syncing
      // is specified for the log. The storage nodes will clear the flag
      // as they forward the message down the chain once the number of
      // nodes in STORE_Header::nsync field has been reached.
      store_flags |= (nsync > 0 ? STORE_Header::SYNC : 0);
      store_hdr_.flags |= STORE_Header::CHAIN;

      extra_.first_amendable_offset = first_amendable_offset;
      if (first_amendable_offset == 0) {
        store_flags |= STORE_Header::AMEND;
      }

      int rv = sendSTORE(copyset,
                         copyset_off_t(0),
                         block_starting_lsn,
                         store_flags,
                         store_span);
      if (rv < 0) { // fatal error
        ld_check(err == E::SYSLIMIT || err == E::NOBUFS);
        return -1;
      }
      if (rv > 0) {                  // success,
        replies_expected_ = ncopies; // terminate the while loop
        outstanding_ += ncopies;
      } else {
        // rv  == 0, transient error, repeat the loop without resetting the
        // iterator. Do not try to chain-send again in order to maximize the
        // chance of completing this append before the client times out.
        chain = false;
      }
    } else {
      // direct-send this wave of STOREs
      if (store_span) {
        wave_send_span_->SetTag("way of sending", "direct");
      }

      store_hdr_.flags &= ~STORE_Header::CHAIN;
      STAT_INCR(getStats(), appender_wave_direct);

      for (int i = 0; i < ndest; i++) {
        const STORE_flags_t node_flags = (i < nsync ? STORE_Header::SYNC : 0) |
            (amendable_set.test(i) ? STORE_Header::AMEND : 0);

        int rv = sendSTORE(copyset,
                           copyset_off_t(i),
                           block_starting_lsn,
                           store_flags | node_flags,
                           store_span);
        if (rv < 0) { // fatal
          ld_check(err == E::SYSLIMIT || err == E::NOBUFS);
          return -1;
        }
        if (rv > 0) {
          ++replies_expected_;
          ++outstanding_;
          ld_check(replies_expected_ <= ncopies);
        }
      }
    }
  } while (replies_expected_ < recipients_.getReplication());
  return 0;
}

int Appender::sendWave() {
  if (prev_wave_send_span_) {
    // there has been another wave that was sent before, so this current wave
    // span follows from that one
    wave_send_span_ = e2e_tracer_->StartSpan(
        "Wave_sending", {FollowsFrom(&prev_wave_send_span_->context())});
  } else {
    if (previous_span_ && e2e_tracer_) {
      // this is the first wave attempted to be sent
      wave_send_span_ = e2e_tracer_->StartSpan(
          "Wave_sending", {FollowsFrom(&previous_span_->context())});
    }
  }

  // refresh copyset manager upon each wave to capture changes in configuration
  // which may change effective nodeset. This is safe because the replication
  // property is immutable for the epoch
  auto new_csm = getCopySetManager();
  if (new_csm.get() != copyset_manager_.get()) {
    // Make sure csm_state_ is destroyed before copyset_manager_ as it must not
    // outlive it.
    csm_state_.reset();
    copyset_manager_ = std::move(new_csm);
    csm_state_ = copyset_manager_->createState();
  } else {
    // copyset_manager_ did not change but `csm_state_` needs to be reset.
    ld_check(csm_state_);
    csm_state_->reset();
  }

  ld_check(copyset_manager_ != nullptr);
  ld_check(csm_state_ != nullptr);

  // cache these on the stack to have a consistent value throughout this
  // call. These Sequencer attributes can change at any time.
  const copyset_size_t cfg_extras = getExtras();
  const copyset_size_t cfg_synced =
      std::min(getSynced(), recipients_.getReplication());

  // add tracing span annotations, only for the first wave, as the information
  // is kept for all waves
  if (wave_send_span_ && !prev_wave_send_span_) {
    wave_send_span_->SetTag("cfg_extras", cfg_extras);
    wave_send_span_->SetTag("cfg_synced", cfg_synced);
  }

  // Building an AppendContext object to be used by CopySetManager. If the
  // StickyCopySetManager is used, it might use this information to determine
  // if we reached the size threshold for the current block.
  ld_check(payload_->valid());
  size_t append_size = (size_t)payload_->size();
  CopySetManager::AppendContext append_ctx{append_size, getLSN()};

  ld_check(started_);
  ld_check(recipients_.getReplication() >= 1);
  ld_check(recipients_.getReplication() <= COPYSET_SIZE_MAX);

  cancelStoreTimer();

  int rv = trySendingWavesOfStores(cfg_synced, cfg_extras, append_ctx);

  if (replies_expected_ < recipients_.getReplication()) {
    // We failed to send a complete wave. Up the current wave id so that
    // the Appender ignores replies to any STORE message we did send.
    recipients_.replace(nullptr, 0, this);
    store_hdr_.wave++;

    if (wave_send_span_) {
      // current wave failed to send all the messages so we finish the
      // corresponding span
      wave_send_span_->SetTag("status", "failed to send complete wave");
      wave_send_span_->Finish();
      // save this current span so that it can be further referenced when
      // creating the span for the next wave
      prev_wave_send_span_ = std::move(wave_send_span_);
    }
  }

  if ((rv == 0 && deferred_stores_ == 0) || err == E::NOBUFS ||
      err == E::INTERNAL) {
    // on success or non-fatal error start a timer to send another
    // wave if we do not get enough replies before it expires
    store_timeout_set_ = true;

    if (!timeout_.hasValue()) {
      timeout_.assign(exponentialStoreTimeout());
      ld_info("Copyset hasn't been selected, "
              "backoff %ldms before trying again.",
              to_msec(timeout_.value()).count());
    }
    activateStoreTimer(timeout_.value());
  }

  return rv;
}

int Appender::start(std::shared_ptr<EpochSequencer> epoch_sequencer,
                    lsn_t lsn) {
  if (started()) {
    ld_check(false);
    err = E::INPROGRESS;
    return -1;
  }

  // hold a shared reference of the epoch sequencer object
  epoch_sequencer_ = std::move(epoch_sequencer);
  ld_check(lsn != LSN_INVALID);

  initStoreTimer();
  initRetryTimer();

  const std::shared_ptr<const Configuration> cfg(getClusterConfig());

  // get the copyset manager from the EpochSequencer. It guarantees to have
  // a consistent (and immutable) nodeset and replication property. However,
  // config changes (i.e., changing of weights) can still change the effective
  // nodeset (which is part of the copyset manager). To solve that, Appender
  // will refresh the copyset manager upon each wave.
  copyset_manager_ = getCopySetManager();
  ld_check(copyset_manager_ != nullptr);

  // In tests, epoch_sequencer may be nullptr
  ld_check(!epoch_sequencer || log_id_ == epoch_sequencer_->getLogID());
  store_hdr_.rid.logid = log_id_;
  store_hdr_.rid.epoch = lsn_to_epoch(lsn);
  store_hdr_.rid.esn = lsn_to_esn(lsn);

  // timestamp = m * now() + c; The default value for m is 1 and
  // c is 0 resulting in timestamp = now(); It has been done like this
  // for testing purposes so that we can write data across a long time
  // in a short time.
  store_hdr_.timestamp =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch())
              .count() *
          getSettings().test_timestamp_linear_transform.first +
      getSettings().test_timestamp_linear_transform.second;
  store_hdr_.last_known_good = lsn_to_esn(getLastKnownGood());
  store_hdr_.sequencer_node_id = getMyNodeID();
  store_hdr_.wave = 0;
  store_hdr_.flags = passthru_flags_;
  store_hdr_.nsync = 0;
  store_hdr_.copyset_size = 0;

  deferred_stores_ = 0;
  replies_expected_ = 0;
  const copyset_size_t replication =
      copyset_manager_->getCopySetSelector()->getReplicationFactor();

  biggest_replication_scope_ = getCurrentBiggestReplicationScope();

  const auto logcfg = cfg->getLogGroupByIDShared(store_hdr_.rid.logid);

  recipients_.reset(replication, getExtras());

  ld_check(store_hdr_.rid.logid != LOGID_INVALID);

  CHECK_WORKER_THREAD();

  int rv = link();
  if (rv != 0) {
    ld_check(err == E::EXISTS);
    // this can only happen if another Appender for this same record
    // is currently active -- an internal error.
    RATELIMIT_CRITICAL(std::chrono::seconds(1),
                       10,
                       "INTERNAL ERROR: an Appender is already active "
                       "for record %s",
                       store_hdr_.rid.toString().c_str());
    ld_check(false);
    err = E::INTERNAL;
    return -1;
  }

  csm_state_ = copyset_manager_->createState();
  started_ = true;

  bool tail_optimized = false;
  if (logcfg != nullptr) {
    backlog_duration_ = logcfg->attrs().backlogDuration().value();
    tail_optimized = logcfg->attrs().tailOptimized().value();
  }

  prepareTailRecord(tail_optimized);
  ld_check(tail_record_ != nullptr);

  STAT_INCR(getStats(), appender_start);

  // Test only setting to disallow appender from retiring. We skip sending the
  // copies to the storage nodes and hence stay in the started stage until
  // someone will abort this appender.
  if (getSettings().test_appender_skip_stores) {
    ld_info("Skipping sending record to storage node skip stores is set.");
    return 0;
  }

  rv = sendWave();

  if (rv < 0 && err != E::NOBUFS) {
    ld_check(err == E::SYSLIMIT); // in release mode this may also be INTERNAL
    epoch_sequencer_.reset();     // we failed to start
    started_ = false;
    return -1;
  }

  if (lsn_to_esn(lsn) == ESN_MIN) {
    STAT_INCR(getStats(), appender_epoch_begin);
  }

  return 0;
}

void Appender::prepareTailRecord(bool include_payload) {
  TailRecordHeader::flags_t flags = TailRecordHeader::OFFSET_WITHIN_EPOCH;
  // TODO (T35832374) : remove if condition when all servers support OffsetMap
  if (getSettings().enable_offset_map) {
    flags |= TailRecordHeader::OFFSET_MAP;
  }
  if (include_payload) {
    // include the payload in the tail record
    flags |= TailRecordHeader::HAS_PAYLOAD;
    flags |= (store_hdr_.flags &
              (STORE_Header::CHECKSUM | STORE_Header::CHECKSUM_64BIT |
               STORE_Header::CHECKSUM_PARITY));
  } else {
    // do not store payload or checksum
    flags |= TailRecordHeader::CHECKSUM_PARITY;
  }

  TailRecordHeader header{
      log_id_,
      store_hdr_.rid.lsn(),
      store_hdr_.timestamp,
      {BYTE_OFFSET_INVALID /* deprecated, offsets_within_epoch used instead */},
      flags,
      {}};

  if (!include_payload) {
    tail_record_ = std::make_shared<TailRecord>(
        header, extra_.offsets_within_epoch, std::shared_ptr<PayloadHolder>());
    return;
  }

  // tail optimized logs, include payload in the tail record
  if (payload_->isEvbuffer()) {
    // payload is evbuffer backed,
    // 1) linearize the evbuffer if not already done
    auto ph_raw = payload_->getPayload();

    auto zero_copied_record = std::make_shared<ZeroCopiedRecord>(
        lsn_t(store_hdr_.rid.lsn()),
        STORE_flags_t(store_hdr_.flags),
        uint64_t(store_hdr_.timestamp),
        esn_t(store_hdr_.last_known_good),
        uint32_t(store_hdr_.wave),
        /*unused copyset*/ copyset_t{},
        extra_.offsets_within_epoch,
        /*unused keys*/ std::map<KeyType, std::string>{},
        Slice{ph_raw},
        payload_);

    tail_record_ = std::make_shared<TailRecord>(
        header, extra_.offsets_within_epoch, std::move(zero_copied_record));
  } else {
    // payload is linear buffer and can be freed on any thread
    tail_record_ = std::make_shared<TailRecord>(
        header, extra_.offsets_within_epoch, payload_);
  }
}

void Appender::retire(RetireReason reason) {
  CHECK_WORKER_THREAD();
  Status st = E::OK;

  ld_check(started());
  ld_check(!retired_);

  retired_ = true;

  if (!recipients_.isFullyReplicated()) {
    // We aborted this wave because we could not fully replicate the record.
    // This can happen for two reasons:
    // - if onTimeout() was called but could not try a new wave because
    //   we are shutting down;
    // - we got a STORE message with E::PREEMPTED.
    // - the Appender was aborted (i.e., EpochSequencer is about to be
    //   destroyed)

    // Make sure RELEASE messages do not get sent.
    release_type_.store(static_cast<ReleaseTypeRaw>(ReleaseType::INVALID));

    // Appender must be fully replicated in order to retire as STORED
    ld_check(reason != RetireReason::STORED);

    // If we have not already sent a reply to the client (which can happen if a
    // previous wave went that far), do it now.
    if (!reply_sent_) {
      NodeID preempted_by = checkIfPreempted(store_hdr_.rid.epoch);
      if (preempted_by.isNodeID()) {
        st = E::PREEMPTED;
        ld_check(getLSN() != LSN_INVALID);
        sendRedirect(preempted_by, E::PREEMPTED, getLSN());
      } else if (!isAcceptingWork()) {
        st = E::SHUTDOWN;
        sendError(Status::SHUTDOWN);
      } else if (reason == RetireReason::ABORTED) {
        st = E::ABORTED;
        // currently reply E::CANCELLED to the client
        // TODO 7467469: improve the client side reporting with per-epoch
        // sequencers. Consider E::ABORTED?
        sendError(Status::CANCELLED);
      } else {
        RATELIMIT_ERROR(std::chrono::seconds(1),
                        10,
                        "INTERNAL ERROR: Appender for record %s (wave %u)"
                        " retired but it was not preempted or aborted and we"
                        " are not shutting down. Recipient set: %s",
                        store_hdr_.rid.toString().c_str(),
                        store_hdr_.wave,
                        recipients_.dumpRecipientSet().c_str());
        st = E::INTERNAL;
        sendError(Status::INTERNAL);
        ld_check(false);
      }
    }

    // as we aborted the wave, we can close the corresponding span
    // do not save it, as we do not expect future waves
    if (wave_send_span_) {
      wave_send_span_->SetTag("status", error_name(st));
      wave_send_span_->Finish();
    }
  }

  Appender::Reaper reaper;
  retireAppender(st, store_hdr_.rid.lsn(), reaper);
}

void Appender::abort(bool linked) {
  retire(RetireReason::ABORTED);
  onComplete(linked);
}

void Appender::Disposer::operator()(Appender* a) {
  // Appender has already been removed from the intrusive map
  // when Disposer is called
  a->abort(/*linked=*/false);
}

void Appender::sendReply(lsn_t lsn, Status status, NodeID redirect) {
  CHECK_WORKER_THREAD();

  if (lsn == LSN_INVALID) {
    ld_check(status != E::OK);
  } else {
    if (started()) {
      ld_check(lsn == store_hdr_.rid.lsn());
    }
    store_hdr_.rid = RecordID(lsn, log_id_);
  }

  APPENDED_Header replyhdr{
      append_request_id_,
      lsn,
      RecordTimestamp::from(std::chrono::milliseconds(store_hdr_.timestamp)),
      redirect,
      status};
  if (status == E::PREEMPTED || status == E::REDIRECTED) {
    ld_check(redirect.isNodeID());
    // In the preemption case, if we know for sure that no copy of the record
    // was successfully replicated, we set the NOT_REPLICATED flag so that the
    // client does not need to worry about silent duplicates. This means that
    // we've received responses from all the recipients across all waves
    // (outstanding_ == 0) and none of them succeeded (!stored_)
    bool not_replicated = !stored_ && outstanding_ == 0;
    if (status == E::PREEMPTED && not_replicated) {
      ld_debug("Setting NOT_REPLICATED flag in response to request:%lu "
               "for lsn:%s",
               append_request_id_.val(),
               lsn_to_string(lsn).c_str());
      replyhdr.flags |= APPENDED_Header::NOT_REPLICATED;
    }
    if (!isNodeAlive(redirect)) {
      replyhdr.flags |= APPENDED_Header::REDIRECT_NOT_ALIVE;
      STAT_ADD(getStats(), append_redirected_not_alive, append_message_count_);
    }
  } else if (status == E::NOTREADY || status == E::REBUILDING) {
    // These error codes are used to notify clients that the node is not taking
    // appends yet, and shouldn't be treated as append failures.
  } else if (status == E::OK) {
    STAT_ADD(getStats(), append_success, append_message_count_);
    LOG_STAT_ADD(getStats(),
                 getClusterConfig(),
                 log_id_,
                 append_success,
                 append_message_count_);
    if (created_on_) { // can be null in tests
      // Bump the per-log-group stats
      if (auto log_path =
              created_on_->getConfiguration()->getLogGroupPath(log_id_)) {
        LOG_GROUP_TIME_SERIES_ADD(getStats(),
                                  append_out_bytes,
                                  log_path.value(),
                                  getPayload()->size());
      }
    }
  } else {
    STAT_ADD(getStats(), append_failed, append_message_count_);
    if (started()) {
      // only if the Appender was started we can ensure that
      // we have a valid logid value
      LOG_STAT_ADD(getStats(),
                   getClusterConfig(),
                   log_id_,
                   append_failed,
                   append_message_count_);
    }
  }

  if (!reply_to_.valid()) {
    // Appender was created directly by AppendRequest::execute().
    replyToAppendRequest(replyhdr);
    return;
  }

  auto socket_proxy = getClientSocketProxy();
  if (!socket_proxy || socket_proxy->isClosed()) {
    ld_debug("Not sending reply to client %s, socket has disconnected.",
             reply_to_.toString().c_str());
    return;
  }

  std::unique_ptr<opentracing::Span> reply_send_span;
  // choosing the parent reference for this reply span is tricky as there is
  // the possibility that sendError which calls sendReply might be early and we
  // do not have the wave span created

  if (wave_send_span_) {
    reply_send_span = e2e_tracer_->StartSpan(
        "APPENDED_message_send", {FollowsFrom(&wave_send_span_->context())});
  } else {
    if (appender_span_) {
      reply_send_span = e2e_tracer_->StartSpan(
          "APPENDED_message_send", {ChildOf(&appender_span_->context())});
    }
  }

  if (reply_send_span) {
    reply_send_span->SetTag("status", error_name(status));
    reply_send_span->SetTag("to", reply_to_.toString().c_str());
    reply_send_span->SetTag("request_id", append_request_id_.val_);
  }

  auto reply = std::make_unique<APPENDED_Message>(replyhdr);
  auto set_status_span_tag = [&reply_send_span](E send_err) -> void {
    if (reply_send_span) {
      reply_send_span->SetTag("status", error_name(send_err));
      reply_send_span->Finish();
    }
  };

  int rv = sender_->sendMessage(std::move(reply), reply_to_);

  if (rv != 0) {
    RATELIMIT_INFO(std::chrono::seconds(10),
                   10,
                   "Failed to send an APPENDED [%s] reply "
                   "to %s. Client request id = %" PRIu64 ". error %s.",
                   error_name(status),
                   reply_to_.toString().c_str(),
                   append_request_id_.val_,
                   error_description(err));
    set_status_span_tag(err);
  } else {
    set_status_span_tag(E::OK);
  }
}

void Appender::sendError(Status reason) {
  Status client_code;

  switch (reason) {
    case E::BADPAYLOAD:
      client_code = E::BADPAYLOAD;
      break;
    case E::NOTFOUND:
      if (getClusterConfig()->localLogsConfig()->isFullyLoaded()) {
        client_code = E::NOTINSERVERCONFIG;
        STAT_ADD(getStats(),
                 append_rejected_not_in_server_config,
                 append_message_count_);
      } else {
        // We cannot trust the the LogsConfig as it has not been fully loaded.
        // We gently ask the client to pick another node.
        client_code = E::NOTREADY;
        STAT_ADD(getStats(),
                 append_rejected_logsconfig_not_ready,
                 append_message_count_);
      }
      break;

    case E::AGAIN:
    case E::NOSEQUENCER:
      STAT_ADD(getStats(), append_rejected_nosequencer, append_message_count_);
      client_code = E::NOSEQUENCER;
      break;

    case E::NOTREADY:
      STAT_ADD(getStats(), append_rejected_not_ready, append_message_count_);
      client_code = E::NOTREADY;
      break;

    case E::NOBUFS:
      STAT_ADD(getStats(), append_rejected_window_full, append_message_count_);
      client_code = E::SEQNOBUFS;
      break;

    case E::TEMPLIMIT:
      STAT_ADD(getStats(), append_rejected_size_limit, append_message_count_);
      client_code = E::SEQNOBUFS;
      break;

    case E::NOSPC:
      STAT_ADD(getStats(), append_rejected_nospace, append_message_count_);
      client_code = E::NOSPC;
      break;

    case E::OVERLOADED:
      STAT_ADD(getStats(), append_rejected_overloaded, append_message_count_);
      client_code = E::OVERLOADED;
      break;

    case E::UNROUTABLE:
      STAT_ADD(getStats(), append_rejected_unroutable, append_message_count_);
      client_code = E::SYSLIMIT;
      break;

    case E::REBUILDING:
      STAT_ADD(getStats(), append_rejected_rebuilding, append_message_count_);
      client_code = E::REBUILDING;
      break;

    case E::DISABLED:
      STAT_ADD(getStats(), append_rejected_disabled, append_message_count_);
      client_code = E::DISABLED;
      break;

    case E::PENDING_FULL:
      STAT_ADD(getStats(), append_rejected_pending_full, append_message_count_);
      client_code = E::SEQNOBUFS;
      break;

    case E::SHUTDOWN:
      STAT_ADD(getStats(), append_rejected_shutdown, append_message_count_);
      client_code = E::SHUTDOWN;
      break;

    case E::SYSLIMIT:
      STAT_ADD(getStats(), append_syslimit, append_message_count_);
      client_code = E::SEQSYSLIMIT;
      break;

    case E::ACCESS:
      STAT_ADD(
          getStats(), append_rejected_permission_denied, append_message_count_);
      client_code = E::ACCESS;
      break;

    case E::ISOLATED:
      STAT_ADD(getStats(), append_rejected_isolated, append_message_count_);
      client_code = E::ISOLATED;
      break;

    case E::CANCELLED:
      STAT_ADD(getStats(), append_rejected_cancelled, append_message_count_);
      client_code = E::CANCELLED;
      break;

    case E::ABORTED:
      // This can only happen if the current epoch is unsuitable to the appender
      ld_check(acceptable_epoch_.hasValue());
      client_code = E::ABORTED;
      break;

    default:
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      10,
                      "Unexpected error code %u (%s)",
                      (unsigned)reason,
                      error_name(reason));
      client_code = E::INTERNAL;
      ld_check(false);
  }
  tracer_.traceAppend(
      payload_->size(),
      seen_,
      started() ? (store_hdr_.flags & STORE_Header::CHAIN) : false, // chained?
      reply_to_,
      Sender::sockaddrOrInvalid(Address(reply_to_)),
      usec_since(creation_time_),
      recipients_,
      log_id_,
      started() ? store_hdr_.rid.lsn() : LSN_INVALID,
      backlog_duration_,
      started() ? store_hdr_.wave : 0,
      std::string(error_name(client_code)),
      std::string(error_name(reason)));

  sendReply(LSN_INVALID, client_code);
}

void Appender::sendRedirect(NodeID to, Status status, lsn_t lsn) {
  switch (status) {
    case E::PREEMPTED:
      STAT_ADD(getStats(), append_preempted, append_message_count_);
      break;
    case E::REDIRECTED:
      STAT_ADD(getStats(), append_redirected, append_message_count_);
      break;
    default:
      ld_check(false && "Invalid status in sendRedirect()");
  }
  sendReply(lsn, status, to);
}

void Appender::onTimeout() {
  ld_check(started());
  ld_check(!recipients_.isFullyReplicated());

  if (store_timeout_set_) {
    if (store_hdr_.wave >= LOG_IF_WAVE_ABOVE) {
      // Separate rate limit for appenders that failed lots of waves.
      // These error messages are expected to be rare and important, don't want
      // them to drown in a flood of first-wave timeouts.

      RATELIMIT_INFO(
          std::chrono::seconds(1),
          10,
          "Appender %s hit a STORE timeout(%sms), wave %u, recipient set: %s",
          store_hdr_.rid.toString().c_str(),
          timeout_.hasValue() ? std::to_string(timeout_.value().count()).c_str()
                              : "<NO VALUE>",
          store_hdr_.wave,
          recipients_.dumpRecipientSet().c_str());
    } else {
      RATELIMIT_INFO(
          std::chrono::seconds(1),
          2,
          "Appender %s hit a STORE timeout(%sms), wave %u, recipient set: %s",
          store_hdr_.rid.toString().c_str(),
          timeout_.hasValue() ? std::to_string(timeout_.value().count()).c_str()
                              : "<NO VALUE>",
          store_hdr_.wave,
          recipients_.dumpRecipientSet().c_str());
    }
    STAT_INCR(getStats(), appender_wave_timedout);
    if (MetaDataLog::isMetaDataLog(store_hdr_.rid.logid)) {
      STAT_INCR(getStats(), metadata_log_appender_wave_timedout);
    }

    if (wave_send_span_) {
      wave_send_span_->SetTag("status", error_name(E::TIMEDOUT));
      wave_send_span_->Finish();

      prev_wave_send_span_ = std::move(wave_send_span_);
    }
  }

  auto worker = Worker::onThisThread(false);
  if (worker &&
      worker->updateable_settings_->enable_store_histogram_calculations) {
    if (store_hdr_.flags & STORE_Header::CHAIN) {
      worker->getWorkerTimeoutStats().onReply(
          recipients_.getFirstOutstandingRecipient(), store_hdr_);
    } else {
      recipients_.forEachOutstandingRecipient(
          [this, &worker](const ShardID& shard) {
            worker->getWorkerTimeoutStats().onReply(shard, store_hdr_);
          });
    }
  }

  // Only mark the first node in the list of outstanding
  // nodes as graylisted, because if we are doing chain-sending
  // it is possible that nodes further down in the chain, may not have
  // been forwarded the STORE message at all.
  //
  // Also examine whether ALL nodes are outstanding;
  // In this corner case, don't graylist any nodes.
  //
  // On the other hand, if chain-sending is disabled, we'll still
  // graylist the slow nodes in future waves.
  if (getSettings().disable_graylisting == false) {
    // Technique for avoiding "death spiral of retries":
    // Don't graylist nodes if they belonged in an all_timed_out wave
    if (!recipients_.allRecipientsOutstanding()) {
      auto shard = recipients_.getFirstOutstandingRecipient();
      if (shard.isValid()) {
        STAT_INCR(getStats(), graylist_shard_added);
        setNotAvailableShard(shard,
                             getSettings().slow_node_retry_interval,
                             NodeSetState::NotAvailableReason::SLOW);
      }
    }
  }

  // There should not be any other timer active.
  ld_check(!storeTimerIsActive());
  ld_check(!retryTimerIsActive());

  // Send a new wave.

  if (!isAcceptingWork() &&
      std::chrono::steady_clock::now() > client_deadline_) {
    // The server is shutting down and client's timeout expired -- stop
    // retrying. TODO: when epoch shutdown is implemented, trigger it here
    // instead of retiring directly.

    retire(RetireReason::SHUTDOWN);
    onComplete();
    // The object may not longer exist here.
    return;
  }

  if (preempted_) {
    // We've been preempted and were waiting for all the responses to STORE
    // messages. However, the timeout fired. We won't wait any longer and just
    // send the redirect to the client.
    // Early-retire the appender. A check for preempted_epoch_ in
    // noteAppenderReaped() will ensure that RELEASE messages are not
    // sent for this epoch.
    retire(RetireReason::PREEMPTED);
    onComplete();
    // `this` may not longer exist here.
    return;
  }

  // when a new wave is sent the e2e tracing span will follow from the previous
  // wave sending span
  sendWave();
}

void Appender::onChainForwardingFailure(unsigned int index) {
  folly::fbvector<Recipient>& recipients = recipients_.getRecipients();

  for (unsigned int i = index; i < recipients.size(); ++i) {
    if (!recipients[i].outcomeKnown()) {
      --outstanding_;
      if (!onRecipientFailed(&recipients[i], Recipient::State::SEND_FAILED)) {
        // The wave was aborted or completed. This Appender may have been
        // destroyed here because onRecipientFailed() calls onComplete() if
        // the wave was completed.
        return;
      }
    }
  }
}

void Appender::onCopySent(Status st, ShardID to, const STORE_Header& mhdr) {
  auto worker = Worker::onThisThread(false);
  if (worker &&
      worker->updateable_settings_->enable_store_histogram_calculations) {
    worker->getWorkerTimeoutStats().onCopySent(st, to, mhdr);
  }

  if (mhdr.wave != store_hdr_.wave) {
    ld_check(mhdr.wave < store_hdr_.wave || mhdr.wave > INT_MAX ||
             store_hdr_.wave > INT_MAX);
    // overflow protection ^ however unlikely an overflow might be

    ld_debug("STORE message %s sent to %s belongs to a previous wave (%u). "
             "Current wave for this Appender is %u.",
             mhdr.rid.toString().c_str(),
             to.toString().c_str(),
             mhdr.wave,
             store_hdr_.wave);
    return;
  }

  ld_check(replies_expected_ > 0);

  if (st == Status::OK) {
    if (mhdr.flags & STORE_Header::CHAIN) {
      for (auto& r : recipients_.getRecipients()) {
        r.setState(Recipient::State::OUTSTANDING);
      }
    } else {
      Recipient* r = recipients_.find(to);
      ld_check(r);
      r->setState(Recipient::State::OUTSTANDING);
    }
    ld_spew("STORE message for %s (wave %u) was passed to TCP for delivery to "
            "%s",
            mhdr.rid.toString().c_str(),
            mhdr.wave,
            to.toString().c_str());
  } else {
    RATELIMIT_INFO(
        std::chrono::seconds(10),
        10,
        "Failed to pass STORE message %s (wave %u) to TCP for delivery "
        "to %s. Reason: %s",
        mhdr.rid.toString().c_str(),
        mhdr.wave,
        to.toString().c_str(),
        error_description(st));

    if (!(mhdr.flags & STORE_Header::CHAIN)) {
      Recipient* r = recipients_.find(to);
      ld_check(r);
      onRecipientFailed(r, Recipient::State::SEND_FAILED);
    } else {
      // Chaining was being used and we failed to send STORE to the first
      // recipient in the wave, which is equivalent to failing to send to all
      // nodes in the chain.
      onChainForwardingFailure(0);
      // `this` might be destroyed if this causes the wave to complete.
      return;
    }
  }
}

void Appender::onSocketClosed(Status st,
                              ShardID shard,
                              Recipient::SocketClosedCallback* cb) {
  Recipient* recipient = recipients_.find(shard);

  // shard must belong to the most recent wave of STOREs sent by this
  // Appender.  This is because we clear recipients_ when we start a
  // new wave. That calls destructors of all Recipient::SocketClosedCallback
  // objects in Recipients of the then-current wave, making them unregister.
  // Since here we are in the context of a callback, it must belong to the most
  // recent wave. A corollary is that the above find() call must succeed.

  ld_check(recipient);
  ld_check(recipient->hasCallback(cb));

  // Also, the callback should only be registered once we successfully sent the
  // STORE message to the node and we still don't know the outcome.

  ld_check(!recipient->requestPending());
  ld_check(!recipient->outcomeKnown());

  RATELIMIT_INFO(std::chrono::seconds(10),
                 10,
                 "Connection to shard %s closed before Appender %s received "
                 "a reply to STORE message in wave %u. Reason: %s",
                 shard.toString().c_str(),
                 store_hdr_.rid.toString().c_str(),
                 store_hdr_.wave,
                 error_description(st));

  onRecipientFailed(recipient, Recipient::State::SOCK_CLOSE);
}

void Appender::setNotAvailableShard(ShardID shard,
                                    std::chrono::seconds retry_interval,
                                    NodeSetState::NotAvailableReason reason) {
  // Note that this increases the number of not available nodes maintained
  // in the nodeset
  setNotAvailableUntil(
      shard,
      std::chrono::steady_clock::now() +
          std::chrono::duration_cast<std::chrono::steady_clock::duration>(
              retry_interval),
      reason);
}

int Appender::onReply(const STORED_Header& header,
                      ShardID from,
                      ShardID rebuildingRecipient) {
  auto worker = Worker::onThisThread(false);
  if (worker &&
      worker->updateable_settings_->enable_store_histogram_calculations) {
    worker->getWorkerTimeoutStats().onReply(from, store_hdr_);
  }

  if (appender_span_) {
    // having an appender span means we have e2e tracing enabled
    std::pair<uint32_t, ShardID> current_info(header.wave, from);

    // look for corresponding span in the map
    auto it = all_store_spans_.find(current_info);

    if (it != all_store_spans_.end()) {
      auto current_store_span = it->second;
      // we received a reply and found the store span corresponding to it
      auto reply_recv_span = e2e_tracer_->StartSpan(
          "STORED_Message_receive", {ChildOf(&current_store_span->context())});
      reply_recv_span->SetTag("from", from.toString());
      reply_recv_span->SetTag("status", error_name(header.status));
      reply_recv_span->Finish();

      current_store_span->Finish();
    }
  }

  // decrement outstanding responses
  --outstanding_;
  if (header.status == E::OK) {
    // remember we had one successful response.
    // do this before checking for staleness or anything
    // else to make sure that copy is accounted for.
    stored_ = true;
  }

  if ((header.flags & STORED_Header::OVERLOADED) ||
      header.status == E::DROPPED) {
    // storage node is currently overloaded, mark the node as temporarily
    // not available in the nodeset.
    // we don't check if the reply belongs to the same wave since it is likely
    // that a new wave is started when the store storage task is dropped from
    // the queue in storage nodes
    setNotAvailableShard(from,
                         getSettings().overloaded_retry_interval,
                         NodeSetState::NotAvailableReason::OVERLOADED);
    STAT_INCR(getStats(), node_overloaded_received);
  }

  // If store succeeded, add to `nodes_stored_amendable_' set.
  // Doing this before the wave staleness check because this information
  // is useful even if we are getting a late reply to a previous wave.
  if (header.status == E::OK) {
    // Maintaining sorted to order to allow lookups with binary search
    auto it = std::lower_bound(
        nodes_stored_amendable_.begin(), nodes_stored_amendable_.end(), from);
    if (it == nodes_stored_amendable_.end() || *it != from) {
      nodes_stored_amendable_.insert(it, from);
    }
  }

  if (header.wave != store_hdr_.wave) { // stale reply
    ld_check(header.wave < store_hdr_.wave || header.wave > INT_MAX ||
             store_hdr_.wave > INT_MAX);
    // overflow protection ^ however unlikely an overflow might be
    ld_debug("Got a reply from %s to a STORE message for record %s that "
             "belongs to an old wave %u. Current wave for that record is %u.",
             from.toString().c_str(),
             store_hdr_.rid.toString().c_str(),
             header.wave,
             store_hdr_.wave);
    return 0;
  }

  if (replies_expected_ == 0) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    10,
                    "PROTOCOL ERROR: got an unexpected reply from %s to a STORE"
                    "message for record %s (wave %u). Recipient set: %s",
                    from.toString().c_str(),
                    store_hdr_.rid.toString().c_str(),
                    header.wave,
                    recipients_.dumpRecipientSet().c_str());
    err = E::PROTO;
    return -1;
  }

  Recipient* recipient = recipients_.find(from);

  if (!recipient) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    10,
                    "PROTOCOL ERROR: got a reply to a STORE message for "
                    "record %s (current wave %u) from %s to which no such "
                    "message was sent. Recipient set: %s",
                    store_hdr_.rid.toString().c_str(),
                    header.wave,
                    from.toString().c_str(),
                    recipients_.dumpRecipientSet().c_str());
    err = E::PROTO;
    return -1;
  }

  if (recipient->requestPending()) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    10,
                    "PROTOCOL ERROR: got a STORED reply from %s for a copy of "
                    "record %s (wave %u) before a STORE was sent to that node. "
                    "Recipient set: %s",
                    from.toString().c_str(),
                    store_hdr_.rid.toString().c_str(),
                    header.wave,
                    recipients_.dumpRecipientSet().c_str());
    err = E::PROTO;
    return -1;
  }

  if (recipient->outcomeKnown() && header.status != Status::FORWARD) {
    RATELIMIT_ERROR(
        std::chrono::seconds(1),
        10,
        "PROTOCOL ERROR: got a STORED reply from %s for a copy of "
        "record %s (wave %u) but we already marked that recipient as "
        "failed or successful. Recipient set: %s",
        from.toString().c_str(),
        store_hdr_.rid.toString().c_str(),
        header.wave,
        recipients_.dumpRecipientSet().c_str());
    err = E::PROTO;
    return -1;
  }

  if (header.status == Status::OK) {
    if (header.flags & STORED_Header::SYNCED) {
      --synced_replies_remaining_;
    }

    // Manage low on space state
    NodeSetState::NotAvailableReason cur_reason =
        copyset_manager_->getNodeSetState()->getNotAvailableReason(from);
    if (header.flags & STORED_Header::LOW_WATERMARK_NOSPC) {
      STAT_INCR(getStats(), node_low_on_space_received);
      if (cur_reason == NodeSetState::NotAvailableReason::NONE) {
        // 0 means this state's retry interval is now
        setNotAvailableShard(
            from,
            std::chrono::seconds(0),
            NodeSetState::NotAvailableReason::LOW_WATERMARK_NOSPC);
      }
    } else {
      if (cur_reason == NodeSetState::NotAvailableReason::LOW_WATERMARK_NOSPC) {
        setNotAvailableShard(from,
                             std::chrono::seconds(0),
                             NodeSetState::NotAvailableReason::NONE);
      }
    }

    onRecipientSucceeded(recipient);
    // `this` may no longer exist here.
    return 0;
  }

  if (header.status == Status::PREEMPTED) {
    const bool soft_only =
        header.flags & STORED_Header::PREMPTED_BY_SOFT_SEAL_ONLY;

    RATELIMIT_INFO(
        std::chrono::seconds(1),
        10,
        "Received PREEMPTED (redirect %s) from shard %s for record %s%s",
        header.redirect.toString().c_str(),
        from.toString().c_str(),
        store_hdr_.rid.toString().c_str(),
        soft_only ? " (soft preempt)" : "");

    if (soft_only && isDraining()) {
      // If: 1) the sequencer is in draining state; AND
      //     2) the preemption is caused by soft seals only;
      // Consider the error as as soft error for the recipient, do NOT
      // preempt (and deactivate) the sequencer. If the appender is not
      // retired and enough nodes have failed, a new wave will be started.
      // If the sequencer is still in draining state, the next wave
      // should include the DRAINING flag in STORE messages, which will
      // hopefully overcome soft seals on storage nodes and finish
      // the store.
      STAT_INCR(getStats(), appender_draining_soft_preempted);
      onRecipientFailed(recipient, Recipient::State::SOFT_PREEMPTED);
      return 0;
    }

    // mark this appender as preempted
    preempted_ = true;
    // mark sequencer as preempted
    noteAppenderPreempted(store_hdr_.rid.epoch, header.redirect);

    if (onRecipientFailed(recipient, Recipient::State::PREEMPTED)) {
      if (store_hdr_.flags & STORE_Header::CHAIN) {
        // This was a chained store and it failed with PREEMPTED. This means
        // that the following nodes in the chain did not receive any store.
        const int next_in_chain = recipients_.indexOf(from) + 1;
        if (next_in_chain < recipients_.size()) {
          onChainForwardingFailure(next_in_chain);
        }
      }
    }
    // the appender may be retired/completed by onRecipientFailed
    return 0;
  }

  if (header.status == Status::FORWARD) {
    // This node could not forward the STORE to the next nodes in the chain.
    // This is equivalent to calling onRecipientFailed to all nodes following
    // it. This does not mean that the node itself could not store the copy.
    const int index = recipients_.indexOf(from);
    ld_check(index >= 0 && index < recipients_.size());
    if (index == recipients_.size() - 1) {
      RATELIMIT_ERROR(
          std::chrono::seconds(1),
          10,
          "PROTOCOL ERROR: got a STORED(st=E::FORWARD) reply from %s "
          "for record %s (wave %u) but this node is the last node in "
          "the chain. Recipient set: %s",
          from.toString().c_str(),
          store_hdr_.rid.toString().c_str(),
          header.wave,
          recipients_.dumpRecipientSet().c_str());
      err = E::PROTO;
      return -1;
    }
    onChainForwardingFailure(index + 1);
    // `this` may not longer exist here.
    return 0;
  }

  const bool severe = header.status == Status::NOTSTORAGE ||
      header.status == Status::NOTINCONFIG;
  if (severe) {
    RATELIMIT_ERROR(
        std::chrono::seconds(10),
        10,
        "Failed to store a copy of record %s (wave %u) on shard %s: %s. "
        "Recipient set: %s",
        store_hdr_.rid.toString().c_str(),
        header.wave,
        from.toString().c_str(),
        error_name(header.status),
        recipients_.dumpRecipientSet().c_str());
  } else {
    RATELIMIT_INFO(
        std::chrono::seconds(10),
        10,
        "Failed to store a copy of record %s (wave %u) on shard %s: %s. "
        "Recipient set: %s",
        store_hdr_.rid.toString().c_str(),
        header.wave,
        from.toString().c_str(),
        error_name(header.status),
        recipients_.dumpRecipientSet().c_str());
  }

  Recipient::State reason;
  if (header.status == Status::NOSPC) {
    // the log storage node is up, but temporarily out of disk space
    // mark the node in the node set as out of space and try again later
    setNotAvailableShard(from,
                         getSettings().nospace_retry_interval,
                         NodeSetState::NotAvailableReason::NO_SPC);
    STAT_INCR(getStats(), node_out_of_space_received);
    reason = Recipient::State::NO_SPC;
  } else if (header.status == Status::DROPPED) {
    // the log storage node dropped our request
    // the code above already marked it as overloaded
    STAT_INCR(getStats(), node_dropped_received);
    reason = Recipient::State::DROPPED;
  } else if (header.status == Status::DISABLED) {
    // the log storage node is persistently not accepting stores for this log
    // mark it as having disabled store and try again later
    setNotAvailableShard(from,
                         getSettings().disabled_retry_interval,
                         NodeSetState::NotAvailableReason::STORE_DISABLED);
    STAT_INCR(getStats(), node_disabled_received);
    reason = Recipient::State::STORE_DISABLED;
  } else if (header.status == Status::REBUILDING) {
    // Some node in copyset is rebuilding, can't store copies on it.
    // Note that it's not necessarily the recipient node, can be its
    // copyset-mate.
    ld_check(rebuildingRecipient.isValid());
    setNotAvailableShard(rebuildingRecipient,
                         getSettings().disabled_retry_interval,
                         NodeSetState::NotAvailableReason::STORE_DISABLED);
    STAT_INCR(getStats(), node_rebuilding_received);
    reason = Recipient::State::SOMEONE_IS_REBUILDING;
  } else if (header.status == Status::CHECKSUM_MISMATCH) {
    // Store failed because checksum verification failed
    // Figure out if it's the sequencer node or storage node's fault
    reason = Recipient::State::CHECKSUM_MISMATCH;
    uint64_t payload_checksum;
    uint64_t expected_checksum;
    bool should_graylist = !getSettings().disable_graylisting;

    if (!getSettings().verify_checksum_before_replicating) {
      // We're not verifying checksums, yet a storage node rejected this store
      // because of checksum mismatch.
      STAT_INCR(getStats(), payload_corruption_ignored);
      RATELIMIT_CRITICAL(
          std::chrono::seconds(10),
          10,
          "checksum mismatch: Storage node %s reported checksum mismatch for "
          "record %s, but checksum is not checked by "
          "sequencer! Ignoring.%s",
          from.toString().c_str(),
          header.rid.toString().c_str(),
          should_graylist ? " Graylisting node." : "");
    } else if (verifyChecksum(&payload_checksum, &expected_checksum)) {
      // Corruption on storage node, likely due to bad hardware or bug
      RATELIMIT_CRITICAL(
          std::chrono::seconds(10),
          10,
          "checksum mismatch: Record %s was corrupted in storage node %s!%s",
          header.rid.toString().c_str(),
          from.toString().c_str(),
          should_graylist ? " Graylisting node." : "");
    } else {
      // Corruption on sequencer node, likely due to bad hardware or bug
      Payload pl = payload_->getFlatPayload();
      Slice payload_slice = Slice(pl.data(), pl.size());
      RATELIMIT_CRITICAL(
          std::chrono::seconds(10),
          10,
          "checksum mismatch: Record %s was corrupted in the sequencer on this "
          "node! Payload: %s, expected checksum: %lu, "
          "checksum in payload: %lu. Attempting to abort "
          "sequencer node!",
          header.rid.toString().c_str(),
          hexdump_buf(payload_slice, 500).c_str(),
          expected_checksum,
          payload_checksum);
      onMemoryCorruption();
      should_graylist = false;
    }
    if (should_graylist) {
      STAT_INCR(getStats(), graylist_shard_added);
      setNotAvailableShard(from,
                           getSettings().slow_node_retry_interval,
                           NodeSetState::NotAvailableReason::SLOW);
    }
  } else {
    reason =
        (header.status == Status::SHUTDOWN ? Recipient::State::SHUTDOWN
                                           : Recipient::State::STORE_FAILED);
  }

  onRecipientFailed(recipient, reason);
  // `this` might be destroyed if this causes the wave to complete.
  return 0;
}

void Appender::onMemoryCorruption() {
  // Corruption on sequencer node due to bad hardware. Since such nodes
  // have been observed to keep corrupting things very frequently, we'll
  // be safe rather than sorry and try and abort the sequencer node so that it
  // doesn't corrupt more things. However, we'll refrain from aborting if >35%
  // of nodes in the cluster is dead.
  auto processor = Worker::onThisThread()->processor_;

  if (processor->isFailureDetectorRunning()) {
    // Failure detector enabled; count dead nodes
    size_t dead_cnt = 0;
    size_t cluster_size = 0;
    processor->getClusterDeadNodeStats(&dead_cnt, &cluster_size);
    size_t limit = ceil(cluster_size * 0.35);

    if (dead_cnt >= limit) {
      // Too many nodes are dead - don't abort!
      // It's very unlikely to get this many failures due to bad hardware and
      // will probably only ever be caused by bugs.
      STAT_INCR(getStats(), payload_corruption_ignored);
      RATELIMIT_CRITICAL(std::chrono::seconds(10),
                         10,
                         "Too many dead nodes in the cluster to abort this "
                         "node, despite evidence that it corrupted a store "
                         "payload! dead_cnt=%ld, cluster_size=%ld, limit=%ld",
                         dead_cnt,
                         cluster_size,
                         limit);
      return;
    } else {
      RATELIMIT_INFO(std::chrono::seconds(10),
                     10,
                     "Enough nodes are alive that we can abort this one "
                     "(dead_cnt=%ld, cluster_size=%ld, limit=%ld)",
                     dead_cnt,
                     cluster_size,
                     limit);
    }
  } else {
    RATELIMIT_INFO(std::chrono::seconds(10),
                   10,
                   "No failure detector, assuming that enough nodes are "
                   "alive");
  }

  // Kill node
  std::abort();
}

bool Appender::verifyChecksum(uint64_t* payload_checksum,
                              uint64_t* expected_checksum) {
  if (!(passthru_flags_ & APPEND_Header::CHECKSUM)) {
    // No checksum to verify
    return true;
  }

  Payload pl_with_checksum = payload_->getPayload();

  size_t checksum_byte_num =
      passthru_flags_ & APPEND_Header::CHECKSUM_64BIT ? 8 : 4;
  *payload_checksum = 0;
  *expected_checksum = 0;

  memcpy(payload_checksum, pl_with_checksum.data(), checksum_byte_num);

  Slice slice(
      static_cast<const char*>(pl_with_checksum.data()) + checksum_byte_num,
      pl_with_checksum.size() - checksum_byte_num);
  checksum_bytes(slice, checksum_byte_num * 8, (char*)expected_checksum);

  return *payload_checksum == *expected_checksum;
}

void Appender::onRecipientSucceeded(Recipient* recipient) {
  ld_check(started());
  // If we were already fully replicated, we would have retired the Appender
  // already.
  ld_check(!recipients_.isFullyReplicated());

  ld_check(!recipient->outcomeKnown());
  recipient->setState(Recipient::State::STORED);
  recipients_.onStored(recipient);

  if (!recipients_.isFullyReplicated()) {
    if (preempted_) {
      // This STORE succeeded, but we've received a preemption before and were
      // waiting for wave to complete. We can stop waiting now and immediately
      // fail the append with the PREEMPTED redirect
      retire(RetireReason::PREEMPTED);
      onComplete();
    }
    return;
  }

  // here we have the recipients_.isFullyReplicated, so we received all the
  // expected store messages, so it is time to close the current wave span
  if (wave_send_span_) {
    wave_send_span_->SetTag("status", error_name(E::OK));
    wave_send_span_->Finish();
  }

  ld_check(!reply_sent_);
  // record the latency of this append
  HISTOGRAM_ADD(getStats(), append_latency, usec_since(creation_time_));
  int64_t latency_usec = usec_since(creation_time_);
  const Sockaddr& client_sock_addr =
      Sender::sockaddrOrInvalid(Address(reply_to_));
  tracer_.traceAppend(
      payload_->size(),
      seen_,
      started() ? (store_hdr_.flags & STORE_Header::CHAIN) : false,
      reply_to_,
      client_sock_addr,
      latency_usec,
      recipients_,
      log_id_,
      started() ? store_hdr_.rid.lsn() : LSN_INVALID,
      backlog_duration_,
      started() ? store_hdr_.wave : 0,
      std::string(error_name(E::OK)),
      std::string(error_name(E::OK)));
  if (std::chrono::microseconds(latency_usec) >
      LOG_IF_APPEND_TOOK_LONGER_THAN) {
    RATELIMIT_WARNING(
        std::chrono::seconds(1),
        5,
        "Slow Appender %lu%s: %.3fs, %u waves, payload: %lu bytes, "
        "client: %s",
        log_id_.val_,
        lsn_to_string(store_hdr_.rid.lsn()).c_str(),
        latency_usec / 1e6,
        store_hdr_.wave,
        payload_->size(),
        client_sock_addr.valid() ? client_sock_addr.toStringNoPort().c_str()
                                 : "invalid");
  }
  sendReply(compose_lsn(store_hdr_.rid.epoch, store_hdr_.rid.esn), E::OK);

  cancelStoreTimer();
  cancelRetryTimer();

  if (release_type_ != static_cast<ReleaseTypeRaw>(ReleaseType::INVALID)) {
    // Build the set of recipients to which we should send a RELEASE message
    // when this Apender is deleted by deleteIfDone().
    recipients_.getReleaseSet(release_);
  }

  // We now have `replication_` copies of the record.
  // We can retire and complete the state machine.

  retire(RetireReason::STORED);
  onComplete();
}

bool Appender::onRecipientFailed(Recipient* recipient,
                                 Recipient::State reason) {
  ld_check(!recipient->outcomeKnown());
  recipient->setState(reason);
  ld_check(recipient->failed());

  --replies_expected_;
  if (preempted_) {
    // We've been preempted.

    // If this is the last message we were expecting from this wave, we can
    // terminate immediately with a PREEMPTED redirect.
    // If there's at least one copy that got successfully replicated, no need
    // to wait, we can also terminate immediately.
    // Otherwise, we'll just wait until the wave finishes
    if (stored_ || replies_expected_ == 0) {
      retire(RetireReason::PREEMPTED);
      onComplete();
      return false;
    }

    // Keep waiting for more nodes to respond.
    // This is needed to check wether any store succeeded before replying to
    // the client, in order to assess whether there is a risk of silent
    // duplicate.
    return true;
  }

  if (replies_expected_ < recipients_.getReplication()) {
    // We won't be able to make progress for this wave. Start a new wave.
    cancelStoreTimer();
    cancelRetryTimer();
    // Check the current NodeSetState to see if we have enough available nodes
    // to immediately resend another wave. If we don't, trigger storer_timer_
    // instead of retry_timer_ so that we wait some time before sending the new
    // wave.
    store_timeout_set_ = false;
    if (checkNodeSet()) {
      activateRetryTimer();
    } else {
      ld_check(timeout_.hasValue());
      activateStoreTimer(timeout_.value());
    }
    // We aborted the wave.
    return false;
  }

  // This wave can still succeed.
  return true;
}

void Appender::deleteExtras() {
  int rv;

  ld_check(recipients_.isFullyReplicated());

  copyset_custsz_t<4> delete_set;
  recipients_.getDeleteSet(delete_set);

  for (const ShardID& shard : delete_set) {
    auto delete_msg = std::make_unique<DELETE_Message>(
        DELETE_Header({store_hdr_.rid, store_hdr_.wave, shard.shard()}));

    rv = sender_->sendMessage(std::move(delete_msg), shard.asNodeID());
    if (rv != 0) {
      RATELIMIT_INFO(
          std::chrono::seconds(10),
          10,
          "Failed to send a DELETE for record %s (wave %u) to %s: %s. "
          "Recipient set: %s",
          store_hdr_.rid.toString().c_str(),
          store_hdr_.wave,
          shard.toString().c_str(),
          error_description(err),
          recipients_.dumpRecipientSet().c_str());
    }
  }
}

void Appender::sendReleases(const ShardID* dests,
                            size_t ndests,
                            const RecordID& /* rid */,
                            ReleaseType release_type) {
  ld_spew("Sending %s RELEASE messages for record %s to %zu nodes",
          release_type_to_string(release_type).c_str(),
          store_hdr_.rid.toString().c_str(),
          ndests);

  for (size_t dest_num = 0; dest_num < ndests; ++dest_num) {
    const ShardID& dest = dests[dest_num];

    auto release_msg = std::make_unique<RELEASE_Message>(
        RELEASE_Header({store_hdr_.rid, release_type, dest.shard()}));

    int rv = sender_->sendMessage(std::move(release_msg), dest.asNodeID());
    if (rv != 0) {
      RATELIMIT_LEVEL(
          err == E::DISABLED ? dbg::Level::DEBUG : dbg::Level::INFO,
          std::chrono::seconds(10),
          10,
          "Failed to send a RELEASE for record %s to %s: %s. Recipient set: %s",
          store_hdr_.rid.toString().c_str(),
          dest.toString().c_str(),
          error_description(err),
          recipients_.dumpRecipientSet().c_str());
    }
  }
}

void Appender::deleteIfDone(uint8_t flag) {
  ld_check(flag == FINISH || flag == REAPED);
  ld_check(!(state_ & flag));

  const uint8_t desired = FINISH | REAPED;
  uint8_t prev = state_.fetch_or(flag);
  uint8_t next = prev | flag;
  ld_check(prev != next);

  // The first thread that completes this fetch with next == desired will get
  // to delete ourselves. Note: not checking if prev != desired here because
  // each flag can be set by only one thread only.
  if (next != desired) {
    return;
  }

  // onComplete() should have unlinked this Appender.
  ld_check(!is_linked());

  delete this;
}

void Appender::onComplete(bool linked) {
  cancelStoreTimer();
  cancelRetryTimer();

  if (recipients_.isFullyReplicated()) {
    deleteExtras();
  }

  // Prevent replies from remaining extras to come back by removing ourselves
  // from w->activeAppenders().
  ld_check(!linked || is_linked());
  unlink();

  // Make sure as much heap-allocated memory is deleted while we are on the
  // worker thread where the Appender started and allocated it.
  csm_state_.reset();
  recipients_.clear();
  copyset_manager_.reset();
  payload_.reset();

  // If onReaped was already called, this will cause this Appender to be
  // deleted. Otherwise, the Appender will be deleted once onReaped() is called.
  deleteIfDone(FINISH);
}

void Appender::onReaped() {
  auto release_type = static_cast<ReleaseType>(release_type_.load());
  lsn_t lsn = getLSN();
  epoch_t last_released_epoch;
  bool lng_changed;
  FullyReplicated replicated =
      (release_type == ReleaseType::INVALID ? FullyReplicated::NO
                                            : FullyReplicated::YES);
  // Check whether last-released LSN and/or LNG changed.
  bool last_release_changed = noteAppenderReaped(
      replicated, lsn, tail_record_, &last_released_epoch, &lng_changed);

  if (last_release_changed) {
    // Last-released LSN changed. Send a global RELEASE message.
    ld_spew("Last released changed. Sending global RELEASE message for "
            "%s",
            store_hdr_.rid.toString().c_str());
    ld_check(release_type == ReleaseType::GLOBAL);
  } else {
    release_type = ReleaseType::INVALID;
    // print some debug info logging
    switch (err) {
      case E::STALE:
        ld_check(lsn_to_epoch(lsn) != last_released_epoch);
        ld_debug("Not sending global RELEASE messages for %s because current "
                 "epoch is now %u",
                 store_hdr_.rid.toString().c_str(),
                 last_released_epoch.val_);
        break;
      case E::PREEMPTED:
        ld_debug("Not sending global RELEASE messages for %s because Sequencer"
                 "was preempted: %s",
                 store_hdr_.rid.toString().c_str(),
                 error_description(err));
        break;
      case E::ABORTED:
        ld_debug("Not sending RELEASE or advancing LNG becasue this "
                 "Appender %s or previous Appenders were aborted: %s.",
                 store_hdr_.rid.toString().c_str(),
                 error_description(err));
        break;
      default:
        ld_error("Unexpected error for not sending global release for "
                 "Appender %s: %s",
                 store_hdr_.rid.toString().c_str(),
                 error_description(err));
        break;
    }

    if (lng_changed) {
      // we might still be to send per-epoch release messages
      // Only the LNG changed. Consider sending a per-epoch RELEASE message.
      if (epochMetaDataAvailable(lsn_to_epoch(lsn))) {
        // Safe to read epoch. Safe to send per-epoch RELEASE message.
        ld_spew("Only the LNG changed. Sending per-epoch RELEASE message for "
                "%s",
                store_hdr_.rid.toString().c_str());

        // this is the only case we set ReleaseType to be PER_EPOCH;
        release_type = ReleaseType::PER_EPOCH;
      } else {
        // Not safe to read epoch. Do not send a RELEASE message.
        ld_debug("Not sending RELEASE message for %s because only the LNG "
                 "changed and epoch does not have metadata available",
                 store_hdr_.rid.toString().c_str());
      }
    }

    release_type_.store(static_cast<ReleaseTypeRaw>(release_type));
  }

  if (release_type != ReleaseType::INVALID) {
    // Send a RELEASE message to all recipients that acknowledged their copy.
    sendReleases(
        release_.data(), release_.size(), store_hdr_.rid, release_type);
    // For gap detection, as well as rebuilding, to work correctly, all nodes
    // eventually need to find out what the last released LSN is. Some nodes
    // (e.g. those with weight=0) don't normally receive records (and
    // consequently, they don't get any releases). The following makes sure
    // that a RELEASE gets eventually delivered to all nodes.
    schedulePeriodicReleases();
  }

  // If onComplete was already called, this will cause this Appender to be
  // deleted. Otherwise, the Appender will be deleted once onComplete() is
  // called.
  deleteIfDone(REAPED);
}

void Appender::checkWorkerThread() {
  ld_check(Worker::onThisThread() == created_on_);
}

void Appender::initStoreTimer() {
  store_timer_.assign(std::bind(&Appender::onTimeout, this));
}

void Appender::initRetryTimer() {
  retry_timer_.assign(std::bind(&Appender::onTimeout, this));
}

void Appender::cancelStoreTimer() {
  store_timer_.cancel();
}
void Appender::fireStoreTimer() {
  store_timer_.activate(std::chrono::microseconds(0));
}
bool Appender::storeTimerIsActive() {
  return store_timer_.isActive();
}
void Appender::activateStoreTimer(std::chrono::milliseconds delay) {
  HISTOGRAM_ADD(
      Worker::stats(), store_timeouts, to_usec(timeout_.value()).count());
  store_timer_.activate(delay);
}

void Appender::cancelRetryTimer() {
  retry_timer_.cancel();
}

bool Appender::retryTimerIsActive() {
  return retry_timer_.isActive();
}
void Appender::activateRetryTimer() {
  retry_timer_.activate(std::chrono::microseconds(0));
}

lsn_t Appender::getLastKnownGood() const {
  return epoch_sequencer_->getLastKnownGood();
}

NodeLocationScope Appender::getBiggestReplicationScope() const {
  return biggest_replication_scope_;
}

NodeLocationScope Appender::getCurrentBiggestReplicationScope() const {
  return epoch_sequencer_->getMetaData()
      ->replication.getBiggestReplicationScope();
}

copyset_size_t Appender::getExtras() const {
  return epoch_sequencer_->getImmutableOptions().extra_copies;
}

copyset_size_t Appender::getSynced() const {
  return epoch_sequencer_->getImmutableOptions().synced_copies;
}

std::shared_ptr<CopySetManager> Appender::getCopySetManager() const {
  return epoch_sequencer_->getCopySetManager();
}

const Settings& Appender::getSettings() const {
  return Worker::settings();
}

int Appender::link() {
  return Worker::onThisThread()->activeAppenders().map.insert(*this);
}

const std::shared_ptr<Configuration> Appender::getClusterConfig() const {
  return Worker::getConfig();
}

NodeID Appender::getMyNodeID() const {
  return Worker::onThisThread()->processor_->getMyNodeID();
}

std::string Appender::describeConnection(const Address& addr) const {
  return Worker::onThisThread()->sender().describeConnection(addr);
}

bool Appender::bytesPendingLimitReached() const {
  return Worker::onThisThread()->sender().bytesPendingLimitReached();
}

bool Appender::isAcceptingWork() const {
  return Worker::onThisThread()->isAcceptingWork();
}

NodeID Appender::checkIfPreempted(epoch_t epoch) {
  return epoch_sequencer_->checkIfPreempted(epoch);
}

void Appender::retireAppender(Status st, lsn_t lsn, Appender::Reaper& reaper) {
  epoch_sequencer_->retireAppender(st, lsn, reaper);
}

void Appender::noteAppenderPreempted(epoch_t epoch, NodeID preempted_by) {
  epoch_sequencer_->noteAppenderPreempted(epoch, preempted_by);
}

bool Appender::checkNodeSet() const {
  return epoch_sequencer_->checkNodeSet();
}

bool Appender::noteAppenderReaped(FullyReplicated replicated,
                                  lsn_t reaped_lsn,
                                  std::shared_ptr<TailRecord> tail_record,
                                  epoch_t* last_released_epoch_out,
                                  bool* lng_changed_out) {
  return epoch_sequencer_->noteAppenderReaped(replicated,
                                              reaped_lsn,
                                              std::move(tail_record),
                                              last_released_epoch_out,
                                              lng_changed_out);
}

bool Appender::epochMetaDataAvailable(epoch_t epoch) const {
  return epoch_sequencer_->epochMetaDataAvailable(epoch);
}

void Appender::setNotAvailableUntil(
    ShardID shard,
    std::chrono::steady_clock::time_point until_time,
    NodeSetState::NotAvailableReason reason) {
  copyset_manager_->getNodeSetState()->setNotAvailableUntil(
      shard, until_time, reason);
}

StatsHolder* Appender::getStats() {
  return Worker::stats();
}

int Appender::registerOnSocketClosed(NodeID nid, SocketCallback& cb) {
  Sender& sender = Worker::onThisThread()->sender();
  int rv = sender.registerOnSocketClosed(Address(nid), cb);
  return rv;
}

void Appender::replyToAppendRequest(APPENDED_Header& replyhdr) {
  Worker* w = Worker::onThisThread();
  auto pos = w->runningAppends().map.find(append_request_id_);
  if (pos != w->runningAppends().map.end()) {
    ld_check(pos->second);
    pos->second->onReplyReceived(replyhdr, Address(reply_to_));
  } else {
    // AppendRequest may have timed out
    ld_debug("Request id %" PRIu64 " not found in the map of running "
             "Append requests",
             append_request_id_.val_);
  }
}

void Appender::schedulePeriodicReleases() {
  epoch_sequencer_->schedulePeriodicReleases();
}

bool Appender::maxAppendersHardLimitReached() const {
  // Note: created_on_ can be nullptr inside tests.
  if (created_on_ == nullptr) {
    return false;
  }

  // Note: ignore appenders size limit for internal logs.
  if (configuration::InternalLogs::isInternal(getLogID())) {
    return false;
  }

  return created_on_->totalSizeOfAppenders_ >
      getSettings().max_total_appenders_size_hard / getSettings().num_workers;
}

void Appender::setLogOffset(OffsetMap offset_map) {
  // Log size should be set before starting append operation
  ld_check(!started());
  ld_check(!retired_);
  extra_.offsets_within_epoch = std::move(offset_map);
  passthru_flags_ |= STORE_Header::OFFSET_WITHIN_EPOCH;
  // TODO (T35832374) : remove if condition when all servers support OffsetMap
  if (getSettings().enable_offset_map) {
    passthru_flags_ |= STORE_Header::OFFSET_MAP;
  }
}

bool Appender::isDraining() const {
  return epoch_sequencer_->getState() == EpochSequencer::State::DRAINING;
}

void Appender::decideAmends(const StoreChainLink copyset[],
                            copyset_size_t copyset_size,
                            bool* chain,
                            std::bitset<COPYSET_SIZE_MAX>* amendable_set,
                            copyset_size_t* first_amendable_offset) const {
  ld_check(amendable_set->count() == 0);

  // No-op if the `nodes_stored_amendable_' set is empty
  if (nodes_stored_amendable_.empty()) {
    *first_amendable_offset = COPYSET_SIZE_MAX;
    return;
  }

  // The codepath adding to `nodes_stored_amendable_' must keep it sorted
  ld_assert(std::is_sorted(
      nodes_stored_amendable_.begin(), nodes_stored_amendable_.end()));

  // Calculate the bitset of amendable nodes (those that already successfully
  // stored the payload in previous waves)
  int non_amendable = 0;
  int last_non_amendable = -1;
  for (int i = static_cast<int>(copyset_size) - 1; i >= 0; --i) {
    if (std::binary_search(nodes_stored_amendable_.begin(),
                           nodes_stored_amendable_.end(),
                           copyset[i].destination)) {
      amendable_set->set(i);
    } else {
      ++non_amendable;
      if (last_non_amendable == -1) {
        last_non_amendable = i;
      }
    }
  }

  // It only makes sense to chain-send if at least 2 copies need payloads
  // (non-amendable); otherwise we can send everything directly as amends are
  // small.
  if (non_amendable < 2) {
    *chain = false;
  }

  if (*chain) {
    *first_amendable_offset = last_non_amendable + 1;
  } else {
    // If chaining, `first_amendable_offset' does not matter; we're
    // direct-sending and will set the AMEND flag according to each specific
    // bit in `amendable_set'.
    *first_amendable_offset = COPYSET_SIZE_MAX;
  }
}

int64_t Appender::getStoreTimeoutMultiplier() const {
  const int wave = store_hdr_.wave;
  ld_check_ge(wave, 1);
  const int64_t multiplier = (INT64_C(1) << std::min(wave - 1, 20));
  return multiplier;
}

std::chrono::milliseconds Appender::exponentialStoreTimeout() const {
  auto selected = getStoreTimeoutMultiplier();

  if (Worker::onThisThread(false)) {
    const auto& settings = Worker::settings().store_timeout;
    selected = std::min(
        selected * settings.initial_delay.count(), settings.max_delay.count());
  }
  return std::chrono::milliseconds(selected);
}

std::chrono::milliseconds
Appender::selectStoreTimeout(const StoreChainLink copyset[], int size) const {
  if (!Worker::onThisThread(false)) {
    return exponentialStoreTimeout();
  }

  const auto& settings = Worker::settings();
  const auto is_enabled = settings.enable_adaptive_store_timeout &&
      settings.enable_store_histogram_calculations;

  if (!is_enabled) {
    return exponentialStoreTimeout();
  }

  const double initial = settings.store_timeout.initial_delay.count();
  auto histogram_based_timeout = initial;

  for (int i = 0; i < size; ++i) {
    auto node = copyset[i].destination.node();
    auto& stats = Worker::onThisThread()->getWorkerTimeoutStats();
    auto estimations =
        stats.getEstimations(WorkerTimeoutStats::Levels::TEN_SECONDS, node);

    if (estimations.hasValue()) {
      const auto percentile = WorkerTimeoutStats::QuantileIndexes::P99_99;
      constexpr int factor = 2;
      const auto estimation = factor * (*estimations)[percentile];
      histogram_based_timeout = std::max(
          histogram_based_timeout,
          std::min(
              static_cast<double>(settings.store_timeout.max_delay.count()),
              std::max(initial, getStoreTimeoutMultiplier() * estimation)));
    }
  }

  auto selected = std::chrono::milliseconds(
      static_cast<int64_t>(histogram_based_timeout + 0.5));

  RATELIMIT_DEBUG(std::chrono::seconds(1),
                  1,
                  "Store timeout based on histograms is %lf ms. "
                  "Wave number is: %d and exponential multiplier is %lu. "
                  "Selected %lu.",
                  histogram_based_timeout,
                  store_hdr_.wave,
                  getStoreTimeoutMultiplier(),
                  selected.count());

  ld_check(selected.count() > 0);
  return selected;
}

bool Appender::isNodeAlive(NodeID node) {
  auto cs = Worker::getClusterState();
  return (cs == nullptr || cs->isNodeAlive(node.index()));
}

std::unique_ptr<SocketProxy> Appender::getClientSocketProxy() const {
  if (created_on_) {
    return created_on_->sender().getSocketProxy(reply_to_);
  }

  return std::unique_ptr<SocketProxy>();
}
}} // namespace facebook::logdevice
