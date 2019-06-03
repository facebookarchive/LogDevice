/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/SequencerBatching.h"

#include <chrono>
#include <unordered_set>

#include "logdevice/common/AppendRequestBase.h"
#include "logdevice/common/Appender.h"
#include "logdevice/common/AppenderPrep.h"
#include "logdevice/common/ClusterState.h"
#include "logdevice/common/InternalAppendRequest.h"
#include "logdevice/common/MetaDataLogWriter.h"
#include "logdevice/common/PayloadHolder.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Socket.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/buffered_writer/BufferedWriteDecoderImpl.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/include/BufferedWriter.h"

namespace facebook { namespace logdevice {

using namespace std::literals::chrono_literals;

using Compression = BufferedWriter::Options::Compression;
using LogAttributes = logsconfig::LogAttributes;

static BufferedWriter::LogOptions get_log_options(logid_t log_id) {
  BufferedWriter::LogOptions opts;

  // The following line is important for proper memory usage accounting.
  // If we don't destroy payloads, we won't get notified of freed payloads,
  // and will run into the total limit on the size of appenders (i.e.
  // checkShard() will fail and no appends will go through.
  opts.destroy_payloads = true;

  auto config = Worker::getConfig();
  const auto& settings = Worker::settings();

  const std::shared_ptr<LogsConfig::LogGroupNode> group =
      config->getLogGroupByIDShared(log_id);

  if (!group) {
    opts.time_trigger = settings.sequencer_batching_time_trigger;
    opts.size_trigger = settings.sequencer_batching_size_trigger;
    opts.compression = settings.sequencer_batching_compression;
    return opts;
  }

  opts.time_trigger = group->attrs().sequencerBatchingTimeTrigger().getValue(
      settings.sequencer_batching_time_trigger);
  opts.size_trigger = group->attrs().sequencerBatchingSizeTrigger().getValue(
      settings.sequencer_batching_size_trigger);
  opts.compression = group->attrs().sequencerBatchingCompression().getValue(
      settings.sequencer_batching_compression);

  return opts;
}

SequencerBatching::SequencerBatching(Processor* processor)
    : sender_(std::make_unique<SenderProxy>()),
      processor_(processor),
      worker_state_machines_(processor_->settings()->num_workers),
      buffered_writer_(new ProcessorProxy(processor_),
                       nullptr, // BufferedWriter::AppendCallback
                       &get_log_options,
                       -1,   // infinite memory limit
                       this) // BufferedWriterAppendSink
{
  buffered_writer_.prependChecksums();
  buffered_writer_.setCallbackInternal(this);
}

SequencerBatching::~SequencerBatching() {
  shutDown(); // no-op if already called
}

void SequencerBatching::shutDown() {
  // Must not be on a worker or BufferedWriter::shutDown() will deadlock
  ld_check(!Worker::onThisThread(false));

  if (shutting_down_.exchange(true)) {
    // already called
    return;
  }
  // Request that BufferedWriter shut down, which is a blocking operation
  // (posts a Request to all workers).
  buffered_writer_.shutDown();
}

namespace {

static int prepare_batch(logid_t log_id,
                         Appender& appender,
                         BufferedWriter::AppendCallback::Context context,
                         std::vector<BufferedWriter::Append>* out) {
  ld_check(appender.getLSNBeforeRedirect() == LSN_INVALID);

  Payload payload =
      const_cast<PayloadHolder*>(appender.getPayload())->getPayload();
  folly::StringPiece range{payload.toStringPiece()};

  STORE_flags_t passthru_flags = appender.getPassthruFlags();
  if (passthru_flags & STORE_Header::CHECKSUM) {
    // NOTE: Assumes checksum was already checked
    range.advance((passthru_flags & STORE_Header::CHECKSUM_64BIT) ? 8 : 4);
  }
  if (!(passthru_flags & STORE_Header::BUFFERED_WRITER_BLOB)) {
    // Not a buffered writer blob, just a plain append
    out->emplace_back(
        log_id, range.str(), context, appender.getAppendAttributes());
    STAT_ADD(Worker::stats(), append_bytes_seq_batching_in, range.size());
    return 0;
  }

  BufferedWriteDecoderImpl decoder;
  std::vector<Payload> outp;
  int rv = decoder.decodeOne(Slice(range.data(), range.size()),
                             outp,
                             std::unique_ptr<DataRecord>(),
                             /* copy_blob_if_uncompressed */ false);
  if (rv == 0) {
    out->reserve(outp.size());
    // NOTE: This makes a copy of every payload as an std::string to conform
    // to the BufferedWriter interface.  There is room for optimization here
    // by keeping `decoder' around and making BufferedWriter accept raw
    // pointers into the blob.
    StatsHolder* stats = Worker::stats();
    for (Payload p : outp) {
      out->emplace_back(
          log_id, p.toString(), context, appender.getAppendAttributes());
      STAT_ADD(stats, append_bytes_seq_batching_in, p.size());
    }
    return 0;
  } else {
    err = E::BADPAYLOAD;
    return -1;
  }
}

} // namespace

bool SequencerBatching::buffer(logid_t log_id,
                               std::unique_ptr<Appender>& appender_in) {
  const std::shared_ptr<LogsConfig::LogGroupNode> group =
      Worker::getConfig()->getLogGroupByIDShared(log_id);

  const bool enable_batching = group
      ? group->attrs().sequencerBatching().getValue(
            processor_->settings()->sequencer_batching)
      : processor_->settings()->sequencer_batching;

  if (shutting_down_.load() || !enable_batching ||
      MetaDataLog::isMetaDataLog(log_id)) {
    return false;
  }

  // Do not use sequencer batching for internal logs.
  if (configuration::InternalLogs::isInternal(log_id)) {
    return false;
  }

  if (appender_in->getLSNBeforeRedirect() != LSN_INVALID) {
    return false;
  }

  // Filterable key can be different from record to record. So we currently
  // do not use append batching.
  // TODO: Remove these 3 lines once sequencer batching for server-side
  // filtering is implemented.
  const auto optional_keys = appender_in->getAppendAttributes().optional_keys;
  if (optional_keys.find(KeyType::FILTERABLE) != optional_keys.end()) {
    return false;
  }

  if (shouldPassthru(*appender_in)) {
    StatsHolder* stats = Worker::stats();
    const size_t payload_size = appender_in->getPayload()->size();
    // Count these as both in and out so that out/in gives an accurate
    // representation of the compression ratio
    STAT_ADD(stats, append_bytes_seq_batching_in, payload_size);
    STAT_ADD(stats, append_bytes_seq_batching_out, payload_size);
    // Also bump a separate stat to have an idea how much this happens
    STAT_ADD(stats, append_bytes_seq_batching_passthru, payload_size);
    return false;
  }

  // We decided to try to buffer, claiming ownership of the append.  Make sure
  // the Appender is destroyed when we leave scope.
  std::unique_ptr<Appender> appender = std::move(appender_in);

  std::unique_ptr<AppendMessageState> machine(new AppendMessageState());
  machine->owner_worker = Worker::onThisThread()->idx_.val_;
  machine->reply_to = appender->getReplyTo();
  machine->socket_proxy = appender->getClientSocketProxy();
  machine->log_id = log_id;
  machine->append_request_id = appender->getClientRequestID();
  // Passing the pointer to the state machine as context to BufferedWriter
  BufferedWriter::AppendCallback::Context context = machine.get();

  int rv;
  std::vector<BufferedWriter::Append> appends;
  rv = prepare_batch(log_id, *appender, context, &appends);

  if (rv != 0) {
    ld_check(err == E::BADPAYLOAD);
    sendReply(*machine, err);
    return true;
  }

  // Using appendAtomic() to ensure that an incoming append that contained a
  // BufferedWriter batch (which we unpacked) does not get split across a
  // batch boundary by BufferedWriter, since such batches would get different
  // LSNs and would succeed or fail independently.
  rv = buffered_writer_.appendAtomic(log_id, std::move(appends));
  if (rv == 0) {
    worker_state_machines_[machine->owner_worker].list.push_back(*machine);
    machine.release();
  } else {
    ld_check(err != E::INVALID_PARAM);
    sendReply(*machine, E::SEQNOBUFS);
  }
  return true;
}

bool SequencerBatching::shouldPassthru(const Appender& appender) const {
  const std::shared_ptr<LogsConfig::LogGroupNode> group =
      Worker::getConfig()->getLogGroupByIDShared(appender.getLogID());

  const auto passthru_threshold = group
      ? group->attrs().sequencerBatchingPassthruThreshold().getValue(
            processor_->settings()->sequencer_batching_passthru_threshold)
      : processor_->settings()->sequencer_batching_passthru_threshold;

  if (passthru_threshold < 0) {
    return false;
  }

  const auto compression = group
      ? group->attrs().sequencerBatchingCompression().getValue(
            processor_->settings()->sequencer_batching_compression)
      : processor_->settings()->sequencer_batching_compression;

  bool compressing = compression != Compression::NONE;

  const auto flags = appender.getPassthruFlags();

  // Passing through a large append without buffering only makes sense if:
  // 1) The write originated from BufferedWriter so is likely to be compressed
  //    already, or
  // 2) We are not set to compress so buffering these bytes would just copy
  //    stuff around and likely not reduce the number of STOREs substantially
  //    (since it is a large append).
  const bool makes_sense =
      (flags & STORE_Header::BUFFERED_WRITER_BLOB) || !compressing;
  return makes_sense && appender.getPayload()->size() >= passthru_threshold;
}

std::pair<Status, NodeID> SequencerBatching::appendBuffered(
    logid_t logid,
    const BufferedWriter::AppendCallback::ContextSet& contexts,
    AppendAttributes attrs,
    const Payload& payload,
    AppendRequestCallback callback,
    worker_id_t target_worker,
    int checksum_bits) {
  // BufferedWriter ought to be flushing on the same thread it wants a reply
  // on
  ld_check(target_worker == Worker::onThisThread()->idx_);

  STAT_ADD(Worker::stats(),
           append_bytes_seq_batching_out,
           payload.size() - checksum_bits / 8);

  ld_debug("log=%lu payload.size=%zu ...", logid.val_, payload.size());

  // NOTE: This is the "client timeout" for an append.  Not clear what this
  // should be set to, as the batch may have contained original appends with
  // different timeouts.  At time of writing, Appender only uses this to
  // decide if it can stop retrying when the server is shutting down, so the
  // value does not matter much.
  uint32_t client_timeout_ms(2000);
  APPEND_flags_t flags = APPEND_Header::BUFFERED_WRITER_BLOB;

  auto ia_callback = [callback, logid](Status st,
                                       lsn_t lsn,
                                       NodeID redirect,
                                       RecordTimestamp ts) {
    callback(
        st, DataRecord(logid, Payload(), lsn, ts.toMilliseconds()), redirect);
  };

  // For accurate measurement of append success/failure, here we need to plumb
  // to Appender the number of APPEND messages that came over the wire and
  // will be stored by a single Appender.  Since each incoming APPEND message
  // is tracked by an `AppendMessageState' instance and the context we pass to
  // BufferedWriter is pointers to those state machines, counting the number
  // of distinct contexts will give the number of APPEND messages.
  std::unordered_set<BufferedWriter::AppendCallback::Context> unique_contexts;
  for (const auto& context : contexts) {
    unique_contexts.insert(context.first);
  }

  auto reply = runBufferedAppend(logid,
                                 std::move(attrs),
                                 payload,
                                 std::move(ia_callback),
                                 flags,
                                 checksum_bits,
                                 client_timeout_ms,
                                 unique_contexts.size());

  if (reply.hasValue()) {
    // Uh oh, runInternalAppend() failed synchronously and invoked the
    // callback with an error code.  Propagate it to BufferedWriter.
    std::pair<Status, NodeID> res;
    res.first = reply.value().status;
    res.second = reply.value().redirect;
    return res;
  }

  return std::make_pair(E::OK, NodeID());
}

class SequencerBatching::DispatchResultsRequest : public Request {
 public:
  DispatchResultsRequest(
      int target_worker,
      std::unordered_map<AppendMessageState*, uint32_t> appends,
      Status status,
      NodeID redirect,
      lsn_t lsn,
      RecordTimestamp timestamp)
      : Request(RequestType::SEQUENCER_BATCHING_DISPATCH_RESULTS),
        target_worker_(target_worker),
        appends_(std::move(appends)),
        lsn_(lsn),
        timestamp_(timestamp),
        redirect_(redirect),
        status_(status) {}

  int getThreadAffinity(int) override {
    return target_worker_;
  }

  int8_t getExecutorPriority() const override {
    return folly::Executor::HI_PRI;
  }

  Execution execute() override {
    for (; !appends_.empty(); appends_.erase(appends_.begin())) {
      std::unique_ptr<AppendMessageState> ams(appends_.begin()->first);
      uint32_t offset = appends_.begin()->second;
      ld_assert(ams->owner_worker.load() == target_worker_);
      Worker::onThisThread()->processor_->sequencerBatching().sendReply(
          *ams, status_, redirect_, lsn_, timestamp_, offset);
      // ~unique_ptr<AppendMessageState> unhooks from `worker_state_machines_'
    }
    return Execution::COMPLETE;
  }

 private:
  int target_worker_;
  std::unordered_map<AppendMessageState*, uint32_t> appends_;
  lsn_t lsn_;
  RecordTimestamp timestamp_;
  NodeID redirect_;
  Status status_;
};

void SequencerBatching::onResult(logid_t log_id,
                                 ContextSet contexts,
                                 Status status,
                                 NodeID redirect,
                                 lsn_t lsn,
                                 RecordTimestamp timestamp) {
  ld_debug("Batch with %zu buffered writes to log %lu came back with "
           "status %s",
           contexts.size(),
           log_id.val_,
           error_name(status));

  // Unpack the ContextSet (which are really AppendMessageState pointers) and
  // group by owner worker.
  //
  // NOTE: Using a map not a vector because all constituent writes in an
  // AppendMessageState will have the same context passed to BufferedWriter,
  // but we only want to notify it once.  The value in the map is the offset
  // in the batch, which will be included in APPENDED_Message.
  std::vector<std::unordered_map<AppendMessageState*, uint32_t>> ptrs(
      worker_state_machines_.size());

  using Context = BufferedWriter::AppendCallback::Context;
  for (int i = 0; i < contexts.size(); ++i) {
    const std::pair<Context, std::string>& ctx = contexts[i];
    AppendMessageState* ptr = static_cast<AppendMessageState*>(ctx.first);
    // Since AppendMessageState instances get destroyed in the BufferedWriter
    // callback and our destructor tears down BufferedWriter first, `ptr' is
    // guaranteed to still exist.
    //
    // emplace() does not overwrite if the key already exists so the value
    // will be the smallest offset for a key.
    ptrs[ptr->owner_worker.load()].emplace(ptr, i);
  }

  // Create Requests for all target workers to send replies to individual
  // writes
  for (int w = 0; w < int(ptrs.size()); ++w) {
    if (!ptrs[w].empty()) {
      std::unique_ptr<Request> req = std::make_unique<DispatchResultsRequest>(
          w, std::move(ptrs[w]), status, redirect, lsn, timestamp);
      // Need to use postWithRetrying() otherwise AppendMessageState instances
      // would leak
      int rv = processor_->postWithRetrying(req);
      ld_check(rv == 0);
    }
  }
}

void SequencerBatching::sendReply(const AppendMessageState& ams,
                                  Status status,
                                  NodeID redirect,
                                  lsn_t lsn,
                                  RecordTimestamp timestamp,
                                  uint32_t offset) {
  if (ams.reply_to.valid() &&
      (!ams.socket_proxy || ams.socket_proxy->isClosed())) {
    // Release the proxy so that socket can be reclaimed and released.
    ld_debug("Not sending reply to client %s, socket has disconnected.",
             ams.reply_to.toString().c_str());
    return;
  }

  APPENDED_flags_t flags = APPENDED_Header::INCLUDES_SEQ_BATCHING_OFFSET;
  if (status == E::PREEMPTED || status == E::REDIRECTED) {
    ld_check(redirect.isNodeID());
    auto cs = Worker::getClusterState();
    if (cs && !cs->isNodeAlive(redirect.index())) {
      flags |= APPENDED_Header::REDIRECT_NOT_ALIVE;
    }
  }

  APPENDED_Header replyhdr{
      ams.append_request_id, lsn, timestamp, redirect, status, flags};
  auto msg = std::make_unique<APPENDED_Message>(replyhdr);
  msg->seq_batching_offset = offset;

  int rv = sender_->sendMessage(std::move(msg), ams.reply_to);
  if (rv != 0) {
    RATELIMIT_WARNING(1s,
                      2,
                      "Failed to send an APPENDED [%s] reply "
                      "to %s. Client request id = %lu. error %s.",
                      error_name(status),
                      ams.reply_to.toString().c_str(),
                      ams.append_request_id.val_,
                      error_description(err));
  }
}

Status SequencerBatching::canSendToWorker() {
  size_t limit = processor_->settings()->max_total_buffered_append_size;
  size_t cur_value = totalBufferedAppendSize_.load();
  if (cur_value >= limit) {
    RATELIMIT_WARNING(1s,
                      2,
                      "Total buffered append size exceeded: %lu >= %lu. "
                      "%lu background tasks.",
                      cur_value,
                      limit,
                      buffered_writer_.recentNumBackground());
    return E::SEQNOBUFS;
  }
  return E::OK;
}

void SequencerBatching::onBytesSentToWorker(ssize_t bytes) {
  totalBufferedAppendSize_ += bytes;
  STAT_ADD(Worker::stats(), append_bytes_seq_batching_buffer_submitted, bytes);
}

void SequencerBatching::onBytesFreedByWorker(size_t bytes) {
  ld_assert(totalBufferedAppendSize_.load() >= bytes);
  totalBufferedAppendSize_ -= bytes;
  STAT_ADD(Worker::stats(), append_bytes_seq_batching_buffer_freed, bytes);
}

Status SequencerBatching::appendProbe() {
  // Allow the append if we would be able to pass it to
  // BufferedWriter for buffering.
  return canSendToWorker();
}

folly::Optional<APPENDED_Header>
SequencerBatching::runBufferedAppend(logid_t logid,
                                     AppendAttributes attrs,
                                     const Payload& payload,
                                     InternalAppendRequest::Callback callback,
                                     APPEND_flags_t flags,
                                     int checksum_bits,
                                     uint32_t timeout_ms,
                                     uint32_t append_message_count) {
  return runInternalAppend(logid,
                           std::move(attrs),
                           payload,
                           std::move(callback),
                           flags,
                           checksum_bits,
                           timeout_ms,
                           append_message_count);
}

}} // namespace facebook::logdevice
