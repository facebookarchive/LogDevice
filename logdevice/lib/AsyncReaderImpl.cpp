/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/lib/AsyncReaderImpl.h"

#include <thread>

#include <folly/Memory.h>

#include "logdevice/common/DataRecordOwnsPayload.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/ReadStreamsBufferedBytesRequest.h"
#include "logdevice/common/ResumeReadingRequest.h"
#include "logdevice/common/Semaphore.h"
#include "logdevice/common/StartReadingRequest.h"
#include "logdevice/common/StopReadingRequest.h"
#include "logdevice/common/Timer.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/buffered_writer/BufferedWriteDecoderImpl.h"
#include "logdevice/common/client_read_stream/AllClientReadStreams.h"
#include "logdevice/common/client_read_stream/ClientReadStream.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/protocol/RECORD_Message.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice {

AsyncReaderImpl::AsyncReaderImpl(std::shared_ptr<ClientImpl> client,
                                 ssize_t buffer_size)
    : client_(std::move(client)),
      processor_(&client_->getProcessor()),
      read_buffer_size_(buffer_size < 0
                            ? processor_->settings()->client_read_buffer_size
                            : static_cast<size_t>(buffer_size)) {}

AsyncReaderImpl::~AsyncReaderImpl() {
  // The destructor should ensure that all reading is stopped.
  folly::SharedMutex::WriteHolder guard(log_state_mutex_);

  Semaphore sem;
  int to_wait = 0;
  std::vector<ReadingHandle> to_destroy_on_this_thread;
  Worker* w = Worker::onThisThread(false);

  auto destroy = [&](ReadingHandle handle) {
    if (w && w->idx_ == handle.worker_id) {
      to_destroy_on_this_thread.push_back(handle);
    } else {
      postStopReadingRequest(handle, [&]() { sem.post(); });
      ++to_wait;
    }
  };

  // Destroy active ClientReadStreams.
  for (auto& kv : log_states_) {
    destroy(kv.second.handle);
  }
  // Destroy ClientReadStreams for which stopReading() was called but the
  // StopReadingRequest hasn't completed yet. This is needed to prevent
  // ClientReadStream callbacks from using a destroyed AsyncReaderImpl.
  // Instead of waiting for the existing StopReadingRequest to complete, just
  // run another one. Duplicate StopReadingRequest are ok.
  {
    std::unique_lock<std::mutex> lock(pending_stops_->mutex);
    for (ReadingHandle h : pending_stops_->handles) {
      destroy(h);
    }
  }

  for (ReadingHandle h : to_destroy_on_this_thread) {
    StopReadingRequest req(h, [] {});
    req.execute();
  }
  guard.unlock();

  // Now wait for all StopReadingRequests's to execute.  After that, it is
  // guaranteed that all ClientReadStream instances inside workers have been
  // destroyed.  There are no more references to this object inside workers.
  // It is safe to quit.
  for (int i = 0; i < to_wait; ++i) {
    sem.wait();
  }
}

void AsyncReaderImpl::setRecordCallback(
    std::function<bool(std::unique_ptr<DataRecord>&)> cb) {
  record_callback_ = std::move(cb);
}

void AsyncReaderImpl::setGapCallback(std::function<bool(const GapRecord&)> cb) {
  gap_callback_ = std::move(cb);
}

void AsyncReaderImpl::setDoneCallback(std::function<void(logid_t)> cb) {
  done_callback_ = std::move(cb);
}

void AsyncReaderImpl::setHealthChangeCallback(
    std::function<void(logid_t, HealthChangeType)> cb) {
  health_change_callback_ = std::move(cb);
}

void AsyncReaderImpl::withoutPayload() {
  without_payload_ = true;
}

void AsyncReaderImpl::payloadHashOnly() {
  payload_hash_only_ = true;
}

void AsyncReaderImpl::forceNoSingleCopyDelivery() {
  force_no_scd_ = true;
}

void AsyncReaderImpl::doNotDecodeBufferedWrites() {
  decode_buffered_writes_ = false;
}

void AsyncReaderImpl::doNotSkipPartiallyTrimmedSections() {
  do_not_skip_partially_trimmed_sections_ = true;
}

void AsyncReaderImpl::getBytesBuffered(std::function<void(size_t)> callback) {
  ld_check(callback);
  std::map<worker_id_t, std::vector<ReadingHandle>> handlesPerWorker;
  {
    folly::SharedMutex::ReadHolder guard(log_state_mutex_);
    for (auto& log_state : log_states_) {
      worker_id_t workerIdx = log_state.second.handle.worker_id;
      auto search = handlesPerWorker.find(workerIdx);
      if (search == handlesPerWorker.end()) {
        handlesPerWorker.emplace(
            workerIdx, std::vector<ReadingHandle>{log_state.second.handle});
      } else {
        search->second.push_back(log_state.second.handle);
      }
    }
  }

  int reqId{0};
  {
    std::unique_lock<std::mutex> lock(pending_stats_->mutex);
    int workersToContact = handlesPerWorker.size();
    reqId = ++statRequestId_;
    pending_stats_->requestsPerCall[reqId] = RequestAcc{workersToContact, 0};
  }

  for (auto& handles : handlesPerWorker) {
    std::weak_ptr<PendingStats> weak(pending_stats_);
    postStatisticsRequest(
        std::move(handles.second), [=](size_t bytesPerWorker) {
          auto pending = weak.lock();
          if (pending) {
            std::unique_lock<std::mutex> lock(pending->mutex);
            RequestAcc& req = pending->requestsPerCall[reqId];
            req.dataSoFar += bytesPerWorker;
            --req.leftovers;

            if (req.leftovers == 0) {
              auto dataSoFar = req.dataSoFar;
              pending->requestsPerCall.erase(reqId);
              lock.unlock();
              callback(dataSoFar);
            }
          }
        });
  }
}

void AsyncReaderImpl::postStatisticsRequest(
    std::vector<ReadingHandle> handles,
    std::function<void(size_t)> cb) const {
  std::unique_ptr<Request> req =
      std::make_unique<ReadStreamsBufferedBytesRequest>(
          std::move(handles), std::move(cb));
  int rv = processor_->postRequest(req);

  ld_check(rv == 0);
}

void AsyncReaderImpl::includeByteOffset() {
  include_byte_offset_ = true;
}

bool AsyncReaderImpl::recordCallbackWrapper(
    std::unique_ptr<DataRecord>& record) {
  // We know the DataRecord comes from ClientReadStream::deliverRecord() and
  // must be a DataRecordOwnsPayload. Downcast so we can access the metadata.
  ld_assert(dynamic_cast<DataRecordOwnsPayload*>(record.get()) != nullptr);

  if (!record_callback_) {
    return true;
  }

  if (buffered_delivery_failed_.load()) {
    // We may have buffered decoded records from a previous batch that we need
    // to attempt to redeliver.  If so, drainBufferedRecords() will deliver
    // the rest.  See also failure handling in handleBufferedWrite().
    int rv = drainBufferedRecords(record->logid, *record);
    if (rv < 0) {
      // Client rejected delivery again.  Need to retry again.
      return false;
    } else if (rv > 0) {
      // There were buffered records from a previous failed delivery.  This
      // means `record' contains the entire batch again.  We already
      // previously decoded it and now delivered all of it, so nothing more to
      // do with it.
      return true;
    }
  }

  DataRecordOwnsPayload* record_with_attributes =
      static_cast<DataRecordOwnsPayload*>(record.get());
  if ((record_with_attributes->flags_ & RECORD_Header::BUFFERED_WRITER_BLOB) &&
      decode_buffered_writes_ && !without_payload_) {
    return handleBufferedWrite(record);
  } else {
    bool rv = record_callback_(record);
    if (!rv) {
      RATELIMIT_DEBUG(std::chrono::seconds(10),
                      10,
                      "Record callback rejected record %lu%s",
                      record->logid.val(),
                      lsn_to_string(record->attrs.lsn).c_str());
    }
    return rv;
  }
}

int AsyncReaderImpl::startReading(logid_t log_id,
                                  lsn_t from,
                                  lsn_t until,
                                  const ReadStreamAttributes* attrs) {
  auto config = processor_->config_->get();
  ld_check(config);

  if (from > until) {
    ld_error("called with from > until for log_id %lu", log_id.val_);
    err = E::INVALID_PARAM;
    return -1;
  }

  if (lsn_to_epoch(from) > EPOCH_MAX) {
    ld_error("reading from an invalid epoch > EPOCH_MAX for log_id %lu",
             log_id.val_);
    err = E::INVALID_PARAM;
    return -1;
  }

  if (!record_callback_) {
    ld_error("called without specifying record callback for log_id %lu",
             log_id.val_);
    err = E::INVALID_PARAM;
    return -1;
  }

  // Check if we're already reading and stop if so.  Carefully handling
  // `log_state_mutex_' because stopReading() also tries to acquire it.
  bool already_reading;
  {
    folly::SharedMutex::ReadHolder guard(log_state_mutex_);
    already_reading = log_states_.find(log_id) != log_states_.end();
  }

  if (already_reading) {
    // Here we pass a no-op callback to stopReading(), not waiting for the
    // request to be processed on the Worker.  The client may suddenly start
    // seeing records from the new read stream.  If the client needs a clean
    // cutover, they can call stopReading() themselves (passing a callback)
    // and then startReading().
    if (stopReading(log_id, std::function<void()>()) != 0) {
      return -1;
    }
  }

  // Now construct the ClientReadStream object and pass it to a worker thread
  // by way of a StartReadingRequest.

  auto settings = processor_->settings();

  read_stream_id_t rsid = processor_->issueReadStreamID();

  namespace arg = std::placeholders;
  auto deps = std::make_unique<ClientReadStreamDependencies>(
      rsid,
      log_id,
      client_->getClientSessionID(),
      // Safe to bind to `this', destructor blocks until ClientReadStream is
      // destroyed
      std::bind(&AsyncReaderImpl::recordCallbackWrapper, this, arg::_1),
      gap_callback_,
      done_callback_,
      client_->getEpochMetaDataCache(),
      // NOTE: The callback keeps a pointer to `this`.  No danger of the pointer
      // dangling because the destructor destroys the ClientReadStream instance.
      [this, log_id](bool healthy) {
        {
          folly::SharedMutex::ReadHolder guard(log_state_mutex_);
          auto it = log_states_.find(log_id);
          if (it == log_states_.end()) {
            return;
          }
          it->second.healthy.store(healthy);
        }
        // If we are here, we have found the provided log_id among those
        // we are reading from - call the health change callback that
        // the user set (if any) without holding log_state_mutex_
        if (health_change_callback_) {
          health_change_callback_(log_id,
                                  healthy ? HealthChangeType::LOG_HEALTHY
                                          : HealthChangeType::LOG_UNHEALTHY);
        }
      });
  auto read_stream = std::make_unique<ClientReadStream>(
      rsid,
      log_id,
      from,
      until,
      settings->client_read_flow_control_threshold,
      buffer_type_,
      read_buffer_size_,
      std::move(deps),
      processor_->config_,
      nullptr,
      attrs);

  if (without_payload_) {
    read_stream->setNoPayload();
  }

  if (payload_hash_only_) {
    read_stream->addStartFlags(START_Header::PAYLOAD_HASH_ONLY);
  }

  if (force_no_scd_) {
    read_stream->forceNoSingleCopyDelivery();
  }

  if (do_not_skip_partially_trimmed_sections_) {
    read_stream->doNotSkipPartiallyTrimmedSections();
  }

  if (include_byte_offset_) {
    read_stream->includeByteOffset();
  }

  // Select a worker thread to route the StartReadingRequest to.  We need to
  // remember it so that we can later route a StopReadingRequest to the same
  // thread.
  //
  // Use load-aware worker assignment to avoid pathological cases like
  // #7621815.
  worker_id_t worker_id = processor_->selectWorkerLoadAware();
  ReadingHandle handle = {worker_id, rsid};

  {
    folly::SharedMutex::WriteHolder guard(log_state_mutex_);
    log_states_.emplace(std::piecewise_construct,
                        std::forward_as_tuple(log_id),
                        std::forward_as_tuple(handle));
  }

  std::unique_ptr<Request> req = std::make_unique<StartReadingRequest>(
      worker_id, log_id, std::move(read_stream));

  int rv = processor_->postRequest(req);
  if (rv != 0) {
    folly::SharedMutex::WriteHolder guard(log_state_mutex_);
    log_states_.erase(log_id);
  }
  return rv;
}

int AsyncReaderImpl::stopReading(logid_t log_id,
                                 std::function<void()> callback) {
  ReadingHandle handle;
  {
    folly::SharedMutex::WriteHolder guard(log_state_mutex_);
    auto it = log_states_.find(log_id);
    if (it == log_states_.end()) {
      ld_error("not reading log %lu, nothing to stop", log_id.val_);
      err = E::NOTFOUND;
      return -1;
    }
    handle = it->second.handle;
    log_states_.erase(it);
  }

  {
    std::unique_lock<std::mutex> lock(pending_stops_->mutex);
    auto ins = pending_stops_->handles.insert(handle);
    ld_check(ins.second);
  }

  std::weak_ptr<PendingStops> weak(pending_stops_);
  postStopReadingRequest(handle, [weak, handle, cb = std::move(callback)] {
    auto pending = weak.lock();
    if (pending) {
      std::unique_lock<std::mutex> lock(pending->mutex);
      pending->handles.erase(handle);
    }
    if (cb) {
      cb();
    }
  });
  return 0;
}

int AsyncReaderImpl::resumeReading(logid_t log_id) {
  folly::SharedMutex::ReadHolder guard(log_state_mutex_);
  auto it = log_states_.find(log_id);
  if (it == log_states_.end()) {
    ld_error("not reading log %lu, nothing to resume", log_id.val_);
    err = E::NOTFOUND;
    return -1;
  }

  std::unique_ptr<Request> req =
      std::make_unique<ResumeReadingRequest>(it->second.handle);
  return processor_->postRequest(req);
}

void AsyncReaderImpl::postStopReadingRequest(ReadingHandle handle,
                                             std::function<void()> cb) {
  std::unique_ptr<Request> req =
      std::make_unique<StopReadingRequest>(handle, std::move(cb));
  int rv = processor_->postImportant(req);
  // postImportant() can only fail during Client shutdown. But it's illegal to
  // destroy a Client while it has an AsyncReader.
  ld_check(rv == 0);
}

int AsyncReaderImpl::isConnectionHealthy(logid_t log_id) const {
  folly::SharedMutex::ReadHolder guard(log_state_mutex_);
  auto it = log_states_.find(log_id);
  if (it == log_states_.end()) {
    err = E::NOTFOUND;
    return -1;
  }
  return it->second.healthy.load() ? 1 : 0;
}

bool AsyncReaderImpl::handleBufferedWrite(std::unique_ptr<DataRecord>& record) {
  // Make a copy of attributes, we'll need them after we pass
  // ownership of `record'.
  DataRecordOwnsPayload* record_with_attributes =
      static_cast<DataRecordOwnsPayload*>(record.get());
  const logid_t log_id = record_with_attributes->logid;
  const DataRecordAttributes attrs = record_with_attributes->attrs;
  const RECORD_flags_t flags = record_with_attributes->flags_;
  record_with_attributes = nullptr; // no longer safe

  auto decoder = std::make_shared<BufferedWriteDecoderImpl>();
  std::vector<Payload> payloads;
  // We use an overload of BufferedWriteDecoderImpl that does not claim
  // ownership of the input DataRecord, in case the client rejects delivery
  // and we need to return the record to ClientReadStream intact.
  int rv = decoder->decodeOne(*record, payloads);
  if (rv != 0) {
    // Whoops, decoding failed. This is tragic and unlikely with checksums
    // but let's generate a DATALOSS gap to inform the client.
    if (!gap_callback_) {
      return true;
    }
    GapRecord gap(log_id, GapType::DATALOSS, attrs.lsn, attrs.lsn);
    return gap_callback_(gap);
  }

  // Decoding succeeded. Now we need to create a DataRecordOwnsPayload for
  // each original record, and call the callback for each one.
  int batch_offset = 0;
  // If the client callback starts rejecting delivery halfway through the
  // batch, we buffer the rest of the batch for redelivery next time
  // ClientReadStream pokes us.
  bool buffer_rest = false;
  // `guard_map' and `log_state' are lazily initialized in the rare case of
  // the application rejecting delivery.
  folly::SharedMutex::ReadHolder guard_map(nullptr);
  LogState* log_state = nullptr;

  for (Payload& payload : payloads) {
    std::unique_ptr<DataRecord> sub_record(new DataRecordOwnsPayload(
        log_id,
        std::move(payload),
        attrs.lsn,
        attrs.timestamp,
        flags & ~RECORD_Header::BUFFERED_WRITER_BLOB,
        nullptr, // no rebuilding metadata
        decoder, // shared ownership of the decoder
        batch_offset++,
        // Report the same offsets for all subrecords. This may be
        // confusing but we don't have better options since offsets
        // currently count the bytes of compressed batches.
        attrs.offsets));
    if (buffer_rest) {
      // The application already rejected a previous record in this batch,
      // just buffer for later redelivery
      log_state->pre_queue.push_back(std::move(sub_record));
      continue;
    }
    if (!record_callback_(sub_record)) {
      RATELIMIT_DEBUG(std::chrono::seconds(10),
                      10,
                      "Record callback rejected sub-record of record %lu%s",
                      log_id.val_,
                      lsn_to_string(attrs.lsn).c_str());
      // We'll buffer the rest.
      buffered_delivery_failed_.store(true);
      buffer_rest = true;
      guard_map = folly::SharedMutex::ReadHolder(log_state_mutex_);
      auto it = log_states_.find(log_id);
      if (it == log_states_.end() ||
          it->second.handle.worker_id != Worker::onThisThread()->idx_) {
        // Corner case -- must have stopped reading the log but word hasn't
        // reached ClientReadStream yet.  Pretend all is fine.
        return true;
      }
      log_state = &it->second;
      ld_check(log_state->pre_queue.empty());
      log_state->pre_queue.push_back(std::move(sub_record));
    }
  }
  // We successfully consumed the batch if we didn't have to buffer
  return !buffer_rest;
}

int AsyncReaderImpl::drainBufferedRecords(logid_t log_id,
                                          const DataRecord& batch) {
  // This is a bit inefficient; we keep reacquiring the lock and repeating
  // hashtable lookups.  It is necessary because the client callback might
  // call stopReading() which blows away the LogState for this log.
  int count = 0;
  bool record_mismatch = false;
  const worker_id_t current_worker_idx = Worker::onThisThread()->idx_;
  while (1) {
    std::unique_ptr<DataRecord> record;
    {
      folly::SharedMutex::ReadHolder guard_map(log_state_mutex_);
      auto it = log_states_.find(log_id);
      if (it == log_states_.end()) {
        break;
      }
      LogState& state = it->second;
      // Check that another worker hasn't "stolen" the log ID from us while we
      // weren't holding the lock
      if (state.handle.worker_id != current_worker_idx ||
          state.pre_queue.empty()) {
        break;
      }
      record = std::move(state.pre_queue.front());
      state.pre_queue.pop_front();
    }

    // The LSN of the batch delivered by ClientReadStream should be the same
    // as subrecords previously decoded from the batch.
    if (record->attrs.lsn != batch.attrs.lsn) {
      RATELIMIT_CRITICAL(std::chrono::seconds(1),
                         1,
                         "Expected record %s of log %lu but got %s",
                         lsn_to_string(batch.attrs.lsn).c_str(),
                         record->logid.val(),
                         lsn_to_string(record->attrs.lsn).c_str());
      // this is unexpected, so we should abort here.
      ld_check(record->attrs.lsn == batch.attrs.lsn);
      // if assert is disabled, we just deliver the buffered records, and then
      // let the recordCallbackWrapper deliver the other record, by pretending
      // that there was no buffered records
      record_mismatch = true;
    }

    if (!record_callback_(record)) {
      RATELIMIT_DEBUG(std::chrono::seconds(10),
                      10,
                      "Record callback rejected record %lu%s",
                      record->logid.val(),
                      lsn_to_string(record->attrs.lsn).c_str());
      ld_check(record != nullptr);
      folly::SharedMutex::ReadHolder guard_map(log_state_mutex_);
      auto it = log_states_.find(log_id);
      if (it != log_states_.end()) {
        LogState& state = it->second;
        if (state.handle.worker_id != current_worker_idx) {
          break;
        }
        state.pre_queue.push_front(std::move(record));
      }
      return -1;
    }
    ++count;
  }
  // We delivered all the buffered records. Reset the atomic boolean.
  return record_mismatch ? 0 : count;
}

}} // namespace facebook::logdevice
