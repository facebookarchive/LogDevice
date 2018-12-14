/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <unordered_map>

#include <folly/SharedMutex.h>

#include "logdevice/common/ReadStreamAttributes.h"
#include "logdevice/common/client_read_stream/ClientReadStreamBufferFactory.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/AsyncReader.h"
#include "logdevice/lib/ClientImpl.h"

namespace facebook { namespace logdevice {

class Semaphore;
class Processor;

class AsyncReaderImpl : public AsyncReader {
 public:
  // see AsyncReader.h for all of these functions:
  void
  setRecordCallback(std::function<bool(std::unique_ptr<DataRecord>&)>) override;
  void setGapCallback(std::function<bool(const GapRecord&)>) override;
  void setDoneCallback(std::function<void(logid_t)>) override;
  void setHealthChangeCallback(
      std::function<void(logid_t, HealthChangeType)>) override;
  int startReading(logid_t log_id,
                   lsn_t from,
                   lsn_t until = LSN_MAX,
                   const ReadStreamAttributes* attrs = nullptr) override;
  int stopReading(logid_t log_id, std::function<void()> callback) override;
  int resumeReading(logid_t log_id) override;
  void withoutPayload() override;
  void payloadHashOnly();
  void forceNoSingleCopyDelivery() override;
  int isConnectionHealthy(logid_t) const override;
  void doNotDecodeBufferedWrites() override;
  void includeByteOffset() override;
  void doNotSkipPartiallyTrimmedSections() override;
  void getBytesBuffered(std::function<void(size_t)> callback) override;

  // specify the buffer type for reading the log
  void setBufferType(ClientReadStreamBufferType buffer_type) {
    buffer_type_ = buffer_type;
  }

  explicit AsyncReaderImpl(std::shared_ptr<ClientImpl> client,
                           ssize_t buffer_size);

  ~AsyncReaderImpl() override;

 private:
  void postStopReadingRequest(ReadingHandle handle, std::function<void()> cb);
  void postStatisticsRequest(std::vector<ReadingHandle> handles,
                             std::function<void(size_t)> cb) const;

  // Wrapper around the application-provided record callback that performs
  // automatic decoding of buffered writes
  bool recordCallbackWrapper(std::unique_ptr<DataRecord>& record);

  // Handles a record that is a buffered write and needs automatic decoding
  bool handleBufferedWrite(std::unique_ptr<DataRecord>& record);

  // Drains any BufferedWriter-originated records that were decoded by
  // handleBufferedWrite() but not successfully delivered to the application.
  // If there are such records, `batch' is expected to be the full batch
  // containing them (redelivered by `ClientReadStream') -- this is asserted
  // by comparing LSNs.
  //
  // Returns -1 if the client again rejected delivery, 0 if there was nothing
  // buffered, >0 if some records were buffered and delivered.
  int drainBufferedRecords(logid_t log_id, const DataRecord& batch);

  // Shared pointer to the parent ClientImpl object.  Prevents it to be
  // destroyed while any of the AsyncReaders still exist.
  std::shared_ptr<ClientImpl> client_;
  Processor* processor_;

  std::function<bool(std::unique_ptr<DataRecord>&)> record_callback_;
  std::function<bool(const GapRecord&)> gap_callback_;
  std::function<void(logid_t)> done_callback_;
  std::function<void(logid_t, HealthChangeType)> health_change_callback_;

  // Indicates withoutPayload() was called
  bool without_payload_ = false;

  // Indicates payloadHashOnly() was called
  bool payload_hash_only_ = false;

  // Indicates forceNoSingleCopyDelivery() was called
  bool force_no_scd_ = false;

  // Indicates whether records that come with the BUFFERED_WRITER_BLOB flag set
  // should be transparently decoded
  bool decode_buffered_writes_ = true;

  // Indicates includeByteOffset() was called.
  bool include_byte_offset_ = false;

  // Indicates doNotSkipPartiallyTrimmedSections() was called.
  bool do_not_skip_partially_trimmed_sections_ = false;

  // Read buffer_size of this reader, it specifies the buffer_size
  // of ClientReadStream it creates
  size_t read_buffer_size_;

  // type of the buffer used by the readstream, by default it uses a circular
  // linear buffer
  ClientReadStreamBufferType buffer_type_{ClientReadStreamBufferType::CIRCULAR};

  struct LogState {
    explicit LogState(ReadingHandle handle) : handle(handle) {}
    // ReadingHandle generated when we started reading, used to stop reading
    // (which requires communication with Worker)
    const ReadingHandle handle;
    // Connection health for the log as reported by ClientReadStream.  Read
    // from application thread, written on Worker thread (ClientReadStream
    // health callback).
    std::atomic<bool> healthy{true};
    // Records that were decoded from a buffered write but could not be
    // immediately delivered (application callback rejected them).
    std::deque<std::unique_ptr<DataRecord>> pre_queue;
  };
  // Logs currently being read from
  std::unordered_map<logid_t, LogState, logid_t::Hash> log_states_;
  // Coordinates access to `log_states_'.  Most of the time the mutex is
  // acquired in read mode and log_states_.find() is used to look up an
  // existing LogState instance.  Starting and stopping log reading requires
  // changes to `log_states_' and acquires the mutex for writing.
  folly::SharedMutex log_state_mutex_;

  // Bloom filter to avoid hashtable lookups for streams that don't contain
  // buffered writes.  As long as this is false, we never need to call
  // drainBufferedRecords().
  std::atomic<bool> buffered_delivery_failed_{false};

  // Read streams with a pending StopReadingRequest.
  struct PendingStops {
    std::mutex mutex;
    std::set<ReadingHandle> handles;
  };
  std::shared_ptr<PendingStops> pending_stops_ =
      std::make_shared<PendingStops>();

  // Use to gather info for a pending stats Request.
  // Trigger callback once there are no more leftover requests.
  struct RequestAcc {
    int leftovers;
    size_t dataSoFar;
  };

  // Pending StatisticsRequests.
  struct PendingStats {
    std::mutex mutex;
    // map from request id to num of responses that the request is waiting for.
    std::map<int, RequestAcc> requestsPerCall;
  };
  std::shared_ptr<PendingStats> pending_stats_ =
      std::make_shared<PendingStats>();

  // Used to differentiate subsequent requests for stats.
  int statRequestId_{0};
};

}} // namespace facebook::logdevice
