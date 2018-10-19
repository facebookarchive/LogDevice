/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <unordered_map>

#include <boost/multi_index/hashed_index.hpp>
#include <boost/multi_index/mem_fun.hpp>
#include <boost/multi_index/member.hpp>
#include <boost/multi_index_container.hpp>
#include <boost/noncopyable.hpp>
#include <folly/MPMCQueue.h>
#include <folly/Optional.h>

#include "logdevice/common/DataRecordOwnsPayload.h"
#include "logdevice/common/ReadStreamAttributes.h"
#include "logdevice/common/Semaphore.h"
#include "logdevice/common/client_read_stream/ClientReadStream.h"
#include "logdevice/common/client_read_stream/ClientReadStreamBufferFactory.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/Client.h"
#include "logdevice/include/Reader.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

class Processor;
class ReaderBridgeImpl;

class ReaderImpl : public Reader {
 public: // Public interface, see Reader.h for all of these functions
  int startReading(logid_t log_id,
                   lsn_t from,
                   lsn_t until,
                   const ReadStreamAttributes* attrs = nullptr) override;
  int stopReading(logid_t log_id) override;
  bool isReading(logid_t) const override;
  bool isReadingAny() const override;
  int setTimeout(std::chrono::milliseconds timeout) override;
  ssize_t read(size_t nrecords,
               std::vector<std::unique_ptr<DataRecord>>* data_out,
               GapRecord* gap_out) override;
  void waitOnlyWhenNoData() override;
  void withoutPayload() override;
  void payloadHashOnly();
  void includeByteOffset() override;
  void doNotSkipPartiallyTrimmedSections() override;
  int isConnectionHealthy(logid_t) const override;
  std::chrono::milliseconds getTimeout() const {
    return timeout_;
  }

 public: // LogDevice-internally-public interface
  ReaderImpl(size_t max_logs,
             ssize_t buffer_size,
             Processor* processor,
             EpochMetaDataCache* epoch_metadata_cache = nullptr,
             std::shared_ptr<Client> client = {},
             std::string csid = "");
  ~ReaderImpl() override;

  // Makes read() not report most gaps (except those at the end of a log).
  void skipGaps() {
    skip_gaps_ = true;
  }

  // Proxy for ClientReadStream::requireFullReadSet()
  void requireFullReadSet() {
    require_full_read_set_ = true;
  }

  // Proxy for ClientReadStream::ignoreReleasedStatus()
  void ignoreReleasedStatus() {
    ignore_released_status_ = true;
  }

  void shipPseudorecords() {
    ship_pseudorecords_ = true;
  }

  void forceNoSingleCopyDelivery() override {
    force_no_scd_ = true;
  }

  void doNotDecodeBufferedWrites() override {
    decode_buffered_writes_ = false;
  }

  void addStartFlags(START_flags_t flags) {
    additional_start_flags_ |= flags;
  }

  // specify the buffer type for reading the log
  void setBufferType(ClientReadStreamBufferType buffer_type) {
    buffer_type_ = buffer_type;
  }

 protected: // tests can override
  virtual int startReadingImpl(logid_t log_id,
                               lsn_t from,
                               lsn_t until,
                               ReadingHandle* handle_out,
                               const ReadStreamAttributes* = nullptr);

  virtual int postStopReadingRequest(ReadingHandle handle,
                                     std::function<void()> callback);

  // protected constructor for tests, processor may be null
  ReaderImpl(size_t max_logs,
             Processor* processor,
             EpochMetaDataCache* epoch_metadata_cache,
             std::shared_ptr<Client> client,
             std::string csid,
             size_t client_read_buffer_size,
             double flow_control_threshold = 0.5);

  // Allows tests to make the destructor a no-op
  bool destructor_stops_reading_ = true;

 private:
  // Maximum number of logs we may be reading at one time
  size_t max_logs_;

  // Read buffer_size of this reader, it specifies the buffer_size
  // of ClientReadStream it creates
  size_t read_buffer_size_;

  // Bridge through which ClientReadStream sends us data.  Implemented in .cpp.
  friend class ReaderBridgeImpl;
  std::unique_ptr<ReaderBridge> bridge_;

  Processor* processor_;
  EpochMetaDataCache* epoch_metadata_cache_;
  std::shared_ptr<Client> client_shared_;
  std::string csid_;

  // Timeout for read(), -1 if infinite
  std::chrono::milliseconds timeout_{-1};

  // Indicates waitOnlyWhenNoData() was called
  bool wait_only_when_no_data_ = false;

  // Indicates withoutPayload() was called
  bool without_payload_ = false;

  // Indicates payloadHashOnly() was called
  bool payload_hash_only_ = false;

  // Indicates requireFullReadSet() was called
  bool require_full_read_set_ = false;

  // Indicates ignoreReleasedStatus() was called
  bool ignore_released_status_ = false;

  // Indicates forceNoSingleCopyDelivery() was called.
  bool force_no_scd_ = false;

  // Indicates includeByteOffset() was called.
  bool include_byte_offset_ = false;

  // Indicates doNotSkipPartiallyTrimmedSections() was called.
  bool do_not_skip_partially_trimmed_sections_ = false;

  // Indicates shipPseudorecords() was called
  bool ship_pseudorecords_ = false;

  bool skip_gaps_ = false;

  // Should read() transparently decode records that come with the
  // BUFFERED_WRITER_BLOB flag set?  This convenient for clients so true by
  // default, however we don't want to do it in a server context.
  bool decode_buffered_writes_ = true;

  // From addStartFlags().
  START_flags_t additional_start_flags_ = 0;

  // type of the buffer used by the readstream, by default it uses a circular
  // linear buffer
  ClientReadStreamBufferType buffer_type_{ClientReadStreamBufferType::CIRCULAR};

  /**
   * This gets put on the MPMCQueue when ClientReadStream sends us something.
   * Each entry wraps either a DataRecord or a GapRecord.
   */
  struct QueueEntry : boost::noncopyable {
    enum class Type : char { EMPTY, DATA, GAP };

    QueueEntry() : type_(Type::EMPTY) {}

    QueueEntry(read_stream_id_t rsid,
               std::unique_ptr<DataRecordOwnsPayload> record,
               uint32_t nrecords)
        : rsid_(rsid), records_(nrecords), type_(Type::DATA) {
      u_.data = record.release();
    }

    QueueEntry(read_stream_id_t rsid, std::unique_ptr<GapRecord> gap)
        : rsid_(rsid), records_(0), type_(Type::GAP) {
      u_.gap = gap.release();
    }

    ~QueueEntry() {
      reset();
    }

    QueueEntry(QueueEntry&& other) noexcept {
      *this = std::move(other);
    }

    QueueEntry& operator=(QueueEntry&& other) noexcept {
      if (this != &other) {
        reset();
        records_ = other.records_;
        type_ = other.type_;
        notify_when_consumed_ = other.notify_when_consumed_;
        rsid_ = other.rsid_;
        u_ = other.u_;
        allow_end_reading_ = other.allow_end_reading_;

        other.type_ = Type::EMPTY;
        memset(&other.u_, 0, sizeof other.u_);
      }
      return *this;
    }

    read_stream_id_t getReadStreamID() const {
      return rsid_;
    }

    const DataRecordOwnsPayload& getData() const {
      ld_check(type_ == Type::DATA);
      ld_check(u_.data);
      return *u_.data;
    }

    std::unique_ptr<DataRecordOwnsPayload> releaseData() {
      ld_check(type_ == Type::DATA);
      std::unique_ptr<DataRecordOwnsPayload> ptr(u_.data);
      type_ = Type::EMPTY;
      u_.data = nullptr;
      return ptr;
    }

    const GapRecord& getGap() const {
      ld_check(type_ == Type::GAP);
      ld_check(u_.gap);
      return *u_.gap;
    }

    uint32_t getRecordCount() const {
      return records_;
    }

    Type getType() const {
      return type_;
    }

    bool shouldNotifyWhenConsumed() const {
      return notify_when_consumed_;
    }

    void setNotifyWhenConsumed(bool notify) {
      notify_when_consumed_ = notify;
    }

    bool getAllowEndReading() const {
      return allow_end_reading_;
    }

    void setAllowEndReading(bool allow) {
      allow_end_reading_ = allow;
    }

   private:
    // Order of members is optimized for space usage.  We use 23 bytes;
    // folly::MPMCQueue adds another 32-bit int for a total 28 bytes per queue
    // entry with padding.  Without __attribute__((__packed__)) the total would
    // be 32 bytes due to padding and alignment.
    read_stream_id_t rsid_;
    // We own either `data' or `gap' depending on `type_'.
    union {
      DataRecordOwnsPayload* data;
      GapRecord* gap;
    } u_;
    // Number of records.  This is normally 1, but may be higher if u_.data was
    // written using BufferedWriter.
    uint32_t records_;
    Type type_ = Type::EMPTY;
    // Does ClientReadStream want us to notify it when we consume this entry?
    bool notify_when_consumed_ = false;
    // Is ReaderImpl allowed to stop reading a log after consuming this queue
    // entry?  When a batch of records is decoded with the same LSN, only the
    // last will have this set to true.
    bool allow_end_reading_ = true;

    void reset() {
      if (type_ == Type::DATA) {
        delete u_.data;
        u_.data = nullptr;
      } else if (type_ == Type::GAP) {
        delete u_.gap;
        u_.gap = nullptr;
      }
    }
  } __attribute__((__packed__));

  // The main queue for ClientReadStream to send stuff through
  folly::MPMCQueue<QueueEntry> queue_;
  // Entries that we picked off queue_ but couldn't process immediately.
  // These should be processed before queue_.  Can contain:
  // - A gap that wasn't immediately delivered because we'd already delivered
  //   data records in the same read() call.
  // - Decoded BufferedWriter writes.
  std::deque<QueueEntry> pre_queue_;

  // The following members comprise the synchronization mechanism between
  // producers (ClientReadStream instances running on worker threads) and the
  // consumer (read() running on an application thread).
  //
  // The consumer picks things off the queue.  If it empties the queue but the
  // client asked for a larger batch of records, it wants to wait for more
  // data to become available.  It sets wait_watermark_ to however many more
  // records it needs, then waits on the condition variable.  On the
  // production side, whenever something is put onto the queue, the producer
  // checks if the consumer is waiting (watermark != -1) and if the queue size
  // has reached the watermark.
  //
  // In this setup, producers usually just check the watermark then go about
  // their business, while the consumer typically waits on the condition
  // variable 0 or 1 times per read().
  //
  // An additional wrinkle is a possible deadlock that can occur with large
  // batches.  Suppose an application requests 10000 records from one log and
  // that the queue is initially empty.  The consumer waits.  Suppose the
  // ClientReadStream buffer size is the default 4096.  A worker puts that
  // many records onto the queue and then waits for the consumer to notify it
  // when it has consumed them, which never happens because the consumer is
  // waiting too.  The solution employed is to wake the consumer whenever the
  // producer puts something onto the queue that the worker asked to be
  // notified about.  The variable notify_count_ tracks how many entries on
  // the queue require notification, so that the consumer knows why it was
  // woken up.
  std::atomic<int64_t> wait_watermark_{-1};
  std::atomic<int64_t> notify_count_{0};
  std::mutex cv_mutex_;
  std::condition_variable cv_;
  // Number of individual data records currently in queue_. May differ from
  // queue_.sizeGuess() since for buffered writes (where we have a single
  // QueueEntry with multiple data records) we actually count the number of
  // records in the batch. This is compared to wait_watermark_ to decide if
  // the consumer needs to be woken up.
  std::atomic<int64_t> record_count_{0};
  // Number of gaps currently in queue_.  Tracked separately from data records
  // because we want every gap to wake blocking reads.
  std::atomic<int64_t> gap_count_{0};

  // Maintain a few tidbids for each log being read from
  struct LogState {
    logid_t log_id;
    ReadingHandle handle;
    lsn_t front_lsn;
    lsn_t until_lsn;

    read_stream_id_t getReadStreamID() const {
      return handle.read_stream_id;
    }
  };

  // A multi-index container allowing hash lookups by log ID and read stream
  // ID.  Lookups by read stream ID are most common because data coming from
  // workers (queue_ entries) is tagged with the read stream ID.
  struct LogIndex {};
  struct ReadStreamIDIndex {};
  boost::multi_index::multi_index_container<
      LogState,
      boost::multi_index::indexed_by<
          // index by log ID
          boost::multi_index::hashed_unique<
              boost::multi_index::tag<LogIndex>,
              boost::multi_index::member<LogState, logid_t, &LogState::log_id>,
              logid_t::Hash>,

          // index by read stream ID
          boost::multi_index::hashed_unique<
              boost::multi_index::tag<ReadStreamIDIndex>,
              boost::multi_index::const_mem_fun<LogState,
                                                read_stream_id_t,
                                                &LogState::getReadStreamID>,
              read_stream_id_t::Hash>>>
      log_states_;

  // Called after a queue entry is consumed that had the `notify_when_consumed`
  // bit set.  We send a Request to the worker, as requested.
  void notifyWorker(LogState& state);

  // Connection health for each log as reported by ClientReadStream
  std::unordered_map<logid_t, bool, logid_t::Hash> health_map_;
  // Lock guarding health_map_ since it can be concurrently accessed by the
  // application (when reading) and a worker (when publishing, which is rare)
  mutable std::mutex health_map_lock_;

  //
  // State and helper methods for current read() call if one is in progress
  //
  bool may_wait_;
  std::chrono::steady_clock::time_point until_;
  size_t nrecords_;
  size_t nread_;

  // Initializes may_wait_ and until_.
  void read_initWaitParams();
  // Attempts to pop some work off queue_, waiting if if necessary (and
  // allowed).  Returns 0 if we got something, -1 if we timed out without
  // getting anything.
  int read_popQueue(QueueEntry& entry_out);
  // Waits for work to appear in the queue.
  void read_wait();
  // Handlers for data and gap records.
  void read_handleData(QueueEntry& entry,
                       LogState* state,
                       std::vector<std::unique_ptr<DataRecord>>* data_out);
  void read_handleGap(QueueEntry& entry,
                      LogState* state,
                      GapRecord* gap_out,
                      bool* break_loop_out);
  // Handler for data records that come with the
  // RECORD_Header::BUFFERED_WRITER_BLOB flag set.  Decodes the blob and puts
  // original records onto `pre_queue_'.  If decoding fails, a DATALOSS gap is
  // generated instead.
  void read_decodeBuffered(QueueEntry& entry);

  friend class TestReader;
};

}} // namespace facebook::logdevice
