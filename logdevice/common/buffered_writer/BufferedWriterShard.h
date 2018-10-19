/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <unordered_map>

#include <folly/small_vector.h>

#include "logdevice/common/buffered_writer/BufferedWriterSingleLog.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/BufferedWriter.h"
#include "logdevice/include/Client.h"

namespace facebook { namespace logdevice {

/**
 * @file Part of BufferedWriter that manages appends for a shard of logs that
 * map to the same LogDevice worker thread.  See doc/buffered-writer.md for an
 * overview of the implementation.
 *
 * All methods must be invoked on the same LogDevice worker thread.
 */

class BufferedWriterShard {
 private:
  using LogOptions = BufferedWriter::LogOptions;

 public:
  using AppendChunk = folly::small_vector<BufferedWriter::Append, 4>;

  /**
   * NOTE: we take a ClientImpl pointer, which the parent BufferedWriter
   * instance pins via a shared_ptr.
   */
  BufferedWriterShard(buffered_writer_id_t id,
                      std::function<LogOptions(logid_t)> get_log_options,
                      BufferedWriterImpl* parent)
      : id_(id), get_log_options_(get_log_options), parent_(parent) {}

  /**
   * Buffers a chunk of appends for logs that all belong to this shard.
   * Distributes appends to BufferedWriterSingleLog instances.
   */
  void append(AppendChunk, bool atomic);

  /**
   * Called during shutdown.  Must be called on the shard's thread, just like
   * all the other BufferedWriterShard functions.  Once it returns, we know that
   * all work by this shard has been completed and no more will start, so it's
   * ok to free resources associated with this shard, in particular the
   * BufferedWriterSingleLog objects and the Batches they contain.
   */
  void quiesce();

  /**
   * Flushes buffered appends for all logs.
   */
  void flushAll();

  // Called by `BufferedWriterSingleLog' instances to inform of possible
  // changes in flushableness
  void setFlushable(BufferedWriterSingleLog& log, const bool flushable) {
    if (log.flushable_list_hook_.is_linked() != flushable) {
      if (flushable) {
        flushable_logs_.push_back(log);
      } else {
        log.flushable_list_hook_.unlink();
      }
    }
  }

  const buffered_writer_id_t id_;
  const std::function<LogOptions(logid_t)> get_log_options_;
  BufferedWriterImpl* parent_;

 private:
  // Intrusive list of `BufferedWriterSingleLog' instances for which
  // flushAll() has some work to do (i.e. there is a Batch instance in the
  // BUILDING state that can be flushed).
  //
  // Without this flushAll() would have to iterate through all `logs_' even
  // though many of them may not have anything to flush.  This has caused a
  // performance issue in the past on a long-running client architected to
  // take advantage of flushAll().
  folly::IntrusiveList<BufferedWriterSingleLog,
                       &BufferedWriterSingleLog::flushable_list_hook_>
      flushable_logs_;

  // This needs to be destroyed before flushable_logs_ because
  // ~BufferedWriterSingleLog() may access flushable_logs_. (That access is
  // pretty pointless since we're not really going to do any flushing, but
  // that's how it's implemented right now.)
  std::unordered_map<logid_t, BufferedWriterSingleLog, logid_t::Hash> logs_;
};
}} // namespace facebook::logdevice
