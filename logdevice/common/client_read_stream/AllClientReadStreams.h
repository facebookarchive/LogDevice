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

#include "logdevice/common/AdminCommandTable-fwd.h"
#include "logdevice/common/ShardAuthoritativeStatusMap.h"
#include "logdevice/common/client_read_stream/ClientReadStream.h"
#include "logdevice/common/protocol/GAP_Message.h"
#include "logdevice/common/protocol/STARTED_Message.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/types.h"

/**
 * @file Container for all read streams active for a single client.
 *
 *       This class is not thread-safe.  Each worker thread is meant to have
 *       its own instance.  The read stream for any one log is assumed to be
 *       pinned to a worker thread.
 */

namespace facebook { namespace logdevice {

struct DataRecordOwnsPayload;
class Processor;

class AllClientReadStreams : public ShardAuthoritativeStatusSubscriber {
 public:
  /**
   * Claims ownership of the ClientReadStream, and kicks off reading by
   * calling start() on it.
   */
  void insertAndStart(std::unique_ptr<ClientReadStream>&& stream);

  /**
   * Deletes a ClientReadStream object.  Called when the application wants to
   * stop reading a log.
   */
  void erase(read_stream_id_t id);

  /**
   * Delivers a record to the correct read stream.  Called by
   * RECORD_Message::onReceived().
   */
  void onDataRecord(ShardID shard,
                    logid_t log_id,
                    read_stream_id_t read_stream_id,
                    std::unique_ptr<DataRecordOwnsPayload>&& record);

  /**
   * Informs the appropriate ClientReadStream of the outcome of trying to send
   * a START message to a shard.
   */
  void onStartSent(read_stream_id_t id, ShardID shard, Status status);

  /**
   * Delivers a STARTED message to the correct read stream.  Called by
   * STARTED_Message::onReceived().
   */
  void onStarted(ShardID from, const STARTED_Message& msg);

  /**
   * Delivers a GAP message to the correct read stream. Called by
   * GAP_Message::onReceived().
   */
  void onGap(ShardID shard, const GAP_Message& msg);

  /**
   * Delivers a ReaderProgressRequest to the correct read stream.
   */
  void onReaderProgress(read_stream_id_t id);

  /**
   * Called when cluster configuration has been updated.
   */
  void noteConfigurationChanged();

  /**
   * Called when the settings have been updated.
   */
  void noteSettingsUpdated();

  void onShardStatusChanged() override;

  /**
   * Forces the map to get cleared and all read streams destroyed.
   */
  void clear() {
    streams_.clear();
  }

  // A helper method for getting ClientReadStream instances from streams_
  ClientReadStream* getStream(read_stream_id_t id);

  void getReadStreamsDebugInfo(InfoClientReadStreamsTable& table) const;

  /**
   * A convenient function for finding all the ClientReadStream instances
   * running on all workers of a Processor and building a dump of their state.
   * WARNING: this function is blocking.
   */
  static std::string getAllReadStreamsDebugInfo(bool pretty,
                                                bool json,
                                                Processor& processor);

  /**
   * Run a function for all read streams. It is safe to have this
   * function to remove the stream from `streams_`.
   *
   * @param cb function to run for all read streams.
   */
  void forEachStream(std::function<void(ClientReadStream& read_stream)> cb);

  void sampleAllReadStreamsDegubInfoToScuba() const;

 private:
  // Actual container
  std::unordered_map<read_stream_id_t,
                     std::unique_ptr<ClientReadStream>,
                     read_stream_id_t::Hash>
      streams_;
};

}} // namespace facebook::logdevice
