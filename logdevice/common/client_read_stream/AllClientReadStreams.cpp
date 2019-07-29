/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/client_read_stream/AllClientReadStreams.h"

#include <folly/small_vector.h>

#include "logdevice/common/AdminCommandTable.h"
#include "logdevice/common/DataRecordOwnsPayload.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/protocol/STOP_Message.h"
#include "logdevice/common/request_util.h"
#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice {

void AllClientReadStreams::insertAndStart(
    std::unique_ptr<ClientReadStream>&& stream) {
  read_stream_id_t id = stream->getID();
  auto insert_result = streams_.insert(std::make_pair(id, std::move(stream)));
  ld_check(insert_result.second); // new read streams should have unique IDs
  // Starting stream after insert, as start() might need to look up the calling
  // instance.
  insert_result.first->second->start();
}

void AllClientReadStreams::erase(read_stream_id_t id) {
  auto it = streams_.find(id);
  if (it != streams_.end()) {
    streams_.erase(it);
  }
}

void AllClientReadStreams::onDataRecord(
    ShardID shard,
    logid_t log_id,
    read_stream_id_t read_stream_id,
    std::unique_ptr<DataRecordOwnsPayload>&& record) {
  auto it = streams_.find(read_stream_id);
  if (it == streams_.end()) {
    // If the stream no longer exists on the client, tell the server to stop
    // sending.
    Worker* worker = Worker::onThisThread();
    ld_check(worker);

    STOP_Header header{log_id, read_stream_id, shard.shard()};
    auto msg = std::make_unique<STOP_Message>(header);
    worker->sender().sendMessage(std::move(msg), shard.asNodeID());
    return;
  }

  ClientReadStream& stream = *it->second;
  stream.onDataRecord(shard, std::move(record));
}

void AllClientReadStreams::onStartSent(read_stream_id_t id,
                                       ShardID shard,
                                       Status status) {
  auto ptr = getStream(id);
  if (ptr) {
    ptr->onStartSent(shard, status);
  }
}

void AllClientReadStreams::onStarted(ShardID from, const STARTED_Message& msg) {
  auto ptr = getStream(msg.header_.read_stream_id);
  if (ptr) {
    ptr->onStarted(from, msg);
  }
}

void AllClientReadStreams::onGap(ShardID shard, const GAP_Message& msg) {
  auto ptr = getStream(msg.getHeader().read_stream_id);
  if (ptr) {
    ptr->onGap(shard, msg);
  }
}

void AllClientReadStreams::onReaderProgress(read_stream_id_t id) {
  auto ptr = getStream(id);
  if (ptr) {
    ptr->onReaderProgress();
  } else {
    ld_spew("could not find ClientReadStream with id %ld", id.val_);
  }
}

void AllClientReadStreams::forEachStream(
    std::function<void(ClientReadStream& read_stream)> cb) {
  ld_check(cb);

  // We want it to be safe for `cb` to cause destruction of any stream inside
  // `streams_`. This is why we first store all read stream ids into this vector
  // and verify the streams do still exist as we iterate on it.
  folly::small_vector<read_stream_id_t> read_streams;
  read_streams.reserve(streams_.size());
  for (const auto& it : streams_) {
    read_streams.push_back(it.first);
  }

  for (read_stream_id_t id : read_streams) {
    ClientReadStream* s = getStream(id);
    if (s) {
      cb(*s);
    }
  }
}

void AllClientReadStreams::noteConfigurationChanged() {
  // noteConfigurationChanged() may delete some ClientReadStream instances but
  // forEachStream makes that safe.
  forEachStream([](ClientReadStream& read_stream) {
    read_stream.noteConfigurationChanged();
  });
}

void AllClientReadStreams::noteSettingsUpdated() {
  forEachStream(
      [](ClientReadStream& read_stream) { read_stream.onSettingsUpdated(); });
}

void AllClientReadStreams::onShardStatusChanged() {
  // onShardStatusChanged() may delete some ClientReadStream instances but
  // forEachStream makes that safe.
  forEachStream([](ClientReadStream& read_stream) {
    read_stream.applyShardStatus("onShardStatusChanged");
  });
}

ClientReadStream* AllClientReadStreams::getStream(read_stream_id_t id) {
  auto it = streams_.find(id);
  if (it == streams_.end()) {
    return nullptr;
  }
  return it->second.get();
}

void AllClientReadStreams::getReadStreamsDebugInfo(
    InfoClientReadStreamsTable& table) const {
  for (auto& it : streams_) {
    it.second->getDebugInfo(table);
  }
}

void AllClientReadStreams::sampleAllReadStreamsDegubInfoToScuba() const {
  for (const auto& stream : streams_) {
    if (stream.second) {
      stream.second->sampleDebugInfo(
          stream.second->getClientReadStreamDebugInfo());
    }
  }
}

std::string
AllClientReadStreams::getAllReadStreamsDebugInfo(bool pretty,
                                                 bool json,
                                                 Processor& processor) {
  // DO NOT rename these columns or you will break monitoring scripts!
  InfoClientReadStreamsTable table(pretty,
                                   "Log ID",
                                   "Id",
                                   "Next LSN to deliver",
                                   "Window high",
                                   "Until LSN",
                                   "Read set size",
                                   "Gap end outside window",
                                   "Trim point",
                                   "Gap shards next lsn",
                                   "Unavailable shards",
                                   "Connection health",
                                   "Redelivery in-progress",
                                   "Filter Version",
                                   "Shards Down",
                                   "Shards Slow",
                                   "Bytes Lagged",
                                   "Timestamp Lagged",
                                   "Last Time Lagging",
                                   "Last Time Stuck",
                                   "Last Reported State",
                                   "Last Tail Info",
                                   "Time Lag Record");

  auto tables = run_on_all_workers(&processor, [&]() {
    InfoClientReadStreamsTable t(table);
    Worker* w = Worker::onThisThread();
    w->clientReadStreams().getReadStreamsDebugInfo(t);
    return t;
  });

  for (int i = 0; i < tables.size(); ++i) {
    table.mergeWith(std::move(tables[i]));
  }

  return table.toString(json);
}

}} // namespace facebook::logdevice
