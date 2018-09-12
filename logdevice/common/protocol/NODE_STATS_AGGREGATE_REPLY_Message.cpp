/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "NODE_STATS_AGGREGATE_REPLY_Message.h"

#include "logdevice/common/Address.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/protocol/ProtocolReader.h"
#include "logdevice/common/protocol/ProtocolWriter.h"

namespace facebook { namespace logdevice {

NODE_STATS_AGGREGATE_REPLY_Message::NODE_STATS_AGGREGATE_REPLY_Message(
    NODE_STATS_AGGREGATE_REPLY_Header header,
    BucketedNodeStats stats)
    : Message(MessageType::NODE_STATS_AGGREGATE_REPLY, TrafficClass::HANDSHAKE),
      header_(std::move(header)),
      stats_(std::move(stats)) {
  ld_check(header_.node_count * header_.bucket_count ==
           stats_.summed_counts->num_elements());

  ld_check(header_.node_count * header_.bucket_count *
               header_.separate_client_count ==
           stats_.client_counts->num_elements());
}

NODE_STATS_AGGREGATE_REPLY_Message::NODE_STATS_AGGREGATE_REPLY_Message()
    : Message(MessageType::NODE_STATS_AGGREGATE_REPLY,
              TrafficClass::HANDSHAKE){};

void NODE_STATS_AGGREGATE_REPLY_Message::serialize(
    ProtocolWriter& writer) const {
  writer.write(header_.msg_id);
  writer.write(header_.node_count);
  writer.write(header_.bucket_count);
  if (writer.proto() >=
      Compatibility::ProtocolVersion::WORST_CLIENT_FOR_BOYCOTT) {
    writer.write(header_.separate_client_count);
  }

  writer.writeVector(stats_.node_ids);

  if (writer.proto() >=
      Compatibility::ProtocolVersion::WORST_CLIENT_FOR_BOYCOTT) {
    writeCountsForVersionWorstClientForBoycott(writer);
  } else {
    writeCountsForVersionNodeStatsAggregate(writer);
  }
}

MessageReadResult
NODE_STATS_AGGREGATE_REPLY_Message::deserialize(ProtocolReader& reader) {
  std::unique_ptr<NODE_STATS_AGGREGATE_REPLY_Message> msg(
      new NODE_STATS_AGGREGATE_REPLY_Message());

  msg->read(reader);

  return reader.resultMsg(std::move(msg));
}

void NODE_STATS_AGGREGATE_REPLY_Message::read(ProtocolReader& reader) {
  reader.read(&header_.msg_id);
  reader.read(&header_.node_count);
  reader.read(&header_.bucket_count);

  if (reader.proto() >=
      Compatibility::ProtocolVersion::WORST_CLIENT_FOR_BOYCOTT) {
    reader.read(&header_.separate_client_count);
  }

  reader.readVector(&stats_.node_ids, header_.node_count);

  if (reader.proto() >=
      Compatibility::ProtocolVersion::WORST_CLIENT_FOR_BOYCOTT) {
    readCountsForVersionWorstClientsForBoycott(reader);
  } else {
    readCountsForVersionNodeStatsAggregate(reader);
  }
}

Message::Disposition
NODE_STATS_AGGREGATE_REPLY_Message::onReceived(const Address& /*from*/) {
  // server/NODE_STATS_AGGREGATE_REPLY_onReceived.cpp should be called instead
  std::abort();
}

uint16_t NODE_STATS_AGGREGATE_REPLY_Message::getMinProtocolVersion() const {
  return Compatibility::NODE_STATS_AGGREGATE;
}

void NODE_STATS_AGGREGATE_REPLY_Message::
    writeCountsForVersionWorstClientForBoycott(ProtocolWriter& writer) const {
  ld_check(writer.proto() >=
           Compatibility::ProtocolVersion::WORST_CLIENT_FOR_BOYCOTT);

  ld_check(header_.node_count * header_.bucket_count ==
           stats_.summed_counts->num_elements());
  for (int i = 0; i < stats_.summed_counts->num_elements(); ++i) {
    auto& element = stats_.summed_counts->data()[i];
    writer.write(element.client_count);
    writer.write(element.successes);
    writer.write(element.fails);
  }

  ld_check(header_.node_count * header_.bucket_count *
               header_.separate_client_count ==
           stats_.client_counts->num_elements());
  for (int i = 0; i < stats_.client_counts->num_elements(); ++i) {
    auto& element = stats_.client_counts->data()[i];
    writer.write(element.successes);
    writer.write(element.fails);
  }
}

void NODE_STATS_AGGREGATE_REPLY_Message::
    writeCountsForVersionNodeStatsAggregate(ProtocolWriter& writer) const {
  ld_check(writer.proto() >=
               Compatibility::ProtocolVersion::NODE_STATS_AGGREGATE &&
           writer.proto() <
               Compatibility::ProtocolVersion::WORST_CLIENT_FOR_BOYCOTT);

  std::vector<uint32_t> successes;
  std::vector<uint32_t> fails;
  successes.reserve(stats_.node_ids.size());
  fails.reserve(stats_.node_ids.size());

  ld_check(header_.node_count * header_.bucket_count ==
           stats_.summed_counts->num_elements());
  for (int node_idx = 0; node_idx < stats_.summed_counts->shape()[0];
       ++node_idx) {
    // previous protocol expects buckets ordered by oldest first, while in
    // the new protocol it's sent with newest first
    for (int period_idx = stats_.summed_counts->shape()[1] - 1; period_idx >= 0;
         --period_idx) {
      const auto& element = (*stats_.summed_counts)[node_idx][period_idx];
      successes.emplace_back(element.successes);
      fails.emplace_back(element.fails);
    }
  }
  writer.writeVector(successes);
  writer.writeVector(fails);
}

void NODE_STATS_AGGREGATE_REPLY_Message::
    readCountsForVersionWorstClientsForBoycott(ProtocolReader& reader) {
  ld_check(reader.proto() >=
           Compatibility::ProtocolVersion::WORST_CLIENT_FOR_BOYCOTT);

  stats_.summed_counts->resize(
      boost::extents[header_.node_count][header_.bucket_count]);

  for (int i = 0; i < stats_.summed_counts->num_elements(); ++i) {
    auto& element = stats_.summed_counts->data()[i];
    reader.read(&element.client_count);
    reader.read(&element.successes);
    reader.read(&element.fails);
  }

  stats_.client_counts->resize(
      boost::extents[header_.node_count][header_.bucket_count]
                    [header_.separate_client_count]);
  for (int i = 0; i < stats_.client_counts->num_elements(); ++i) {
    auto& element = stats_.client_counts->data()[i];
    reader.read(&element.successes);
    reader.read(&element.fails);
  }
}

void NODE_STATS_AGGREGATE_REPLY_Message::readCountsForVersionNodeStatsAggregate(
    ProtocolReader& reader) {
  ld_check(reader.proto() >=
               Compatibility::ProtocolVersion::NODE_STATS_AGGREGATE &&
           reader.proto() <
               Compatibility::ProtocolVersion::WORST_CLIENT_FOR_BOYCOTT);

  stats_.summed_counts->resize(
      boost::extents[header_.node_count][header_.bucket_count]);

  std::vector<uint32_t> successes;
  std::vector<uint32_t> fails;

  reader.readVector(&successes, stats_.summed_counts->num_elements());
  reader.readVector(&fails, stats_.summed_counts->num_elements());

  ld_check(successes.size() == header_.node_count * header_.bucket_count);
  ld_check(fails.size() == header_.node_count * header_.bucket_count);

  for (int node_idx = 0; node_idx < header_.node_count; ++node_idx) {
    for (int period_idx = 0; period_idx < header_.bucket_count; ++period_idx) {
      auto& element =
          (*stats_.summed_counts)[node_idx]
                                 // reverse the order, the old protocol orders
                                 // by oldest, the new protocol orders by newest
                                 [(header_.bucket_count - 1) - period_idx];
      // can make no guarantees about the amount of clients since it's not
      // tracked in the old protocol
      element.client_count = 1;

      auto flattened_idx = node_idx * header_.bucket_count + period_idx;
      ld_check(flattened_idx < successes.size());
      ld_check(flattened_idx < fails.size());

      element.successes += successes[flattened_idx];
      element.fails += fails[flattened_idx];
    }
  }
}
}} // namespace facebook::logdevice
