/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/protocol/NODE_STATS_AGGREGATE_REPLY_Message.h"

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
  writer.write(header_.separate_client_count);
  writer.writeVector(stats_.node_ids);
  writeCountsForVersionWorstClientForBoycott(writer);
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
  reader.read(&header_.separate_client_count);
  reader.readVector(&stats_.node_ids, header_.node_count);
  readCountsForVersionWorstClientsForBoycott(reader);
}

Message::Disposition
NODE_STATS_AGGREGATE_REPLY_Message::onReceived(const Address& /*from*/) {
  // server/NODE_STATS_AGGREGATE_REPLY_onReceived.cpp should be called instead
  std::abort();
}

void NODE_STATS_AGGREGATE_REPLY_Message::
    writeCountsForVersionWorstClientForBoycott(ProtocolWriter& writer) const {
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
    readCountsForVersionWorstClientsForBoycott(ProtocolReader& reader) {
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

}} // namespace facebook::logdevice
