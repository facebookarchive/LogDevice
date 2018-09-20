/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/Request.h"
#include "logdevice/common/protocol/FixedSizeMessage.h"

namespace facebook { namespace logdevice {

struct MEMTABLE_FLUSHED_Header {
  explicit MEMTABLE_FLUSHED_Header(FlushToken memtable_id,
                                   ServerInstanceId server_instance_id,
                                   uint32_t shard_idx,
                                   node_index_t node_index)
      : memtable_id_(memtable_id),
        server_instance_id_(server_instance_id),
        shard_idx_(shard_idx),
        node_index_(node_index) {}

  MEMTABLE_FLUSHED_Header() = default;

  FlushToken memtable_id_;
  ServerInstanceId server_instance_id_;
  uint32_t shard_idx_;
  node_index_t node_index_;
  uint8_t padding_[2] = {};
};

static_assert(sizeof(MEMTABLE_FLUSHED_Header) ==
                  (sizeof(MEMTABLE_FLUSHED_Header::memtable_id_) +
                   sizeof(MEMTABLE_FLUSHED_Header::server_instance_id_) +
                   sizeof(MEMTABLE_FLUSHED_Header::shard_idx_) +
                   sizeof(MEMTABLE_FLUSHED_Header::node_index_) +
                   sizeof(MEMTABLE_FLUSHED_Header::padding_)),
              "MEMTABLE_FLUSHED_Header is not aligned");

using MEMTABLE_FLUSHED_Message = FixedSizeMessage<MEMTABLE_FLUSHED_Header,
                                                  MessageType::MEMTABLE_FLUSHED,
                                                  TrafficClass::REBUILD>;

}} // namespace facebook::logdevice
