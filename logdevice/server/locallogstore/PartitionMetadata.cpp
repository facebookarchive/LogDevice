/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/locallogstore/PartitionMetadata.h"

#include <algorithm>

#include "logdevice/common/util.h"

namespace facebook { namespace logdevice {

PartitionDirtyMetadata::PartitionDirtyMetadata(const DirtiedByMap& dbm,
                                               bool under_replicated)
    : under_replicated_(under_replicated) {
  for (const auto& kv : dbm) {
    if (kv.second.dirtyUntil() != FlushToken_INVALID) {
      node_index_t nidx;
      DataClass dc;
      std::tie(nidx, dc) = kv.first;
      dirtied_by_[(size_t)dc].push_back(nidx);
    }
  }
  // Sorting isn't necessary, but makes debugging/comparing records easier.
  for (auto& dv : dirtied_by_) {
    std::sort(dv.begin(), dv.end());
  }
}

Slice PartitionDirtyMetadata::serialize() const {
  Header h;

  h.len = sizeof(h);
  uint32_t record_size = h.len;
  h.flags = under_replicated_ ? Header::UNDER_REPLICATED : 0;
  h.dnc_array_offset = 0;
  h.dnc_array_len = 0;
  for (auto& dv : dirtied_by_) {
    if (!dv.empty()) {
      h.dnc_array_len += sizeof(DirtyNodesForClass);
      record_size +=
          sizeof(DirtyNodesForClass) + sizeof(node_index_t) * dv.size();
    }
  }
  if (h.dnc_array_len != 0) {
    h.dnc_array_offset = h.len;
  }

  serialize_buffer_.resize(record_size);
  memcpy(serialize_buffer_.data(), &h, sizeof(h));

  uint32_t dnc_offset = h.dnc_array_offset;
  uint32_t data_offset =
      std::max((uint32_t)h.len, h.dnc_array_offset + h.dnc_array_len);
  size_t dci = 0;
  for (const auto& dv : dirtied_by_) {
    DirtyNodesForClass dnc;

    if (!dv.empty()) {
      dnc.len = sizeof(dnc);
      dnc.data_class = dci;
      dnc.nid_array_offset = data_offset;
      dnc.nid_array_len = sizeof(node_index_t) * dv.size();

      memcpy(serialize_buffer_.data() + dnc_offset, &dnc, sizeof(dnc));
      dnc_offset += sizeof(dnc);

      memcpy(
          serialize_buffer_.data() + data_offset, dv.data(), dnc.nid_array_len);
      data_offset += dnc.nid_array_len;
    }
    dci++;
  }
  ld_check(record_size == data_offset);

  return Slice(serialize_buffer_.data(), record_size);
}

int PartitionDirtyMetadata::deserialize(Slice blob) {
  // Reset to null state
  for (auto& dv : dirtied_by_) {
    dv.clear();
  }

  Header h;
  const uint8_t* data = static_cast<const uint8_t*>(blob.data);
  if (blob.size < sizeof(h)) {
    ld_check(false);
    err = E::MALFORMED_RECORD;
    return -1;
  }

  memcpy(&h, data, sizeof(h));
  if (h.len < sizeof(h) || h.len > blob.size ||
      (h.dnc_array_len != 0 &&
       (h.dnc_array_offset < h.len || h.dnc_array_offset > blob.size))) {
    ld_check(false);
    err = E::MALFORMED_RECORD;
    return -1;
  }

  under_replicated_ = (h.flags & Header::UNDER_REPLICATED) != 0;

  uint32_t dnc_offset = h.dnc_array_offset;
  uint32_t data_offset = dnc_offset + h.dnc_array_len;
  while (dnc_offset < blob.size &&
         (dnc_offset - h.dnc_array_offset) < h.dnc_array_len) {
    DirtyNodesForClass dnc;

    if ((blob.size - dnc_offset) < sizeof(dnc)) {
      ld_check(false);
      err = E::MALFORMED_RECORD;
      return -1;
    }

    memcpy(&dnc, data + dnc_offset, sizeof(dnc));
    dnc_offset += dnc.len;

    if (dnc.nid_array_offset < data_offset ||
        dnc.nid_array_offset >= blob.size ||
        (dnc.nid_array_len % sizeof(node_index_t)) != 0 ||
        (blob.size - dnc.nid_array_offset) < dnc.nid_array_len) {
      ld_check(false);
      err = E::MALFORMED_RECORD;
      return -1;
    }
    data_offset = dnc.nid_array_offset + dnc.nid_array_len;

    if (static_cast<DataClass>(dnc.data_class) >= DataClass::MAX) {
      // Forward compatibility
      continue;
    }

    if (dnc.nid_array_len != 0) {
      auto& dv = dirtied_by_[dnc.data_class];
      dv.resize(dnc.nid_array_len / sizeof(node_index_t));
      memcpy(dv.data(), data + dnc.nid_array_offset, dnc.nid_array_len);
    }
  }

  return 0;
}

std::string PartitionDirtyMetadata::toString() const {
  std::string str = "PartitionDirtyMetadata (";
  size_t dci = 0;
  for (const auto& dv : dirtied_by_) {
    if (!dv.empty()) {
      str += "{";
      str += dataClassPrefixes()[dci];
      str += ":";
      bool first_node = true;
      for (auto& node : dv) {
        if (!first_node) {
          str += ",";
        }
        first_node = false;
        str += std::to_string(node);
      }
      str += "}";
    }
    dci++;
  }
  str += ")";
  return str;
}

}} // namespace facebook::logdevice
