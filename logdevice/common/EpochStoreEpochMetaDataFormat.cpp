/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/EpochStoreEpochMetaDataFormat.h"

#include <cstdio>
#include <cstring>

#include "logdevice/include/Record.h"

namespace facebook { namespace logdevice {

namespace EpochStoreEpochMetaDataFormat {

int fromLinearBuffer(const char* buf,
                     size_t size,
                     EpochMetaData* metadata,
                     logid_t logid,
                     const NodesConfiguration& cfg,
                     NodeID* nid_out) {
  if (!buf || !metadata || size > BUFFER_LEN_MAX) {
    err = E::INVALID_PARAM;
    return -1;
  }

  metadata->reset();
  // need to ensure a null-terminate linear buffer so that we can safely
  // use sscanf(3)
  char val[BUFFER_LEN_MAX + 1];
  memcpy(val, buf, size);
  val[size] = '\0';

  if (!val[0]) {
    // empty buffer
    err = E::EMPTY;
    return -1;
  }

  uint32_t epoch_val;
  NodeID nid;
  node_index_t node_index = 0;
  node_gen_t node_generation;
  int size_parsed = 0;
  // use "%n" to collect num of characters parsed
  int nfields = sscanf(val,
                       "%u@N%hd:%hu#%n",
                       &epoch_val,
                       &node_index,
                       &node_generation,
                       &size_parsed);

  if (size_parsed == 0 && nfields >= 1 && nfields < 3) {
    // NodeID not found, try again without the nodeID format
    // to get the correct size_parsed
    nfields = sscanf(val, "%u#%n", &epoch_val, &size_parsed);
  }

  if (nfields < 1 || size_parsed <= 0 || size_parsed > size) {
    err = E::BADMSG;
    return -1;
  }

  if (val[size_parsed - 1] != '#') {
    // binary data section must begin with '#'
    err = E::BADMSG;
    return -1;
  }

  if (nfields >= 3) {
    nid = NodeID(node_index, node_generation);
    if (!nid.isNodeID()) {
      err = E::BADMSG;
      return -1;
    }
  }

  Payload payload(buf + size_parsed, size - size_parsed);
  if (metadata->fromPayload(payload, logid, cfg) ||
      metadata->h.epoch.val_ != epoch_val) {
    err = E::BADMSG;
    return -1;
  }
  if (nid_out) {
    *nid_out = nid;
  }
  // fromPayload ensures the metadata is valid
  ld_check(metadata->isValid());
  return 0;
}

int toLinearBuffer(const EpochMetaData& metadata,
                   char* buf,
                   size_t size,
                   const folly::Optional<NodeID>& node_id) {
  if (!metadata.isValid() || !buf || size == 0 ||
      (node_id && !node_id->isNodeID())) {
    err = E::INVALID_PARAM;
    return -1;
  }

  if (size < sizeInLinearBuffer(metadata, node_id)) {
    err = E::NOBUFS;
    return -1;
  }

  int header_size = 0;
  if (node_id) {
    header_size = snprintf(buf,
                           size,
                           "%u@N%hd:%hu#",
                           metadata.h.epoch.val_,
                           node_id->index(),
                           node_id->generation());
  } else {
    header_size = snprintf(buf, size, "%u#", metadata.h.epoch.val_);
  }

  ld_check(header_size > 0);
  int len = metadata.toPayload(buf + header_size, size - header_size);
  // we already confirmed that the record is valid and buf has enough size
  ld_check(len > 0);
  ld_assert(len + header_size == sizeInLinearBuffer(metadata, node_id));
  return header_size + len;
}

int sizeInLinearBuffer(const EpochMetaData& metadata,
                       const folly::Optional<NodeID>& node_id) {
  if (!metadata.isValid() || (node_id && !node_id->isNodeID())) {
    err = E::INVALID_PARAM;
    return -1;
  }

  int header_size = 0;
  // dry-run snprintf to get the size of the ascii header
  if (node_id) {
    header_size = snprintf(nullptr,
                           0,
                           "%u@N%hd:%hu#",
                           metadata.h.epoch.val_,
                           node_id->index(),
                           node_id->generation());
  } else {
    header_size = snprintf(nullptr, 0, "%u#", metadata.h.epoch.val_);
  }
  ld_check(header_size > 0);
  return header_size + metadata.sizeInPayload();
}

}}} // namespace facebook::logdevice::EpochStoreEpochMetaDataFormat
