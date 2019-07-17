/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/EpochMetaData.h"

namespace facebook { namespace logdevice {

/**
 * @file Utility functions for manipulating log epoch metadata stored in
 *       the epoch store.
 */

namespace EpochStoreEpochMetaDataFormat {

// TODO: calculate it based on max size of a nodeset
const size_t BUFFER_LEN_MAX = 4096;

/**
 * Attempts to parse the given buffer to extract epoch metadata information
 * and the (optional) NodeID of logdeviced that was last running
 * a Sequencer for that epoch of the log.
 *
 * @param buf           linear buffer to be read, not nul-terminated
 * @param size          length of @buf in bytes
 * @metadata            EpochMetaData object to fill in
 * @nid_out             if non-nullptr, NodeID extracted from the buffer.
 *                      This will be an invalid NodeID if the buffer is
 *                      empty or does not contain a NodeID
 *
 * @return              0 on success, member metadata should be filled with
 *                      valid EpochMetaData, node_id should have value of a
 *                      valid NodeID if the buffer contains NodeID information.
 *
 *                      -1 if input or the buffer is empty/invalid/malformed,
 *                      and err is set to:
 *                      E::EMPTY         linear buffer is empty
 *                      E::BADMSG        linear buffer is malformed
 *                      E::INVALID_PARAM input params are invalid
 *                      In these cases the metadata object state is invalid.
 */
int fromLinearBuffer(const char* buf,
                     size_t size,
                     EpochMetaData* metadata,
                     logid_t logid,
                     const NodesConfiguration& cfg,
                     NodeID* nid_out = nullptr);

/**
 * Attempt to write the content of the log metadata to a given linear buffer.
 *
 * @param metadata      EpochMetaData object to get the content from
 * @param buf           linear buffer to be written
 * @param size          length of @buf in bytes
 * @nid                 if it has a value and nid is valid, NodeID will also be
 *                      written to the linear buffer
 *
 * @return              on success, return size written in the buffer
 *                      on failure, -1 is returned, and err is set to:
 *                         E::INVALID_PARAM  object is in an invalid state
 *                         E::NOBUFS         not enough buffer space to hold
 *                                           the object
 */
int toLinearBuffer(const EpochMetaData& metadata,
                   char* buf,
                   size_t size,
                   const folly::Optional<NodeID>& node_id);

/**
 * Calculate the size of the object as if it is written to a linear buffer
 * using toLinearBuffer().
 *
 * @param metadata      EpochMetaData object to get the content from
 * @nid                 if it has a value, indicates NodeID to be written
 *
 * @return             on success, return the size estimate in the buffer
 *                     on failure, -1 is returned, and err is set to:
 *                        E::INVALID_PARAM  object is in an invalid state
 */
int sizeInLinearBuffer(const EpochMetaData& metadata,
                       const folly::Optional<NodeID>& node_id);

} // namespace EpochStoreEpochMetaDataFormat

}} // namespace facebook::logdevice
