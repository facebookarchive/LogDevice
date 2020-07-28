/**
 * Copyright (c) 2020-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

namespace cpp2 facebook.logdevice.thrift

// This file defines format for storing multiple PayloadGroups.
// Note that same format is also used to store single PayloadGroup,
// when batching is not used.

typedef binary (cpp2.type = "folly::IOBuf") IOBuf

typedef i32 PayloadKey

// Stores information about payload in payload group
struct PayloadDescriptor {
  // Size of the payload before compression
  1: i32 uncompressed_size;
}

// Wrapper for PayloadDescriptor to allow list of optionals.
struct OptionalPayloadDescriptor {
  1: optional PayloadDescriptor descriptor;
}

// Represents list of payloads in multiple records having the same key.
struct CompressedPayloads {
  // Algorithm used for compressing payload field
  // as defined in facebook::logdevice::Compression
  1: byte compression;
  // List of payload descriptors. i-th descriptor corresponds to i-th record
  // in a batch. If record doesn't have payload with corresponding key, entry
  // in this list has a null decriptor.
  2: list<OptionalPayloadDescriptor> descriptors;
  // Compressed concatenation of all payloads with corresponding key.
  // Original payloads can be reconstructed by uncompressing this field,
  // and then splitting it into pieces of the sizes specified in descriptors.
  3: IOBuf payload;
}

// Top level object for storing a batch of compressed payload groups.
struct CompressedPayloadGroups {
  1: map<PayloadKey, CompressedPayloads> payloads;
}
