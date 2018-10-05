/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/Optional.h>

#include "logdevice/common/types_internal.h"

namespace facebook { namespace logdevice {

class ProtocolWriter;
class ProtocolReader;

/**
 * Abstract type for a piece of data/metadata that can be serialized and
 * deserialized to and from 1) event buffer (i.e., messages) and 2) linear
 * buffer (i.e., memory or persistent storage).
 */

class SerializableData {
 public:
  /**
   * Serializes the object into the buffer handled by ProtocoWriter, taking
   * advantage of optimizations for serialization into evbuffer if the
   * destination is an event buffer.
   *
   * @return  nothing is returned. But if there is an error on serialization,
   *          @param writer should enter error state (i.e., writer.error()
   *          == true).
   */
  virtual void serialize(ProtocolWriter& writer) const = 0;

  /**
   * Fill the content of the object by reading from the buffer handled by the
   * given ProtocolReader.
   *
   * @param  evbuffer_zero_copy  if true, use evbuffer zero copy to get the
   *                             payload. In order to use that, writing to
   *                             evbuffer must be supported by the supplied
   *                             ProtocolReader.
   *
   * @param  expected_size       Optional parameter used by certain type of
   *                             subclasses that cannot determine the expected
   *                             size of the data during deserialization. If set
   *                             and it reads more than expected_size from
   *                             _reader_, reader will be set to E::BADMSG.
   *                             Moreover, deserialize() must consume exactly
   *                             expected_size but it can decide whether or not
   *                             consider trailing bytes as legal. This can be
   *                             ignored by subclasses.
   *
   * @return  nothing is returned. But if there is an error on deserialization,
   *          @param reader should enter error state (i.e., reader.error()
   *          == true).
   */
  virtual void
  deserialize(ProtocolReader& reader,
              bool evbuffer_zero_copy,
              folly::Optional<size_t> expected_size = folly::none) = 0;

  /**
   * @return   the name of the data.
   */
  virtual const char* name() const = 0;

  virtual ~SerializableData() {}

  ////// Utility methods

  /**
   * Copy the serialized data into a pre-allocated @buffer which must
   * be contiguous and have a valid length of @size.
   *
   * @return  num of bytes written on success; -1 on failure, and set err to
   *          the ProtocolWriter status defined in serialize(ProtocolWriter&)
   *          method. Typical errors include:
   *           INVALID_PARAM  object is not valid
   *           NOBUFS         @size is not enough to hold the data
   */
  int serialize(void* buffer, size_t size) const;

  /**
   * Returns serialized size, or -1 if the data cannot be serialized
   * (e.g., invalid).
   */
  ssize_t sizeInLinearBuffer() const {
    return serialize(nullptr, 0);
  }

  /**
   * Fill the content of the object by reading a linear buffer
   *
   * @return  num of bytes read from buffer on success, -1 on failure and
   *          set err to the ProcotolReader status defined in deserialize()
   *          virtual method above. Typical errors include:
   *           BADMSG  content of the buffer does not contain valid data
   */
  int deserialize(Slice buffer,
                  folly::Optional<size_t> expected_size = folly::none);

  int deserialize(void* payload,
                  size_t size,
                  folly::Optional<size_t> expected_size = folly::none) {
    return deserialize(Slice(payload, size), expected_size);
  }
};

}} // namespace facebook::logdevice
