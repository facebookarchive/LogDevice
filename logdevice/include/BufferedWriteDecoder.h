/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>
#include <vector>

#include "logdevice/include/Record.h"

namespace facebook { namespace logdevice {

/**
 * @file Extracts original payloads from a set of records read from LogDevice,
 * that were written to LogDevice using the BufferedWriter utility class.
 *
 * This class is not thread-safe; all calls must be made on the same thread.
 *
 * See logdevice/test/BufferedWriterTest.cpp for an example of how to use.
 */

class BufferedWriteDecoderImpl;

class BufferedWriteDecoder {
 public:
  /**
   * Creates a BufferedWriteDecoder instance.  It can be used to decode
   * batched writes made by any BufferedWriter.
   */
  static std::unique_ptr<BufferedWriteDecoder> create();

  /**
   * Returns the number of individual records stored in a single
   * DataRecord.
   */
  static int getBatchSize(const DataRecord& record, size_t* size_out);

  /**
   * This method is meant to be used with data records returned by the Reader
   * API.
   *
   * Original payloads are appended to `payloads_out' and they point into
   * memory owned by this decoder after the call has returned.  It is only
   * safe to use the returned Payload instances as long as this decoder still
   * exists.  (For any successfully decoded DataRecord instances, this class
   * assumes ownership of the payloads, which is why `records' is taken by
   * reference.)
   *
   * If is fine to use the same decoder instance to decode multiple batches of
   * records read from LogDevice. However, the decoder pins memory so it should
   * typically be short-lived (its lifetime is tied to the Payload instances in
   * payloads_out as explained above).
   *
   * @returns On success, returns 0.  If some DataRecord's failed to decode,
   *          return -1, leaving malformed records in `records'.
   */
  int decode(std::vector<std::unique_ptr<DataRecord>>&& records,
             std::vector<Payload>& payloads_out);

  /**
   * Same as decode() but for a single record only.
   *
   * @returns On success, returns 0.  If the DataRecord failed to decode,
   *          return -1.
   */
  int decodeOne(std::unique_ptr<DataRecord>&& record,
                std::vector<Payload>& payloads_out);

  virtual ~BufferedWriteDecoder() {}

 private:
  BufferedWriteDecoder() {} // can be constructed by the factory only
  BufferedWriteDecoder(const BufferedWriteDecoder&) = delete;
  BufferedWriteDecoder& operator=(const BufferedWriteDecoder&) = delete;

  friend class BufferedWriteDecoderImpl;
  BufferedWriteDecoderImpl* impl(); // downcasts (this)
};
}} // namespace facebook::logdevice
