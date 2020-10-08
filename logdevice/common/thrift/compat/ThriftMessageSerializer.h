/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/io/IOBuf.h>

#include "logdevice/common/if/gen-cpp2/ApiModel_types.h"
#include "logdevice/common/protocol/Message.h"
#include "logdevice/common/protocol/ProtocolHeader.h"

namespace facebook { namespace logdevice {

/**
 * Serializes/deserializes Message object to/from folly::IOBuf and
 * thrift::Message.
 */
class ThriftMessageSerializer {
 public:
  /**
   * @param context Serialization/deserialization context (e.g.
   *                connection description) for logging and debug
   *                purposes.
   */
  explicit ThriftMessageSerializer(const std::string& context)
      : context_(context) {}

  /**
   * Serializes message intto Thrift structure with respect to given
   * protocol version.
   *
   * @param protocol Protocol version to comply
   *
   * @return pointer to Thrift message containing result or nullptr if
   *        serialization failed. In case of failure caller should check
   *        global error code.
   */
  std::unique_ptr<thrift::Message> toThrift(const Message&,
                                            uint16_t protocol) const;

  /**
   * Converts Thrift compatibility message into corresponding Message object.
   *
   * @param protocol Protocol version to comply
   *
   * @return pointer to deserialized Message object or nullptr if
   *         serialization failed. In case of failure caller should check
   *         global error code.
   */
  std::unique_ptr<Message> fromThrift(thrift::Message&&, uint16_t protocol);

 private:
  // Reads header from given IOBuf, returns folly::none iff reading fails
  folly::Optional<ProtocolHeader> readHeader(folly::IOBuf*, uint16_t protocol);
  // Validates received protocol header and returns false iff the header is
  // malformed
  bool validateHeader(const ProtocolHeader&);
  // Reads message body using provided protocol reader
  std::unique_ptr<Message> readMessage(const ProtocolHeader&, ProtocolReader&);
  // Called on message body deserialization error, converts low-level error code
  // to ones expected by client
  void onError(const ProtocolHeader& header, uint16_t protocol);

  const std::string context_;
};

}} // namespace facebook::logdevice
