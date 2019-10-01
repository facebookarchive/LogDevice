/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/ConnectionListener.h"

#include <gtest/gtest.h>

using namespace facebook::logdevice;

namespace facebook { namespace logdevice {
struct CLTParams {
  explicit CLTParams(TLSHeader const& buf, bool expected)
      : buf(buf), expected(expected) {}
  TLSHeader const& buf;
  bool expected;
};

class ConnectionListenerTest {
 public:
  void isTLSHeaderTest(CLTParams params) {
    bool tlsState = ConnectionListener::isTLSHeader(params.buf);
    EXPECT_EQ(params.expected, tlsState);
  }
};
}} // namespace facebook::logdevice

TEST(ConnectionListenerTest, IsTLSHeader) {
  ConnectionListenerTest conn_listen_test;
  TLSHeader input_buf = {SecureConnectionTag::SSL_HANDSHAKE_RECORD_TAG,
                         SecureConnectionTag::TLS_TAG,
                         SecureConnectionTag::TLS_MIN_PROTOCOL};
  // Test correct protocol versions
  while (input_buf[2] <= SecureConnectionTag::TLS_MAX_PROTOCOL) {
    CLTParams params{input_buf, true};
    conn_listen_test.isTLSHeaderTest(params);
    input_buf[2]++;
  }
  // Test incorrect protocol version
  input_buf = {SecureConnectionTag::SSL_HANDSHAKE_RECORD_TAG,
               SecureConnectionTag::TLS_TAG,
               SecureConnectionTag::TLS_MAX_PROTOCOL + 1};
  CLTParams params{input_buf, false};
  conn_listen_test.isTLSHeaderTest(params);

  // Test SSLv3 protocol version
  input_buf = {SecureConnectionTag::SSL_HANDSHAKE_RECORD_TAG,
               SecureConnectionTag::TLS_TAG,
               0x00};
  CLTParams paramsSsl{input_buf, false};
  conn_listen_test.isTLSHeaderTest(paramsSsl);
}
