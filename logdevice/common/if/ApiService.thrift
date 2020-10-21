/*
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
include "common/fb303/if/fb303.thrift"
include "logdevice/common/if/ApiModel.thrift"
include "logdevice/server/digest/if/Digest.thrift"

namespace cpp2 facebook.logdevice.thrift

// *** LogDevice server API
service LogDeviceAPI extends fb303.FacebookService {
  // Compatibility part of API starts
  //
  // These methods used as a temporary solution to enable Thrift migration and
  // emulate existing RPC protocol on top the Thrift.

  /**
    * Opens stream enabling the server to send messages back to
    * the client outside of normal RPC request-response rounds
    */
  ApiModel.SessionResponse, stream<ApiModel.Message> createSession(
    1: ApiModel.SessionRequest request
  );

  /**
    * Sends single message from the client to the server
    */
  ApiModel.MessageReceipt sendMessage(1: ApiModel.Message message);
  //
  // Compatibility part of API ends

  // 1. Log Digest Service - https://fburl.com/ld-digest-service
  /**
    * Query a snapshot of the log digest within a given window. This is a
    * short-lived RPC and the server will terminate it when the response stream
    * is finished.
    */
  Digest.QueryResponse, stream<Digest.QueryStreamResponse>
      query(1: Digest.QueryRequest request) (cpp.coroutine);

  /**
    * Subscribe to the log digest with an optional initial backfill. This is a
    * long-lived RPC and the server will push the latest updates of the digest
    * to the stream as they come.
    */
  Digest.SubscribeResponse, stream<Digest.SubscribeStreamResponse>
      subscribe(1: Digest.SubscribeRequest request) (cpp.coroutine);
}
