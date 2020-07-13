/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

include "common/fb303/if/fb303.thrift"
include "logdevice/common/if/api_model.thrift"

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
   api_model.SessionResponse, stream<api_model.Message> createSession(1:
      api_model.SessionRequest request);

   /**
    * Sends single message from the client to the server
    */
   api_model.MessageReceipt sendMessage(1: api_model.Message message);

   // Compatibility part of API ends
}
