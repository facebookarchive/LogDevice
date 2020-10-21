/*
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 *
 * This file contains the service definition of the Log Digest Service and
 * its related supporting types.
 */
namespace cpp2 facebook.logdevice.thrift.digest

// *** Represents a point in time within a log.
union LogTag {
   // *** LSN.
   1: i64 lsn;
   // *** Storage time, in milliseconds (unix epoch).
   2: i64 storage_time_ms;
}

// *** Request type for the Query RPC.
struct QueryRequest {
   // *** Log ID to get the digest for.
   1: i64 log_id;

   /**
    * Optional start tag. If specified, the digest response will start from
    * this tag. Otherwise it will start from the oldest lsn available.
    */
   2: optional LogTag start_tag;

   /**
    * Optional end tag. If specified, the digest response will end at this
    * storage time. Otherwise it will end at the latest available lsn at
    * the time of processing the request.
    */
   3: optional LogTag end_tag;
}

// *** Request type for the Subscribe RPC.
struct SubscribeRequest {
   // *** Log ID to get the digest for.
   1: i64 log_id;

   /**
    * Optional start tag. If specified, the digest response will start from
    * this tag. Otherwise it will start from the oldest lsn available.
    */
   2: optional LogTag start_tag;
}

// *** A sorted list of LSNs for a log id.
struct DigestFragment {
   1: i64 log_id;
   2: list<i64> lsns;
}

// *** Response type for digest queries.
struct QueryResponse {
}

// *** Stream response type for digest queries.
struct QueryStreamResponse {
   1: DigestFragment fragment;
}

// *** Response type for digest subscriptions.
struct SubscribeResponse {
}

// *** Stream response type for digest subscriptions.
struct SubscribeStreamResponse {
   1: DigestFragment fragment;
}
