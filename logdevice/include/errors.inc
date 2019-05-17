/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
/* can be included multiple times */

#ifndef ERROR_CODE
#error ERROR_CODE() macro not defined
#define ERROR_CODE(...)
#endif


ERROR_CODE(OK                       ,    0, "success")
ERROR_CODE(NOTFOUND                 ,    1, "requested identifier does not exist")
ERROR_CODE(TIMEDOUT                 ,    2, "request or connection timed out")
ERROR_CODE(NOSEQUENCER              ,    3, "no sequencer was found for log")
ERROR_CODE(CONNFAILED               ,    4, "connection failed")
ERROR_CODE(NOTCONN                  ,    5, "not connected to destination")
ERROR_CODE(TOOBIG                   ,    6, "argument or return value too big")
ERROR_CODE(TOOMANY                  ,    7, "operation exceeded limit")
ERROR_CODE(PREEMPTED                ,    8, "log writer was preempted by another one")
ERROR_CODE(NOBUFS                   ,    9, "buffer space limit was reached")
ERROR_CODE(NOMEM                    ,   10, "failure of a global memory allocator") // e.g. malloc()
ERROR_CODE(INTERNAL                 ,   11, "internal error. Likely a bug in LogDevice code.")
// SYSLIMIT: for example, the maximum number of threads. Changing the
// configuration of your execution environment may fix this.
ERROR_CODE(SYSLIMIT                 ,   12, "an operation hit a limit set by the OS or runtime")
ERROR_CODE(TEMPLIMIT                ,   13, "an operation hit a limit but may succeed if retried")
ERROR_CODE(PERMLIMIT                ,   14, "an operation hit a limit, cannot succeed if retried")
ERROR_CODE(ACCESS                   ,   15, "permission denied")
ERROR_CODE(ALREADY                  ,   16, "connection is already in progress")
ERROR_CODE(ISCONN                   ,   17, "socket already connected")
ERROR_CODE(UNREACHABLE              ,   18, "outgoing connections to peer of this socket are not supported")
// UNROUTABLE: most likely a config error
ERROR_CODE(UNROUTABLE               ,   19, "attempt to connect to an address to which there is no route")
ERROR_CODE(BADMSG                   ,   20, "invalid message format")
// DISABLED: connection failed because the server is currently marked down
// (disabled) after a series of unsuccessful connection attempts
ERROR_CODE(DISABLED                 ,   21, "server is marked down")
ERROR_CODE(EXISTS                   ,   22, "object already exists")
ERROR_CODE(SHUTDOWN                 ,   23, "shutting down")
ERROR_CODE(NOTINCONFIG              ,   24, "server is not in cluster configuration")
ERROR_CODE(PROTONOSUPPORT           ,   25, "unsupported LogDevice protocol version")
ERROR_CODE(PROTO                    ,   26, "protocol error")
ERROR_CODE(PEER_CLOSED              ,   27, "connection closed by peer")
ERROR_CODE(SEQNOBUFS                ,   28, "sequencer is out of buffer space for log")
ERROR_CODE(WOULDBLOCK               ,   29, "operation would block")
ERROR_CODE(ABORTED                  ,   30, "operation aborted")
ERROR_CODE(INPROGRESS               ,   31, "operation is already in progress")
ERROR_CODE(CANCELLED                ,   32, "message was cancelled")
// NOTSTORAGE: request that can only be completed by a storage node was sent
// to a node not configured to run a local log store, such as a pure sequencer
// node.
ERROR_CODE(NOTSTORAGE               ,   33, "not a storage node")
ERROR_CODE(AGAIN                    ,   34, "temporary error, try again later")
ERROR_CODE(PARTIAL                  ,   35, "operation partially succeeded")
ERROR_CODE(GAP                      ,   36, "gap in sequence numbers detected")
ERROR_CODE(TRUNCATED                ,   37, "result truncated")
ERROR_CODE(STALE                    ,   38, "object or identifier is stale")
ERROR_CODE(NOSPC                    ,   39, "not enough free disk space on local log store")
ERROR_CODE(OVERLOADED               ,   40, "storage node is overloaded")
ERROR_CODE(PENDING                  ,   41, "have pending Appenders")
ERROR_CODE(PENDING_FULL             ,   42, "pending Appenders queue full")
ERROR_CODE(FAILED                   ,   43, "request failed")
ERROR_CODE(SEQSYSLIMIT              ,   44, "sequencer process has reached a limit set by the OS or runtime")
ERROR_CODE(REBUILDING               ,   45, "rebuilding is in progress")
ERROR_CODE(REDIRECTED               ,   46, "writer has been redirected to a different node")
ERROR_CODE(RETRY                    ,   47, "operation will be retried")
ERROR_CODE(BADPAYLOAD               ,   48, "payload is invalid")
ERROR_CODE(NOSSLCONFIG              ,   49, "SSL not configured for node")
ERROR_CODE(NOTREADY                 ,   50, "operation cannot be processed at this time")
ERROR_CODE(DROPPED                  ,   51, "request was dropped")
ERROR_CODE(FORWARD                  ,   52, "could not forward STORE request to next node in the chain")
ERROR_CODE(NOTSUPPORTED             ,   53, "not supported")
ERROR_CODE(NOTINSERVERCONFIG        ,   54, "identifier is not in server's config")
ERROR_CODE(ISOLATED                 ,   55, "node is isolated")
ERROR_CODE(SSLREQUIRED              ,   56, "Socket has to be upgraded to SSL")
ERROR_CODE(CBREGISTERED             ,   57, "Will call callback to retry operation.")
ERROR_CODE(LOW_ON_SPC               ,   58, "Met low watermark of free disk space on local log store")
ERROR_CODE(PEER_UNAVAILABLE         ,   59, "server is unavailable")
ERROR_CODE(NOTSUPPORTEDLOG          ,   60, "not supported for this log")
ERROR_CODE(DATALOSS                 ,   61, "data is lost")
ERROR_CODE(NEVER_CONNECTED          ,   62, "connection never attempted")
// Errors about invalid entities

ERROR_CODE(INVALID_PARAM            ,  100, "invalid parameter")
ERROR_CODE(INVALID_CLUSTER          ,  101, "invalid LogDevice cluster name")
ERROR_CODE(INVALID_CONFIG           ,  102, "various errors in config contents")
// INVALID_THREAD: for example, thread not running an event loop
ERROR_CODE(INVALID_THREAD           ,  103, "function cannot execute on this thread")
ERROR_CODE(INVALID_IP               ,  104, "invalid server IP address in cluster config")
ERROR_CODE(INVALID_OPERATION        ,  105, "invalid operation for mode")
ERROR_CODE(UNKNOWN_SETTING          ,  106, "unrecognized setting name")
ERROR_CODE(INVALID_SETTING_VALUE    ,  107, "invalid value of a setting")
ERROR_CODE(UPTODATE                 ,  108, "value was already up to date")
ERROR_CODE(EMPTY                    ,  109, "value was empty/uninitialized")
ERROR_CODE(DESTINATION_MISMATCH     ,  110, "destination node id mismatch")
ERROR_CODE(INVALID_ATTRIBUTES       ,  111, "invalid logs config attributes")
ERROR_CODE(NOTEMPTY                 ,  112, "deleting a directory that is not empty")
ERROR_CODE(NOTDIR                   ,  113, "path is not a directory")
ERROR_CODE(ID_CLASH                 ,  114, "the ID range clashes with an existing log group")
ERROR_CODE(LOGS_SECTION_MISSING     ,  115, "Configuration file misses logs section")
ERROR_CODE(CHECKSUM_MISMATCH        ,  116, "checksum verification failed")
ERROR_CODE(COND_WRITE_NOT_READY     ,  117, "temporarily unable to perform conditional write")
ERROR_CODE(COND_WRITE_FAILED        ,  118, "condition write failed because of prev lsn mismatch")

// Configuration errors

ERROR_CODE(FILE_OPEN                ,  200, "error opening config file")
ERROR_CODE(FILE_READ                ,  201, "error reading config file contents")

// Local log store errors

ERROR_CODE(LOCAL_LOG_STORE_WRITE    ,  300, "error writing to local log store")
ERROR_CODE(CAUGHT_UP                ,  301, "no records with supplied LSN or greater")
ERROR_CODE(UNTIL_LSN_REACHED        ,  302, "client-provided final LSN reached")
ERROR_CODE(WINDOW_END_REACHED       ,  303, "hit client flow control window end")
ERROR_CODE(BYTE_LIMIT_REACHED       ,  304, "hit max_record_bytes_queued")
ERROR_CODE(MALFORMED_RECORD         ,  305, "malformed record in local log store")
ERROR_CODE(LOCAL_LOG_STORE_READ     ,  306, "error reading from local log store")

// Traffic shadowing errors

ERROR_CODE(SHADOW_DISABLED          ,  401, "traffic shadowing disabled for client")
ERROR_CODE(SHADOW_UNCONFIGURED      ,  402, "traffic shadowing not enabled for log range")
ERROR_CODE(SHADOW_FAILED            ,  403, "traffic shadowing failed, unspecified error")
ERROR_CODE(SHADOW_BUSY              ,  404, "traffic shadowing was busy, try again later")
ERROR_CODE(SHADOW_LOADING           ,  405, "traffic shadow parameters are being loaded")
ERROR_CODE(SHADOW_SKIP              ,  406, "skipping shadow due to configured ratio")

// Storage membership errors
ERROR_CODE(VERSION_MISMATCH, 500, "version mismatch")
ERROR_CODE(SOURCE_STATE_MISMATCH,
           501,
           "source state doesn't match the transition rules")
ERROR_CODE(CONDITION_MISMATCH,
           502,
           "provided conditions do not match the required conditions")

// Maintenance errors
ERROR_CODE(MAINTENANCE_CLASH, 600, "Maintenance clashes with an existing one")

#undef ERROR_CODE
