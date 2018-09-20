/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
/* can be included multiple times */

#ifndef MESSAGE_TYPE
#error MESSAGE_TYPE() macro not defined
#define MESSAGE_TYPE(...)
#endif

MESSAGE_TYPE(APPEND,   'a') // append a new record to a log, no LSN assigned yet
MESSAGE_TYPE(APPENDED, 'A') // reply to append
MESSAGE_TYPE(START,    't') // readers send this to request delivery of records
MESSAGE_TYPE(STARTED,  'T') // reply to START
MESSAGE_TYPE(STOP,     'p') // readers send this to stop delivery of records
                            // to them
MESSAGE_TYPE(WINDOW,   'w') // clients send this to update sending windows on
                            // storage nodes
MESSAGE_TYPE(RECORD,   '.') // storage nodes send these to deliver records to
                            // a reader
MESSAGE_TYPE(STORE,    's') // store a record with an LSN assigned on a
                            // storage node
MESSAGE_TYPE(STORED,   'S') // reply to STORE
MESSAGE_TYPE(MUTATED,  'U') // reply to a mutation (part of log recovery)
MESSAGE_TYPE(RELEASE,  'r') // release records for delivery
MESSAGE_TYPE(DELETE,   'd') // delete an extra copy of a record
MESSAGE_TYPE(DELETE_LOG_METADATA, 'D') // admin requested deletion of log
                                       // metadata
MESSAGE_TYPE(DELETE_LOG_METADATA_REPLY, 'R') // Response to DELETE_LOG_METADATA

MESSAGE_TYPE(GAP,      '-') // storage nodes send these to readers to report a
                            // gap in the numbering sequence
MESSAGE_TYPE(TRIM,     'm') // trim a log
MESSAGE_TYPE(TRIMMED,  'M') // reply to TRIM
MESSAGE_TYPE(FINDKEY, 'f')  // client library broadcasts this to storage nodes
                            // for the findTime() and findKey() APIs
MESSAGE_TYPE(FINDKEY_REPLY, 'F') // reply to FINDKEY
MESSAGE_TYPE(CLEAN,    'c') // sequencer sends this to storage nodes that
                            // did not participate in log recovery
MESSAGE_TYPE(CLEANED,  'C') // reply to CLEAN

MESSAGE_TYPE(SEAL,     'l') // seal recent epochs before recovery can begin
MESSAGE_TYPE(SEALED,   'L') // reply to SEAL

MESSAGE_TYPE(GET_SEQ_STATE, 'q')       // storage nodes send these to sequencers
                                       // requesting state for a log
MESSAGE_TYPE(GET_SEQ_STATE_REPLY, 'Q') // reply to GET_SEQ_STATE, sent by
                                       // sequencers
MESSAGE_TYPE(HELLO, 'i')    // clients and nodes send this into actively opened
                            // connections in order to start communication
MESSAGE_TYPE(ACK,   'I')   // server at passive connection end sends this in
                            // response to a HELLO

MESSAGE_TYPE(GOSSIP, 'G')   // used to exchange information about dead nodes

MESSAGE_TYPE(GET_LOG_INFO, 'j')       // request for configuration of a
                                      // particular log
MESSAGE_TYPE(GET_LOG_INFO_REPLY, 'J') // configuration of the log requested.
MESSAGE_TYPE(CONFIG_CHANGED, 'g')     // we send this on config change
MESSAGE_TYPE(CONFIG_ADVISORY, 'X')    // used to send the config version
MESSAGE_TYPE(CONFIG_FETCH, 'z')       // used to fetch a config

MESSAGE_TYPE(IS_LOG_EMPTY, 'e')        // request to know whether a log is empty
MESSAGE_TYPE(IS_LOG_EMPTY_REPLY, 'E')  // response to IS_LOG_EMPTY request

MESSAGE_TYPE(GET_HEAD_ATTRIBUTES, 'h') // request to get log head attributes
                                       // such as trim point and its timestamp
MESSAGE_TYPE(GET_HEAD_ATTRIBUTES_REPLY, 'H') // response to GET_HEAD_ATTRIBUTES

MESSAGE_TYPE(CHECK_SEAL, 'b')       // sequencer node sends this to storage
                                    // nodes to detect preemption, in the
                                    // context of GET_SEQ_STATE message
MESSAGE_TYPE(CHECK_SEAL_REPLY, 'B') // reply to CHECK_SEAL, sent by
                                    // storage nodes
MESSAGE_TYPE(GET_CLUSTER_STATE, 'k')       // request and response for the state
MESSAGE_TYPE(GET_CLUSTER_STATE_REPLY, 'K') // of the cluster (dead nodes)
MESSAGE_TYPE(SHUTDOWN, 'x') // Severs shutting down gracefully will
                            // send this message on all open connections
MESSAGE_TYPE(SHARD_STATUS_UPDATE, 'o') // storage nodes inform readers of the
                                       // authoritativeness of shards
MESSAGE_TYPE(CHECK_NODE_HEALTH, 'n') // request to know node's current health status
MESSAGE_TYPE(CHECK_NODE_HEALTH_REPLY, 'N') // response to CHECK_NODE_HEALTH request

// Ask the sequencer if it's ready to accept an append
MESSAGE_TYPE(APPEND_PROBE, '?')
MESSAGE_TYPE(APPEND_PROBE_REPLY, '/')

MESSAGE_TYPE(GET_EPOCH_RECOVERY_METADATA, 'v')       // used by purging
MESSAGE_TYPE(GET_EPOCH_RECOVERY_METADATA_REPLY, 'V')

MESSAGE_TYPE(LOGS_CONFIG_API, '$')
MESSAGE_TYPE(LOGS_CONFIG_API_REPLY, '%')

MESSAGE_TYPE(MEMTABLE_FLUSHED, ':')

MESSAGE_TYPE(NODE_STATS, '<') // sent from client to node containing node stats
MESSAGE_TYPE(NODE_STATS_REPLY, '>') // sent to acknowledge the NODE_STATS

MESSAGE_TYPE(NODE_STATS_AGGREGATE, '[') // sent by a controller node to request
                                        // a span of node stats from nodes
MESSAGE_TYPE(NODE_STATS_AGGREGATE_REPLY, ']') // contains the requested stats

MESSAGE_TYPE(GET_TRIM_POINT, '}')
MESSAGE_TYPE(GET_TRIM_POINT_REPLY, '{')

MESSAGE_TYPE(DATA_SIZE, 'O')    // request to approximate data size by
                                // time range, log
MESSAGE_TYPE(DATA_SIZE_REPLY, 'P')  // response to DATA_SIZE request

MESSAGE_TYPE(TEST, char(1))

#undef MESSAGE_TYPE
