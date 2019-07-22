---
id: LDQuery
title: LDQuery
sidebar_label: LDQuery
---

## append\_outliers
Provides debugging information for the Sequencer Boycotting feature, which consists in boycotting sequencer nodes that are outliers when we compare their append success rate with the rest of the clusters.  This table contains the state of a few nodes in the cluster that are responsible for gathering stats from all other nodes, aggregate them and decide the list of outliers.

|   Column   |   Type   |   Description   |
|------------|:--------:|-----------------|
| node\_id | int | Node ID this row is for. |
| observed\_node\_id | int | Node id for which we are measuring stats. |
| appends\_success | long | Number of appends that this node completed successfully. |
| appends\_failed | long | Number of appends that this node did not complete successfully. |
| msec\_since | long | Time in milliseconds since the node has been considered an outlier. |
| is\_outlier | bool | True if this node is considered an outlier. |

## append\_throughput
For each sequencer node, reports the estimated per-log-group append throughput over various time periods.  Because different logs in the same log group may have their sequencer on different nodes in the cluster, it is necessary to aggregate these rates across all nodes in the cluster to get an estimate of the global append throughput of a log group.  If Sequencer Batching is enabled, this table reports the rate of appends incoming, ie before batching and compression on the sequencer.

|   Column   |   Type   |   Description   |
|------------|:--------:|-----------------|
| node\_id | int | Node ID this row is for. |
| log\_group\_name | string | The name of the log group. |
| throughput\_1min | long | Throughput average in the past 1 minute. |
| throughput\_5min | long | Throughput average in the past 5 minutes. |
| throughput\_10min | long | Throughput average in the past 10 minutes. |

## catchup\_queues
CatchupQueue is a state machine that manages all the read streams of one client on one socket.  It contains a queue of read streams for which there are new records to be sent to the client (we say these read streams are not caught up).  Read streams from that queue are processed (or "woken-up") in a round-robin fashion. The state machine is implemented in logdevice/common/CatchupQueue.h.

|   Column   |   Type   |   Description   |
|------------|:--------:|-----------------|
| node\_id | int | Node ID this row is for. |
| client | string | Id of the client. |
| queued\_total | long | Number of read streams queued in that CatchupQueue (ie read streams that are not caught up). |
| queued\_immediate | long | Number of read streams that are not queued with a delay, see "queue\_delayed". |
| queued\_delayed | long | (Number of read streams that are queued with a delay.  When these read streams are queued, CatchupQueue waits for a configured amount of time before dequeuing the stream for processing. This happens if the log is configured with the "delivery\_latency"  option, which enables better batching of reads when tailing.  See "deliveryLatency" logdevice/include/LogAttributes.h. |
| record\_bytes\_queued | long | (CatchupQueue also does accounting of how many bytes are enqueued in the socket's output evbuffer. CatchupQueue wakes up several read streams until the buffer reaches the limit set by the option --output-max-records-kb (see logdevice/common/Settings.h). |
| storage\_task\_in\_flight | int | Each read stream is processed one by one.  When a read stream is processed, it will first try to read some records from the worker thread if there are some records that can be read from RocksDB's block cache. When all records that could be read from the worker thread were read, and if there are more records that can be read, the read stream will issue a storage task to read such records in a slow storage thread.  This flag indicates whether or not there is such a storage task currently in flight. |
| ping\_timer\_active | int | Ping timer is a timer that is used to ensure we eventually try to schedule more reads under certain conditions.  This column indicates whether the timer is currently active. |

## chunk\_rebuildings
In-flight ChunkRebuilding state machines - each responsible for re-replicating a short range of records for the same log wich consecutive LSNs and the same copyset (see ChunkRebuilding.h). See also: shard\_rebuildings, log\_rebuildings

|   Column   |   Type   |   Description   |
|------------|:--------:|-----------------|
| node\_id | int | Node ID this row is for. |
| log\_id | log_id | Log ID of the records. |
| shard | int | Index of the shard to which the records belong. |
| min\_lsn | lsn | LSN of first record in the chunk. |
| max\_lsn | lsn | LSN of last record in the chunk. |
| chunk\_id | long | ID of the chunk, unique within a process. |
| block\_id | long | Sticky copyset block to which the records belong. Block can be split into multiple chunks. |
| total\_bytes | long | Sum of records' payload+header sizes. |
| oldest\_timestamp | time | Timestamp of the first record in the chunk. |
| stores\_in\_flight | long | Number of records for which we're in the process of storing new copies. |
| amends\_in\_flight | long | Number of records for which we're in the process of amending copysets of existing copies, excluding our own copy. |
| amend\_self\_in\_flight | long | Number of records for which we're in the process of amending copysets of our own copy. |
| started | time | Time when the ChunkRebuilding was started. |

## client\_read\_streams
ClientReadStream is the state machine responsible for reading records of a log on the client side.  The state machine connects to all storage nodes that may contain data for a log and request them to send new records as they are appended to the log.  For each ClientReadStream there is one ServerReadStream per storage node the ClientReadStream is talking to.  The "readers" table lists all existing ServerReadStreams.  Because LDQuery does not fetch any debugging information from clients connected to the cluster, the only ClientReadStreams that will be shown in this table are for internal read streams on the server.

|   Column   |   Type   |   Description   |
|------------|:--------:|-----------------|
| node\_id | int | Node ID this row is for. |
| log\_id | log_id | Id of the log being read. |
| id | long | Internal identifier for the read stream. |
| next\_lsn\_to\_deliver | long | Next LSN that needs to be delivered to the client. |
| window\_high | lsn | Current window.  This is used for flow control.  ClientReadStream instructs storage nodes to not send records with LSNs higher than this value.  ClientReadStream slides the window as it is able to make progress (see the --client-read-flow-control-threshold setting). |
| until\_lsn | lsn | LSN up to which the ClientReadStream must read. |
| read\_set\_size | long | Number of storage nodes this ClientReadStream is reading from.  Usally equal to the size of the log's nodeset but may be smaller if some nodes from that nodeset are rebuilding. |
| gap\_end\_outside\_window | lsn | Sometimes there are no more records to deliver that have a LSN smaller than "window\_high".  When a storage node reaches the end of the window, it sends a GAP message to inform of the next LSN it will be able to ship past the window.  This value is the smallest LSN greater than "window\_high" reported by storage nodes and is used for determining the right endpoint of a gap interval in such a situation. |
| trim\_point | lsn | When a storage node reaches a log's trim point, it informs ClientReadStream through a gap message.  This is ClientReadStream's current view of the log's trim point. |
| gap\_nodes\_next\_lsn | string | Contains the list of nodes for which we know they don't have a record with LSN "next\_lsn\_to\_deliver".  Alongside the node id is the LSN of the next record or gap this storage node is expected to send us. |
| unavailable\_nodes | string | List of nodes that ClientReadStream knows are unavailable and thus is not trying to read from. |
| connection\_health | string | Summary of authoritative status of the read session. An AUTHORITATIVE session has a least an f-majority of nodes participating. Reported dataloss indicates all copies of a record were lost, or, much less likely, the only copies of the data are on the R-1 nodes that are currently unavailable, and the cluster failed to detect or remediate the failure that caused some copies to be lost. A NON\_AUTHORITATIVE session has less than an f-majority of nodes participating, but those not participating have had detected failures, are not expected to participate, and are being rebuilt. Most readers will stall in this case. Readers that proceed can see dataloss gaps for records that are merely currently unavailable, but will become readable once failed nodes are repaired.  An UNHEALTHY session has too many storage nodes down but not marked as failed, to be able to read even non-authoritatively. |
| redelivery\_inprog | bool | True if a retry of delivery to the application is outstanding. |
| filter\_version | long | Read session version.  This is bumped every time parameters (start, until, SCD, etc.) are changed by the client. |

## cluster\_state
Fetches the state of the gossip-based failure detector from the nodes of the cluster.  When the status column is OK, the nodes\_state column contains a string-representation of the cluster state as seen by the node, as comma-separated list of elements of the form <node\_id>:{A\|D}, where A means alive and D means dead. eg: N1:A,N2:A,N3:D,N4:A.  When the status is anything but OK, it means the request failed for this node, and it may be dead itself.

|   Column   |   Type   |   Description   |
|------------|:--------:|-----------------|
| node\_id | long | Id of the node. |
| status | string | Status of the node. |
| dead\_nodes | string | List of node IDs that this node believes to be dead. |
| boycotted\_nodes | string | List of boycotted nodes. |

## epoch\_store
EpochStore is the data store that contains epoch-related metadata for all the logs provisioned on a cluster.   This table allows querying the metadata in epoch-store for a set of logs.

|   Column   |   Type   |   Description   |
|------------|:--------:|-----------------|
| log\_id | log_id | Id of the log. |
| status | string | "OK" if the query to the epoch store succeeded for that log id.  If the log could not be found (which only happens if the user provided query constraints on the "log\_id" column), set to NOTFOUND.  If we failed to contact the epoch store, set to one of NOTCONN, ACCESS, SYSLIMIT, FAILED. |
| since | long | Epoch since which the metadata ("replication", "storage\_set", "flags") are in effect. |
| epoch | long | Next epoch to be assigned to a sequencer. |
| replication | string | Current replication property of the log. |
| storage\_set\_size | long | Number of shards in storage\_set. |
| storage\_set | string | Set of shards that may have data records for the log in epochs ["since", "epoch" - 1]. |
| flags | string | Internal flags.  See "logdevice/common/EpochMetaData.h" for the description of each flag. |
| nodeset\_signature | long | Hash of the parts of config that potentially affect the nodeset. |
| target\_nodeset\_size | long | Storage set size that was requested from NodeSetSelector. Can be different from storage\_set\_size for various reasons, see EpochMetaData.h |
| nodeset\_seed | long | Random seed used when selecting nodeset. |
| lce | long | Last epoch considered clean for this log.  Under normal conditions, this is equal to "epoch" - 2.   If this value is smaller, this means that the current sequencer needs to run the Log Recovery procedure on epochs ["lce" + 1, "epoch" - 2] and readers will be unable to read data in these epochs until they are cleaned. |
| meta\_lce | long | Same as "lce" but for the metadata log of this data log. |
| written\_by | string | Id of the last node in the cluster that updated the epoch store for that log. |
| tail\_record | string | Human readable string that describes tail record |

## event\_log
Dump debug information about the EventLogStateMachine objects running on nodes in the cluster.  The event log is the Replicated State Machine that coordinates rebuilding and contains the authoritative status of all shards in the cluster.  This table can be used to debug whether all nodes in the cluster are caught up to the same state.

|   Column   |   Type   |   Description   |
|------------|:--------:|-----------------|
| node\_id | int | Node ID this row is for. |
| delta\_log\_id | log_id | Id of the delta log. |
| snapshot\_log\_id | log_id | Id of the snapshot log. |
| version | lsn | Version of the state. |
| delta\_read\_ptr | lsn | Read pointer in the delta log. |
| delta\_replay\_tail | lsn | On startup, the state machine reads the delta log up to that lsn before delivering the initial state to subscribers. |
| snapshot\_read\_ptr | lsn | Read pointer in the snapshot log. |
| snapshot\_replay\_tail | lsn | On startup, the state machine reads the snapshot log up to that lsn before delivering the initial state to subscribers. |
| stalled\_waiting\_for\_snapshot | lsn | If not null, this means the state machine is stalled because it missed data in the delta log either because it saw a DATALOSS or TRIM gap.  The state machine will be stalled until it sees a snapshot with a version greather than this LSN.  Unless another node writes a snapshot with a bigger version, the operator may have to manually write a snapshot to recover the state machine. |
| delta\_appends\_in\_flight | long | How many deltas are currently being appended to the delta log by this node. |
| deltas\_pending\_confirmation | long | How many deltas are currently pending confirmation on this node, ie these are deltas currently being written with the CONFIRM\_APPLIED flag, and the node is waiting for the RSM to sync up to that delta's version to confirm whether or not it was applied. |
| snapshot\_in\_flight | long | Whether a snapshot is being appended by this node.  Only one node in the cluster is responsible for creating snapshots (typically the node with the smallest node id that's alive according to the failure detector). |
| delta\_log\_bytes | long | Number of bytes of delta records that are past the last snapshot. |
| delta\_log\_records | long | Number of delta records that are past the last snapshot. |
| delta\_log\_healthy | bool | Whether the ClientReadStream state machine used to read the delta log reports itself as healthy, ie it has enough healthy connections to the storage nodes in the delta log's storage set such that it should be able to not miss any delta. |
| propagated\_version | lsn | Version of the last state that was fully propagated to all state machines (in particular, to RebuildingCoordinator). |

## graylist
Provides information on graylisted storage nodes per worker per node. This works only for the outlier based graylisting.

|   Column   |   Type   |   Description   |
|------------|:--------:|-----------------|
| node\_id | int | Node ID this row is for. |
| worker\_id | int | The id of the worker running on the node. |
| graylisted\_node\_index | int | The graylisted node ID |

## historical\_metadata
This table contains information about historical epoch metadata for all logs.  While the "epoch\_store" table provides information about the current epoch metadata of all logs, this table provides a history of that metadata for epoch ranges since the epoch of the first record that is not trimmed.

|   Column   |   Type   |   Description   |
|------------|:--------:|-----------------|
| log\_id | log_id | Id of the log |
| status | string | "OK" if the query to retrieve historical metadata succeeded for that log id.  If the log is not in the config (which only happens if the user provided query constraints on the "log\_id" column), set to INVALID\_PARAM.  If we failed to read the log's historical metadata, set to one of TIMEDOUT, ACCESS, FAILED. |
| since | long | Epoch since which the metadata ("replication", "storage\_set", "flags") are in effect. |
| epoch | long | Epoch up to which the metadata is in effect. |
| replication | string | Replication property for records in epochs ["since", "epoch"]. |
| storage\_set\_size | long | Number of shards in storage\_set. |
| storage\_set | string | Set of shards that may have data records for the log in epochs ["since", "epoch"]. |
| flags | string | Internal flags.  See "logdevice/common/EpochMetaData.h" for the description of each flag. |

## historical\_metadata\_legacy
Same as "historical\_metadata", but retrieves the metadata less efficiently by reading the metadata logs directly instead of contacting the sequencer.  Provides two additional "lsn" and "timestamp" columns to identify the metadata log record that contains the metadata.

|   Column   |   Type   |   Description   |
|------------|:--------:|-----------------|
| log\_id | log_id | Id of the log |
| status | string | "OK" if the query to retrieve historical metadata succeeded for that log id.  If the log is not in the config (which only happens if the user provided query constraints on the "log\_id" column), set to INVALID\_PARAM.  If we failed to read the log's historical metadata, set to one of TIMEDOUT, ACCESS, FAILED. |
| since | long | Epoch since which the metadata ("replication", "storage\_set", "flags") are in effect. |
| epoch | long | Epoch up to which the metadata is in effect. |
| replication | string | Replication property for records in epochs ["since", "epoch"]. |
| storage\_set\_size | long | Number of shards in storage\_set. |
| storage\_set | string | Set of shards that may have data records for the log in epochs ["since", "epoch"]. |
| flags | string | Internal flags.  See "logdevice/common/EpochMetaData.h" for the description of each flag. |
| lsn | lsn | LSN of the metadata log record that contains this metadata |
| timestamp | long | Timestamp of the metadata log record that contains this metadata |

## info
A general information table about the nodes in the cluster, like server start time, package version etc.

|   Column   |   Type   |   Description   |
|------------|:--------:|-----------------|
| node\_id | int | Node ID this row is for. |
| pid | long | Process ID of logdeviced. |
| version | string | A string that holds the version and revision, built by who and when. |
| package | string | Package name and hash. |
| build\_user | string | Unixname of user who built this package |
| build\_time | string | Date and Time of the build. |
| start\_time | string | Date and Time of when the daemon was started on that node. |
| server\_id | string | Server Generated ID. |
| shards\_missing\_data | string | A list of the shards that are empty and waiting to be rebuilt. |
| min\_proto | long | Minimum protocol version supported. |
| max\_proto | long | Maximum protocol version supported. |
| is\_auth\_enabled | bool | Whether authentication is enabled for this node. |
| auth\_type | string | Authentication Type.  Can be null or one of "self\_identification" (insecure authentication method where the server trusts the client), "ssl" (the client can provide a TLS certificate by using the "ssl-load-client-cert" and "ssl-cert-path" settings).  See "AuthenticationType" in "logdevice/common/SecurityInformation.h" for more information. |
| is\_unauthenticated\_allowed | bool | Is anonymous access allowed to this server (set only if authentication is enabled, ie "auth\_type" is not null). |
| is\_permission\_checking\_enabled | bool | Do we check permissions? |
| permission\_checker\_type | string | Permission checker type.  Can be null or one of "config" (this method stores the permission data in the config file), "permission\_store" (this method stores the ACLs in the config file while the permissions and users are stored in an external store).  See "PermissionCheckerType" in "logdevice/common/SecurityInformation.h" for more information. |
| rocksdb\_version | string | Version of RocksDB. |

## info\_config
A table that dumps information about all the configurations loaded by each node in the cluster.   For each node, there will be one row for the node's configuration which is in the main config.  A second row will be present for each node if they load the log configuration from a separate config file (see "include\_log\_config" section in the main config).

|   Column   |   Type   |   Description   |
|------------|:--------:|-----------------|
| node\_id | int | Node ID this row is for. |
| uri | string | URI of the config. |
| source | int | ID of the node this config originates from.  This may not necessarily be the same as "node" as nodes can synchronize configuration between each other. |
| hash | string | Hash of the config. |
| last\_modified | time | Date and Time when the config was last modified. |
| last\_loaded | time | Date and Time when the config was last loaded. |

## is\_log\_empty
This table provides a way to check which logs are empty or not. It is implemented on top of the islogEmpty API (see "logdevice/include/Client.h").

|   Column   |   Type   |   Description   |
|------------|:--------:|-----------------|
| log\_id | log_id | ID of the log. |
| status | string | Response status of running isLogEmpty() on this log. Check the documentation of isLogEmpty() for the list of error codes. |
| empty | bool | Wether the log is empty, null if "status" != OK. |

## iterators
This table allows fetching the list of RocksDB iterators on all storage nodes.

|   Column   |   Type   |   Description   |
|------------|:--------:|-----------------|
| node\_id | int | Node ID this row is for. |
| column\_family | string | Name of the column family that the iterator is open on. |
| log\_id | log_id | ID of the log the iterator is reading. |
| is\_tailing | long | 1 if it is a tailing iterator, 0 otherwise. |
| is\_blocking | long | 1 if this is an iterator which is allowed to block when the blocks it reads are not in RocksDB's block cache, 0 otherwise. |
| type | string | Type of the iterator. See "IteratorType" in "logdevice/server/locallogstore/IteratorTracker.h" for the list of iterator types. |
| rebuilding | long | 1 if this iterator is used for rebuilding, 0 if in other contexts. |
| high\_level\_id | long | A unique identifier that the high-level iterator was assigned to.  Used to tie higher-level iterators with lower-level iterators created by them. |
| created\_timestamp | time | Timestamp when the iterator was created. |
| more\_context | string | More information on where this iterator was created. |
| last\_seek\_lsn | lsn | Last LSN this iterator was seeked to. |
| last\_seek\_timestamp | time | When the iterator was last seeked. |
| version | long | RocksDB superversion that this iterator points to. |

## log\_groups
A table that lists the log groups configured in the cluster.  A log group is an interval of log ids that share common configuration property.

|   Column   |   Type   |   Description   |
|------------|:--------:|-----------------|
| name | string | Name of the log group. |
| logid\_lo | log_id | Defines the lower bound (inclusive) of the range of log ids in this log group. |
| logid\_hi | log_id | Defines the upper bound (inclusive) of the range of log ids in this log group. |
| replication\_property | string | Replication property configured for this log group. |
| synced\_copies | int | Number of copies that must be acknowledged by storage nodes are synced to disk before the record is acknowledged to the client as fully appended. |
| max\_writes\_in\_flight | int | The largest number of records not released for delivery that the sequencer allows to be outstanding. |
| backlog\_duration\_sec | int | Time-based retention of records of logs in that log group.  If null or zero, this log group does not use time-based retention. |
| storage\_set\_size | int | Size of the storage set for logs in that log group.  The storage set is the set of shards that may hold data for a log. |
| delivery\_latency | int | For logs in that log group, maximum amount of time that we can delay delivery of newly written records.  This option increases delivery latency but improves server and client performance. |
| scd\_enabled | int | Indicates whether the Single Copy Delivery optimization is enabled for this log group.  This efficiency optimization allows only one copy of each record to be served to readers. |
| custom\_fields | string | Custom text field provided by the user. |

## log\_rebuildings
This table dumps debugging information about the state of LogRebuilding state machines (see "logdevice/server/LogRebuilding.h") which are state machines running on donor storage nodes and responsible for rebuilding records of that log.

|   Column   |   Type   |   Description   |
|------------|:--------:|-----------------|
| node\_id | int | Node ID this row is for. |
| log\_id | log_id | ID of the log. |
| shard | int | Index of the shard from which the LogRebuilding state machine is reading. |
| started | time | Date and Time of when that state machine was started. |
| rebuilding\_set | string | Information provided to the LogRebuilding state machine which defines the list of shards that lost record copies which need to be re-replicated elsewhere. Expressed in the form "<shard-id>*?[<dirty-ranges>],...". "*" indicates that the shard may be up but we want to drain its data by replicating it elsewhere.  If <dirty-ranges> is not empty, this means that the storage shard only lost data within the specified ranges. |
| version | lsn | Rebuilding version.  This version comes from the event log RSM that coordinates rebuilding.  See the "event\_log" table. |
| until\_lsn | lsn | LSN up to which the log must be rebuilt.  See "logdevice/server/rebuilding/RebuildingPlanner.h" for how this LSN is computed. |
| max\_timestamp | time | Maximum timestamp that this LogRebuilding state machine is instructed to read through. |
| rebuilt\_up\_to | lsn | Next LSN to be considered by this state machine for rebuilding. |
| num\_replicated | long | Number of records replicated by this state machine so far. |
| bytes\_replicated | long | Number of bytes replicated by this state machine so far. |
| rr\_in\_flight | long | Number of stores currently in flight, ie pending acknowledgment from the recipient storage shard. |
| nondurable\_stores | long | Number of stores for which we are pending acknowledgment that they are durable on disk. |
| durable\_stores | long | Number of records that were durably replicated and now are pending an amend. |
| rra\_in\_flight | long | Number of amends that are in flight, ie pending acknowledgment from the recipient storage shard. |
| nondurable\_amends | long | Number of amends for which we are pending acknowledgment that they are durable on disk. |
| last\_storage\_task\_status | string | Status of the last batch of records we read from disk. |

## log\_storage\_state
Tracks all in-memory metadata for logs on storage nodes (see "info log\_storage\_state" admin command).

|   Column   |   Type   |   Description   |
|------------|:--------:|-----------------|
| node\_id | int | Node ID this row is for. |
| log\_id | log_id | Log id for which the storage state is. |
| shard | int | Shard for which that log storage state is. |
| last\_released | lsn | Last Released LSN as seen by the shard.  If this does not match the sequencer's last\_released\_lsn, (See "sequencer" table) this means the shard has not completed purging. |
| last\_released\_src | string | Where the last\_released value was gotten from.  Either "sequencer" if the sequencer sent a release message, or "local log store" if the value was persisted on disk in the storage shard. |
| trim\_point | lsn | Trim point for that log on this storage node. |
| per\_epoch\_metadata\_trim\_point | lsn | Trim point of per-epoch metadata.  PerEpochLogMetadata whose epoch is <= than this value should be trimmed. |
| seal | long | Normal seal. The storage node will reject all stores with sequence numbers belonging to epochs that are <= than this value. |
| sealed\_by | string | Sequencer node that set the normal seal. |
| soft\_seal | long | Similar to normal seal except that the sequencer did not explictly seal the storage node, the seal is implicit because a STORE message was sent by a sequencer for a new epoch. |
| soft\_sealed\_by | string | Sequencer node that set the soft seal. |
| last\_recovery\_time | long | Latest time (number of microseconds since steady\_clock's epoch) when some storage node tried to recover the state.  To not be confused with Log recovery. |
| log\_removal\_time | long | See LogStorageState::log\_removal\_time\_. |
| lce | long | Last clean epoch.  Updated when the sequencer notifies this storage node that it has performed recovery on an epoch. |
| latest\_epoch | long | Latest seen epoch from the sequencer. |
| latest\_epoch\_offset | string | Offsets within the latest epoch |
| permanent\_errors | long | Set to true if a permanent error such as an IO error has been encountered.  When this is the case, expect readers to not be able to read this log on this storage shard. |

## logsconfig\_rsm
Dump debug information about the LogsConfigStateMachine objects running on nodes in the cluster.  The config log is the replicated state machine that stores the logs configuration of a cluster.

|   Column   |   Type   |   Description   |
|------------|:--------:|-----------------|
| node\_id | int | Node ID this row is for. |
| delta\_log\_id | log_id | Id of the delta log. |
| snapshot\_log\_id | log_id | Id of the snapshot log. |
| version | lsn | Version of the state. |
| delta\_read\_ptr | lsn | Read pointer in the delta log. |
| delta\_replay\_tail | lsn | On startup, the state machine reads the delta log up to that lsn before delivering the initial state to subscribers. |
| snapshot\_read\_ptr | lsn | Read pointer in the snapshot log. |
| snapshot\_replay\_tail | lsn | On startup, the state machine reads the snapshot log up to that lsn before delivering the initial state to subscribers. |
| stalled\_waiting\_for\_snapshot | lsn | If not null, this means the state machine is stalled because it missed data in the delta log either because it saw a DATALOSS or TRIM gap.  The state machine will be stalled until it sees a snapshot with a version greather than this LSN.  Unless another node writes a snapshot with a bigger version, the operator may have to manually write a snapshot to recover the state machine. |
| delta\_appends\_in\_flight | long | How many deltas are currently being appended to the delta log by this node. |
| deltas\_pending\_confirmation | long | How many deltas are currently pending confirmation on this node, ie these are deltas currently being written with the CONFIRM\_APPLIED flag, and the node is waiting for the RSM to sync up to that delta's version to confirm whether or not it was applied. |
| snapshot\_in\_flight | long | Whether a snapshot is being appended by this node.  Only one node in the cluster is responsible for creating snapshots (typically the node with the smallest node id that's alive according to the failure detector). |
| delta\_log\_bytes | long | Number of bytes of delta records that are past the last snapshot. |
| delta\_log\_records | long | Number of delta records that are past the last snapshot. |

## logsdb\_directory
Contains debugging information about the LogsDB directory on storage shards.

|   Column   |   Type   |   Description   |
|------------|:--------:|-----------------|
| node\_id | int | Node ID this row is for. |
| shard | int | ID of the shard. |
| log\_id | long | ID of the log. |
| partition | long | ID of the partition. |
| first\_lsn | lsn | Lower bound of the LSN range for that log in this partition. |
| max\_lsn | lsn | Upper bound of the LSN range for that log in this partition. |
| flags | string | Flags for this partition. "UNDER\_REPLICATED" means that some writes for this partition were lost (for instance due to the server crashing) and these records have not yet been rebuilt. |
| approximate\_size\_bytes | long | Approximate data size in this partition for the given log. |

## logsdb\_metadata
List of auxiliary RocksDB column families used by LogsDB (partitioned local log store). "metadata" column family contains partition directory and various logdevice metadata, per-log and otherwise. "unpartitioned" column family contains records of metadata logs and event log.

|   Column   |   Type   |   Description   |
|------------|:--------:|-----------------|
| node\_id | int | Node ID this row is for. |
| shard | long | Index of the local log store shard that this column family belongs to. |
| column\_family | string | Name of the column family. |
| approx\_size | long | Estimated size of the column family in bytes. |
| l0\_files | long | Number of sst (immutable table) files belonging to this column family. |
| immutable\_memtables | long | Number of inactive memtables that are still kept in memory.  See 'partitions' table for details. |
| memtable\_flush\_pending | long | Number of memtables that are in the process of being flushed to disk. |
| active\_memtable\_size | long | Size in bytes of the active memtable. |
| all\_memtables\_size | long | Size in bytes of all memtables. See 'partitions' table for details. |
| est\_num\_keys | long | Estimated number of keys stored in the column family.  The estimate tends to be poor because of merge operator. |
| est\_mem\_by\_readers | long | Estimated memory used by rocksdb iterators in this column family, excluding block cache. See 'partitions' table for details. |
| live\_versions | long | Number of live "versions" of this column family in RocksDB. See 'partitions' table for details. |

## nodes
Lists the nodes in the cluster from the configuration.

|   Column   |   Type   |   Description   |
|------------|:--------:|-----------------|
| node\_id | long | Id of the node |
| address | string | Ip and port that should be used for communication with the node |
| ssl\_address | string | Same as "address" but with SSL |
| generation | long | Generation of the node.  This value is bumped each time the node is swapped, sent to repair, or has one of its drives sent to repair. |
| location | string | Location of the node: <region>.<cluster>.<row>.<rack> |
| sequencer | int | 1 if this node is provisioned for the sequencing role. Otherwise 0. Provisioned roles must be enabled in order to be considered active. See 'sequencer\_enabled'. |
| storage | int | 1 if this node is provisioned for the storage role. Otherwise 0. Provisioned roles must be enabled in order to be considered active. See 'storage\_state'. |
| sequencer\_enabled | int | 1 if sequencing on this node is enabled. Othewise 0. |
| sequencer\_weight | real | A non-negative value indicating how many logs this node should be a sequencer for relative to other nodes in the cluster.  A value of 0 means this node cannot run sequencers. |
| is\_storage | int | 1 if this node is provisioned for the storage role. Otherwise 0. Provisioned roles must be enabled in order to be considered active. See 'storage\_state'. |
| storage\_state | string | Determines the current state of the storage node. One of "read-write", "read-only" or "none". |
| storage\_weight | real | A positive value indicating how much STORE traffic this storage node should receive relative to other storage nodes in the cluster. |
| num\_shards | long | Number of storage shards on this node.  0 if this node is not a storage node. |
| is\_metadata\_node | int | 1 if this node is in the metadata nodeset. Otherwise 0. |

## partitions
List of LogsDB partitions that store records.  Each shard on each node has a sequence of partitions.  Each partition corresponds to a time range a few minutes or tens of minutes log.  Each partition is a RocksDB column family.

|   Column   |   Type   |   Description   |
|------------|:--------:|-----------------|
| node\_id | int | Node ID this row is for. |
| shard | long | Index of the local log store shard that this partitions belongs to. |
| id | long | Sequence number of this partition. Partitions in each shard are numbered in chronological order without gaps. |
| start\_time | time | The beginning of the time range that this partition corresponds to. For most partitions it's equal to the time when partition was created. The exception is partitions created retroactively to accommodate rebuilt data. |
| min\_time | time | Approximate minimum timestamp of records stored in this partition. |
| max\_time | time | Approximate maximum timestamp of records stored in this partition. |
| min\_durable\_time | time | Persisted (WAL sync has occurred since the value was written) version of 'min\_time'. |
| max\_durable\_time | time | Persisted (WAL sync has occurred since the value was written) version of 'max\_time'. |
| last\_compacted | time | Last time when the partition was compacted. |
| approx\_size | long | Estimated size of the partition in bytes. |
| l0\_files | long | Number of sst (immutable table) files belonging to this partition. |
| immutable\_memtables | long | Number of inactive memtables (im-memory write buffers) that are still kept in memory. The most common cases are memtables in the process of being flushed to disk (memtable\_flush\_pending) and memtables pinned by iterators. |
| memtable\_flush\_pending | long | Number of memtables (im-memory write buffers) that are in the process of being flushed to disk. |
| active\_memtable\_size | long | Size in bytes of the active memtable. |
| all\_not\_flushed\_memtables\_size | long | Size in bytes of all memtables that weren't flushed to disk yet. The difference all\_memtables\_size-all\_not\_flushed\_memtables\_size is usually equal to the total size of memtables pinned by iterators. |
| all\_memtables\_size | long | Size in bytes of all memtables. Usually these are: active memtable, memtables that are being flushed and memtables pinned by iterators. |
| est\_num\_keys | long | Estimated number of keys stored in the partition.  They're usually records and copyset index entries. The estimate tends to be poor when merge operator is used. |
| est\_mem\_by\_readers | long | Estimated memory used by rocksdb iterators in this partition, excluding block cache. This pretty much only includes SST file indexes loaded in memory. |
| live\_versions | long | Number of live "versions" of this column family in RocksDB.  One (current) version is always live. If this value is greater than one, it means that some iterators are pinning some memtables or sst files. |
| current\_version | long | The current live version |
| append\_dirtied\_by | string | Nodes that have uncommitted append data in this partition. |
| rebuild\_dirtied\_by | string | Nodes that have uncommitted rebuild data in this partition. |

## purges
List the PurgeUncleanEpochs state machines currently active in the cluster. The responsability of this state machine is to delete any records that were deleted during log recovery on nodes that did not participate in that recovery. See "logdevice/server/storage/PungeUncleanEpochs.h" for more information.

|   Column   |   Type   |   Description   |
|------------|:--------:|-----------------|
| node\_id | int | Node ID this row is for. |
| log\_id | log_id | Log ID the purge state machine is for. |
| state | string | State of the state machine. |
| current\_last\_clean\_epoch | long | Last clean epoch considered by the state machine. |
| purge\_to | long | Epoch up to which this state machine should purge. The state machine will purge epochs in range ["current\_last\_clean\_epoch", "purge\_to"]. |
| new\_last\_clean\_epoch | long | New "last clean epoch" metadata entry to write into the local log store once purging completes. |
| sequencer | string | ID of the sequencer node that initiated purging. |
| epoch\_state | string | Dump the state of purging for each epoch.  See "logdevice/server/storage/PurgeSingleEpoch.h" |

## readers
Tracks all ServerReadStreams. A ServerReadStream is a stream of records sent by a storage node to a client running a ClientReadStream (see "client\_read\_streams" table).

|   Column   |   Type   |   Description   |
|------------|:--------:|-----------------|
| node\_id | int | Node ID this row is for. |
| shard | int | Shard on which the stream is reading. |
| client | string | Name of the client (similar to so the "client" column of the "sockets table"). |
| log\_id | log_id | Id of the log being read. |
| start\_lsn | lsn | LSN from which the ClientReadStream started reading. |
| until\_lsn | lsn | LSN up to which ClientReadStream is interested in reading. |
| read\_pointer | lsn | LSN to the next record to be read by the storage node. |
| last\_delivered | lsn | Last LSN sent to the ClientReadStream either through a data record or a gap. |
| last\_record | lsn | LSN of the last record the storage node delivered to the ClientReadStream. This value is not necessarily equal to "last\_delivered" as the storage node does not store all records of a log. |
| window\_high | lsn | Current window used by the ClientReadStream. This instructs the storage node to not send records with LSNs higher than this value. When the read pointer reaches this value, the stream is caught up and the storage node will wake up the stream when the window is slid by ClientReadStream. |
| last\_released | lsn | Last LSN released for delivery by the sequencer. If this value is stale, check if purging could be the reason using the "purges" table. |
| catching\_up | int | Indicates whether or not the stream is catching up, ie there are records that can be delivered now to the client, and the stream is enqueued in a CatchupQueue (see the "catchup\_queues" table). |
| window\_end | int | Should be true if "read\_pointer" is past "window\_high". |
| known\_down | string | List of storage nodes that the ClientReadStream is not able to receive records from. This list is used so that other storage nodes can send the records that nodes in that list were supposed to send. If this columns shows "ALL\_SEND\_ALL", this means that the ClientReadStream is not running in Single-Copy-Delivery mode, meaning it requires all storage nodes to send every record they have.  This can be either because SCD is not enabled for the log, or because data is not correctly replicated and ClientReadStream is not able to reconstitute a contiguous sequencer of records from what storage nodes are sending, or because the ClientReadStream is going through an epoch boundary. |
| filter\_version | long | Each time ClientReadStream rewinds the read streams (and possibly changes the "known\_down" list), this counter is updated. Rewinding a read stream means asking the storage node to rewind to a given LSN. |
| last\_batch\_status | string | Status code that was issued when the last batch of records was read for this read stream.  See LocalLogStoreReader::read() in logdevice/common/LocalLogStore.h. |
| created | time | Timestamp of when this ServerReadStream was created. |
| last\_enqueue\_time | time | Timestamp of when this ServerReadStream was last enqueued for processing. |
| last\_batch\_started\_time | time | Timestamp of the last time we started reading a batch of records for this read stream. |
| storage\_task\_in\_flight | int | True if there is currently a storage task running on a slow storage thread for reading a batch of records. |

## record
This table allows fetching information about individual record copies in the cluster.  The user must provide query constraints on the "log\_id" and "lsn" columns.  This table can be useful to introspect where copies of a record are stored and see their metadata.  Do not use it to serve production use cases as this query runs very inneficiently (it bypasses the normal read protocol and instead performs a point query on all storage nodes in the cluster).

|   Column   |   Type   |   Description   |
|------------|:--------:|-----------------|
| node\_id | int | Node ID this row is for. |
| log\_id | log_id | ID of the log this record is for. |
| lsn | lsn | Sequence number of the record. |
| shard | int | ID of the shard that holds this record copy. |
| wave | int | If "is\_written\_by\_recovery" is 0, contains the wave of that record. |
| recovery\_epoch | int | If "is\_written\_by\_recovery" is 1, contains the "sequencer epoch" of the log recovery. |
| timestamp | string | Timestamp in milliseconds of the record. |
| last\_known\_good | int | Highest ESN in this record's epoch such that at the time this messagewas originally sent by a sequencer all records with this and lower ESNs in this epoch were known to the sequencer to be fully stored on R nodes. |
| copyset | string | Copyset of the record. |
| flags | string | Flags for that record.  See "logdevice/common/LocalLogStoreRecordFormat.h" to see the list of flags. |
| offset\_within\_epoch | string | Amount of data written to that record within the epoch. |
| optional\_keys | string | Optional keys provided by the user.  See "AppendAttributes" in "logdevice/include/Record.h". |
| is\_written\_by\_recovery | bool | Whether this record was replicated by the Log Recovery. |
| payload | string | Payload in hex format. |

## record\_cache
Dumps debugging information about the EpochRecordCache entries in each storage shard in the cluster.  EpochRecordCache caches records for a log and epoch that are not yet confirmed as fully stored by the sequencer (ie they are "unclean").

|   Column   |   Type   |   Description   |
|------------|:--------:|-----------------|
| node\_id | int | Node ID this row is for. |
| log\_id | log_id | Log ID for this EpochRecordCache enrty. |
| shard | int | Shard ID for this EpochRecordCache entry. |
| epoch | long | The epoch of this EpochRecordCache entry. |
| payload\_bytes | long | Total size of payloads of records above LNG held by this EpochRecordCache. |
| num\_records | long | Number of records above LNG held by this EpochRecordCache. |
| consistent | bool | True if the cache is in consistent state and it is safe to consult it as the source of truth. |
| disabled | bool | Whether the cache is disabled. |
| head\_esn | long | ESN of the head of the buffer. |
| max\_esn | long | Largest ESN ever put in the cache. |
| first\_lng | long | The first LNG the cache has ever seen since its creation. |
| offset\_within\_epoch | string | Most recent value of the amount of data written in the given epoch as seen by this shard. |
| tail\_record\_lsn | long | LSN of the tail record of this epoch. |
| tail\_record\_ts | long | Timestamp of the tail record of this epoch. |

## record\_csi
This table allows fetching information about individual record copies in the cluster.  The user must provide query constraints on the "log\_id" and "lsn" columns.  This table can be useful to introspect where copies of a record are stored and see their metadata.  Do not use it to serve production use cases as this query runs very inneficiently (it bypasses the normal read protocol and instead performs a point query on all storage nodes in the cluster). This table is different from the record table in the sense that it only queries the copyset index and therefore can be more efficient. It can be used to check for divergence between the data and copyset index.

|   Column   |   Type   |   Description   |
|------------|:--------:|-----------------|
| node\_id | int | Node ID this row is for. |
| log\_id | log_id | ID of the log this record is for. |
| lsn | lsn | Sequence number of the record. |
| shard | int | ID of the shard that holds this record copy. |
| wave | int | If "is\_written\_by\_recovery" is 0, contains the wave of that record. |
| recovery\_epoch | int | If "is\_written\_by\_recovery" is 1, contains the "sequencer epoch" of the log recovery. |
| timestamp | string | Timestamp in milliseconds of the record. |
| last\_known\_good | int | Highest ESN in this record's epoch such that at the time this messagewas originally sent by a sequencer all records with this and lower ESNs in this epoch were known to the sequencer to be fully stored on R nodes. |
| copyset | string | Copyset of the record. |
| flags | string | Flags for that record.  See "logdevice/common/LocalLogStoreRecordFormat.h" to see the list of flags. |
| offset\_within\_epoch | string | Amount of data written to that record within the epoch. |
| optional\_keys | string | Optional keys provided by the user.  See "AppendAttributes" in "logdevice/include/Record.h". |
| is\_written\_by\_recovery | bool | Whether this record was replicated by the Log Recovery. |
| payload | string | Payload in hex format. |

## recoveries
Dumps debugging information about currently running Log Recovery procedures.  See "logdevice/common/LogRecoveryRequest.h" and "logdevice/common/EpochRecovery.h".

|   Column   |   Type   |   Description   |
|------------|:--------:|-----------------|
| node\_id | int | Node ID this row is for. |
| log\_id | log_id | Log being recovered. |
| epoch | long | Epoch being recovered. |
| state | string | State of the EpochRecovery state machine. Can be one of "FETCHING\_LCE" (We are fetching LCE from the epoch store, an EpochRecovery state machine will be created for all epochs between  LCE+1 and "epoch"), "READING\_METADATA\_LOG" (We are reading metadata log in order to retrieve metadata necessary to start the EpochRecovery state machines), "READING\_SEQUENCER\_METADATA" (We are waiting for the epoch metadata of "epoch" to appear in the metadata log), "ACTIVE" (EpochRecovery is active), "INACTIVE" (EpochRecovery is scheduled for activation). |
| lng | long | EpochRecovery's estimate of LNG for this log. |
| dig\_sz | long | Number of entries in the digest. |
| dig\_fmajority | long | Whether or not an f-majority of shards in that epoch's storage set have completed the digest phase. |
| dig\_replic | long | Whether or not the set of shards that have completed the digest meet the replication requirements. |
| dig\_author | long | Whether the digest is authoritative.  If the  digest is not authoritative, this means too many shards in the storage set are under-replicated.  This is an emergency procedure in which recovery will not plug holes in order to ensure DATALOSS gaps are reported to readers. In this mode, some legitimate holes may be reported as false positive DATALOSS gaps to the reader. |
| holes\_plugged | long | Number of holes plugged by this EpochRecovery. |
| holes\_replicate | long | Number of holes re-replicated by this EpochRecovery. |
| holes\_conflict | long | Number of hole/record conflicts found by this EpochRecovery. |
| records\_replicate | long | Number of records re-replicated by this EpochRecovery. |
| n\_mutators | long | Number of active Mutators.  A mutator is responsible for replicating a hole or record.  See "logdevice/common/Mutator.h". |
| recovery\_state | string | State of each shard in the recovery set.  Possible states: "s" means that the shard still has not sent a SEALED reply, "S" means that the shard has sent a SEALED reply, "d" means that this shard is sending a digest, "D" means that this shard completed the digest, "m" means that this shard has completed the digest and is eligible to participate in the mutation phase, "c" means that the shard has been sent a CLEAN request, "C" means that the shard has successfully processed the CLEANED request.  Recovery will stall if too many nodes are in the "s", "d" or "c" phases.  Suffix "(UR) indicates that the shard is under-replicated.  Suffix "(AE)" indicates that the shard is empty. |
| created | time | Date and Time of when this EpochRecovery was created. |
| restarted | time | Date and Time of when this EpochRecovery was last restarted. |
| n\_restarts | long | Number of times this EpochRecovery was restarted. |

## sequencers
This table dumps information about all the Sequencer objects in the cluster.  See "logdevice/common/Sequencer.h".

|   Column   |   Type   |   Description   |
|------------|:--------:|-----------------|
| node\_id | int | Node ID this row is for. |
| log\_id | log_id | Log ID this sequencer is for. |
| metadata\_log\_id | string | ID of the corresponding metadata log. |
| state | string | State of the sequencer.  Can be one of: "UNAVAILABLE" (Sequencer has not yet gotten a valid epoch metadata with an epoch number), "ACTIVATING" (Sequencer is in the process of getting an epoch number and retrieving metadata from the epoch store), "ACTIVE" (Sequencer is able to replicate), "PREEMPTED" (Sequencer has been preempted by another sequencer "preempted\_by", appends to this node will be redirected to it), "PERMANENT\_ERROR" (Permanent process-wide error such as running out of ephemera ports). |
| epoch | long | Epoch of the sequencer. |
| next\_lsn | lsn | Next LSN to be issued to a record by this sequencer. |
| meta\_last\_released | lsn | Last released LSN of the metadata log. |
| last\_released | lsn | Last released LSN for the data log. |
| last\_known\_good | lsn | Last known good LSN for this data log.  This is the highest ESN such that all records up to that ESN are known to be fully replicated. |
| in\_flight | long | Number of appends currently in flight. |
| last\_used\_ms | long | Timestamp of the last record appended by this sequencer. |
| state\_duration\_ms | long | Amount of time in milliseconds the sequencer has been in the current "state". |
| nodeset\_state | string | Contains debugging information about shards in that epoch's storage set.  "H" means that the shard is healthy.  "L" means that the shard reached the low watermark for space usage.  "O" means that the shard reported being overloaded.  "S" means that the shard is out of space.  "U" means that the sequencer cannot establish a connection to the shard.  "D" means that the shard's local log store is not accepting writes.  "G" means that the shard is greylisting for copyset selection because it is too slow.  "P" means that the sequencer is currently probling the health of this shard. |
| preempted\_epoch | long | Epoch of the sequencer that preempted this sequencer (if any). |
| preempted\_by | long | ID of the sequencer that preempted this sequencer (if any). |
| draining | long | Epoch that is draining (if any).  Draining means that the sequencer stopped accepting new writes but is completing appends curretnly in flight. |
| metadata\_log\_written | long | Whether the epoch metadata used by this sequencer has been written to the metadata log. |
| trim\_point | lsn | The current trim point for this log. |
| last\_byte\_offset | string | Offsets of the tail record. |
| bytes\_per\_second | real | Append throughput averaged over the last throughput\_window\_seconds seconds. |
| throughput\_window\_seconds | real | Time window over which append throughput estimate bytes\_per\_second was obtained. |
| seconds\_until\_nodeset\_adjustment | real | Time until the next potential nodeset size adjustment or nodeset randomization. Zero if nodeset adjustment is disabled or if the sequencer reactivation is in progress. |

## settings
Dumps the state of all settings for all nodes in the cluster.

|   Column   |   Type   |   Description   |
|------------|:--------:|-----------------|
| node\_id | int | Node ID this row is for. |
| bundle\_name | string | Name of the bundle this setting is for. |
| name | string | Name of the setting. |
| current\_value | string | Current value of the setting. |
| default\_value | string | Default value of the setting. |
| from\_cli | string | Value provided by the CLI, or null. |
| from\_config | string | Value provided by the config or null. |
| from\_admin\_cmd | string | Value provided by the "set" admin command or null. |

## shard\_authoritative\_status
Show the current state in the event log. This contains each shard's authoritative status (see "logdevice/common/AuthoritativeStatus.h"), as well as additional information related to rebuilding.

|   Column   |   Type   |   Description   |
|------------|:--------:|-----------------|
| node\_id | long | Id of the node. |
| shard | long | Id of the shard. |
| rebuilding\_version | string | Rebuilding version: the LSN of the last SHARD\_NEEDS\_REBUILD delta from the event log. |
| authoritative\_status | string | Authoritative status of the shard. |
| donors\_remaining | string | If authoritative status is UNDERREPLICATION, list of donors that have not finished rebuilding the under-replicated data. |
| drain | int | Whether the shard is being drained or has been drained. |
| dirty\_ranges | string | Time ranges where this shard may be missing data.  This happens if the LogDevice process on this storage node crashed before committing data to disk. |
| rebuilding\_is\_authoritative | int | Whether rebuilding is authoritative.  A non authoritative rebuilding means that too many shards lost data such that all copies of some records may be unavailable.  Some readers may stall when this happens and there are some shards that are still marked as recoverable. |
| data\_is\_recoverable | int | Indicates whether the shard's data has been marked as unrecoverable using `ldshell mark-unrecoverable`. If all shards in the rebuilding set are marked unrecoverable, shards for which rebuilding completed will transition to AUTHORITATIVE\_EMPTY status even if that rebuilding is non authoritative. Note that if logdeviced is started on a shard whose corresponding disk has been wiped by a remediation, the shard's data will automatically be considered unrecoverable. |
| source | string | Entity that triggered rebuilding for this shard. |
| details | string | Reason for rebuilding this shard. |
| rebuilding\_started\_ts | string | When rebuilding was started. |
| rebuilding\_completed\_ts | string | When the shard transitioned to AUTHORITATIVE\_EMPTY. |

## shard\_authoritative\_status\_spew
Like shard\_authoritative\_status\_verbose but has even more columns.

|   Column   |   Type   |   Description   |
|------------|:--------:|-----------------|
| node\_id | long | Id of the node. |
| shard | long | Id of the shard. |
| rebuilding\_version | string | Rebuilding version: the LSN of the last SHARD\_NEEDS\_REBUILD delta from the event log. |
| authoritative\_status | string | Authoritative status of the shard. |
| donors\_remaining | string | If authoritative status is UNDERREPLICATION, list of donors that have not finished rebuilding the under-replicated data. |
| drain | int | Whether the shard is being drained or has been drained. |
| dirty\_ranges | string | Time ranges where this shard may be missing data.  This happens if the LogDevice process on this storage node crashed before committing data to disk. |
| rebuilding\_is\_authoritative | int | Whether rebuilding is authoritative.  A non authoritative rebuilding means that too many shards lost data such that all copies of some records may be unavailable.  Some readers may stall when this happens and there are some shards that are still marked as recoverable. |
| data\_is\_recoverable | int | Indicates whether the shard's data has been marked as unrecoverable using `ldshell mark-unrecoverable`. If all shards in the rebuilding set are marked unrecoverable, shards for which rebuilding completed will transition to AUTHORITATIVE\_EMPTY status even if that rebuilding is non authoritative. Note that if logdeviced is started on a shard whose corresponding disk has been wiped by a remediation, the shard's data will automatically be considered unrecoverable. |
| source | string | Entity that triggered rebuilding for this shard. |
| details | string | Reason for rebuilding this shard. |
| rebuilding\_started\_ts | string | When rebuilding was started. |
| rebuilding\_completed\_ts | string | When the shard transitioned to AUTHORITATIVE\_EMPTY. |
| mode | string | Whether the shard participates in its own rebuilding |
| acked | int | Whether the node acked the rebuilding. (Why would such nodes remain in the rebuilding set at all? No one remembers now.) |
| ack\_lsn | string | LSN of the SHARD\_ACK\_REBUILT written by this shard. |
| ack\_version | string | Version of the rebuilding that was acked. |
| donors\_complete | string |  |
| donors\_complete\_authoritatively | string |  |

## shard\_authoritative\_status\_verbose
Like shard\_authoritative\_status but has more columns and prints all the shards contained in the RSM state, including the noisy ones, e.g. nodes that were removed from config. Can be useful for investigating specifics of the event log RSM behavior, but not for much else.

|   Column   |   Type   |   Description   |
|------------|:--------:|-----------------|
| node\_id | long | Id of the node. |
| shard | long | Id of the shard. |
| rebuilding\_version | string | Rebuilding version: the LSN of the last SHARD\_NEEDS\_REBUILD delta from the event log. |
| authoritative\_status | string | Authoritative status of the shard. |
| donors\_remaining | string | If authoritative status is UNDERREPLICATION, list of donors that have not finished rebuilding the under-replicated data. |
| drain | int | Whether the shard is being drained or has been drained. |
| dirty\_ranges | string | Time ranges where this shard may be missing data.  This happens if the LogDevice process on this storage node crashed before committing data to disk. |
| rebuilding\_is\_authoritative | int | Whether rebuilding is authoritative.  A non authoritative rebuilding means that too many shards lost data such that all copies of some records may be unavailable.  Some readers may stall when this happens and there are some shards that are still marked as recoverable. |
| data\_is\_recoverable | int | Indicates whether the shard's data has been marked as unrecoverable using `ldshell mark-unrecoverable`. If all shards in the rebuilding set are marked unrecoverable, shards for which rebuilding completed will transition to AUTHORITATIVE\_EMPTY status even if that rebuilding is non authoritative. Note that if logdeviced is started on a shard whose corresponding disk has been wiped by a remediation, the shard's data will automatically be considered unrecoverable. |
| source | string | Entity that triggered rebuilding for this shard. |
| details | string | Reason for rebuilding this shard. |
| rebuilding\_started\_ts | string | When rebuilding was started. |
| rebuilding\_completed\_ts | string | When the shard transitioned to AUTHORITATIVE\_EMPTY. |
| mode | string | Whether the shard participates in its own rebuilding |
| acked | int | Whether the node acked the rebuilding. (Why would such nodes remain in the rebuilding set at all? No one remembers now.) |
| ack\_lsn | string | LSN of the SHARD\_ACK\_REBUILT written by this shard. |
| ack\_version | string | Version of the rebuilding that was acked. |

## shard\_rebuildings
Show debugging information about the ShardRebuilding state machines (see "logdevice/server/rebuilding/ShardRebuildingV1.h").  This state machine is responsible for running all LogRebuilding state machines (see "logs\_rebuilding" table) for all logs in a donor shard.

|   Column   |   Type   |   Description   |
|------------|:--------:|-----------------|
| node\_id | int | Node ID this row is for. |
| shard\_id | long | Donor shard. |
| rebuilding\_set | string | Rebuilding set considered.  See "rebuilding\_set" column of the "log\_rebuilding" table. |
| version | lsn | Rebuilding version.  This version comes from the event log RSM that coordinates rebuilding.  See the "event\_log" table. |
| global\_window\_end | time | End of the global window (if enabled with --rebuilding-global-window).  This is a time window used to synchronize all ShardRebuilding state machines across all donor shards. |
| local\_window\_end | time | ShardRebuilding schedules reads for all logs within a time window called the local window.  This shows the end of the current window. |
| num\_logs\_waiting\_for\_plan | long | Number of logs that are waiting for a plan.  See "logdevice/include/RebuildingPlanner.h". |
| num\_logs\_catching\_up | long | Number of LogRebuilding state machines currently active. |
| num\_logs\_queued\_for\_catch\_up | long | Number of LogRebuilding state machines that are inside the local window and queued for catch up. |
| num\_logs\_in\_restart\_queue | long | Number of LogRebuilding state machines that are ready to be restarted as soon as a slot is available.  Logs are scheduled for a restart if we waited too long for writes done by the state machine to be acknowledged as durable. |
| total\_memory\_used | long | Total amount of memory used by all LogRebuilding state machines. |
| stall\_timer\_active | int | If true, all LogRebuilding state machines are stalled until memory usage decreased. |
| num\_restart\_timers\_active | long | Number of logs that have completed but for which we are still waiting for acknowlegments that writes were durable. |
| num\_active\_logs | long | Set of logs being rebuilt for this shard.  The shard completes rebuilding when this number reaches zero. |
| participating | int | true if this shard is a donor for this rebuilding and hasn't finished rebuilding yet. |
| time\_by\_state | string | Time spent in each state. 'stalled' means either waiting for global window or aborted because of a persistent error. V2 only |
| task\_in\_flight | int | True if a storage task for reading records is in queue or in flight right now. V2 only. |
| persistent\_error | int | True if we encountered an unrecoverable error when reading. Shard shouldn't stay in this state for more than a few seconds: it's expected that RebuildingCoordinator will request a rebuilding for this shard, and rebuilding will rewind without this node's participation. V2 only. |
| read\_buffer\_bytes | long | Bytes of records that we've read but haven't started re-replicating yet. V2 only. |
| records\_in\_flight | long | Number of records that are being re-replicated right now. V2 only. |
| read\_pointer | string | How far we have read: partition, log ID, LSN. V2 only. |
| progress | real | Approximately what fraction of the work is done, between 0 and 1. -1 if the implementation doesn't support progress estimation. |

## shards
Show information about all shards in a cluster.

|   Column   |   Type   |   Description   |
|------------|:--------:|-----------------|
| node\_id | int | Node ID this row is for. |
| shard | long | Shard the information is for. |
| is\_failing | long | If true, the server could not open the DB for this shard on startup.  This can happen if the disk on which this shard resides is broken for instance. |
| accepting\_writes | string | Status indicating if this shard is accepting writes.  Can be one of: "OK" (the shard is accepting writes), "LOW\_ON\_SPC" (the shard is accepting writes but is low on free space), "NOSPC" (the shard is not accepting writes because it is low on space), "DISABLED" (The shard will never accept writes. This can happen if the shard entered fail-safe mode). |
| rebuilding\_state | string | "NONE": the shard is not rebuilding.  "WAITING\_FOR\_REBUILDING": the shard is missing data and is waiting for rebuilding to start.  "REBUILDING": the shard is missing data and rebuilding was started. |
| default\_cf\_version | long | Returns current version of the data.  if LogsDB is  enabled, this will return the version of the default column familiy. |

## sockets
Tracks all sockets on all nodes in the cluster.

|   Column   |   Type   |   Description   |
|------------|:--------:|-----------------|
| node\_id | int | Node ID this row is for. |
| state | string | State of the socket.   I: The socket is Inactive;   C: The socket is connecting;   H: The socket is doing the handshake at the LD protocol level;   A: The socket is active. |
| name | string | Name of the socket. If the other end is a client, the format is similar to the column "client" of the table "catchup\_queues" and the column "client" of the table "readers". If the other end is another node in the cluster, describes that's node's id and ip. |
| pending\_kb | real | Number of bytes that are available for writting on the socket's output evbuffer. If this value is high this usually means that the other end is not able to read messages as fast as we are writting them. |
| available\_kb | real | Number of bytes that are available for reading on the socket's input evbuffer.  If this value is high this usually means that the other end is writting faster than this node is able to read. |
| read\_mb | real | Number of bytes that were read from the socket. |
| write\_mb | real | Number of bytes that were written to the socket. |
| read\_cnt | int | Number of messages that were read from the socket. |
| write\_cnt | int | Number of messages that were written to the socket. |
| proto | int | Protocol that was handshaken. Do not trust this value if the socket's state is not active. |
| sendbuf | int | Size of the send buffer of the underlying TCP socket. |
| peer\_config\_version | int | Last config version that the peer advertised |
| is\_ssl | int | Set to true if this socket uses SSL. |
| fd | int | The file descriptor of the underlying os socket. |

## stats
Return statistics for all nodes in the cluster.  See "logdevice/common/stats/".  See "stats\_rocksdb" for statitistics related to RocksDB.

|   Column   |   Type   |   Description   |
|------------|:--------:|-----------------|
| node\_id | int | Node ID this row is for. |
| name | string | Name of the stat counter. |
| value | long | Value of the stat counter. |

## stats\_rocksdb
Return RocksDB statistics for all nodes in the cluster.

|   Column   |   Type   |   Description   |
|------------|:--------:|-----------------|
| node\_id | int | Node ID this row is for. |
| name | string | Name of the stat counter. |
| value | long | Value of the stat counter. |

## storage\_tasks
List of storage tasks currently pending on the storage thread queues. Note that this does not include the task that is currently executing, nor the tasks that are queueing on the per-worker storage task queues.  Querying this table prevents the storage tasks from being popped off the queue while it's executing, so be careful with it.

|   Column   |   Type   |   Description   |
|------------|:--------:|-----------------|
| node\_id | int | Node ID this row is for. |
| shard | long | Index of the local log store shard to query storage tasks on. |
| priority | string | Priority of the storage task. The tasks with a higher priority get executed before tasks with a lower priority. |
| is\_write\_queue | bool | True if this is a task from the write\_queue, otherwise this is a task from the ordinary storage task queue for the thread. |
| sequence\_no | long | Sequence No. of the task. For task that are on the same queue with the same priority, tasks with lower sequence No. will execute first. |
| thread\_type | string | The type of the thread the task is queueing for. |
| task\_type | string | Type of the task, if specified. |
| enqueue\_time | time | Time when the storage task has been inserted into the queue. |
| durability | string | The durability requirement for this storage task (applies to writes). |
| log\_id | log_id | Log ID that the storage task will perform writes/reads on. |
| lsn | lsn | LSN that the storage task will act on. The specific meaning of this field varies depending on the task type. |
| client\_id | string | ClientID of the client that initiated the storage task. |
| client\_address | string | Address of the client that initiated the storage task. |
| extra\_info | string | Other information specific to particular task type. |

## stored\_logs
List of logs that have at least one record currently present in LogsDB, per shard. Doesn't include internal logs (metadata logs, event log, config log).  Note that it is possible that all the existing records are behind the trim point but haven't been removed from the DB yet (by dropping or compacting partitions); see also the "rocksdb-partition-compaction-schedule" setting.

|   Column   |   Type   |   Description   |
|------------|:--------:|-----------------|
| node\_id | int | Node ID this row is for. |
| log\_id | log_id | Log ID present in this shard. |
| shard | long | Shard that contains this log. |
| highest\_lsn | lsn | Highest LSN that this shard has ever seen for this log. |
| highest\_partition | long | ID of the highest LogsDB partition that contains at least one record for this log. You can use the "partitions" table to inspect LogsDB partitions. |
| highest\_timestamp\_approx | time | Approximate value of the highest timestamp of records for this log on this shard. This is an upper bound, as long as timestamps are non-decreasing with LSN in this log.  Can be overestimated by up to "rocksdb-partition-duration" setting. |

## sync\_sequencer\_requests
List the currently running SyncSequencerRequests on that cluster.  See "logdevice/common/SyncSequencerRequest.h".

|   Column   |   Type   |   Description   |
|------------|:--------:|-----------------|
| node\_id | int | Node ID this row is for. |
| log\_id | log_id | Log ID the SyncSequencerRequest is for. |
| until\_lsn | lsn | Next LSN retrieved from the sequencer. |
| last\_released\_lsn | lsn | Last released LSN retrieved from the sequencer. |
| last\_status | string | Status of the last GetSeqStateRequest performed by SyncSequencerRequest. |

