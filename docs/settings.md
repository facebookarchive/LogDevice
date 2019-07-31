---
id: Settings
title: Configuration settings
sidebar_label: Settings
---

## Admin API/server
|   Name    |   Description   |  Default  |   Notes   |
|-----------|-----------------|:---------:|-----------|
| admin-port | TCP port on which the server listens to for admin commands, supports commands over SSL | 6440 | requires&nbsp;restart, server&nbsp;only |
| admin-unix-socket | Path to the unix domain socket the server will use to listen for admin thrift interface |  | requires&nbsp;restart, server&nbsp;only |
| enable-cluster-maintenance-state-machine | Enables the internal state replicated state machine that holds the maintenance definitions requested by the rebuilding supervisor or via the  admin API. Enabling the state machine will also enable posting internal maintenance requests instead of writing to event log directly | false | requires&nbsp;restart, server&nbsp;only |
| enable-maintenance-manager | Start Maintenance Manager. This will automatically enable the maintenance state machine as well (--enable-cluster-maintenance-state-machine). | false | requires&nbsp;restart, server&nbsp;only |
| enable-safety-check-periodic-metadata-update | Safety check to update its metadata cache periodically | false | server&nbsp;only |
| maintenance-log-snapshotting | Allow the maintenance log to be snapshotted onto a snapshot log. This requires the maintenance log group to contain two logs, the first one being the snapshot log and the second one being the delta log. | false | server&nbsp;only |
| maintenance-log-snapshotting-period | Controls time based snapshotting. New maintenancelog snapshot will be created after this period if there are new deltas | 1h | server&nbsp;only |
| maintenance-manager-reevaluation-timeout | Timeout after which a new run is scheduled in MaintenanceManager. Used for periodic reevaluation of the state in the absence of any state changes | 2min | server&nbsp;only |
| read-metadata-from-sequencers | Safety checker to read the metadata of logs directly from sequencers. | true | server&nbsp;only |
| safety-check-failure-sample-size | The number of sample epochs returned by the Maintenance API for each maintenance if safety check blocks the operation. | 10 | server&nbsp;only |
| safety-check-max-batch-size | The maximum number of logs to be checked in a single batch. Larger batches mean faster performance but means blocking the CPU thread pool for longer (not yielding often enough) | 15000 | server&nbsp;only |
| safety-check-max-logs-in-flight | The number of concurrent logs that we runs checks against during execution of the CheckImpact operation either internally during a maintenance or through the Admin API's checkImpact() call | 1000 | server&nbsp;only |
| safety-check-metadata-update-period | The period between automatic metadata updates for safety checker internal cache | 10min | server&nbsp;only |

## Batching and compression
|   Name    |   Description   |  Default  |   Notes   |
|-----------|-----------------|:---------:|-----------|
| buffered-writer-bg-thread-bytes-threshold | BufferedWriter can send batches to a background thread.  For small batches, where the overhead dominates, this will just slow things down.  If the total size of the batch is less than this, it will constructed / compressed on the Worker thread, blocking other appends to all logs in that shard.  If larger, it will be enqueued to a helper thread. | 4096 |  |
| buffered-writer-zstd-level | Zstd compression level to use in BufferedWriter. | 1 |  |
| sequencer-batching | Accumulate appends from clients and batch them together to create fewer records in the system. This setting is only used when the log group doesn't override it | false | server&nbsp;only |
| sequencer-batching-compression | Compression setting for sequencer batching (if used). It can be 'none' for no compression; 'zstd' for ZSTD; 'lz4' for LZ4; or lz4\_hc for LZ4 High Compression. The default is ZSTD. When enabled, this gets applied to the first new batch. This setting is only used when the log group doesn't override it | zstd | server&nbsp;only |
| sequencer-batching-passthru-threshold | Sequencer batching (if used) will pass through any appends with payload size over this threshold (if positive).  This saves us a compression round trip when a large batch comes in from BufferedWriter and the benefit of batching and recompressing would be small. | -1 | server&nbsp;only |
| sequencer-batching-size-trigger | Sequencer batching (if used) flushes buffered appends for a log when the total amount of buffered uncompressed data reaches this many bytes (if positive). When enabled, this gets applied to the first new batch. This setting is only used when the log group doesn't override it | -1 | server&nbsp;only |
| sequencer-batching-time-trigger | Sequencer batching (if used) flushes buffered appends for a log when the oldest buffered append is this old. When enabled, this gets applied to the first new batch. This setting is only used when the log group doesn't override it | 1s | server&nbsp;only |

## Configuration
|   Name    |   Description   |  Default  |   Notes   |
|-----------|-----------------|:---------:|-----------|
| admin-client-capabilities | If set, the client will have the capabilities for administrative operations such as changing NodesConfiguration. Usually used by emergency tooling. Beware that admin clients use a different NodesConfigurationStore that may not support a large fan-out, so this settings shouldn't be applied to large number of clients (e.g., through client\_settings in settings config). | false | client&nbsp;only |
| check-metadata-log-empty-timeout | Timeout for request that verifies that a metadata log does not already exist for a log that is presumed new and whose metadata provisioning has been initiated by a sequencer activation | 300s | server&nbsp;only |
| client-config-fetch-allowed | If true, servers will be allowed to fetch configs from the client side of a connection during config synchronization. | true | server&nbsp;only |
| client-default-dscp | Use default DSCP to setup to client sockets at Sender.Range was defined by https://tools.ietf.org/html/rfc4594#section-1.4.4 | 0 | requires&nbsp;restart |
| config-path | location of the cluster config file to use. Format: [file:]<path-to-config-file> or configerator:<configerator-path> |  | CLI&nbsp;only, requires&nbsp;restart, server&nbsp;only |
| enable-logsconfig-manager | If true, logdeviced will load the logs configuration from the internal replicated storage and will ignore the logs section in the config file. This also enables the remote management API for logs config. | true |  |
| enable-nodes-configuration-manager | If set, NodesConfigurationManager and its workflow will be enabled. | false | requires&nbsp;restart |
| file-config-update-interval | interval at which to poll config file for changes (if reading config from file on disk | 10000ms |  |
| initial-config-load-timeout | maximum time to wait for initial server configuration until giving up | 15s | CLI&nbsp;only, requires&nbsp;restart, server&nbsp;only |
| logsconfig-manager-grace-period | Grace period before making a change to the logs config available to the server. | 0ms |  |
| logsconfig-max-delta-bytes | How many bytes of deltas to keep in the logsconfig deltas log before we snapshot it. | 10485760 | server&nbsp;only |
| logsconfig-max-delta-records | How many delta records to keep in the logsconfig deltas log before we snapshot it. | 4000 | server&nbsp;only |
| logsconfig-snapshotting-period | Controls time based snapshotting. New logsconfig snapshot will be created after this period if there are new log configuration deltas | 1h | server&nbsp;only |
| max-sequencer-background-activations-in-flight | Max number of concurrent background sequencer activations to run. Background sequencer activations perform log metadata changes (reprovisioning) when the configuration attributes of a log change. | 20 | server&nbsp;only |
| nodes-configuration-init-retry-timeout | timeout settings for the exponential backoff retry behavior for initializing Nodes Configuration for the first time | 500ms..5s |  |
| nodes-configuration-init-timeout | defines the maximum time allowed on the initial nodes configuration fetch. | 60s |  |
| nodes-configuration-manager-intermediary-shard-state-timeout | Timeout for proposing the transition for a shard from an intermediary state to its 'destination' state | 180s |  |
| nodes-configuration-manager-store-polling-interval | Polling interval of NodesConfigurationManager to NodesConfigurationStore to read NodesConfiguration | 3s |  |
| nodes-configuration-seed-servers | The seed string that will be used to fetch the initial nodes configuration. It can be in the form string:<server1>,<server2>,etc. Or you can provide an smc tier via 'smc:<smc\_tier>'. If it's empty, NCM client bootstraping is not used. |  | client&nbsp;only |
| on-demand-logs-config | Set this to true if you want the client to get log configuration on demand from the server when log configuration is not included in the main config file. | false | requires&nbsp;restart, client&nbsp;only |
| on-demand-logs-config-retry-delay | When a client's attempt to get log configuration information from server on demand fails, the client waits this much before retrying. | 5ms..1s | client&nbsp;only |
| remote-logs-config-cache-ttl | The TTL for cache entries for the remote logs config. If the logs config is not available locally and is fetched from the server, this will determine how fresh the log configuration used by the client will be. | 60s | requires&nbsp;restart, client&nbsp;only |
| sequencer-background-activation-retry-interval | Retry interval on failures while processing background sequencer activations for reprovisioning. | 500ms | server&nbsp;only |
| sequencer-epoch-store-write-retry-delay | The retry delay for sequencer writing log metadata into the epoch store during log reconfiguration. | 5s..1min-2x | server&nbsp;only |
| sequencer-historical-metadata-retry-delay | The retry delay for sequencer reading metadata log for historical epoch metadata during log reconfiguration. | 5s..1min-2x | server&nbsp;only |
| sequencer-metadata-log-write-retry-delay | The retry delay for sequencer writing into its own metadata log during log reconfiguration. | 500ms..30s-2x | server&nbsp;only |
| sequencer-reactivation-delay-secs | Some sequencer reactivations may be postponed when the changes that triggered the reactivation are not important enough to be propogated immediately. E.g., changes to replication factor or window size, need to be made immediately visible on the other hand changes changes to the nodeset due to say the 'exclude\_from\_nodeset' flag being set as part of a passive drain can be postponed. If the reactivations can be postponed then the delay is chosen to be a radnom delay seconds between the above range. If 0 then don't postpone  | 60s..3600s | server&nbsp;only |
| server-based-nodes-configuration-polling-wave-timeout | timeout settings for server based Nodes Configuration Store's multi-wavebackoff retry behavior | 500ms..10s |  |
| server-based-nodes-configuration-store-polling-responses | how many successful responses for server based Nodes Configuration Storepolling to wait for each round | 2 |  |
| server-based-nodes-configuration-store-timeout | The timeout of the Server Based Nodes Configuration Store's NODES\_CONFIGURATION polling round. | 60s |  |
| server-default-dscp | Use default DSCP to setup to server sockets at Sender.Range was defined by https://tools.ietf.org/html/rfc4594#section-1.4.4 | 0 | requires&nbsp;restart, server&nbsp;only |
| server\_based\_nodes\_configuration\_store\_polling\_extra\_requests | how many extra requests to send for server based Nodes Configuration Store polling in addition to the required response for each wave | 1 |  |
| shutdown-on-my-node-id-mismatch | Gracefully shutdown whenever the server's NodeID changes | true | server&nbsp;only |
| use-nodes-configuration-manager-nodes-configuration | If true and enable\_nodes\_configuration\_manager is set, logdevice will use the nodes configuration from the NodesConfigurationManager. | false | requires&nbsp;restart |
| zk-config-polling-interval | polling and retry interval for Zookeeper config source | 1000ms | CLI&nbsp;only |

## Core settings
|   Name    |   Description   |  Default  |   Notes   |
|-----------|-----------------|:---------:|-----------|
| admin-enabled | Is Admin API enabled? | true | requires&nbsp;restart, **experimental**, server&nbsp;only |
| append-timeout | Timeout for appends. If omitted the client timeout will be used. |  | client&nbsp;only |
| command-port | TCP port on which the server listens to for admin commands, supports commands over SSL | 5440 | requires&nbsp;restart, server&nbsp;only |
| enable-hh-wheel-backed-timers | Enables the new version of timers which run on a different threadand use HHWheelTimer backend. | true | requires&nbsp;restart |
| enable-is-log-empty-v2 | When enabled, the V2 implementation will be used to process all isLogEmpty requests. | false | client&nbsp;only |
| enable-store-histograms-calculations | Enables estimation of store timeouts per worker per node. | false | server&nbsp;only |
| external-loglevel | One of the following: critical, error, warning, info, debug, none | critical | server&nbsp;only |
| findkey-timeout | Findkey API call timeout. If omitted the client timeout will be used. |  | client&nbsp;only |
| log-file | write server error log to specified file instead of stderr |  | server&nbsp;only |
| loglevel | One of the following: critical, error, warning, info, debug, none | info | server&nbsp;only |
| logsconfig-timeout | Timeout for LogsConfig API requests. If omitted the client timeout will be used. |  | client&nbsp;only |
| max-nodes | Number of preallocated nodes in the cluster. Used for sizing data structures of the failure detector. | 512 | requires&nbsp;restart, server&nbsp;only |
| meta-api-timeout | Timeout for trims/isLogEmpty/tailLSN/datasize API/etc. If omitted the client timeout will be used. |  | client&nbsp;only |
| my-location | {client-only setting}. Specifies the location of the machine running the client. Used for determining whether to use SSL based on --ssl-boundary. Also used in local SCD reading. Format: "{region}.{dc}.{cluster}.{row}.{rack}". |  | requires&nbsp;restart, client&nbsp;only |
| port | TCP port on which the server listens for non-SSL clients | 16111 | CLI&nbsp;only, requires&nbsp;restart, server&nbsp;only |
| rsm-include-read-pointer-in-snapshot | Allow inclusion of read pointer in RSM snapshots. Note that if this is set to true IT IS UNSAFE TO CHANGE IT BACK TO FALSE! | false |  |
| server-id | optional server ID, reported by INFO admin command |  | requires&nbsp;restart, server&nbsp;only |
| shutdown-timeout | amount of time to wait for the server to shut down before terminating the process. Consider modifying --time-delay-before-force-abort when changing this value. | 120s | server&nbsp;only |
| store-histogram-min-samples-per-bucket | How many stores should the store histogram wait for before reporting latency estimates | 30 | server&nbsp;only |
| time-delay-before-force-abort | Time delay before force abort of remaining work is attempted during shutdown. The value is in 50ms time periods. The quiescence condition is checked once every 50ms time period. When the timer expires for the first time, all pending requests are aborted and the timer is restarted. On second expiration all remaining TCP connections are reset (RST packets sent). | 400 | server&nbsp;only |
| unmap-caches | unmap RocksDB block cache before dumping core (reduces core file size) | true | server&nbsp;only |
| user | user to switch to if server is run as root |  | requires&nbsp;restart, server&nbsp;only |
| zk-create-root-znodes | If "false", the root znodes for a tier should be pre-created externally before logdevice can do any ZooKeeper epoch store operations | true | **experimental**, server&nbsp;only |

## Failure detector
|   Name    |   Description   |  Default  |   Notes   |
|-----------|-----------------|:---------:|-----------|
| cluster-state-refresh-interval | how frequently to search for the sequencer in case of an append timeout | 10s | client&nbsp;only |
| enable-initial-get-cluster-state | Enable executing a GetClusterState request to retrieve the state of the cluster as soon as the client is created | true | client&nbsp;only |
| failover-blacklist-threshold | How many gossip intervals to ignore a node for after it performed a graceful failover | 100 | server&nbsp;only |
| failover-wait-time | How long to wait for the failover request to be propagated to other nodes | 3s | server&nbsp;only |
| gcs-wait-duration | How long to wait for get-cluster-state reply to come, to initialize state of cluster nodes. Bringup is sent after this reply comes or after timeout | 1s | server&nbsp;only |
| gossip-interval | How often to send a gossip message. Lower values improve detection time, but make nodes more chatty. | 100ms | requires&nbsp;restart, server&nbsp;only |
| gossip-logging-duration | How long to keep logging FailureDetector/Gossip related activity after server comes up. | 0s | server&nbsp;only |
| gossip-mode | How to select a node to send a gossip message to. One of: 'round-robin', 'random' (default) | random | requires&nbsp;restart, server&nbsp;only |
| gossip-threshold | Specifies after how many gossip intervals of inactivity a node is marked as dead. Lower values reduce detection time, but make false positives more likely. | 30 | server&nbsp;only |
| gossip-time-skew | How much delay is acceptable in receiving a gossip message. | 10s | server&nbsp;only |
| ignore-isolation | Ignore isolation detection. If set, a sequencer will accept append operations even if a majority of nodes appear to be dead. | false | server&nbsp;only |
| min-gossips-for-stable-state | After receiving how many gossips, should the FailureDetector consider itself stable and start doing state transitions of cluster nodes based on incoming gossips. | 3 | server&nbsp;only |
| suspect-duration | How long to keep a node in an intermediate state before marking it as available. Larger values make the cluster less prone to node flakiness, but extend the time needed for sequencer nodes to start participating. | 10s | server&nbsp;only |

## LogsDB
|   Name    |   Description   |  Default  |   Notes   |
|-----------|-----------------|:---------:|-----------|
| manual-compact-interval | minimal interval between consecutive manual compactions on log storesthat are out of disk space | 1h | server&nbsp;only |
| rocksdb-background-wal-sync | Perform all RocksDB WAL syncs on a background thread rather than synchronously on a 'fast' storage thread executing the write. | true | server&nbsp;only |
| rocksdb-directory-consistency-check-period | LogsDB will compare all on-disk directory entries with the in-memory directory no more frequently than once per this period of time. | 5min | server&nbsp;only |
| rocksdb-free-disk-space-threshold-low | Keep free disk space above this fraction of disk size by marking node full if we exceed it, and let the sequencer initiate space-based retention. Only counts logdevice data, so storing other data on the disk could cause it to fill up even with space-based retention enabled. 0 means disabled. | 0 | server&nbsp;only |
| rocksdb-io-tracing-shards | List of shards for which to enable IO tracing. IO tracing prints information about every single IO operation (like file read() and write() calls) to the log at info level. It's very spammy, use with caution. |  | server&nbsp;only |
| rocksdb-metadata-compaction-period | Metadata column family will be compacted at least this often if it has more than one sst file. This is needed to avoid performance issues in rare cases. Full scenario: suppose all writes to this node stopped; eventually all logs will be fully trimmed, and logsdb directory will be emptied by deleting each key; these deletes will usually be flushed in sst files different than the ones where the original entries are; this makes iterator operations very expensive because merging iterator has to skip all these deleted entries in linear time; this is especially bad for findTime. If we compact every hour, this badness would last for at most an hour. | 1h | server&nbsp;only |
| rocksdb-new-partition-timestamp-margin | Newly created partitions will get starting timestamp `now + new\_partition\_timestamp\_margin`. This absorbs the latency of creating partition and possible small clock skew between sequencer and storage node. If creating partition takes longer than that, or clock skew is greater than that, FindTime may be inaccurate. For reference, as of August 2017, creating a partition typically takes ~200-800ms on HDD with ~1100 existing partitions. | 10s | server&nbsp;only |
| rocksdb-num-metadata-locks | number of lock stripes to use to perform LogsDB metadata updates | 256 | requires&nbsp;restart, server&nbsp;only |
| rocksdb-partition-compaction-schedule | If set, indicate that the node wil run compaction. This is a list of durations indicating at what age to compact partition.  e.g. "3d, 7d" means that each partition will be compacted twice: when all logs with backlog of up to 3 days are trimmed from it, and when all logs with backlog of up to 7 days are trimmed from it. "auto" (default) means use all backlog durations from config. "disabled" disables partition compactions. | auto | server&nbsp;only |
| rocksdb-partition-compactions-enabled | perform background compactions for space reclamation in LogsDB | true | server&nbsp;only |
| rocksdb-partition-count-soft-limit | If the number of partitions in a shard reaches this value, some measures will be taken to limit the creation of new partitions: partition age limit is tripled; partition file limit is ignored; partitions are not pre-created on startup; partitions are not prepended for records with small timestamp. This limit is intended mostly as protection against timestamp outliers: e.g. if we receive a STORE with zero timestamp, without this limit we would create over a million partitions to cover the time range from 1970 to now. | 2000 | server&nbsp;only |
| rocksdb-partition-duration | create a new partition when the latest one becomes this old; 0 means infinity | 15min | server&nbsp;only |
| rocksdb-partition-file-limit | create a new partition when the number of level-0 files in the existing partition exceeds this threshold; 0 means infinity | 200 | server&nbsp;only |
| rocksdb-partition-flush-check-period | How often a flusher thread will go over all shard looking for memtables to flush. Flusher thread is responsible for deciding to flush memtables on various triggers like data age, idle time and size. If flushes are managed by logdevice, flusher thread is responsible for persisting any data on the system. This setting is tuned based on 3 things: memory size on node, write throughput node can support, and how fast data can be persisted. 0 disables all manual flushes done in tests to disable all flushes in the system. | 200ms | server&nbsp;only |
| rocksdb-partition-hi-pri-check-period | how often a background thread will check if new partition should be created | 2s | server&nbsp;only |
| rocksdb-partition-lo-pri-check-period | how often a background thread will trim logs and check if old partitions should be dropped or compacted, and do the drops and compactions | 30s | server&nbsp;only |
| rocksdb-partition-partial-compaction-file-num-threshold-old | don't consider file ranges for partial compactions (used during rebuilding) that are shorter than this for old partitions (>1d old). | 100 | server&nbsp;only |
| rocksdb-partition-partial-compaction-file-num-threshold-recent | don't consider file ranges for partial compactions (used during rebuilding) that are shorter than this, for recent partitions (<1d old). | 10 | server&nbsp;only |
| rocksdb-partition-partial-compaction-file-size-threshold | the largest L0 files that it is beneficial to compact on their own. Note that we can still compact larger files than this if that enables usto compact a longer range of consecutive files. | 50000000 | server&nbsp;only |
| rocksdb-partition-partial-compaction-largest-file-share | Partial compaction candidate file ranges that contain a file that comprises a larger propotion of the total file size in the range than this setting, will not be considered. | 0.7 | server&nbsp;only |
| rocksdb-partition-partial-compaction-max-file-size | the maximum size of an l0 file to consider for compaction. If not set, defaults to 2x --rocksdb-partition-partial-compaction-file-size-threshold | 0 | server&nbsp;only |
| rocksdb-partition-partial-compaction-max-files | the maximum number of files to compact in a single partial compaction | 120 | server&nbsp;only |
| rocksdb-partition-partial-compaction-max-num-per-loop | How many partial compactions to do in a row before re-checking if there are higher priority things to do (like dropping partitions). This value is not important; used for tests. | 4 | server&nbsp;only |
| rocksdb-partition-partial-compaction-old-age-threshold | A partition is considered 'old' from the perspective of partial compaction if it is older than the above hours. Otherwise it is considered a recent partition. Old and recent partitions have different thresholds: partition\_partial\_compaction\_file\_num\_threshold\_old and partition\_partial\_compaction\_file\_num\_threshold\_recent, when being considered for partial compaction. | 6h | server&nbsp;only |
| rocksdb-partition-partial-compaction-stall-trigger | Stall rebuilding writes if partial compactions are outstanding in at least this many partitions. 0 means infinity. | 50 | server&nbsp;only |
| rocksdb-partition-redirty-grace-period | Minumum guaranteed time period for a node to re-dirty a partition after a MemTable is flushed without incurring a syncronous write penalty to update the partition dirty metadata. | 5s | server&nbsp;only |
| rocksdb-partition-size-limit | create a new partition when size of the latest partition exceeds this threshold; 0 means infinity | 6G | server&nbsp;only |
| rocksdb-partition-timestamp-granularity | minimum and maximum timestamps of a partition will be updated this often | 5s | server&nbsp;only |
| rocksdb-pinned-memtables-limit-percent | Memory budget for flushed memtables pinned by iterators, as percentage of --rocksdb-memtable-size-per-node. More precisely, each shard will reject writes if its total memtable size (active+flushing+pinned) is greater than rocksdb-memtable-size-per-node/num-shards*(1 + rocksdb-pinned-memtables-limit-percent/100). Currently set to a high value because rejecting writes is too disruptive in practice, better to use more memory and hope we won't OOM. TODO (#45309029): make cached iterators not pin memtables, then decrease this setting back to something like 50%. | 200 | server&nbsp;only |
| rocksdb-prepended-partition-min-lifetime | Avoid dropping newly prepended partitions for this amount of time. | 300s | server&nbsp;only |
| rocksdb-print-details | If true, print information about each flushed memtable and each partial compaction. It's not very spammy, an event every few seconds at most. The same events are also always logged by rocksdb to LOG file, but with fewer logdevice-specific details. | false | server&nbsp;only |
| rocksdb-proactive-compaction-enabled | If set, indicate that we're going to proactively compact all partitions (besides two latest) that were never compacted. Compacting will be done in low priority background thread | false | server&nbsp;only |
| rocksdb-read-find-time-index | If set to true, the operation findTime will use the findTime index to seek to the LSN instead of doing a binary search in the partition. | false | server&nbsp;only |
| rocksdb-read-only | Open LogsDB in read-only mode | false | requires&nbsp;restart, server&nbsp;only |
| rocksdb-sbr-force | If true, space based retention will be done on the storage side, irrespective of whether sequencer initiated it or not. This is meant to make a node's storage available in case there is a critical bug. | false | **experimental**, server&nbsp;only |
| rocksdb-test-clamp-backlog | Override backlog duration of all logs to be <= this value. This is a quick hack for testing, don't use in production! The override applies only in a few places, not to everything using the log attributes. E.g. disable-data-log-rebuilding is not aware of this setting and will use full retention from log attributes. | 0 | server&nbsp;only |
| rocksdb-track-iterator-versions | Track iterator versions for the "info iterators" admin command | false | server&nbsp;only |
| rocksdb-unconfigured-log-trimming-grace-period | A grace period to delay trimming of records that are no longer in the config. The intent is to allow the oncall enough time to restore a backup of the config, in case the log(s) shouldn't have been removed. | 4d | server&nbsp;only |
| rocksdb-use-age-size-flush-heuristic | If true, we use `age * size` of the MemTable to decide if we need to flush it, otherwise we use `age` for that. | true | **experimental**, server&nbsp;only |
| rocksdb-use-copyset-index | If set to true, the read path will use the copyset index to skip records that do not pass copyset filters. This greatly improves the efficiency of reading and rebuilding if records are large (1KB or bigger). For small records, the overhead of maintaining the copyset index negates the savings. **WARNING**: if this setting is enabled, records written without --write-copyset-index will be skipped by the copyset filter and will not be delivered to readers. Enable --write-copyset-index first and wait for all data records written before --write-copyset-index was enabled (if any) to be trimmed before enabling this setting. | true | requires&nbsp;restart, server&nbsp;only |
| rocksdb-verify-checksum-during-store | If true, verify checksum on every store. Reject store on failure and return E::CHECKSUM\_MISMATCH. | true | server&nbsp;only |
| rocksdb-worker-blocking-io-threshold | Log a message if a blocking file deletion takes at least this long on a Worker thread | 10ms | server&nbsp;only |
| sbr-low-watermark-check-interval | Time after which space based trim check can be done on a nodeset | 60s | server&nbsp;only |
| sbr-node-threshold | threshold fraction of full nodes which triggers space-based retention, if enabled (sequencer-only option), 0 means disabled | 0 | server&nbsp;only |

## Monitoring
|   Name    |   Description   |  Default  |   Notes   |
|-----------|-----------------|:---------:|-----------|
| client-readers-flow-tracer-lagging-metric-num-sample-groups | Maximum number of samples that are kept by ClientReadersFlowTracer for computing relative reading speed in relation to writing speed. See client\_readers\_flow\_tracer\_lagging\_slope\_threshold. | 3 | client&nbsp;only |
| client-readers-flow-tracer-lagging-metric-sample-group-size | Number of samples in ClientReadersFlowTracer that are aggregated and recorded as one entry. See client-readers-flow-tracer-lagging-metric-sample-group-size. | 20 | client&nbsp;only |
| client-readers-flow-tracer-lagging-slope-threshold | If a reader's lag increase at at least this rate, the reader is considered lagging (rate given as variation of time lag per time unit). If the desired read ratio needs to be x% of the write ratio, set this threshold to be (1 - x / 100). | -0.3 | client&nbsp;only |
| client-readers-flow-tracer-period | Period for logging in logdevice\_readers\_flow scuba table and for triggering certain sampling actions for monitoring. Set it to 0 to disable feature. | 0s | client&nbsp;only |
| client-readers-flow-tracer-unhealthy-publish-weight | Weight given to traces of unhealthy readers when publishing samples (for improved debuggability). | 5.0 | client&nbsp;only |
| disable-trace-logger | If disabled, NoopTraceLogger will be used, otherwise FBTraceLogger is used | false | requires&nbsp;restart |
| message-tracing-log-level | For messages that pass the message tracing filters, emit a log line at this level. One of: critical, error, warning, notify, info, debug, spew | info |  |
| message-tracing-peers | Emit a log line for each sent/received message to/from the specified address(es). Separate different addresses with a comma, prefix unix socket paths with 'unix://'. An empty unix path will match all unix paths |  |  |
| message-tracing-types | Emit a log line for each sent/received message of the type(s) specified. Separate different types with a comma. 'all' to trace all messages. Prefix the value with '~' to trace all types except the given ones, e.g. '~WINDOW,RELEASE' will trace messages of all types except WINDOW and RELEASE. |  |  |
| publish-single-histogram-stats | If true, single histogram values will be published alongside the rate values. | false |  |
| reader-lagging-threshold | Amount of time we wait before we report a read stream that is considered lagging. | 2min |  |
| reader-stalled-grace-period | Amount of time we wait before declaring a reader stalled because we can't read the metadata or data log. When this grace period expires, the client stat "read\_streams\_stalled" is bumped and record to scuba  | 30s |  |
| reader-stuck-threshold | Amount of time we wait before we report a read stream that is considered stuck. | 121s |  |
| request-exec-threshold | Request Execution time beyond which it is considered slow, and 'worker\_slow\_requests' stat is bumped | 10ms |  |
| shadow-client-timeout | Timeout to use for shadow clients. See traffic-shadow-enabled. | 30s | client&nbsp;only |
| slow-background-task-threshold | Background task execution time beyond which it is considered slow, and we log it | 100ms |  |
| stats-collection-interval | How often to collect and submit stats upstream.  Set to <=0 to disable collection of stats. | 60s | requires&nbsp;restart |
| traffic-shadow-enabled | Controls the traffic shadowing feature. Defaults to false to disable shadowing on all clients writing to a cluster. Must be set to true to allow traffic shadowing, which will then be controlled on a per-log basic through parameters in LogsConfig. | false | client&nbsp;only |
| watchdog-abort-on-stall | Should we abort logdeviced if watchdog detected stalled workers. | false |  |
| watchdog-bt-ratelimit | Maximum allowed rate of printing backtraces. | 10/120s | requires&nbsp;restart |
| watchdog-poll-interval | Interval after which watchdog detects stuck workers | 5000ms | requires&nbsp;restart |
| watchdog-print-bt-on-stall | Should we print backtrace of stalled workers. | true |  |

## Network communication
|   Name    |   Description   |  Default  |   Notes   |
|-----------|-----------------|:---------:|-----------|
| checksumming-blacklisted-messages | Used to control what messages shouldn't be checksummed at the protocol layer |  | requires&nbsp;restart, **experimental** |
| checksumming-enabled | A switch to turn on/off checksumming for all LogDevice protocol messages. If false: no checksumming is done, If true: checksumming-blacklisted-messages is consulted. | false | **experimental** |
| command-conn-limit | Maximum number of concurrent admin connections | 32 | server&nbsp;only |
| connect-throttle | timeout after it which two nodes retry to connect when they loose a a connection. Used in ConnectThrottle to ensure we don't retry too  often. Needs restart to load the new values. | 1ms..10s | requires&nbsp;restart |
| connect-timeout | connection timeout when establishing a TCP connection to a node | 100ms |  |
| connect-timeout-retry-multiplier | Multiplier that is applied to the connect timeout after every failed connection attempt | 3 |  |
| connection-backlog | (server-only setting) Maximum number of incoming connections that have been accepted by listener (have an open FD) but have not been processed by workers (made logdevice protocol handshake). | 2000 | server&nbsp;only |
| connection-retries | the number of TCP connection retries before giving up | 4 |  |
| handshake-timeout | LogDevice protocol handshake timeout | 1s |  |
| include-destination-on-handshake | Include the destination node ID in the LogDevice protocol handshake. If the actual node ID of the connection target does not match the intended destination ID, the connection is terminated. | true |  |
| incoming-messages-max-bytes-limit | maximum byte limit of unprocessed messages within the system. | 524288000 | requires&nbsp;restart |
| inline-message-execution | Indicates whether message should be processed right after deserialization. Usually within new worker model all messages are processed after posting them into the work context. This option works only when worker context is run with previous eventloop architecture. | false | requires&nbsp;restart |
| max-protocol | maximum version of LogDevice protocol that the server/client will accept | 98 |  |
| max-time-to-allow-socket-drain | After hitting NOBUFS, amount of time a socket is allowed to successfully send a single message before it is closed. | 3min |  |
| nagle | enable Nagle's algorithm on TCP sockets. Changing this setting on-the-fly will not apply it to existing sockets, only to newly created ones | false |  |
| outbuf-kb | max output buffer size (userspace extension of socket sendbuf) in KB. Changing this setting on-the-fly will not apply it to existing sockets, only to newly created ones | 32768 |  |
| outbytes-mb | per-thread limit on bytes pending in output evbuffers (in MB) | 512 |  |
| rcvbuf-kb | TCP socket rcvbuf size in KB. Changing this setting on-the-fly will not apply it to existing sockets, only to newly created ones | -1 |  |
| read-messages | read up to this many incoming messages before returning to libevent | 128 |  |
| sendbuf-kb | TCP socket sendbuf size in KB. Changing this setting on-the-fly will not apply it to existing sockets, only to newly created ones | -1 |  |
| tcp-keep-alive-intvl | TCP keepalive interval. The interval between successive probes.If negative the OS default will be used. | -1 |  |
| tcp-keep-alive-probes | TCP keepalive probes. How many unacknowledged probes before the connection is considered broken. If negative the OS default will be used. | -1 |  |
| tcp-keep-alive-time | TCP keepalive time. This is the time, in seconds, before the first probe will be sent. If negative the OS default will be used. | -1 |  |
| tcp-user-timeout | The time in miliseconds that transmitted data may remain unacknowledgedbefore TCP will close the connection. 0 for system default. -1 to disable. default is 5min = 300000 | 300000 |  |
| use-tcp-keep-alive | Enable TCP keepalive for all connections | true |  |

## Performance
|   Name    |   Description   |  Default  |   Notes   |
|-----------|-----------------|:---------:|-----------|
| allow-reads-on-workers | If false, all rocksdb reads are done from storage threads. If true, a cache-only reading attempt is made from worker thread first, and a storage thread task is scheduled only if the cache wasn't enough to fulfill the read. Disabling this can be used for: working around rocksdb bugs; working around latency spikes caused by cache-only reads being slow sometimes | true | **experimental**, server&nbsp;only |
| disable-check-seals | if true, 'get sequencer state' requests will not be sending 'check seal' requests that they normally do in order to confirm that this sequencer is the most recent one for the log. This saves network and CPU, but may cause getSequencerState() calls to return stale results. Intended for use in production emergencies only. | false | server&nbsp;only |
| findtime-force-approximate | (server-only setting) Override the client-supplied FindKeyAccuracy with FindKeyAccuracy::APPROXIMATE. This makes the resource requirements of FindKey requests small and predictable, at the expense of accuracy | false | server&nbsp;only |
| write-find-time-index | Set this to true if you want findTime index to be written. A findTime index speeds up findTime() requests by maintaining an index from timestamps to LSNs in LogsDB data partitions. | false | server&nbsp;only |

## Read path
|   Name    |   Description   |  Default  |   Notes   |
|-----------|-----------------|:---------:|-----------|
| all-read-streams-rate | Rate of sampling all client read streams debug info to Scuba | 100ms | client&nbsp;only |
| authoritative-status-overrides | Force the given authoritative statuses for the given shards. Comma-separated list of overrides, each override of form 'N<node>S<shard>:<status>' or 'N<node>S<shard1>-<shard2>:<status>'. E.g. 'N7:S0-15:UNDERREPLICATION,N8:S2:UNDERREPLICATION' will set status of shards 0-15 of node 7 and shard 2 of node 8 to UNDERREPLICATION. This is useful for recovering from situations where internal logs or metadata logs are unreadable because too many nodes are unavailable or lost their data. In such situation, use this setting to temporarily override the state of shards that are unavailable (not running logdeviced) to UNDERREPLICATION, then, optionally, write SHARD\_UNRECOVERABLE events for the same shards to event log. |  | server&nbsp;only |
| client-epoch-metadata-cache-size | maximum number of entries in the client-side epoch metadata cache. Set it to 0 to disable the epoch metadata cache. | 50000 | requires&nbsp;restart, client&nbsp;only |
| client-initial-redelivery-delay | Initial delay to use when reader application rejects a record or gap | 1s |  |
| client-is-log-empty-grace-period | After receiving responses to an isLogEmpty() request from an f-majority of nodes, wait up to this long for more nodes to chime in if there is not yet consensus. | 5s | **experimental**, client&nbsp;only |
| client-max-redelivery-delay | Maximum delay to use when reader application rejects a record or gap | 30s |  |
| client-read-buffer-size | number of records to buffer per read stream in the client object while reading. If this setting is changed on-the-fly, the change will only apply to new reader instances | 512 |  |
| client-read-flow-control-threshold | threshold (relative to buffer size) at which the client broadcasts window update messages (less means more often) | 0.7 |  |
| data-log-gap-grace-period | When non-zero, replaces gap-grace-period for data logs. | 0ms |  |
| enable-all-read-streams-sampling | Enables sampling of debug info from client's all read streams. | false | client&nbsp;only |
| enable-read-throttling | Throttle Disk I/O due to log read streams | false | server&nbsp;only |
| gap-grace-period | gap detection grace period for all logs, including data logs, metadata logs, and internal state machine logs. Millisecond granularity. Can be 0. | 100ms |  |
| grace-counter-limit | Maximum number of consecutive grace periods a storage node may fail to send a record or gap (if in all read all mode) before it is considered disgraced and client read streams no longer wait for it. If all nodes are disgraced or in GAP state, a gap record is issued. May be 0. Set to -1 to disable grace counters and use simpler logic: no disgraced nodes, issue gap record as soon as grace period expires. | 2 |  |
| log-state-recovery-interval | interval between consecutive attempts by a storage node to obtain the attributes of a log residing on that storage node Such 'log state recovery' is performed independently for each log upon the first request to start delivery of records of that log. The attributes to be recovered include the LSN of the last cumulatively released record in the log, which may have to be requested from the log's sequencer over the network. | 500ms | requires&nbsp;restart, server&nbsp;only |
| max-record-bytes-read-at-once | amount of RECORD data to read from local log store at once | 1048576 | server&nbsp;only |
| metadata-log-gap-grace-period | When non-zero, replaces gap-grace-period for metadata logs. | 0ms |  |
| output-max-records-kb | amount of RECORD data to push to the client at once | 1024 |  |
| reader-reconnect-delay | When a reader client loses a connection to a storage node, delay after which it tries reconnecting. | 10ms..30s | client&nbsp;only |
| reader-retry-window-delay | When a reader client fails to send a WINDOW message, delay after which it retries sending it. | 10ms..30s | client&nbsp;only |
| reader-started-timeout | How long a reader client waits for a STARTED reply from a storage node before sending a new START message. | 30s..5min | client&nbsp;only |
| real-time-eviction-threshold-bytes | When the real time buffer reaches this size, we evict entries. | 80000000 | requires&nbsp;restart, **experimental**, server&nbsp;only |
| real-time-max-bytes | Max size (in bytes) of released records that we'll keep around to use for real time reads.  Includes some cache overhead, so for small records, you'll store less record data than this. | 100000000 | requires&nbsp;restart, **experimental**, server&nbsp;only |
| real-time-reads-enabled | Turns on the experimental real time reads feature. | false | **experimental**, server&nbsp;only |
| scd-copyset-reordering-max | SCDCopysetReordering values that clients may ask servers to use.  Currently available options: none, hash-shuffle (default), hash-shuffle-client-seed. hash-shuffle results in only one storage node reading a record block from disk, and then serving it to multiple readers from the cache. hash-shuffle-client-seed enables multiple storage nodes to participate in reading the log, which can be benefit non-disk-bound workloads. | hash-shuffle |  |
| unreleased-record-detector-interval | Time interval at which to check for unreleased records in storage nodes. Any log which has unreleased records, and for which no records have been released for two consecutive unreleased-record-detector-intervals, is suspected of having a dead sequencer. Set to 0 to disable check. | 30s | server&nbsp;only |

## Reader failover
|   Name    |   Description   |  Default  |   Notes   |
|-----------|-----------------|:---------:|-----------|
| reader-slow-shards-detection | If true, readers in SCD mode will detect shards that are very slow andmay ask the other storage shards to filter them out | disabled | client&nbsp;only |
| reader-slow-shards-detection-moving-avg-duration | When slow shards detection is enabled, duration to use for the moving average | 30min | client&nbsp;only |
| reader-slow-shards-detection-outlier-duration | When slow shards detection is enabled, amount of time that we'll consider a shard an outlier if it is slow. | 1min..30min | client&nbsp;only |
| reader-slow-shards-detection-outlier-duration-decrease-rate | When slow shards detection is enabled, rate at which we decrease the time after which we'll try to reinstate an outlier in the read set. If the value is 0.25, for each second of healthy reading we will decrease that time by 0.25s. | 0.25 | client&nbsp;only |
| reader-slow-shards-detection-required-margin | When slow shards detection is enabled, sensitivity of the outlier detection algorithm. For instance, if set to 3.0, only consider an outlier a shard that is 300% slower than the others. The required margin is adaptive and may increase or decrease but will be capped at a minimum defined by this setting. | 10.0 | client&nbsp;only |
| reader-slow-shards-detection-required-margin-decrease-rate | Rate at which we decrease the required margin when we are healthy. If the value is 0.25 for instance, we will reduce the required margin by 0.25 for every second spent reading. | 0.25 | client&nbsp;only |
| scd-all-send-all-timeout | Timeout after which ClientReadStream fails over to asking all storage nodes to send everything they have if it is not able to make progress for some time | 600s |  |
| scd-timeout | Timeout after which ClientReadStream considers a storage node down if it does not send any data for some time but the socket to it remains open. | 300s |  |

## Rebuilding
|   Name    |   Description   |  Default  |   Notes   |
|-----------|-----------------|:---------:|-----------|
| allow-conditional-rebuilding-restarts | Used to gate the feature described in T22614431. We want to enable it only after all clients have picked up the corresponding change in RSM protocol. | false | **experimental**, server&nbsp;only |
| auto-mark-unrecoverable-timeout | If too many storage nodes or shards are declared down by the failure detector or RocksDB, readers stall. If this setting is 'max' (default), readers remain stalled until some shards come back, or until the shards are marked \_unrecoverable\_ (permanently lost) by writing a special in the event log via an admin tool. If this setting contains a time value, upon the timer expiration the shards are marked unrecoverable automatically. This allows reader clients to skip over all records that could only be delivered by now unrecoverable shards, and continue reading more recent records. | max | server&nbsp;only |
| disable-data-log-rebuilding | If set then data logs are not rebuilt. This may be enabled for clusters with very low retention, where the probability of data-loss due to a 2nd or 3rd failure is low and the work done during rebuild interferes with the primary workload. | false | requires&nbsp;restart, server&nbsp;only |
| event-log-grace-period | grace period before considering event log caught up | 10s | server&nbsp;only |
| event-log-max-delta-bytes | How many bytes of deltas to keep in the event log before we snapshot it. | 10485760 | server&nbsp;only |
| event-log-max-delta-records | How many delta records to keep in the event log before we snapshot it. | 100 | server&nbsp;only |
| event-log-retention | How long to keep a history of snapshots and deltas for the event log. Unused if the event log has never been snapshotted or if event log trimming is disabled with disable-event-log-trimming. | 14d | server&nbsp;only |
| event-log-snapshot-compression | Use ZSTD compression to compress event log snapshots | true |  |
| event-log-snapshotting | Allow the event log to be snapshotted onto a snapshot log. This requires the event log group to contain two logs, the first one being the snapshot log and the second one being the delta log. | true | requires&nbsp;restart |
| eventlog-snapshotting-period | Controls time based snapshotting. New eventlog snapshot will be created after this period if there are new deltas | 1h | server&nbsp;only |
| max-log-rebuilding-size-mb | Maximum amount of memory that can be consumed by a single LogRebuilding state machine. V1 only. | 5 | server&nbsp;only |
| max-node-rebuilding-percentage | Do not initiate rebuilding if more than this percentage of storage nodes in the cluster appear to have been lost or have shards that appear to require rebuilding. | 35 | server&nbsp;only |
| planner-scheduling-delay | Delay between a shard rebuilding plan request and its execution to allow many shards to be grouped and planned together. | 1min | server&nbsp;only |
| rebuild-dirty-shards | On start-up automatically rebuild LogsDB partitions left dirty by a prior unsafe shutdown of this node. This is called mini-rebuilding. The setting should be on unless you are running with --append-store-durability=sync\_write, or don't care about data loss. | true | server&nbsp;only |
| rebuild-store-durability | The minimum guaranteed durablity of rebuilding writes before a storage node will confirm the STORE as successful. Can be one of "memory", "async\_write", or "sync\_write". See --append-store-durability for a description of these options. | async\_write | server&nbsp;only |
| rebuilding-checkpoint-interval-mb | Write a per-log rebuilding checkpoint once per this many megabytes of rebuilt data in the log. A rebuilding checkpoints contains an LSN through which the log has been rebuilt by this donor and the rebuilding version number identifying this rebuilding run. If a node restarts in the middle of a rebuilding run, it resumes rebuilding of a log from that log's last checkpoint. V1 only. | 100 | server&nbsp;only |
| rebuilding-dont-wait-for-flush-callbacks | Regardless of the value of 'rebuild-store-durability', assume any successfully completed store is durable without waiting for flush notifications. NOTE: Use of this setting will lead to silent under-replication when 'rebuild-store-durability' is set to 'MEMORY'. Use for testing and I/O characterization only. | false | requires&nbsp;restart, server&nbsp;only |
| rebuilding-global-window | the size of rebuilding global window expressed in units of time. The global rebuilding window is an experimental feature similar to the local window, but tracking rebuilding reads across all storage nodes in the cluster rather than per node. Whereas the local window improves the locality of reads, the global window is expected to improve the locality of rebuilding writes. | max | **experimental**, server&nbsp;only |
| rebuilding-local-window | Rebuilding will try to keep the difference between max and min in-flight records' timestamps less than this value. | 60min | server&nbsp;only |
| rebuilding-local-window-uses-partition-boundary | If true, the local window will be moved on partition boundaries. If false, it will instead be moved on fixed time intervals, as set by --rebuilding-local-window. V1 only, V2 always reads partition by partition. | true | server&nbsp;only |
| rebuilding-max-amends-in-flight | maximum number of requests to update (amend) a rebuilt record's copyset that a rebuilding donor node can have in flight at the same time, per log. Rebuilding v1 only. | 100 | server&nbsp;only |
| rebuilding-max-batch-bytes | max amount of data that a node can read in one batch for rebuilding | 10M | server&nbsp;only |
| rebuilding-max-get-seq-state-in-flight | maximum number of 'get sequencer state' requests that a rebuilding donor node can have in flight at the same time. Every storage node participating in rebuilding gets the sequencer state for all logs residing on that node before beginning to re-replicate records. This is done in order to determine the LSN at which to stop rebuilding the log. | 100 | server&nbsp;only |
| rebuilding-max-logs-in-flight | Maximum number of logs that a donor node can be rebuilding at the same time. V1 only. | 1 | server&nbsp;only |
| rebuilding-max-record-bytes-in-flight | Maximum total size of rebuilding STORE requests that a rebuilding donor node can have in flight at the same time, per shard. Only used by rebuilding v2. | 100M | server&nbsp;only |
| rebuilding-max-records-in-flight | Maximum number of rebuilding STORE requests that a rebuilding donor node can have in flight at the same time. Rebuilding v1: per log, rebuilding v2: per shard. | 200 | server&nbsp;only |
| rebuilding-planner-sync-seq-retry-interval | retry interval for individual 'get sequencer state' requests issued by rebuilding via SyncSequencerRequest API, with exponential backoff | 60s..5min | server&nbsp;only |
| rebuilding-rate-limit | Rebuilding V2 only. Limit on how fast rebuilding reads, in bytes per unit of time, per shard. Example: 5M/1s will make rebuilding read at most one megabyte per second in each shard. Note that it counts pre-filtering bytes; if rebuilding has high read amplification (e.g. if copyset index is disabled or is not very effective because records are small), much fewer bytes per second will actually get re-replicated. Also note that this setting doesn't affect batch size; e.g. if --rebuilding-max-batch-bytes=10M and --rebuilding-rate-limit=1M/1s, rebuilding will probably read a 10 MB batch every 10 seconds. | unlimited | server&nbsp;only |
| rebuilding-restarts-grace-period | Grace period used to throttle how often rebuilding can be restarted. This protects the server against a spike of messages in the event log that would cause a restart. | 20s | server&nbsp;only |
| rebuilding-store-timeout | Maximum timeout for attempts by rebuilding to store a record copy or amend a copyset on a specific storage node. This timeout only applies to stores and amends that appear to be in flight; a smaller timeout (--rebuilding-retry-timeout) is used if something is known to be wrong with the store, e.g. we failed to send the message, or we've got an unsuccessful reply, or connection closed after we sent the store. | 240s..480s | server&nbsp;only |
| rebuilding-use-iterator-cache | Place rebuilding iterators in the LogsDB iterator cache. V1 only. | false | server&nbsp;only |
| rebuilding-use-rocksdb-cache | Allow rebuilding reads to use RocksDB block cache. Recommended: enable for rebuilding v1, disable for rebuilding v2. | false | server&nbsp;only |
| rebuilding-v2 | Enables a new implementation of rebuilding. The old one is deprecated. | true | server&nbsp;only |
| rebuilding-wait-purges-backoff-time | Retry timeout for waiting for local shards to purge a log before rebuilding it. | 1s..10s | **experimental**, server&nbsp;only |
| record-durability-timeout | Time for which LogRebuilding/RebuidlingCoordinator will wait for pending records to be durable before restarting the rebuilding for the log. | 960s | **experimental**, server&nbsp;only |
| reject-stores-based-on-copyset | If true, logdevice will prevent writes to nodes that are being drained (rebuilt in RELOCATE mode). Not recommended to set to false unless you're having a production issue. | true | server&nbsp;only |
| self-initiated-rebuilding-grace-period | grace period in seconds before triggering full node rebuilding after detecting the node has failed. | 1200s | server&nbsp;only |
| shard-is-rebuilt-msg-delay | In large clusters SHARD\_IS\_REBULT messages can arrive in a thundering herd overwhelming thread 0 processing those messages. The messages will be delayed by a random time in seconds between the min and the max value specified in the range above. 0 means no delay. NOTE: changing this value only applies to upcoming rebuildings, if you want to apply it to ongoing rebuildings, you'll need to restart the node. | 5s..300s | server&nbsp;only |
| total-log-rebuilding-size-per-shard-mb | Maximum amount of memory that can be consumed by all LogRebuilding state machines, per shard. V1 only. | 100 | server&nbsp;only |

## Recovery
|   Name    |   Description   |  Default  |   Notes   |
|-----------|-----------------|:---------:|-----------|
| concurrent-log-recoveries | limit on the number of logs that can be in recovery at the same time | 400 | server&nbsp;only |
| enable-record-cache | Enable caching of unclean records on storage nodes. Used to minimize local log store access during log recovery. | true | requires&nbsp;restart, server&nbsp;only |
| get-erm-for-empty-epoch | If true, Purging will get the EpochRecoveryMetadata even if the epoch is empty locally | true | **experimental**, server&nbsp;only |
| max-active-cached-digests | maximum number of active cached digest streams on a storage node at the same time | 2000 | requires&nbsp;restart, server&nbsp;only |
| max-cached-digest-record-queued-kb | amount of RECORD data to push to the client at once for cached digesting | 256 | requires&nbsp;restart, server&nbsp;only |
| max-concurrent-purging-for-release-per-shard | max number of concurrently running purging state machines for RELEASE messages per each storage shard for each worker | 4 | requires&nbsp;restart, server&nbsp;only |
| mutation-timeout | initial timeout used during the mutation phase of log recovery to store enough copies of a record or a hole plug | 500ms | server&nbsp;only |
| purging-use-metadata-log-only | If true, the NodeSetFinder within PurgeUncleanEpochs will useonly the metadata log as source for fetching historical metadata.used only for migration | false | server&nbsp;only |
| record-cache-max-size | Maximum size enforced for the record cache, 0 for unlimited. If positive and record cache size grows more than that, it will start evicting records from the cache. This is also the maximum total number of bytes allowed to be persisted in record cache snapshots. For snapshot limit, this is enforced per-shard with each shard having its own limit of (max\_record\_cache\_snapshot\_bytes / num\_shards). | 4294967296 | server&nbsp;only |
| record-cache-monitor-interval | polling interval for the record cache eviction thread for monitoring the size of the record cache. | 2s | server&nbsp;only |
| recovery-grace-period | Grace period time used by epoch recovery after it acquires an authoritative incomplete digest but wants to wait more time for an authoritative complete digest. Millisecond granularity. Can be 0.  | 100ms | server&nbsp;only |
| recovery-seq-metadata-timeout | Retry backoff timeout used for checking if the latest metadata log record is fully replicated during log recovery. | 2s..60s | server&nbsp;only |
| recovery-timeout | epoch recovery timeout. Millisecond granularity. | 120s | server&nbsp;only |
| single-empty-erm | A single E:EMPTY response for an epoch is sufficient for GetEpochRecoveryMetadataRequest to consider the epoch as empty if this option is set. | true | **experimental**, server&nbsp;only |

## Resource management
|   Name    |   Description   |  Default  |   Notes   |
|-----------|-----------------|:---------:|-----------|
| append-stores-max-mem-bytes | Maximum total size of in-flight StoreStorageTasks from appenders and recoveries. Evenly divided among shards. | 2G | server&nbsp;only |
| eagerly-allocate-fdtable | enables an optimization to eagerly allocate the kernel fdtable at startup | false | requires&nbsp;restart, server&nbsp;only |
| fd-limit | maximum number of file descriptors that the process can allocate (may require root priviliges). If equal to zero, do not set any limit. | 0 | requires&nbsp;restart, server&nbsp;only |
| flow-groups-run-deadline | Maximum delay (plus one cycle of the event loop) between a request to run FlowGroups and Sender::runFlowGroups() executing. | 5ms | server&nbsp;only |
| flow-groups-run-yield-interval | Maximum duration of Sender::runFlowGroups() before yielding to the event loop. | 2ms | server&nbsp;only |
| lock-memory | On startup, call mlockall() to lock the text segment (executable code) of logdeviced in RAM. | false | requires&nbsp;restart, server&nbsp;only |
| max-inflight-storage-tasks | max number of StorageTask instances that one worker thread may have in flight to each database shard | 4096 | requires&nbsp;restart, server&nbsp;only |
| max-payload-size | The maximum payload size that will be accepted by the client library or the server. Can't be larger than 33554432 bytes. | 1048576 |  |
| max-record-read-execution-time | Maximum execution time for reading records. 'max' means no limit. | 1s | **experimental**, server&nbsp;only |
| max-server-read-streams | max number of read streams clients can establish to the server, per worker | 150000 | server&nbsp;only |
| max-total-appenders-size-hard | Total size in bytes of running Appenders accross all workers after which we start rejecting new appends. | 629145600 | server&nbsp;only |
| max-total-appenders-size-soft | Total size in bytes of running Appenders accross all workers after which we start taking measures to reduce the Appender residency time. | 524288000 | server&nbsp;only |
| max-total-buffered-append-size | Total size in bytes of payloads buffered in BufferedWriters in sequencers for server-side batching and compression. Appends will be rejected when this threshold is significantly exceeded. | 1073741824 | server&nbsp;only |
| num-reserved-fds | expected number of file descriptors to reserve for use by RocksDB files and server-to-server connections within the cluster. This number is subtracted from --fd-limit (if set) to obtain the maximum number of client TCP connections that the server will be willing to accept.  | 0 | requires&nbsp;restart, server&nbsp;only |
| per-worker-storage-task-queue-size | max number of StorageTask instances to buffer in each Worker for each local log store shard | 1 | requires&nbsp;restart, server&nbsp;only |
| queue-drop-overload-time | max time after worker's storage task queue is dropped before it stops being considered overloaded | 1s | server&nbsp;only |
| queue-size-overload-percentage | percentage of per-worker-storage-task-queue-size that can be buffered before the queue is considered overloaded | 50 | server&nbsp;only |
| read-storage-tasks-max-mem-bytes | Maximum amount of memory that can be allocated by read storage tasks. | 16106127360 | server&nbsp;only |
| rebuilding-stores-max-mem-bytes | Maxumun total size of in-flight StoreStorageTasks from rebuilding. Evenly divided among shards. | 2G | server&nbsp;only |
| rocksdb-low-ioprio | IO priority to request for low-pri rocksdb threads. This works only if current IO scheduler supports IO priorities.See man ioprio\_set for possible values. "any" or "" to keep the default.  |  | requires&nbsp;restart, server&nbsp;only |
| slow-ioprio | IO priority to request for 'slow' storage threads. Storage threads in the 'slow' thread pool handle high-latency RocksDB IO requests,  primarily data reads. Not all kernel IO schedulers supports IO priorities.See man ioprio\_set for possible values."any" or "" to keep the default. |  | requires&nbsp;restart, server&nbsp;only |

## RocksDB
|   Name    |   Description   |  Default  |   Notes   |
|-----------|-----------------|:---------:|-----------|
| iterator-cache-ttl | expiration time of idle RocksDB iterators in the iterator cache. | 20s | server&nbsp;only |
| rocksdb-advise-random-on-open | if true, will hint the underlying file system that the file access pattern is random when an SST file is opened | false | requires&nbsp;restart, server&nbsp;only |
| rocksdb-allow-fallocate | If false, fallocate() calls are bypassed in rocksdb | true | requires&nbsp;restart, server&nbsp;only |
| rocksdb-arena-block-size | granularity of memtable allocations | 4194304 | requires&nbsp;restart, server&nbsp;only |
| rocksdb-block-size | approximate size of the uncompressed data block; rocksdb memory usage for index is around [total data size] / block\_size * 50 bytes; on HDD consider using a much bigger value to keep memory usage reasonable | 500K | requires&nbsp;restart, server&nbsp;only |
| rocksdb-bloom-bits-per-key | Controls the size of bloom filters in sst files. Set to 0 to disable bloom filters. "Key" in the bloom filter is log ID and entry type (data record, CSI entry or findTime index entry). Iterators then use this information to skip files that don't contain any records of the requested log. The default value of 10 corresponds to false positive rate of ~1%. Note that LogsDB already skips partitions that don't have the requested logs, so bloom filters only help for somewhat bursty write patterns - when only a subset of files in a partition contain a given log. However, even if appends to a log are steady, sticky copysets may make the streams of STOREs to individual nodes bursty.Another scenario where bloomfilters can be effective is during rebuilding. Rebuilding works a few logs at a time and if the (older partition) memtables are frequently flushed due to memory pressure then then they are likely to contain only a small number of logs in them. | 10 | server&nbsp;only |
| rocksdb-bloom-block-based | If true, rocksdb will use a separate bloom filter for each block of sst file. These small bloom filters will be at least 9 bytes each (even if bloom-bits-per-key is smaller). For data records, usually each block contains only one log, so the bloom filter size will be around max(72, bloom\_bits\_per\_key) + 2 * bloom\_bits\_per\_key  per log per sst (the "2" corresponds to CSI and findTime index entries; if one or both is disabled, it's correspondingly smaller). | false | server&nbsp;only |
| rocksdb-bytes-per-sync | when writing files (except WAL), sync once per this many bytes written. 0 turns off incremental syncing, the whole file will be synced after it's written | 1048576 | requires&nbsp;restart, server&nbsp;only |
| rocksdb-bytes-written-since-throttle-eval-trigger | The maximum amount of buffered writes allowed before a forced throttling evaluation is triggered. This helps to avoid condition where too many writes come in for a shard, while flush thread is sleeping and we go over memory budget. | 20M | server&nbsp;only |
| rocksdb-cache-high-pri-pool-ratio | Ratio of rocksdb block cache reserve for index and filter blocks if --rocksdb-cache-index-with-high-priority is enabled, and for small blocks if --rocksdb-cache-small-blocks-with-high-priority is positive. | 0.0 | requires&nbsp;restart, server&nbsp;only |
| rocksdb-cache-index | put index and filter blocks in the block cache, allowing them to be evicted | false | requires&nbsp;restart, server&nbsp;only |
| rocksdb-cache-index-with-high-priority | Cache index and filter block in high pri pool of block cache, making them less likely to be evicted than data blocks. | false | requires&nbsp;restart, server&nbsp;only |
| rocksdb-cache-numshardbits | This setting is not important. Width in bits of the number of shards into which to partition the uncompressed block cache. 0 to disable sharding. -1 to pick automatically. See rocksdb/cache.h. | 4 | requires&nbsp;restart, server&nbsp;only |
| rocksdb-cache-size | size of uncompressed RocksDB block cache | 10G | requires&nbsp;restart, server&nbsp;only |
| rocksdb-cache-small-block-threshold-for-high-priority | SST blocks smaller than this size will get high priority (see --rocksdb-cache-high-pri-pool-ratio). | 30K | server&nbsp;only |
| rocksdb-compaction-access-sequential | suggest to the OS that input files will be accessed sequentially during compaction | true | requires&nbsp;restart, server&nbsp;only |
| rocksdb-compaction-max-bytes-at-once | This is the unit for IO scheduling for compaction. It's used only if the DRR scheduler is being used. Each share received from the scheduler allows compaction filtering to proceed with these many bytes. If the scheduler is configured for request based scheduling (current default) each principal is allowed X number of requests based on its share and irrespective of the number of bytes for processed for each request. In this case it'll be the above bytes. | 1048576 | server&nbsp;only |
| rocksdb-compaction-ratelimit | limits how fast compactions can read uncompressed data, in bytes; format is <count><suffix>/<duration><unit>. Example: 5M/500ms means compaction will read 5MB per 500ms. This is applied to each compaction independently (e.g. if multiple shards are compacting simultaneously the total rate can be over the limit). Unlimited by default. IMPORTANT: This limits the rate of uncompressed data. If rocksdb compressed data 2X, the actual disk read rate will be around 1/2 of this limit. | 30M/1s | server&nbsp;only |
| rocksdb-compaction-readahead-size | if non-zero, perform reads of this size (in bytes) when doing compaction; big readahead can decrease efficiency of compactions that remove a lot of records (compaction skips trimmed records using seeks) | 4096 | requires&nbsp;restart, server&nbsp;only |
| rocksdb-compaction-style | compaction style: 'universal' (default) or 'level'; if using 'level', also set --num-levels to at least 2 | universal | requires&nbsp;restart, server&nbsp;only |
| rocksdb-compressed-cache-numshardbits | This setting is not important. Same as --rocksdb-cache-numshardbits but for the compressed cache (if enabled) | 4 | requires&nbsp;restart, server&nbsp;only |
| rocksdb-compressed-cache-size | size of compressed RocksDB block cache (0 to turn off) | 0 | requires&nbsp;restart, server&nbsp;only |
| rocksdb-compression-type | compression algorithm: 'snappy' (default), 'none', 'zlib', 'bzip2', 'lz4', 'lz4hc', 'zstd' | none | requires&nbsp;restart, server&nbsp;only |
| rocksdb-db-write-buffer-size | Soft limit on the total size of memtables per shard; when exceeded, oldest memtables will automatically be flushed. This may soon be superseded by a more global --rocksdb-memtable-size-per-node limit that should be set to <num\_shards> * what you'd set this to. If you set this logdevice will no longer manage any flushes and all responsibility of flushing memtable is taken by rocksdb. | 0 | requires&nbsp;restart, server&nbsp;only |
| rocksdb-disable-iterate-upper-bound | disable iterate\_upper\_bound optimization in RocksDB | false | server&nbsp;only |
| rocksdb-enable-insert-hint | Enable rocksdb insert hint optimization. May reduce CPU usage for inserting keys into rocksdb, with small memory overhead. | true | requires&nbsp;restart, server&nbsp;only |
| rocksdb-enable-statistics | if set, instruct RocksDB to collect various statistics | true | requires&nbsp;restart, server&nbsp;only |
| rocksdb-flush-block-policy | Controls how RocksDB splits SST file data into blocks. 'default' starts a new block when --rocksdb-block-size is reached. 'each\_log', in addition to what 'default' does, starts a new block when log ID changes. 'each\_copyset', in addition to what 'each\_log' does, starts a new block when copyset changes. Both 'each\_' don't start a new block if current block is smaller than --rocksdb-min-block-size. 'each\_log' should be safe to use in all cases. 'each\_copyset' should only be used when sticky copysets are enabled with --enable-sticky-copysets (otherwise it would start a block for almost every record). | each\_log | requires&nbsp;restart, server&nbsp;only |
| rocksdb-index-block-restart-interval | Number of keys between restart points for prefix encoding of keys in index blocks.  Typically one of two values: 1 for no prefix encoding, 16 for prefix encoding (smaller memory footprint of the index). | 16 | requires&nbsp;restart, server&nbsp;only |
| rocksdb-index-shortening | Controls the precision of block boundaries in RocksDB sst file index. More shortening -> smaller indexes (i.e. less memory usage) but potentially worse iterator seek performance. Possible values are: 'none', 'shorten-separators', 'shorten-all'. Unless you're really low on memory, you should probably just use 'none' and not worry about it.There should be no reason to use 'shorten-all' - it saves a negligible amount of memory but makes iterator performance noticeably worse, especially with direct IO or insufficient block cache size. Deciding between 'none' and 'shorten-separators' is not straightforward, probably better to just do it experimentally, by looking memory usage and disk read rate. Also keep in mind that sst index size is approximately inversely proportional to --rocksdb-block-size or --rocksdb-min-block-size. | none | requires&nbsp;restart, server&nbsp;only |
| rocksdb-ld-managed-flushes | If set to false (deprecated), decision about when and what memtables to flush is taken by rocksdb using it's internal policy. If set to true, all decisions about flushing are taken by logdevice. It uses rocksdb-memtable-size-per-node and rocksdb-write-buffer-size settings to decide if it's necessary to flush memtables. Requires enable rocksdb-memtable-size-per-node to be under 32GB, and db-write-buffer-size set to zero; otherwise, we silently fall back to rocksdb-managed flushes. | true | server&nbsp;only |
| rocksdb-level0-file-num-compaction-trigger | trigger L0 compaction at this many L0 files. This applies to the unpartitioned and metadata column families only, not to LogsDB data partitions. | 10 | requires&nbsp;restart, server&nbsp;only |
| rocksdb-level0-slowdown-writes-trigger | start throttling writers at this many L0 files. This applies to the unpartitioned and metadata column families only, not to LogsDB data partitions. | 25 | requires&nbsp;restart, server&nbsp;only |
| rocksdb-level0-stop-writes-trigger | stop accepting writes (block writers) at this many L0 files. This applies to the unpartitioned and metadata column families only, not to LogsDB data partitions. | 30 | requires&nbsp;restart, server&nbsp;only |
| rocksdb-low-pri-write-stall-threshold-percent | Node stalls rebuilding stores when sum of unflushed memory size and active memory size is above per shard memory limit, and active memory size goes beyond low\_pri\_write\_stall\_threshold\_percent of per shard memory limit. | 5 | server&nbsp;only |
| rocksdb-max-background-compactions | Maximum number of concurrent rocksdb-initiated background compactions per shard. Note that this value is not important since most compactions are not "background" as far as rocksdb is concerned. They're done from \_logsdb\_ thread and are limited to one per shard at a time, regardless of this option. | 2 | requires&nbsp;restart, server&nbsp;only |
| rocksdb-max-background-flushes | maximum number of concurrent background memtable flushes per shard. Flushes run on the rocksdb hipri thread pool | 1 | requires&nbsp;restart, server&nbsp;only |
| rocksdb-max-bytes-for-level-base | maximum combined data size for L1 | 10G | requires&nbsp;restart, server&nbsp;only |
| rocksdb-max-bytes-for-level-multiplier | L\_n -> L\_n+1 data size multiplier | 8 | requires&nbsp;restart, server&nbsp;only |
| rocksdb-max-open-files | maximum number of concurrently open RocksDB files; -1 for unlimited | 10000 | requires&nbsp;restart, server&nbsp;only |
| rocksdb-max-write-buffer-number | maximum number of concurrent write buffers getting flushed. Rocksdb stalls writes to the column family, on reaching this many flushed memtables. If ld\_managed\_flushes is true, this setting is ignored, and rocksdb is instructed to not stall writes, write throttling is done by LD based on shard memory consumption rather than number of memtables pending flush. | 2 | requires&nbsp;restart, server&nbsp;only |
| rocksdb-memtable-size-low-watermark-percent | low\_watermark\_percent is some percent of memtable\_size\_per\_node and indicates the target consumption to reach if total consumption goes above memtable\_size\_per\_node. Like memtable\_size\_per\_node, low\_watermark is sharded and individually applied to every shard. The difference between memtable\_size\_per\_node and low\_watermark should roughly match the size of metadata memtable while flusher thread was sleeping. Flushing extra has a big plus, metadata memtable flushes usually are few hundred KB or low MB value, and if difference between low\_watermark and memtable\_size\_per\_node is in order of tens of MB that makes dependent metadata memtable flushes almost free. | 60 | server&nbsp;only |
| rocksdb-memtable-size-per-node | soft limit on the total size of memtables per node; when exceeded, oldest memtable in the shard whose growth took the total memory usage over the threshold will automatically be flushed. This is a soft limit in the sense that flushing may fall behind or freeing memory be delayed for other reasons, causing us to exceed the limit. --rocksdb-db-write-buffer-size overrides this if it is set, but it will be deprecated eventually. | 10G | **experimental**, server&nbsp;only |
| rocksdb-metadata-block-size | approximate size of the uncompressed data block for metadata column family (if --rocksdb-partitioned); if zero, same as --rocksdb-block-size | 0 | requires&nbsp;restart, server&nbsp;only |
| rocksdb-metadata-bloom-bits-per-key | Similar to --rocksdb-bloom-bits-per-key but for metadata column family. You probably don't want to enable this. This option is here just for completeness. It's not expected to have any positive effect since almost all reads from metadata column family bypass bloom filters (with total\_order\_seek = true). | 0 | server&nbsp;only |
| rocksdb-metadata-cache-numshardbits | This setting is not important. Same as --rocksdb-cache-numshardbits but for the metadata cache | 4 | requires&nbsp;restart, server&nbsp;only |
| rocksdb-metadata-cache-size | size of uncompressed RocksDB block cache for metadata | 1G | requires&nbsp;restart, server&nbsp;only |
| rocksdb-min-block-size | minimum size of the uncompressed data block; only used when --rocksdb-flush-block-policy is not default; on SSD consider reducing this value | 16384 | requires&nbsp;restart, server&nbsp;only |
| rocksdb-min-manual-flush-interval | Deprecated after introduction of ld\_manged\_flushes. Checkout rocksdb-flush-trigger-check-interval to control time interval between flush trigger checks. How often a background thread will flush buffered writes if either the data age, partition idle, or data amount triggers indicate a flush should occur. 0 disables all manual flushes. | 120s | server&nbsp;only |
| rocksdb-num-bg-threads-hi | Number of high-priority rocksdb background threads to run. These threads are shared among all shards. If -1, num\_shards * max\_background\_flushes is used. | -1 | requires&nbsp;restart, server&nbsp;only |
| rocksdb-num-bg-threads-lo | Number of low-priority rocksdb background threads to run. These threads are shared among all shards. If -1, num\_shards * max\_background\_compactions is used. | -1 | requires&nbsp;restart, server&nbsp;only |
| rocksdb-num-levels | number of LSM-tree levels if level compaction is used | 1 | requires&nbsp;restart, server&nbsp;only |
| rocksdb-partition-data-age-flush-trigger | Maximum wait after data are written before being flushed to stable storage. 0 disables the trigger. | 1200s | server&nbsp;only |
| rocksdb-partition-idle-flush-trigger | Maximum wait after writes to a time partition cease before any uncommitted data are flushed to stable storage. 0 disables the trigger. | 600s | server&nbsp;only |
| rocksdb-read-amp-bytes-per-bit | If greater than 0, will create a bitmap to estimate rocksdb read amplification and expose the result through READ\_AMP\_ESTIMATE\_USEFUL\_BYTES and READ\_AMP\_TOTAL\_READ\_BYTES stats. | 32 | requires&nbsp;restart, server&nbsp;only |
| rocksdb-sample-for-compression | If set then 1 in N rocksdb blocks will be compressed to estimate compressibility of data. This is just used for stats collection and helpful to determine whether compression will be beneficial at the rocksdb level or any other level. Two stat values are updated: sampled\_blocks\_compressed\_bytes\_fast and sampled\_blocks\_compressed\_bytes\_slow. One for a fast compression algo like lz4 and other other for a high compression algo like zstd. The stored data is left uncompressed. 0 means no sampling. | 20 | requires&nbsp;restart, server&nbsp;only |
| rocksdb-skip-list-lookahead | number of keys to examine in the neighborhood of the current key when searching within a skiplist (0 to disable the optimization) | 3 | requires&nbsp;restart, server&nbsp;only |
| rocksdb-sst-delete-bytes-per-sec | ratelimit in bytes/sec on deletion of SST files per shard; 0 for unlimited. | 0 | server&nbsp;only |
| rocksdb-table-format-version | Version of rockdb block-based sst file format. See rocksdb/table.h for details. You probably don't need to change this. | 4 | requires&nbsp;restart, server&nbsp;only |
| rocksdb-target-file-size-base | target L1 file size for compaction | 67108864 | requires&nbsp;restart, server&nbsp;only |
| rocksdb-uc-max-merge-width | maximum number of files in a single universal compaction run | 4294967295 | requires&nbsp;restart, server&nbsp;only |
| rocksdb-uc-max-size-amplification-percent | target size amplification percentage for universal compaction | 200 | requires&nbsp;restart, server&nbsp;only |
| rocksdb-uc-min-merge-width | minimum number of files in a single universal compaction run | 2 | requires&nbsp;restart, server&nbsp;only |
| rocksdb-uc-size-ratio | arg is a percentage. If the candidate set size for compaction is arg% smaller than the next file size, then include next file in the candidate set. | 1M | requires&nbsp;restart, server&nbsp;only |
| rocksdb-update-stats-on-db-open | load stats from property blocks of several files when opening the database in order to optimize compaction decisions. May significantly impact the time needed to open the db. | false | requires&nbsp;restart, server&nbsp;only |
| rocksdb-use-direct-io-for-flush-and-compaction | If true, rocksdb will use O\_DIRECT for flushes and compactions (both input and output files). | false | requires&nbsp;restart, server&nbsp;only |
| rocksdb-use-direct-reads | If true, rocksdb will use O\_DIRECT for most file reads. | false | requires&nbsp;restart, server&nbsp;only |
| rocksdb-wal-bytes-per-sync | when writing WAL, sync once per this many bytes written. 0 turns off incremental syncing | 1M | requires&nbsp;restart, server&nbsp;only |
| rocksdb-write-buffer-size | When any RocksDB memtable ('write buffer') reaches this size it is made immitable, then flushed into a newly created L0 file. This setting may soon be superceded by a more dynamic --memtable-size-per-node limit.  | 100G | server&nbsp;only |

## Security
|   Name    |   Description   |  Default  |   Notes   |
|-----------|-----------------|:---------:|-----------|
| audit-log | Path for log file storing information about all trim point changes. For log rotation using logrotate send SIGHUP to process after rotation to reopen the log. |  | requires&nbsp;restart, server&nbsp;only |
| require-permission-message-types | Check permissions only for the received message of the type(s) specified. Separate different types with a comma. 'all' to apply to all messages. Prefix the value with '~' to include all types except the given ones, e.g. '~WINDOW,RELEASE' will check permssions for messages of all types except WINDOW and RELEASE. | START | server&nbsp;only |
| require-ssl-on-command-port | Requires SSL for admin commands sent to the command port. --ssl-cert-path, --ssl-key-path and --ssl-ca-path settings must be properly configured | false | **experimental**, server&nbsp;only |
| ssl-boundary | Enable SSL in cross-X traffic, where X is the setting. Example: if set to "rack", all cross-rack traffic will be sent over SSL. Can be one of "none", "node", "rack", "row", "cluster", "data\_center" or "region". If a value other than "none" or "node" is specified on the client, --my-location has to be specified as well. | none |  |
| ssl-ca-path | Path to CA certificate. |  | requires&nbsp;restart |
| ssl-cert-path | Path to LogDevice SSL certificate. |  | requires&nbsp;restart |
| ssl-cert-refresh-interval | TTL for an SSL certificate that we have loaded from disk. | 300s | requires&nbsp;restart |
| ssl-key-path | Path to LogDevice SSL key. |  | requires&nbsp;restart |
| ssl-load-client-cert | Set to include client certificate for mutual ssl authenticaiton | false |  |
| ssl-on-gossip-port | If true, gossip port will reject all plaintext connections. Only SSL connections will be accepted. WARNING: Any change to this setting should only be performed while send-to-gossip-port = false, in order to avoid failure detection issues while the setting change propagates through the cluster. | false | server&nbsp;only |

## Sequencer State
|   Name    |   Description   |  Default  |   Notes   |
|-----------|-----------------|:---------:|-----------|
| check-seal-req-min-timeout | before a sequencer returns its state in response to a 'get sequencer state' request the sequencer checks that it is the most recent (highest numbered) sequencer for the log. It performs the check by sending a 'check seal' request to a valid copyset of nodes in the nodeset of the sequencer's epoch. The 'check seal' request looks for a seal record placed by a higher-numbered sequencer. This setting sets the timeout for 'check seal' requests. The timeout is set to the smaller of this value and half the value of --seq-state-reply-timeout. | 500ms | server&nbsp;only |
| epoch-draining-timeout | Maximum time allowed for sequencer to drain one epoch. Sequencer will abort draining the epoch if it takes longer than the timeout. A sequencer 'drains' its epoch (waits for all appenders to complete) while reactivating to serve a higher epoch. | 2s | server&nbsp;only |
| get-trimpoint-interval | polling interval for the sequencer getting the trim point from all storage nodes | 600s | server&nbsp;only |
| nodeset-adjustment-min-window | When automatic nodeset size adjustment is enabled, only do the adjustment if we've got append throughput information for at least thisperiod of time. More details: we choose nodeset size based on log's average append throughput in a moving window of size --nodeset-adjustment-period. The average is maintained by the sequencer. If the sequencer was activated recently, we may not have a good estimate of log's append throughput. This setting says how long to wait after sequencer activation before allowing adjusting nodeset size based on that sequencer's throughput. | 1h | server&nbsp;only |
| nodeset-adjustment-period | If not zero, nodeset size for each log will be periodically adjusted based on logs's measured throughput. This settings controls how often such adjustments will be considered. The nodeset size is chosen proportionally to throughput, replication factor and backlog duration. The nodeset\_size log attribute acts as the minimim allowed nodeset size, used for low-throughput logs and logs with infinite backlog duration. If --nodeset-adjustment-period is changed from nonzero to zero, all adjusted nodesets get immediately updated back to normal size. | 6h | server&nbsp;only |
| nodeset-adjustment-target-bytes-per-shard | When automatic nodeset size adjustment is enabled, (--nodeset-adjustment-period), this setting controls the size of the chosen nodesets. The size is chosen so that each log takes around this much space on each shard. More precisely, `nodeset\_size = append\_bytes\_per\_sec * backlog\_duration * replication\_factor / nodeset\_adjustment\_target\_bytes\_per\_shard`. Appropriate value for this setting is around 0.1% - 1% of disk size. | 10G | server&nbsp;only |
| nodeset-max-randomizations | When automatic nodeset size adjustment wants to enlarge nodeset to unreasonably big size N > 127, we instead set nodeset size to 127 but re-randomize the nodeset min(N/127, nodeset\_max\_randomizations) times during retention period. If you make it too big, the union of historical nodesets will get big (127 * n), and findTime, isLogEmpty etc may become expensive. If you set it too small, and the cluster has high-throughput high-retention logs, space usage may be not very balanced. | 4 | server&nbsp;only |
| nodeset-size-adjustment-min-factor | When automatic nodeset size adjustment is enabled, we skip adjustments that are smaller than this factor. E.g. if this setting is set to 2, we won't bother updating nodeset if its size would increase or decrease by less than a factor of 2. If set to 0, nodesets will be unconditionally updated every --nodeset-adjustment-period, and will also be randomized each time, as opposed to using consistent hashing. | 2 | server&nbsp;only |
| reactivation-limit | Maximum allowed rate of sequencer reactivations. When exceeded, further appends will fail. | 5/1s | requires&nbsp;restart, server&nbsp;only |
| read-historical-metadata-timeout | maximum time interval for a sequencer to get historical epoch metadata through reading the metadata log before retrying. | 10s | server&nbsp;only |
| seq-state-backoff-time | how long to wait before resending a 'get sequencer state' request after a timeout. | 1s..10s |  |
| seq-state-reply-timeout | how long to wait for a reply to a 'get sequencer state' request before retrying (usually to a different node) | 2s |  |
| update-metadata-map-interval | Sequencer has a timer for periodically reading metadata logs and refreshing the in memory metadata\_map\_. This setting specifies the interval for this timer | 1h | server&nbsp;only |

## Sequencer boycotting
|   Name    |   Description   |  Default  |   Notes   |
|-----------|-----------------|:---------:|-----------|
| node-stats-boycott-adaptive-duration-decrease-rate | (experimental) the additive decrease rate of the adaptive boycottingduration | 1min | **experimental**, server&nbsp;only |
| node-stats-boycott-adaptive-duration-decrease-time-step | (experimental) the time step of the decrease of the adaptive boycottingduration | 30s | **experimental**, server&nbsp;only |
| node-stats-boycott-adaptive-duration-increase-factor | (experimental) the multiplicative increase factor of the adaptiveboycotting duration | 2 | **experimental**, server&nbsp;only |
| node-stats-boycott-duration | How long a boycott should be active for. 0 will ensure that boycotts has no effect, but controller nodes will still report outliers | 30min | server&nbsp;only |
| node-stats-boycott-grace-period | If a node is an consecutively deemed an outlier for this amount of time, allow it to be boycotted | 300s | server&nbsp;only |
| node-stats-boycott-max-adaptive-duration | (experimental) The maximum adaptive boycotting duration | 24h | **experimental**, server&nbsp;only |
| node-stats-boycott-min-adaptive-duration | (experimental) The minmum (and default) adaptive boycotting duration | 30min | **experimental**, server&nbsp;only |
| node-stats-boycott-relative-margin | If this is set to 0.05, a node's append success ratio has to be 5% smaller than the average success ratio of all nodes in the cluster. While node-stats-boycott-sensitivity is an absolute threshold, this setting defines a sensitivity threshold relative to the average of all success ratios. Only used if node-stats-boycott-use-rmsd is true | 0.15 | server&nbsp;only |
| node-stats-boycott-required-client-count | Require at least values from this many clients before a boycott may occur | 1 | server&nbsp;only |
| node-stats-boycott-required-std-from-mean | A node has to have a success ratio lower than (mean - X * STD) to be considered an outlier. X being the value of node-stats-boycott-required-std-from-mean | 3 | server&nbsp;only |
| node-stats-boycott-sensitivity | If node-stats-boycott-sensitivity is set to e.g. 0.05, then nodes with a success ratio at or above 95% will not be boycotted | 0 | server&nbsp;only |
| node-stats-boycott-use-adaptive-duration | (experimental) Use the new adaptive boycotting durations instead of the fixed one | false | **experimental**, server&nbsp;only |
| node-stats-boycott-use-rmsd | Use a new outlier detection algorithm | true | server&nbsp;only |
| node-stats-controller-aggregation-period | The period at which the controller nodes requests stats from all nodes in the cluster. Should be smaller than node-stats-retention-on-nodes | 30s | server&nbsp;only |
| node-stats-controller-check-period | A node will check if it's a controller or not with the given period | 60s | server&nbsp;only |
| node-stats-controller-response-timeout | A controller node waits this long between requesting stats from the other nodes, and aggregating the received stats | 2s | server&nbsp;only |
| node-stats-max-boycott-count | How many nodes may be boycotted. 0 will in addition to not allowing any nodes to be boycotted, it also ensures no nodes become controller nodes | 0 | server&nbsp;only |
| node-stats-remove-worst-percentage | Will throw away the worst X% of values reported by clients, to a maximum of node-count * node-stats-send-worst-client-count | 0.2 | server&nbsp;only |
| node-stats-retention-on-nodes | Save node stats sent from the clients on the nodes for this duration | 300s | server&nbsp;only |
| node-stats-send-period | Send per-node stats into the cluster with this period. Currently only 30s of stats is tracked on the clients, so a value above 30s will not have any effect. | 15s | client&nbsp;only |
| node-stats-send-retry-delay | When sending per-node stats into the cluster, and the message failed, wait this much before retrying. | 5ms..1s | requires&nbsp;restart, client&nbsp;only |
| node-stats-send-worst-client-count | Once a node has aggregated the values sent from writers, there may be some amount of writers that are in a bad state and report 'false' values. By setting this value, the `node-stats-send-worst-client-count` worst values reported by clients per node will be sent separately to the controller, which can then take a decision if the writer is functioning correctly or not. | 20 | server&nbsp;only |
| node-stats-timeout-delay | Wait this long for an acknowledgement that the sent node stats message was received before sending the stats to another node | 2s | client&nbsp;only |

## State machine execution
|   Name    |   Description   |  Default  |   Notes   |
|-----------|-----------------|:---------:|-----------|
| background-queue-size | Maximum number of events we can queue to background thread.  A single queue is shared by all threads in a process. | 100000 | requires&nbsp;restart |
| execute-requests | number of HI\_PRI requests to process per worker event loop iteration | 13 |  |
| lo\_requests\_per\_iteration | number of LO\_PRI requests to process per worker event loop iteration | 1 |  |
| mid\_requests\_per\_iteration | number of MID\_PRI requests to process per worker event loop iteration | 2 |  |
| num-background-workers | The number of workers dedicated for processing time-insensitive requests and operations | 4 | requires&nbsp;restart, server&nbsp;only |
| num-processor-background-threads | Number of threads in Processor's background thread pool. Background threads are used by, e.g., BufferedWriter to construct/compress large batches.  If 0 (default), use num-workers. | 0 | requires&nbsp;restart |
| num-workers | number of worker threads to run, or "cores" for one thread per CPU core | cores | requires&nbsp;restart |
| prioritized-task-execution | Enable prioritized execution of requests within CPU executor. Setting this false ignores per request and per message ExecutorPriority. | true | requires&nbsp;restart |
| test-mode | Enable functionality in integration tests. Currently used for admin commands that are only enabled for testing purposes. | false | CLI&nbsp;only, requires&nbsp;restart, server&nbsp;only |
| worker-request-pipe-capacity | size each worker request queue to hold this many requests | 524288 | requires&nbsp;restart |

## Storage
|   Name    |   Description   |  Default  |   Notes   |
|-----------|-----------------|:---------:|-----------|
| free-disk-space-threshold | threshold (relative to total diskspace) of minimal free disk space for storage partitions to accept writes. This should be a fraction between 0.0 (exclusive) and 1.0 (exclusive). Storage nodes will reject writes to partitions with free disk space less than the threshold in order to guarantee that compactions can be performed. Note: this only applies to RocksDB local storage. | 0.2 | server&nbsp;only |
| ignore-cluster-marker | If cluster marker is missing or doesn't match, overwrite it and carry on. Cluster marker is a file that LogsDB writes in the data directory of each shard to identify the shard id, node id, and cluster name to which the data in that directory belongs. Cluster marker mismatch indicates that a drive or node was moved to another cluster or another shard, and the data must not be used. | false | requires&nbsp;restart, server&nbsp;only |
| local-log-store-path | path to local log store (if storage node) |  | requires&nbsp;restart, server&nbsp;only |
| logstore-monitoring-interval | interval between consecutive health checks on the local log store | 10s | server&nbsp;only |
| max-in-flight-monitor-requests | maximum number of in-flight monitoring requests (e.g. manual compaction) posted by the monitoring thread | 1 | requires&nbsp;restart, server&nbsp;only |
| max-queued-monitor-requests | max number of log store monitor requests buffered in the monitoring thread queue | 32 | requires&nbsp;restart, server&nbsp;only |
| rocksdb-auto-create-shards | Auto-create shard data directories if they do not exist | false | requires&nbsp;restart, server&nbsp;only |
| storage-task-read-backlog-share | The share for principal read-backlog in the DRR scheduler. | 5 | server&nbsp;only |
| storage-task-read-compaction-partial-share | The share for principal read-compaction-partial in the DRR scheduler. | 1 | server&nbsp;only |
| storage-task-read-compaction-retention-share | The share for principal read-compaction-retention in the DRR scheduler. | 3 | server&nbsp;only |
| storage-task-read-findkey-share | The share for principal read-findkey in the DRR scheduler. | 10 | server&nbsp;only |
| storage-task-read-internal-share | The share for principal read-internal in the DRR scheduler. | 10 | server&nbsp;only |
| storage-task-read-metadata-share | The share for principal read-metadata in the DRR scheduler. | 10 | server&nbsp;only |
| storage-task-read-misc-share | The share for principal read-misc in the DRR scheduler. | 1 | server&nbsp;only |
| storage-task-read-rebuild-share | The share for principal read-rebuild in the DRR scheduler. | 3 | server&nbsp;only |
| storage-task-read-tail-share | The share for principal read-tail in the DRR scheduler. | 8 | server&nbsp;only |
| storage-tasks-drr-quanta | Default quanta per-principal. 1 implies request based scheduling. Use something like 1MB for byte based scheduling. | 1 | server&nbsp;only |
| storage-tasks-use-drr | Use DRR for scheduling read IO's. | false | requires&nbsp;restart, server&nbsp;only |
| storage-thread-delaying-sync-interval | Interval between invoking syncs for delayable storage tasks. Ignored when undelayable task is being enqueued. | 100ms | server&nbsp;only |
| storage-threads-per-shard-default | size of the storage thread pool for small client requests and metadata operations, per shard. If zero, the 'slow' pool will be used for such tasks.  | 2 | requires&nbsp;restart, server&nbsp;only |
| storage-threads-per-shard-fast | size of the 'fast' storage thread pool, per shard. This storage thread pool executes storage tasks that write into RocksDB. Such tasks normally do not block on IO. If zero, slow threads will handle write tasks. | 2 | requires&nbsp;restart, server&nbsp;only |
| storage-threads-per-shard-fast-stallable | size of the thread pool (per shard) executing low priority write tasks, such as writing rebuilding records into RocksDB. Measures are taken to not schedule low-priority writes on this thread pool when there is work for 'fast' threads. If zero, normal fast threads will handle low-pri write tasks | 1 | requires&nbsp;restart, server&nbsp;only |
| storage-threads-per-shard-slow | size of the 'slow' storage thread pool, per shard. This storage thread pool executes storage tasks that read log records from RocksDB, both to serve read requests from clients, and for rebuilding. Those are likely to block on IO. | 2 | requires&nbsp;restart, server&nbsp;only |
| write-batch-bytes | min number of payload bytes for a storage thread to write in one batch unless write-batch-size is reached first | 1048576 | server&nbsp;only |
| write-batch-size | max number of records for a storage thread to write in one batch | 1024 | server&nbsp;only |

## Testing
|   Name    |   Description   |  Default  |   Notes   |
|-----------|-----------------|:---------:|-----------|
| abort-on-failed-catch | When an ld\_catch() fails, call abort().  If not, just continue executing.  We'll log either way. | `true` in debug builds, `false` in release builds |  |
| abort-on-failed-check | When an ld\_check() fails, call abort().  If not, just continue executing.  We'll log either way. | `false` in the client, `true` elsewhere |  |
| assert-on-data | Trigger asserts on data in RocksDB (or that received from the network). Should not be used in prod. | false | server&nbsp;only |
| client-test-force-stats | force instantiation of StatsHolder within ClientImpl even if stats publishing is disabled | false | requires&nbsp;restart, client&nbsp;only |
| command-unix-socket | Path to the unix domain socket the server will use to listen for admin commands, supports commands over SSL |  | requires&nbsp;restart, server&nbsp;only |
| disable-event-log-trimming | Disable trimming of the event log (for tests only) | false | server&nbsp;only |
| disable-logsconfig-trimming | Disable the trimming of logsconfig delta log. Used for testing only. | false | server&nbsp;only |
| disable-rebuilding | Disable rebuilding. Do not use in production. Only used by tests. | false | requires&nbsp;restart, server&nbsp;only |
| disable-trim-past-tail-check | Disable check for trim past tail. Used for testing log trimming. | false | client&nbsp;only |
| dont-serve-findtimes-for-logs | Logs for which findtimes will not be served |  | server&nbsp;only |
| dont-serve-findtimes-status | status that should be returned for logs that are in "dont-serve-findtimes-for-logs" | FAILED | server&nbsp;only |
| dont-serve-reads-for-logs | Logs for which reads will not be served |  | server&nbsp;only |
| dont-serve-reads-status | status that should be returned for logs that are in "dont-serve-reads-for-logs" | FAILED | server&nbsp;only |
| dont-serve-stores-for-logs | Logs for which stores will not be served |  | server&nbsp;only |
| dont-serve-stores-status | status that should be returned for logs that are in "dont-serve-stores-for-logs" | FAILED | server&nbsp;only |
| epoch-store-path | directory containing epoch files for logs (for testing only) |  | requires&nbsp;restart, server&nbsp;only |
| esn-bits | How many bits to use for sequence numbers within an epoch.  LSN bits [n, 32) are guaranteed to be 0. Used for testing ESN exhaustion. | 32 | requires&nbsp;restart, server&nbsp;only |
| hold-store-replies | If set, we hold all STORED messages (which are replies to STORE messages), until the last one comes is.  Has some race conditions and other down sides, so only use in tests.  Used to ensure that all storage nodes have had a chance to process the STORE messages, even if one returns PREEMPTED or another error condition. | false | requires&nbsp;restart, server&nbsp;only |
| include-cluster-name-on-handshake | The cluster name of the connection initiator will be included in the LogDevice protocol handshake. If the cluster name of the initiator does not match the actual cluster name of the destination, the connection is terminated. We don't know of any good reasons to disable this option. If you disable it and move some hosts from one cluster to another, you may have a bad time: some clients or servers may not pick up the update and keep talking to the hosts as if they weren't moved; this may corrupt metadata. Used for testing and internally created connections only. | true |  |
| loglevel-overrides | Comma-separated list of <module>:<loglevel>. eg: Server.cpp:debug,Sequencer.cpp:notify |  | server&nbsp;only |
| msg-error-injection-chance | percentage chance of a forced message error on a Socket. Used to exercise error handling paths. | 0 | requires&nbsp;restart, server&nbsp;only |
| msg-error-injection-status | status that should be returned for a simulated message transmission error | NOBUFS | requires&nbsp;restart |
| nodes-configuration-file-store-dir | If set, the source of truth of nodes configuration will be under this dir instead of the default (zookeeper) store. Only effective when --enable-nodes-configuration-manager=true; Used by integration testing. |  | requires&nbsp;restart |
| rebuild-without-amends | During rebuilding, send a normal STORE rather than a STORE with the AMEND flag, when updating the copyset of nodes that already have a copy of the record. This option is used by integration tests to fully divorce append content from records touched by rebuilding. | false | server&nbsp;only |
| rebuilding-read-only | Rebuilding is read-only (for testing only). Use on-donor if rebuilding should not send STORE messages, or on-recipient if these should be sent but discarded by the recipient (LogsDB only) | none | server&nbsp;only |
| rebuilding-retry-timeout | Delay before a record rebuilding retries a failed operation | 5s..30s | server&nbsp;only |
| rocksdb-test-corrupt-stores | Used for testing only. If true, a node will report all stores it receives as corrupted. | false | server&nbsp;only |
| skip-recovery | Skip recovery. For tests only. When this option is enabled, recovery does not recover any data but instead immediately marks all epochs as clean in the epoch store and purging immediately marks all epochs as clean in the local log store. This feature should be used as a last resort if a cluster's availability is hurt by recovery and it is important to quickly restore availability at the cost of some inconsistencies. On-the-fly changes of this setting will only apply to new LogRecoveryRequests and will not affect recoveries that are already in progress. | false | server&nbsp;only |
| test-appender-skip-stores | Allow appenders to skip sending data to storage node. Currently used in tests to make sure an appender state machine is running | false | server&nbsp;only |
| test-bypass-recovery | If set, sequencers will not automatically run recovery upon activation. Recovery can be started using the 'startrecovery' admin command.  Note that last released lsn won't get advanced without recovery. | false | requires&nbsp;restart, server&nbsp;only |
| test-do-not-pick-in-copysets | Copyset selectors won't pick these nodes. Comma-separated list of node indexes, e.g. '1,2,3'. Used in tests. |  | server&nbsp;only |
| test-get-cluster-state-recipients | Force get-cluster-state recipients as a comma-separated list of node ids |  | client&nbsp;only |
| test-reject-hello | if set to the name of an error code, reject all HELLOs with the specified error code. Currently supported values are ACCESS and PROTONOSUPPORT. Used for testing. | OK | requires&nbsp;restart, server&nbsp;only |
| test-sequencer-corrupt-stores | Simulates bad hardware flipping a bit in the payload of a STORE message. | false | server&nbsp;only |
| test-stall-rebuilding | Makes rebuilding pretend to start but not make any actual progress. Used in tests. | false | server&nbsp;only |
| test-timestamp-linear-transform | Coefficents for tranforming the timestamp of records for test. The value should contain two integrs sperated by ','. For example'm,c'. Records timestamp is tranformed as m * now() + c.A default value of '1,0' makes the timestamp = now() which is expectedfor all the normal use cases. | 1,0 | requires&nbsp;restart, server&nbsp;only |
| unix-socket | Path to the unix domain socket the server will use to listen for non-SSL clients |  | CLI&nbsp;only, requires&nbsp;restart, server&nbsp;only |

## Uncategorized
|   Name    |   Description   |  Default  |   Notes   |
|-----------|-----------------|:---------:|-----------|
| rebuilding-max-batch-time | Max amount of time rebuilding read storage task is allowed to take before yielding to other storage tasks. Only supported by rebuilding V2 (partition by partition). "max" for unlimited. | 1000ms | server&nbsp;only |
| rebuilding-max-malformed-records-to-tolerate | Controls how rebuilding donors handle unexpected values in local log store (e.g. caused by bugs, forward incompatibility, or other processes writing unexpected things to rocksdb directly).If rebuilding encounters invalid records, it skips them and logs warnings. But if it encounters at least this many of them in the same log, it freaks out, logs a critical error and stalls indefinitely. The rest of the server keeps trying to run normally, to the extent to which you can run normally when you can't parse most of the records in the DB. | 1000 | requires&nbsp;restart, server&nbsp;only |
| sync-metadata-log-writes | If set, storage nodes will wait for wal sync of metadata log writes before sending the STORED ack. | true | server&nbsp;only |

## Write path
|   Name    |   Description   |  Default  |   Notes   |
|-----------|-----------------|:---------:|-----------|
| append-store-durability | The minimum guaranteed durablity of record copies before a storage node confirms the STORE as successful. Can be one of "memory" if record is to be stored in a RocksDB memtable only (logdeviced memory), "async\_write" if record is to be additionally written to the RocksDB WAL file (kernel memory, frequently synced to disk), or "sync\_write" if the record is to be written to the memtable and WAL, and the STORE acknowledged only after the WAL is synced to disk by a separate WAL syncing thread using fdatasync(3). | async\_write | server&nbsp;only |
| appender-buffer-process-batch | batch size for processing per-log queue of pending writes | 20 | server&nbsp;only |
| appender-buffer-queue-cap | capacity of per-log queue of pending writes while sequencer  is initializing or activating | 10000 | requires&nbsp;restart, server&nbsp;only |
| byte-offsets | Enables the server-side byte offset calculation feature.NOTE: There is no guarantee of byte offsets result correctness if featurewas switched on->off->on in period shorter than retention value forlogs. | false | server&nbsp;only |
| check-node-health-request-timeout | Timeout for health check probes that sequencers send to unresponsive storage nodes. If no reply arrives after timeout, another probe is sent. | 120s | server&nbsp;only |
| checksum-bits | how big a checksum to include with newly appended records (0, 32 or 64) | 32 |  |
| copyset-locality-min-scope | Tl;dr: if you experience data distribution imbalance caused by hot logs, and you have plenty of unused cross-rack/cross-region bandwidth, try changing this setting to "root"; otherwise the default "rack" is just fine.  More details: let X be the value of this setting, and let Y be the biggest scope in log's replicateAcross property; if Y < X, nothing happens; if Y >= X, at least one copy of each record will be stored in sequencer's domain of scope Y (not X), when it's possible without affecting average data distribution. This, combined with chain-sending, typically reduces the number of cross-Y hops by one per record. | rack | server&nbsp;only |
| disable-chain-sending | never send a wave of STORE messages through a chain | false | server&nbsp;only |
| disable-graylisting | setting this to true disables graylisting nodes by sequencers in the write path | false | server&nbsp;only |
| disable-outlier-based-graylisting | setting this to true disables the outlier based graylisting nodes by sequencers in the write path | true | **experimental**, server&nbsp;only |
| disabled-retry-interval | Time interval during which a sequencer will not route record copies to a storage node that reported a permanent error. | 30s | server&nbsp;only |
| enable-adaptive-store-timeout | decides whether to enable an adaptive store timeout | false | **experimental**, server&nbsp;only |
| enable-offset-map | Enables the server-side OffsetMap calculation feature.NOTE: There is no guarantee of byte offsets result correctness if featurewas switched on->off->on in period shorter than retention value forlogs. | false | server&nbsp;only |
| enable-sticky-copysets | If set, sequencers will enable sticky copysets. Doesn't affect the copyset index. | true | requires&nbsp;restart, server&nbsp;only |
| epoch-metadata-use-new-storage-set-format | Serialize copysets using ShardIDs instead of node\_index\_t inside EpochMetaData. TODO(T15517759): enable by default once Flexible Log Sharding is fully implemented and this has been thoroughly tested. | false | **experimental** |
| gray-list-threshold | if the number of storage nodes graylisted on the write path of a log exceeds this fraction of the log's nodeset size the gray list will be cleared to make sure that copysets can still be picked | 0.25 | server&nbsp;only |
| graylisting-grace-period | The duration through which a node need to be consistently an outlier to get graylisted | 300s | server&nbsp;only |
| graylisting-monitored-period | The duration through which a recently ungraylisted node will be monitored and graylisted as soon as it becomes an outlier | 120s | server&nbsp;only |
| graylisting-refresh-interval | The interval at which the graylists are refreshed | 30s | server&nbsp;only |
| isolated-sequencer-ttl | How long we wait before disabling isolated sequencers. A sequencer is declared isolated if nodes outside of the innermost failure domain of the sequencer's epoch appear unreachable to the failure detector. For example, a sequencer of a rack-replicated log epoch is declared isolated if the failure detector can't reach any nodes outside of that sequencer node's rack. A disabled sequencer rejects all append requests. | 1200s | server&nbsp;only |
| no-redirect-duration | when a sequencer activates upon request from a client, it does not redirect its clients to a different sequencer node for this amount of time (even if for instance the primary sequencer just started up and an older sequencer may be up and running) | 5s | server&nbsp;only |
| node-health-check-retry-interval | Time interval during which a node health check probe will not be sent if there is an outstanding request for the same node in the nodeset | 5s | server&nbsp;only |
| nodeset-state-refresh-interval | Time interval that rate-limits how often a sequencer can refresh the states of nodes in the nodeset in use | 1s | server&nbsp;only |
| nospace-retry-interval | Time interval during which a sequencer will not route record copies to a storage node that reported an out of disk space condition. | 60s | server&nbsp;only |
| overloaded-retry-interval | Time interval during which a sequencer will not route record copies to a storage node that reported itself overloaded (storage task queue too long). | 1s | server&nbsp;only |
| payload-inline | max message payload size that we store in a flat buffer after header | 1024 |  |
| release-broadcast-interval | the time interval for periodic broadcasts of RELEASE messages by sequencers of regular logs. Such broadcasts are not essential for correct cluster operation. They are used as the last line of defence to make sure storage nodes deliver all records eventually even if a regular (point-to-point) RELEASE message is lost due to a TCP connection failure. See also --release-broadcast-interval-internal-logs. | 300s | server&nbsp;only |
| release-broadcast-interval-internal-logs | Same as --release-broadcast-interval but instead applies to internal logs, currently the event logs and logsconfig logs | 5s | server&nbsp;only |
| release-retry-interval | RELEASE message retry period | 20s | server&nbsp;only |
| shadow-client-creation-retry-interval | Failed shadow appends because shadow client was not available, enqueue a client recreation request. The retry mechanism retries the enqueued attempt after these many seconds. See ShadowClient.cpp for a detailed explanation. 0 disables the retry feature. 1 silently drops all client creations so that they only get created from the retry path. | 60s | client&nbsp;only |
| slow-node-retry-interval | After a sequencer's request to store a record copy on a storage node times out that sequencer will graylist that node for at least this time interval. The sequencer will not pick graylisted nodes for copysets unless --gray-list-threshold is reached or no valid copyset can be selected from nodeset nodes not yet graylisted. For outlier-based graylisting increases exponentially for each new graylisting up until 10x of this value and decreases at linear rate down to this value when not graylisted | 600s | server&nbsp;only |
| sticky-copysets-block-max-time | The time since starting the last block, after which the copyset manager will consider it expired and start a new one. | 10min | requires&nbsp;restart, server&nbsp;only |
| sticky-copysets-block-size | The total size of processed appends (in bytes), after which the sticky copyset manager will start a new block. | 33554432 | requires&nbsp;restart, server&nbsp;only |
| store-timeout | timeout for attempts to store a record copy on a specific storage node. This value is used by sequencers only and is NOT the client request timeout. | 10ms..1min | server&nbsp;only |
| unroutable-retry-interval | Time interval during which a sequencer will not pick for copysets a storage node whose IP address was reported unroutable by the socket layer | 60s | server&nbsp;only |
| use-sequencer-affinity | If true, the routing of append requests to sequencers will first try to find a sequencer in the location given by sequencerAffinity() before looking elsewhere. | false |  |
| verify-checksum-before-replicating | If set, sequencers and rebuilding will verify checksums of records that have checksums. If there is a mismatch, sequencer will reject the append. Note that this setting doesn't make storage nodes verify checksums. Note that if not set, and --rocksdb-verify-checksum-during-store is set, a corrupted record kills write-availability for that log, as the appender keeps retrying and storage nodes reject the record. | true | server&nbsp;only |
| write-copyset-index | If set, storage nodes will write the copyset index for all records. This must be set before --rocksdb-use-copyset-index is enabled. Doesn't affect copyset stickiness | true | server&nbsp;only |
| write-shard-id-in-copyset | Serialize copysets using ShardIDs instead of node\_index\_t on disk. TODO(T15517759): enable by default once Flexible Log Sharding is fully implemented and this has been thoroughly tested. | false | **experimental**, server&nbsp;only |

