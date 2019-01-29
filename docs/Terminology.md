---
---
_acknowledge (a record)._ A record is acknowledged to the writer when it is durably stored, according to the replication policy for that log.

_ALL_SEND_ALL._ A fallback mode for the read path where each storage node sends all records. It's less efficient than single-copy delivery (SCD), but better for detecting gaps.

_appender._ An in-memory object that makes sure a record is durably written to a copyset.

_authoritative status_ The authoritative status of a shard indicates what role this shard is playing in the replication of data records.


_bridge gap._ On the read path, a bridge gap indicates that there are no records stored between two LSNs across 2 epochs. It indicates a benign gap, not a gap due to data loss.

_bridge record._ On the storage layer, a record that represents a bridge gap.  

_compaction._ The process of combining multiple SST files into a single SST file to improve efficiency. Records that are older than their retention period may be removed during this process. Compaction occurs on a schedule. It may also be started by LogDevice as part of rebuilding.

_copyset._ The set of nodes on which a given record is actually stored.

_dataloss gap._ A gap between two sequence numbers that indicates that records were lost.

_delta log._ See _snapshot_.

_disaggregated cluster._ A cluster in which there are two types of hardware, where one type only handles the sequencing of records, and the other type stores records. These are referred to as sequencer and storage nodes, respectively.

_donor._ During rebuilding, the shard from which the record is being copied.

_draining (of storage nodes)._ Storage nodes are drained when LogDevice intends to remove nodes from the cluster either temporarily (such as for maintenance) or indefinitely (for example, to shrink a cluster). No data is written to a node that is draining.

_draining (of a sequencer)._ The sequencer stops accepting new appends but waits for all appenders in its sliding window to finish their replication and retire.

_durability._ Data has been stored, redundantly, so that it is not lost or compromised.

_epoch._ The upper 32 bits of the log sequence number (LSN).

_ESN (epoch sequence number)._ The lower 32 bits of the log sequence number.

_event log._ An internal log that tracks shard authoritative states and transitions (such as rebuilding).

_f-majority._ A set of nodes that intersects every possible copyset. If a reader cannot find a particular record on an f-majority of nodes, then that record must be under-replicated or lost.

_findtime._ A kind of query to a LogDevice cluster that, given a timestamp, returns an LSN that approximately corresponds to that time.

_gap record._ A special kind of record that indicates there is no data at this LSN. Types include bridge, dataloss, and hole plug.

_gossip._ The protocol that nodes follow to contact each other for their status to arrive at a consensus view of the cluster.

_historical nodeset._ When an epoch changes, the nodeset is permitted to change as well. Nodesets that belong to older epochs are called historical nodesets.

_hole plug._ A special record indicating that there is no record with this LSN. No record was acknowledged with that sequence number. It indicates a benign gap, not a gap due to data loss.

_internal log._ A LogDevice cluster uses one of its own logs to maintain some shared internal state.

_L0 file._ A file used by RocksDB to store records. It's created when the in-memory tables are flushed to disk. Each RocksDB partition has a separate set of L0 files.

_log._ A record-oriented, append-only, trimmable file. Once appended, a record is immutable. Each record is uniquely identified by a monotonically increasing sequence number. Readers read records sequentially from any point inside the retention period.

_log ID._ Every log is identified by a numeric ID. Each log has a corresponding metadata log, which has an ID of 2^63 + log_id.

_logsconfig._ A pair of internal logs (and a replicated state machine that corresponds to them) that stores the configuration for logs.

_LSN (log sequence number)._ A sequential ID used to order records in a log. The upper 32 bits are the epoch number, and the lower 32 are the offset within the epoch, or epoch sequence number (ESN). Normally, this number increases sequentially, but certain events, such as sequencer failover or changing replication settings, cause an "epoch bump", in which the epoch is incremented and the ESN is reset. Often formatted as `eXXXnYYYY`, where `XXX` is the epoch and `YYYY` is the ESN.

_metadata log._ Every log has an associated metadata log that records things like historical nodesets and replication constraints.

_mini-rebuilding._ A rebuilding limited to a specified time range.

_node._ Synonym for server or host. Each node runs an instance of the LogDevice server.

_nodeset._ The set of disks that may have data for an epoch of a log. This is now known as a storage set, but this term is still used throughout LogDevice.

_non-authoritative rebuilding._ If the number of shards that is down is more than the replication factor, it may not be possible to fully re-replicate all of the data records. When this happens, rebuilding is best effort, or non-authoritative. This may cause readers to stall if data is lost because some nodes remain unavailable. This requires a manual operation to unstall the readers and accept the data loss.

_partial compaction._ When several small L0 files are compacted into a larger L0 file, leaving most L0 files alone. Partial compaction is triggered automatically when there is a size disparity between L0 files.

_partition._ The database is made up of partitions. Each partition corresponds to a time range and contains all the record with timestamps that fall into that range. The typical length of that range is about 15 minutes and the data size in the partition is a few gigabytes. A new partition is created every few minutes to accommodate new data. Usually newly appended data is written to the latest partition, but all partitions are always writable. In particular, during rebuilding, many records are written to old partitions.

Reading works in two levels: at a higher level, the reader steps from partition to partition sequentially; at a lower level, it steps from record to record inside the partition until it reaches the end of partition.

_rebuilding._ The process of re-replicating records after a storage failure.

_record._ The smallest unit of data in a log. A record includes the log id, LSN, copyset, timestamp, and the payload.

_recovery._ The process of guaranteeing sanity at the tail of an epoch after a sequencer failure.

_release._ A record is released to the reader when it is durably stored and there are no in-flight records with a smaller LSN.

_rewind._ If a client is reading in SCD mode and determines there may be a gap, it tells the storage nodes to rewind and fall back to ALL_SEND_ALL mode.

_RSM (replicated state machine)._ A state machine that tails a pair of internal logs to build a state that is eventually consistent across all hosts in the cluster.

_SCD (single-copy delivery)._ An optimization used in the read path. Only the primary node for the record reads and sends the record to the client.

_sealing._ The first stage of recovery, in which the old epoch is fenced off to prevent any new writes happening to it.

_sequencer._ An object responsible for assigning LSNs to incoming records. Can also refer to a node that can run sequencers.

_shard._ A storage unit in a node.

_snapshot._ Each internal log is really two logs: the delta log and the snapshot log. The delta stores recent changes, and every so often, the changes are accumulated into a snapshot and written to the snapshot log.

_SST (Sorted Sequence Table) file._ Same as L0 file in RocksDB.

_storage set._ The set of shards that contain all records in an epoch.

_trim._ Delete older records in a log. Trimming can be retention-based, space-based, or on demand.

_wave._ If the appender cannot successfully store a record in a copyset, it picks a new one and tries again. Each attempt is called a wave.
