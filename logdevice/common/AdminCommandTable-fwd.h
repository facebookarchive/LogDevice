/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <string>

#include "logdevice/common/types_internal.h"

namespace facebook { namespace logdevice {

template <typename... Args>
class AdminCommandTable;

enum class E : std::uint16_t;
struct Address;
struct ClientID;
class Sockaddr;
typedef E Status;

namespace admin_command_table {

// Use this type instead of `lsn_t` for colums that should contain an lsn if you
// wish to have the content of the columns be made human readable when prettify
// is set to true.
LOGDEVICE_STRONG_TYPEDEF(lsn_t, LSN);
LOGDEVICE_STRONG_TYPEDEF(uint64_t, BYTE_OFFSET);

} // namespace admin_command_table

// Declare the type of all the tables used by admin commands.

typedef AdminCommandTable<ClientID, /* Client */
                          int,      /* Queued total */
                          int,      /* Queued Immediate */
                          int,      /* Queued delayed */
                          int,      /* Record Bytes Queued */
                          bool,     /* Storage task in flight */
                          bool,     /* Ping Timer Active */
                          bool      /* Blocked */
                          >
    InfoCatchupQueuesTable;

typedef AdminCommandTable<shard_index_t,              /* Shard */
                          ClientID,                   /* Client */
                          logid_t,                    /* Log id */
                          admin_command_table::LSN,   /* Start LSN */
                          admin_command_table::LSN,   /* Until LSN */
                          admin_command_table::LSN,   /* Read Pointer */
                          admin_command_table::LSN,   /* Last Delivered */
                          admin_command_table::LSN,   /* Last Record */
                          admin_command_table::LSN,   /* Window High */
                          admin_command_table::LSN,   /* Last Released */
                          bool,                       /* Catching UP */
                          bool,                       /* Window END */
                          std::string,                /* Known down */
                          filter_version_t::raw_type, /* Filter version */
                          std::string,                /* Last Batch Status */
                          std::chrono::milliseconds,  /* Created */
                          std::chrono::milliseconds,  /* Last Enqueue Time */
                          std::chrono::milliseconds,  /* Last Batch Started Time
                                                       */
                          bool,     /* Storage task in flight */
                          uint64_t, /* Version */
                          bool,     /* Throttled? */
                          int64_t,  /* milliseconds elapsed since throttled */
                          int64_t   /* Read shaping - current meter level */
                          >
    InfoReadersTable;

typedef AdminCommandTable<std::string, /* State */
                          Address,     /* Name */
                          float,       /* Pending (KB) */
                          float,       /* Available (KB) */
                          float,       /* Read (MB) */
                          float,       /* Write (MB) */
                          int,         /* Read-cnt */
                          int,         /* Write-cnt */
                          int,         /* Proto */
                          size_t,      /* Send buf-sz */
                          uint32_t,    /* Peer Config Version */
                          bool,        /* Is ssl */
                          int          /* FD of the underlying socket */
                          >
    InfoSocketsTable;

typedef AdminCommandTable<logid_t,                   /* Log id */
                          int64_t,                   /* Shard */
                          admin_command_table::LSN,  /* Min LSN */
                          admin_command_table::LSN,  /* Max LSN */
                          uint64_t,                  /* Chunk ID */
                          uint64_t,                  /* Block ID */
                          uint64_t,                  /* Total bytes */
                          std::chrono::milliseconds, /* Oldest timestamp */
                          uint64_t,                  /* Stores in flight */
                          uint64_t,                  /* Amends in flight */
                          uint64_t,                  /* Amend self in flight */
                          std::chrono::milliseconds  /* Started */
                          >
    InfoRebuildingChunksTable;

typedef AdminCommandTable<logid_t,                   /* Log id */
                          int64_t,                   /* Shard */
                          std::chrono::milliseconds, /* Started */
                          std::string,               /* Rebuilding set */
                          admin_command_table::LSN,  /* Version */
                          admin_command_table::LSN,  /* Until LSN */
                          std::chrono::milliseconds, /* Max timestamp */
                          admin_command_table::LSN,  /* Rebuilt up to */
                          uint64_t,                  /* Num replicated */
                          uint64_t,                  /* Bytes replicated */
                          uint64_t, /* RecordRebuilding In flight */
                          uint64_t, /* Num Stores waiting for flush */
                          uint64_t, /* Num records waiting for amends to start*/
                          uint64_t, /* RecordRebuildingAmend In flight */
                          uint64_t, /* Num Amends waiting for flush*/
                          std::string /* Last storage task status */
                          >
    InfoRebuildingLogsTable;

typedef AdminCommandTable<uint32_t,                  /* Shard id */
                          std::string,               /* Rebuilding set */
                          admin_command_table::LSN,  /* Version */
                          std::chrono::milliseconds, /* Global window end */
                          std::chrono::milliseconds, /* Local window end */
                          int64_t,     /* Num logs waiting for plan */
                          uint64_t,    /* Num logs catching up */
                          uint64_t,    /* Num logs queued for catchup */
                          uint64_t,    /* Num logs in restart queue */
                          uint64_t,    /* Total memory used */
                          bool,        /* Stall timer active */
                          uint64_t,    /* Num restart timers active */
                          uint64_t,    /* Num active logs */
                          bool,        /* Participating */
                          std::string, /* Time by state */
                          bool,        /* Task in flight */
                          bool,        /* Persistent error */
                          uint64_t,    /* Read buffer bytes */
                          uint64_t,    /* Records in flight */
                          std::string, /* Read pointer */
                          double       /* Progress */
                          >
    InfoRebuildingShardsTable;

typedef AdminCommandTable<int,                       /* Shard */
                          uint64_t,                  /* ID */
                          std::chrono::milliseconds, /* Start Time */
                          std::chrono::milliseconds, /* Min Time */
                          std::chrono::milliseconds, /* Max Time */
                          std::chrono::milliseconds, /* Last Compacted */
                          uint64_t,                  /* Approx. Size */
                          uint64_t,                  /* L0 files */
                          std::string,               /* Immutable Memtables */
                          std::string, /* Memtable Flush Pending */
                          std::string, /* Active Memtable Size */
                          std::string, /* All Not Flushed Memtables Size */
                          std::string, /* All Memtables Size */
                          std::string, /* Est Num Keys */
                          std::string, /* Est Mem by Readers */
                          std::string, /* Live Versions */
                          uint64_t,    /* Current version */
                          std::chrono::milliseconds, /* Min Durable Time */
                          std::chrono::milliseconds, /* Max Durable Time */
                          std::string,               /* Append Dirtied By */
                          std::string,               /* Rebuild Dirtied By */
                          bool,                      /* Under Replicated */
                          uint64_t /* Approx. Obsolete Bytes */
                          >
    InfoPartitionsTable;

typedef AdminCommandTable<int,         /* Shard */
                          std::string, /* Column Family */
                          uint64_t,    /* Approx. Size */
                          uint64_t,    /* L0 files */
                          std::string, /* Immutable Memtables */
                          std::string, /* Memtable Flush Pending */
                          std::string, /* Active Memtable Size */
                          std::string, /* All Memtables Size */
                          std::string, /* Est Num Keys */
                          std::string, /* Est Mem by Readers */
                          std::string  /* Live Versions */
                          >
    InfoLogsDBMetadataTable;

typedef AdminCommandTable<logid_t,     /* Log id */
                          epoch_t,     /* Epoch */
                          std::string, /* State */
                          esn_t,       /* Lng */
                          uint64_t,    /* Digest sz */
                          bool,        /* Digest Fmajority */
                          bool,        /* Digest Replication Req */
                          bool,        /* Digest Authoritative */
                          uint32_t,    /* Num Holes to generate */
                          uint32_t,    /* Num Holes to re-replicate */
                          uint32_t,    /* Num Hole-record conflicts */
                          uint32_t,    /* Num Records to re-replicate */
                          uint64_t,    /* N. Mutators */
                          std::string, /* Recovery State */
                          std::chrono::milliseconds, /* Created */
                          std::chrono::milliseconds, /* Restarted */
                          uint16_t                   /* Num restarts */
                          >
    InfoRecoveriesTable;

inline std::array<std::string, 17> getInfoRecoveriesTableColumns() {
  return {{"Log id",
           "Epoch",
           "State",
           "Lng",
           "Dig. sz",
           "Dig. Fmajority",
           "Dig. Replic.",
           "Dig. Author.",
           "Holes Plugged",
           "Holes Replicate",
           "Holes Conflict",
           "Records Replicate",
           "N. Mutators",
           "Recovery State",
           "Created",
           "Restarted",
           "N. restarts"}};
}

typedef AdminCommandTable<logid_t,                    /* Log id */
                          read_stream_id_t::raw_type, /* Id */
                          admin_command_table::LSN,   /* Next lsn to deliver */
                          admin_command_table::LSN,   /* Window High */
                          admin_command_table::LSN,   /* Until lsn */
                          uint64_t,                   /* Read set size */
                          admin_command_table::LSN, /* Gap end outside window */
                          admin_command_table::LSN, /* Trim point */
                          std::string,              /* Gap nodes next lsn */
                          std::string,              /* Unavailable nodes */
                          std::string,              /* Connection health */
                          bool,                     /* Redelivery In-progress */
                          filter_version_t::raw_type, /* Current Filter version
                                                       */
                          std::string,                /* Shards down */
                          std::string,                /* Shards slow */
                          int64_t,                    /* bytes_lagged */
                          int64_t,                    /* timestamp_lagged */
                          std::chrono::milliseconds,  /* last_time_lagging */
                          std::chrono::milliseconds,  /* last_time_stuck */
                          std::string,                /* last_reported_state */
                          std::string,                /* last_tail_info */
                          std::string                 /* time_lag_record */
                          >
    InfoClientReadStreamsTable;

// TODO 16950644: remove v1 entries when purging v1 is deprecated
typedef AdminCommandTable<logid_t,     /* Log id */
                          std::string, /* State */
                          epoch_t,     /* Current Last Clean Epoch */
                          epoch_t,     /* Purge To */
                          epoch_t,     /* New Last Clean Epoch */
                          std::string, /* Sequencer */
                          std::string  /* state of epochs being purged */
                          >
    InfoPurgesTable;

typedef AdminCommandTable<pid_t,       /* Process ID */
                          std::string, /* Version */
                          std::string, /* Build Info in Json */
                          std::string, /* Package */
                          std::string, /* Build User */
                          std::string, /* Build Time */
                          std::string, /* start datetime */
                          uint64_t,    /* uptime in seconds */
                          std::string, /* Server ID */
                          std::string, /* Shards Rebuilding list in Json */
                          std::string, /* Log level */
                          int32_t,     /* Min Supported Protocol */
                          int32_t,     /* Max Supported Protocol */
                          bool,        /* Is authentication enabled */
                          std::string, /* Authentication type */
                          bool,        /* Is unauthenticated allowed? */
                          bool,        /* Is permission_checking enabled? */
                          std::string, /* Permission Checker Type" */
                          std::string, /* RocksDB version */
                          std::string, /* NodeID */
                          bool         /* Is LogsConfig Manager Enabled */
                          >
    InfoTable;

typedef AdminCommandTable<uint64_t,    /* Log Tree Version */
                          std::string, /* Narrowest Replication in Json */
                          int32_t,     /* Smallest Replication Factor */
                          std::string  /* Tolerable Failure Domains in Json */
                          >
    ReplicationInfoTable;

typedef AdminCommandTable<std::string, /* Column family */
                          logid_t,     /* Log id */
                          bool,        /* is_tailing */
                          bool,        /* is_blocking */
                          std::string, /* type */
                          bool,        /* Created for rebuilding */
                          uint64_t,    /* Unique identifier */
                          std::chrono::milliseconds, /* Created timestamp */
                          std::string, /* More context on the iterator */
                          admin_command_table::LSN,  /* Last seek LSN */
                          std::chrono::milliseconds, /* Last seek timestamp */
                          uint64_t /* RocksDB version after last seek */
                          >
    InfoIteratorsTable;

typedef AdminCommandTable<uint64_t,    /* Shard ID */
                          bool,        /* Failing */
                          Status,      /* Accepting writes */
                          std::string, /* Rebuilding state */
                          uint64_t,    /* Default CF version */
                          std::string  /* Dirty state */
                          >
    InfoShardsTable;

typedef AdminCommandTable<logid_t,                  /* Log ID */
                          uint64_t,                 /* Shard */
                          epoch_t,                  /* Epoch */
                          size_t,                   /* Payload Bytes */
                          size_t,                   /* Num Records */
                          bool,                     /* Consistent */
                          bool,                     /* Disabled */
                          esn_t,                    /* Head ESN */
                          esn_t,                    /* Max ESN */
                          esn_t,                    /* First LNG */
                          std::string,              /* Offsets within epoch */
                          admin_command_table::LSN, /* Tail record lsn */
                          std::chrono::milliseconds /* Tail record ts */
                          >
    InfoRecordCacheTable;

typedef AdminCommandTable<std::string,  /* Config URI */
                          node_index_t, /* Origin Server of the config */
                          std::string,  /* Config hash */
                          std::chrono::milliseconds, /* Last modified time */
                          std::chrono::milliseconds  /* Last loaded time */
                          >
    InfoConfigTable;

typedef AdminCommandTable<uint64_t, /* Parition ID */
                          bool      /* Hi-pri */
                          >
    InfoManualCompactionsTable;

typedef AdminCommandTable<logid_t,                  /* Delta log id */
                          logid_t,                  /* Snapshot log id */
                          admin_command_table::LSN, /* Version */
                          admin_command_table::LSN, /* Delta read ptr */
                          admin_command_table::LSN, /* Delta replay tail */
                          admin_command_table::LSN, /* Snapshot read ptr */
                          admin_command_table::LSN, /* Snapshot replay tail */
                          admin_command_table::LSN, /* Stalled waiting for
                                                       snapshot */
                          size_t, /* Delta appends in flight */
                          size_t, /* Deltas pending confirmation */
                          bool,   /* Snapshot in flight */
                          size_t, /* Delta log bytes */
                          size_t, /* delta log records */
                          bool,   /* delta log read stream healthy */
                          admin_command_table::LSN /* Propagated version */
                          >
    InfoReplicatedStateMachineTable;

typedef AdminCommandTable<uint64_t,                  /* Shard ID */
                          std::string,               /* Priority */
                          bool,                      /* Write queue */
                          size_t,                    /* Sequence No */
                          std::string,               /* Thread type */
                          std::string,               /* Task type */
                          std::chrono::milliseconds, /* Enqueue time */
                          std::string,               /* Durability */
                          logid_t,                   /* Log ID */
                          admin_command_table::LSN,  /* LSN */
                          ClientID,   /* Client that started the task */
                          Sockaddr,   /* Address of the client */
                          std::string /* Extra info */
                          >
    InfoStorageTasksTable;

typedef AdminCommandTable<int,                       /* Stats NodeID */
                          uint32_t,                  /* Successes */
                          uint32_t,                  /* Failures */
                          std::chrono::milliseconds, /* Time bucket */
                          bool                       /* Is Outlier */
                          >
    InfoAppendOutliersTable;

struct InfoStorageTasksTableFieldOffsets {
  static constexpr int SHARD_ID = 0;
  static constexpr int PRIORITY = 1;
  static constexpr int IS_WRITE_QUEUE = 2;
  static constexpr int SEQUENCE_NO = 3;
  static constexpr int THREAD_TYPE = 4;
  static constexpr int TASK_TYPE = 5;
  static constexpr int ENQUEUE_TIME = 6;
  static constexpr int DURABILITY = 7;
  static constexpr int LOG_ID = 8;
  static constexpr int LSN = 9;
  static constexpr int CLIENT_ID = 10;
  static constexpr int CLIENT_ADDRESS = 11;
  static constexpr int EXTRA_INFO = 12;
};

typedef AdminCommandTable<logid_t,                  /* Log ID */
                          uint32_t,                 /* Shard */
                          lsn_t,                    /* Highest LSN */
                          uint64_t,                 /* Highest partition */
                          std::chrono::milliseconds /* Highest timestamp approx
                                                     */
                          >
    InfoStoredLogsTable;

typedef AdminCommandTable<logid_t,                  /* Log ID */
                          uint64_t,                 /* shard */
                          admin_command_table::LSN, /* last_released */
                          std::string,              /* last_released_src */
                          admin_command_table::LSN, /* trim_point */
                          epoch_t,     /* per_epoch_metadata_trim_point */
                          epoch_t,     /* seal */
                          std::string, /* sealed_by */
                          epoch_t,     /* soft_seal */
                          std::string, /* soft_sealed_by */
                          std::chrono::microseconds, /* last_recovery_time */
                          std::chrono::seconds,      /* log_removal_time */
                          epoch_t,                   /* lce */
                          epoch_t,                   /* latest_epoch */
                          std::string,               /* latest_epoch_offsets */
                          bool                       /* permanent_errors */
                          >
    InfoLogStorageStateTable;

typedef AdminCommandTable<node_index_t,              /* node_index */
                          bool,                      /* is_boycotted */
                          std::chrono::milliseconds, /* boycott_duration */
                          std::chrono::milliseconds  /* boycott_star_time */
                          >
    InfoBoycottTable;

typedef AdminCommandTable<uint32_t,                 /* shard */
                          logid_t,                  /* Log ID */
                          uint64_t,                 /* partition */
                          admin_command_table::LSN, /* first lsn */
                          admin_command_table::LSN, /* max lsn */
                          std::string,              /* flags */
                          uint64_t                  /* approximate_size_bytes */
                          >
    PrintLogsDBDirectoriesTable;

typedef AdminCommandTable<logid_t,     /* Log ID */
                          std::string, /* size string */
                          std::string  /* details */
                          >
    DataSizeTable;
typedef AdminCommandTable<std::string, /* lag range */
                          uint64_t,    /* throughput */
                          float        /* percent of backlog throughput */
                          >
    LagBucketTable;

typedef AdminCommandTable<logid_t,    /* Log ID */
                          Status,     /* Result status */
                          bool,       /* Empty */
                          uint32_t,   /* isLogEmpty latency */
                          std::string /* Details */
                          >
    IsLogEmptyTable;
typedef AdminCommandTable<logid_t,    /* Log ID */
                          Status,     /* Result status V1 */
                          bool,       /* Empty V1 */
                          uint32_t,   /* isLogEmpty latency */
                          Status,     /* Result status V2 */
                          bool,       /* Empty V2 */
                          uint32_t,   /* isLogEmptyV2 latency */
                          std::string /* Details */
                          >
    IsLogEmptyVersionComparisonTable;

}} // namespace facebook::logdevice
