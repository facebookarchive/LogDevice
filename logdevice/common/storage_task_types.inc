/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
/* can be included multiple times */

#ifndef STORAGE_TASK_TYPE
#error STORAGE_TASK_TYPE() macro not defined
#define STORAGE_TASK_TYPE(...)
#endif

// Args: enum_name, string_name, publish_stats
// Tasks that have publish_stats == false will be aggregated together into one
// bucket when publishing

STORAGE_TASK_TYPE(COMPACT_PARTITION, "CompactPartitionStorageTask", false)
STORAGE_TASK_TYPE(DELETE, "DeleteStorageTask", false)
STORAGE_TASK_TYPE(DELETE_LOG_METADATA, "DeleteLogMetadataStorageTask", false)
STORAGE_TASK_TYPE(DUMP_RELEASE_STATE, "DumpReleaseStateStorageTask", false)
STORAGE_TASK_TYPE(EPOCH_OFFSET, "EpochOffsetStorageTask", false)
STORAGE_TASK_TYPE(FINDKEY, "FindKeyStorageTask", true)
STORAGE_TASK_TYPE(GET_EPOCH_RECOVERY_METADATA, "GetEpochRecoveryMetadataStorageTask", false)
STORAGE_TASK_TYPE(GET_HEAD_ATTRIBUTES, "GetHeadAttributesStorageTask", false)
STORAGE_TASK_TYPE(INFO_RECORD, "InfoRecordStorageTask", false)
STORAGE_TASK_TYPE(MERGE_PER_EPOCH_METADATA, "MergeMutablePerEpochLogMetadataStorageTask", false)
STORAGE_TASK_TYPE(PURGE_DELETE_RECORDS, "PurgeDeleteRecordsStorageTask", true)
STORAGE_TASK_TYPE(PURGE_READ_LAST_CLEAN, "PurgeReadLastCleanStorageTask", false)
STORAGE_TASK_TYPE(PURGE_WRITE_EPOCH_RECOVERY_METADATA, "PurgeWriteEpochRecoveryMetadataStorageTask", false)
STORAGE_TASK_TYPE(PURGE_WRITE_LAST_CLEAN, "PurgeWriteLastCleanStorageTask", false)
STORAGE_TASK_TYPE(READ, "ReadStorageTask", true)
STORAGE_TASK_TYPE(READ_LNG, "ReadLngStorageTask", false)
STORAGE_TASK_TYPE(READ_LOG_REBUILDING_CHECKPOINT, "ReadLogRebuildingCheckpointStorageTask", false)
STORAGE_TASK_TYPE(REBUILDING_AMEND_SELF, "AmendSelfStorageTask", false)
STORAGE_TASK_TYPE(REBUILDING_ENUMERATE_LOGS, "RebuildingEnumerateMetadataLogsStorageTask", false)
STORAGE_TASK_TYPE(REBUILDING_READ, "RebuildingReadStorageTask", true)
STORAGE_TASK_TYPE(REBUILDING_WRITE_COMPLETE_METADATA, "WriteShardRebuildingCompleteMetadataStorageTask", false)
STORAGE_TASK_TYPE(RECORD_CACHE_REPOPULATION, "RecordCacheRepopulationStorageTask", false)
STORAGE_TASK_TYPE(RECOVER_LOG_STATE, "RecoverLogStateStorageTask", false)
STORAGE_TASK_TYPE(RECOVER_SEAL, "RecoverSealStorageTask", false)
STORAGE_TASK_TYPE(SEAL, "SealStorageTask", false)
STORAGE_TASK_TYPE(SOFT_SEAL, "SoftSealStorageTask", false)
STORAGE_TASK_TYPE(STOP_EXEC, "StopExecStorageTask", false)
STORAGE_TASK_TYPE(STORE, "StoreStorageTask", false)
STORAGE_TASK_TYPE(UPDATE_PER_EPOCH_METADATA, "UpdatePerEpochLogMetadataStorageTask", false)
STORAGE_TASK_TYPE(UPDATE_TRIM_POINTS, "UpdateTrimPointsStorageTask", true)
STORAGE_TASK_TYPE(WRITE_BATCH, "WriteBatchStorageTask", true)
STORAGE_TASK_TYPE(WRITE_LOG_REBUILDING_CHECKPOINT, "WriteLogRebuildingCheckpointStorageTask", false)
STORAGE_TASK_TYPE(WRITE_TRIM_METADATA, "WriteTrimMetadataStorageTask", false)
STORAGE_TASK_TYPE(UPDATE_PARTITION_TIMESTAMP, "Partition::TimestampUpdateTask", false)

#undef STORAGE_TASK_TYPE
