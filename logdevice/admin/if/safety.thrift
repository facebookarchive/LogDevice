/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

include "logdevice/admin/if/common.thrift"
include "logdevice/admin/if/nodes.thrift"

namespace cpp2 facebook.logdevice.thrift
namespace py3 logdevice.admin
namespace php LogDevice
namespace wiki Thriftdoc.LogDevice.Safety


enum OperationImpact {
  INVALID = 0,
  /**
   * This means that rebuilding will not be able to complete given the current
   * status of ShardDataHealth. This assumes that rebuilding may need to run at
   * certain point. This can happen if we have epochs that have lost so many
   * _writable_ shards in its storage-set that it became impossible to amend
   * copysets according to the replication property.
   */
  REBUILDING_STALL = 1,
  /**
   * This means that if we perform the operation, we will not be able to
   * generate a nodeset that satisfy the current replication policy for all logs
   * in the log tree.
   */
  WRITE_AVAILABILITY_LOSS = 2,
  /**
   * This means that this operation _might_ lead to stalling readers in cases
   * were they need to establish f-majority on some records.
   */
  READ_AVAILABILITY_LOSS = 3,
}

/**
 * A data structure representing human readable information about a
 * specific shard
 */
struct ShardMetadata {
  1: nodes.ShardDataHealth data_health,
  2: bool is_alive,
  3: nodes.ShardStorageState storage_state,
  4: optional string location,
  5: common.Location location_per_scope,
}

typedef list<ShardMetadata> StorageSetMetadata

/**
 * A data structure that describe the operation impact on a specific epoch in a
 * log.
 */
struct ImpactOnEpoch {
  /**
   * if log_id == 0 this is the metadata log.
   */
  1: common.unsigned64 log_id,
  /**
   * if log_id == 0, epoch will be zero as well.
   */
  2: common.unsigned64 epoch,
  /**
   * What is the storage set for this epoch (aka. NodeSet)
   */
  3: common.StorageSet storage_set,
  /**
   * What is the replication policy for this particular epoch
   */
  4: common.ReplicationProperty replication,
  5: list<OperationImpact> impact,
  /**
   * Extra information about the storage set to help visualize the impact
   * to the user.
   */
  6: optional StorageSetMetadata storage_set_metadata,
}

struct CheckImpactRequest {
  /*
   * Which shards/nodes we would like to check state change against. Using the
   * ShardID data structure you can refer to individual shards or entire storage
   * or sequencer nodes based on their address or index. This can be empty if
   * you would like to check what is the current state of the cluster.
   */
  1: common.ShardSet shards,
  /*
   * This can be unset ONLY if disable_sequencers is set to true or if the list
   * of shards is empty.
   */
  2: optional nodes.ShardStorageState target_storage_state,
  /*
   * Do we want to validate if sequencers will be disabled on these nodes as
   * well?
   */
  3: optional bool disable_sequencers,
  /*
   * The set of shards that you would like to update their state
   * How much of the location-scope do we want to keep as a safety margin. This
   * assumes that X number of LocationScope can be impacted along with the list
   * of shards/nodes that you supply in the request.
   */
  4: optional common.ReplicationProperty safety_margin,
  /*
   * Choose which log-ids to check for safety. Remember that we will always
   * check the metadata and internal logs. This is to ensure that no matter
   * what, operations are safe to these critical logs. If this is unset, this
   * will check all logs in the cluster. If it's set to an empty list, only
   * internal and metadata logs will be checked (given that check_metadata_logs
   * and/or check_internal_logs are set to true)
   */
  5: optional list<common.unsigned64> log_ids_to_check,
  /*
   * Defaulted to true. In case we found a reason why we think the operation is
   * unsafe, we will not check the rest of the logs.
   */
  6: optional bool abort_on_negative_impact = true,
  /*
   * In case the operation is unsafe, how many example ImpactOnEpoch records you
   * want in return?
   */
  7: optional i32 return_sample_size = 50,
  8: optional bool check_metadata_logs = true,
  9: optional bool check_internal_logs = true,
}

struct CheckImpactResponse {
  /*
   * empty means that no impact, operation is SAFE.
   */
  1: list<OperationImpact> impact,
  /*
   * Only set if there is impact. This indicates whether there will be effect on
   * the metadata logs or the internal state machine logs.
   */
  2: optional bool internal_logs_affected,
  /*
   * A sample of the affected epochs by this operation.
   */
  3: optional list<ImpactOnEpoch> logs_affected,
  /**
   * The total time spent on this check in seconds.
   */
  4: optional i32 total_duration,
  /**
   * The total number of logs checked by the server.
   */
  5: optional i32 total_logs_checked,
}
