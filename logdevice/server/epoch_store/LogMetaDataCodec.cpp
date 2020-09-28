/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/server/epoch_store/LogMetaDataCodec.h"

#include "logdevice/common/PayloadHolder.h"
#include "logdevice/common/ThriftCommonStructConverter.h"

namespace facebook { namespace logdevice {

epoch_store::thrift::LogMetaData
LogMetaDataThriftConverter::toThrift(const LogMetaData& log_metadata) {
  epoch_store::thrift::LogMetaData thrift_log_metadata;

  // Deserialize EpochMetaData
  epochMetaDataToThrift(
      log_metadata.current_epoch_metadata, thrift_log_metadata);
  metaPropertiesToThrift(
      log_metadata.epoch_store_properties, thrift_log_metadata);

  // Data LCE
  thrift_log_metadata.set_data_last_clean_epoch(
      log_metadata.data_last_clean_epoch.val());
  if (log_metadata.data_tail_record.isValid()) {
    thrift_log_metadata.set_data_tail_record(
        tailRecordToThrift(log_metadata.data_tail_record));
  }

  // Metadata LCE
  thrift_log_metadata.set_metadata_last_clean_epoch(
      log_metadata.metadata_last_clean_epoch.val());

  if (log_metadata.metadata_tail_record.isValid()) {
    thrift_log_metadata.set_metadata_tail_record(
        tailRecordToThrift(log_metadata.metadata_tail_record));
  }

  // Metadata
  thrift_log_metadata.set_version(log_metadata.version.val());
  thrift_log_metadata.set_last_modified_at_ms(
      log_metadata.last_changed_timestamp.toMilliseconds().count());

  return thrift_log_metadata;
}

std::shared_ptr<LogMetaData> LogMetaDataThriftConverter::fromThrift(
    const epoch_store::thrift::LogMetaData& thrift_log_metadata) {
  auto log_metadata = std::make_shared<LogMetaData>();

  // Deserialize EpochMetaData
  log_metadata->current_epoch_metadata =
      epochMetaDataFromThrift(thrift_log_metadata);
  log_metadata->epoch_store_properties =
      metaPropertiesFromThrift(thrift_log_metadata);

  // Data LCE
  log_metadata->data_last_clean_epoch =
      epoch_t(thrift_log_metadata.get_data_last_clean_epoch());
  log_metadata->data_tail_record =
      tailRecordFromThrift(thrift_log_metadata.get_data_tail_record());

  // Metadata LCE
  log_metadata->metadata_last_clean_epoch =
      epoch_t(thrift_log_metadata.get_metadata_last_clean_epoch());
  log_metadata->metadata_tail_record =
      tailRecordFromThrift(thrift_log_metadata.get_metadata_tail_record());

  // Metadata
  log_metadata->version =
      LogMetaData::Version(thrift_log_metadata.get_version());
  log_metadata->last_changed_timestamp = SystemTimestamp(
      std::chrono::milliseconds(thrift_log_metadata.get_last_modified_at_ms()));

  return log_metadata;
}

StorageSet LogMetaDataThriftConverter::storageSetFromThrift(
    const epoch_store::thrift::StorageSet& thrift_set) {
  StorageSet set;
  for (const auto& shard : thrift_set) {
    set.emplace_back(shard.get_node_idx(), shard.get_shard_idx());
  }
  return set;
}

epoch_store::thrift::StorageSet
LogMetaDataThriftConverter::storageSetToThrift(const StorageSet& set) {
  epoch_store::thrift::StorageSet thrift_set;
  for (const auto& shard : set) {
    epoch_store::thrift::ShardID thrift_shard;
    thrift_shard.set_node_idx(shard.node());
    thrift_shard.set_shard_idx(shard.shard());
    thrift_set.emplace_back(std::move(thrift_shard));
  }
  return thrift_set;
}

EpochMetaData::NodeSetParams
LogMetaDataThriftConverter::nodesetParamsFromThrift(
    const epoch_store::thrift::NodeSetParams& thrift_nodeset_params) {
  EpochMetaData::NodeSetParams nodeset_params;

  nodeset_params.signature = thrift_nodeset_params.get_signature();
  nodeset_params.target_nodeset_size =
      thrift_nodeset_params.get_target_nodeset_size();
  nodeset_params.seed = thrift_nodeset_params.get_seed();

  return nodeset_params;
}

epoch_store::thrift::NodeSetParams
LogMetaDataThriftConverter::nodesetParamsToThrift(
    const EpochMetaData::NodeSetParams& nodeset_params) {
  epoch_store::thrift::NodeSetParams thrift_nodeset_params;

  thrift_nodeset_params.set_signature(nodeset_params.signature);
  thrift_nodeset_params.set_target_nodeset_size(
      nodeset_params.target_nodeset_size);
  thrift_nodeset_params.set_seed(nodeset_params.seed);

  return thrift_nodeset_params;
}

EpochMetaData LogMetaDataThriftConverter::epochMetaDataFromThrift(
    const epoch_store::thrift::LogMetaData& thrift_log_metadata) {
  const auto& current_metadata =
      thrift_log_metadata.get_current_replication_config();

  auto epoch_metadata = EpochMetaData(
      storageSetFromThrift(current_metadata.get_storage_set()),
      toLogDevice<ReplicationProperty>(current_metadata.get_replication()),
      epoch_t(thrift_log_metadata.get_next_epoch()),
      epoch_t(current_metadata.get_effective_since()),
      current_metadata.weights_ref().has_value()
          ? current_metadata.weights_ref().value()
          : std::vector<double>{});

  epoch_metadata.replication_conf_changed_at = RecordTimestamp::from(
      std::chrono::milliseconds(current_metadata.get_creation_timestamp()));

  if (thrift_log_metadata.nodeset_params_ref().has_value()) {
    epoch_metadata.nodeset_params = nodesetParamsFromThrift(
        thrift_log_metadata.nodeset_params_ref().value());
  }

  epoch_metadata.epoch_incremented_at =
      RecordTimestamp::from(std::chrono::milliseconds(
          thrift_log_metadata.get_epoch_incremented_at_ms()));

  // Set flags
  if (thrift_log_metadata.get_disabled()) {
    epoch_metadata.h.flags |= MetaDataLogRecordHeader::DISABLED;
  }
  if (current_metadata.get_written_in_metadatalog()) {
    epoch_metadata.h.flags |= MetaDataLogRecordHeader::WRITTEN_IN_METADATALOG;
  }

  return epoch_metadata;
}

void LogMetaDataThriftConverter::epochMetaDataToThrift(
    const EpochMetaData& epoch_metadata,
    epoch_store::thrift::LogMetaData& thrift_log_metadata) {
  thrift_log_metadata.set_next_epoch(epoch_metadata.h.epoch.val());
  thrift_log_metadata.set_epoch_incremented_at_ms(
      epoch_metadata.epoch_incremented_at.toMilliseconds().count());
  thrift_log_metadata.current_replication_config_ref()->set_effective_since(
      epoch_metadata.h.effective_since.val());
  thrift_log_metadata.current_replication_config_ref()->set_creation_timestamp(
      epoch_metadata.replication_conf_changed_at.toMilliseconds().count());
  thrift_log_metadata.current_replication_config_ref()->set_storage_set(
      storageSetToThrift(epoch_metadata.shards));
  thrift_log_metadata.current_replication_config_ref()->set_replication(
      ::facebook::logdevice::toThrift<thrift::ReplicationProperty>(
          epoch_metadata.replication));
  thrift_log_metadata.current_replication_config_ref()->set_weights(
      epoch_metadata.weights);
  thrift_log_metadata.current_replication_config_ref()
      ->set_written_in_metadatalog(
          epoch_metadata.h.flags &
          MetaDataLogRecordHeader::WRITTEN_IN_METADATALOG);
  thrift_log_metadata.set_nodeset_params(
      nodesetParamsToThrift(epoch_metadata.nodeset_params));

  // Set flags
  thrift_log_metadata.set_disabled(epoch_metadata.h.flags &
                                   MetaDataLogRecordHeader::DISABLED);
}

EpochStoreMetaProperties LogMetaDataThriftConverter::metaPropertiesFromThrift(
    const epoch_store::thrift::LogMetaData& thrift_log_metadata) {
  EpochStoreMetaProperties prop;
  if (thrift_log_metadata.epoch_incremented_by_ref().has_value()) {
    prop.last_writer_node_id =
        NodeID(thrift_log_metadata.epoch_incremented_by_ref().value(),
               thrift_log_metadata.get_epoch_incremented_by_generation());
  }
  return prop;
}

void LogMetaDataThriftConverter::metaPropertiesToThrift(
    const EpochStoreMetaProperties& meta,
    epoch_store::thrift::LogMetaData& thrift_log_metadata) {
  if (meta.last_writer_node_id.has_value()) {
    thrift_log_metadata.set_epoch_incremented_by(
        meta.last_writer_node_id->index());
    thrift_log_metadata.set_epoch_incremented_by_generation(
        meta.last_writer_node_id->generation());
  }
}

TailRecord LogMetaDataThriftConverter::tailRecordFromThrift(
    const epoch_store::thrift::TailRecord& thrift_tail_record) {
  TailRecordHeader hdr{};
  hdr.log_id = logid_t(thrift_tail_record.get_logid());
  hdr.lsn = lsn_t(thrift_tail_record.get_lsn());
  hdr.timestamp = thrift_tail_record.get_timestamp();
  hdr.flags = thrift_tail_record.get_flags_value();
  hdr.u_DEPRECATED.byte_offset_DEPRECATED = BYTE_OFFSET_INVALID;

  OffsetMap offset_map;
  for (const auto& [k, v] : thrift_tail_record.get_offset_map()) {
    offset_map.setCounter(k, v);
  }

  // The fact that TailRecord has a payload is being deprecated and not
  // currently used. LogMetaData doesn't serialize the TailRecord's payload
  // and uses a dummy payload when the HAS_PAYLOAD flag is set.
  return TailRecord(std::move(hdr), std::move(offset_map), PayloadHolder());
}

epoch_store::thrift::TailRecord
LogMetaDataThriftConverter::tailRecordToThrift(const TailRecord& tail_record) {
  epoch_store::thrift::TailRecord thrift_record;
  thrift_record.set_logid(tail_record.header.log_id.val());
  thrift_record.set_lsn(tail_record.header.lsn);
  thrift_record.set_timestamp(tail_record.header.timestamp);
  thrift_record.set_flags_value(tail_record.header.flags);
  thrift_record.set_offset_map(tail_record.offsets_map_.getCounterMap());

  return thrift_record;
}

}} // namespace facebook::logdevice
