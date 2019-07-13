/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#define __STDC_FORMAT_MACROS

#include "logdevice/server/admincommands/AdminCommandFactory.h"

#include "logdevice/common/CopySetManager.h"
#include "logdevice/common/MetaDataLogWriter.h"
#include "logdevice/common/commandline_util_chrono.h"
#include "logdevice/common/config.h"
#include "logdevice/server/admincommands/BlockCatchupQueue.h"
#include "logdevice/server/admincommands/BoycottReset.h"
#include "logdevice/server/admincommands/CloseSocket.h"
#include "logdevice/server/admincommands/Compact.h"
#include "logdevice/server/admincommands/CreateCheckpoint.h"
#include "logdevice/server/admincommands/CustomCounters.h"
#include "logdevice/server/admincommands/DeprecatedStats.h"
#include "logdevice/server/admincommands/DumpQueuedMessages.h"
#include "logdevice/server/admincommands/Failsafe.h"
#include "logdevice/server/admincommands/GossipBlacklist.h"
#include "logdevice/server/admincommands/Info.h"
#include "logdevice/server/admincommands/InfoAppendOutliers.h"
#include "logdevice/server/admincommands/InfoBoycotts.h"
#include "logdevice/server/admincommands/InfoCatchupQueues.h"
#include "logdevice/server/admincommands/InfoClientReadStreams.h"
#include "logdevice/server/admincommands/InfoConfig.h"
#include "logdevice/server/admincommands/InfoEventLog.h"
#include "logdevice/server/admincommands/InfoGossip.h"
#include "logdevice/server/admincommands/InfoGraylist.h"
#include "logdevice/server/admincommands/InfoIterators.h"
#include "logdevice/server/admincommands/InfoLogsConfigRsm.h"
#include "logdevice/server/admincommands/InfoLogsDBMetadata.h"
#include "logdevice/server/admincommands/InfoPartitions.h"
#include "logdevice/server/admincommands/InfoPurges.h"
#include "logdevice/server/admincommands/InfoReaders.h"
#include "logdevice/server/admincommands/InfoRebuildings.h"
#include "logdevice/server/admincommands/InfoRecord.h"
#include "logdevice/server/admincommands/InfoRecordCache.h"
#include "logdevice/server/admincommands/InfoRecoveries.h"
#include "logdevice/server/admincommands/InfoReplication.h"
#include "logdevice/server/admincommands/InfoSST.h"
#include "logdevice/server/admincommands/InfoSequencers.h"
#include "logdevice/server/admincommands/InfoSettings.h"
#include "logdevice/server/admincommands/InfoShardOperationalState.h"
#include "logdevice/server/admincommands/InfoShards.h"
#include "logdevice/server/admincommands/InfoSockets.h"
#include "logdevice/server/admincommands/InfoStorageTasks.h"
#include "logdevice/server/admincommands/InfoStoredLogs.h"
#include "logdevice/server/admincommands/InfoSyncSequencerRequests.h"
#include "logdevice/server/admincommands/InjectShardFault.h"
#include "logdevice/server/admincommands/ListOrEraseMetadata.h"
#include "logdevice/server/admincommands/LogStorageStateCommand.h"
#include "logdevice/server/admincommands/NewConnections.h"
#include "logdevice/server/admincommands/Partitions.h"
#include "logdevice/server/admincommands/PauseOrUnpauseFileEpochStore.h"
#include "logdevice/server/admincommands/PrintLogsDBDirectories.h"
#include "logdevice/server/admincommands/RSMWriteSnapshot.h"
#include "logdevice/server/admincommands/Rebuilding.h"
#include "logdevice/server/admincommands/Record.h"
#include "logdevice/server/admincommands/Setting.h"
#include "logdevice/server/admincommands/StartRecovery.h"
#include "logdevice/server/admincommands/Stats.h"
#include "logdevice/server/admincommands/StatsHistogram.h"
#include "logdevice/server/admincommands/StatsJemalloc.h"
#include "logdevice/server/admincommands/StatsRocks.h"
#include "logdevice/server/admincommands/StatsThroughput.h"
#include "logdevice/server/admincommands/Stop.h"
#include "logdevice/server/admincommands/StoreTimeouts.h"
#include "logdevice/server/admincommands/TrafficShaping.h"
#include "logdevice/server/admincommands/UpDown.h"
#include "logdevice/server/storage_tasks/ReadStorageTask.h"

namespace facebook { namespace logdevice {

TestAdminCommandFactory::TestAdminCommandFactory() : AdminCommandFactory() {
  using Restriction = AdminCommand::RestrictionLevel;
  selector_.add<commands::EraseRecord>(
      "record erase", Restriction::LOCALHOST_ONLY);
}

AdminCommandFactory::AdminCommandFactory() {
  // To support restriction levels, add line in command's class:
  // using AdminCommand::AdminCommand;
  // OR, if the class has constructor/s implemented, call the AdminCommand
  // constructor from there.
  using Restriction = AdminCommand::RestrictionLevel;

  selector_.add<commands::Stop>("stop");
  selector_.add<commands::Stop>("quit");
  selector_.add<commands::FastShutdown>("fast_shutdown");

  selector_.add<commands::Info>("info");
  selector_.add<commands::InfoAppendOutliers>("info append_outliers");
  selector_.add<commands::InfoGossip>("info gossip");
  selector_.add<commands::InfoBoycotts>("info boycotts");
  selector_.add<commands::InfoGraylist>("info graylist");
  selector_.add<commands::InfoSST>("info sst");
  selector_.add<commands::InfoSockets>("info sockets");
  selector_.add<commands::InfoConfig>("info config");
  selector_.add<commands::InfoSequencers>("info sequencers");
  selector_.add<commands::InfoReaders>("info readers");
  selector_.add<commands::InfoCatchupQueues>("info catchup_queues");
  selector_.add<commands::InfoClientReadStreams>("info client_read_streams");
  selector_.add<commands::InfoEventLog>("info event_log");
  selector_.add<commands::InfoLogsConfigRsm>("info logsconfig_rsm");
  selector_.add<commands::InfoRecoveries>("info recoveries");
  selector_.add<commands::InfoPurges>("info purges");
  selector_.add<commands::InfoRecord>("info record");
  selector_.add<commands::InfoPartitions>("info partitions");
  selector_.add<commands::InfoLogsDBMetadata>("info logsdb metadata");
  selector_.add<commands::ListOrEraseMetadata>("info metadata",
                                               /* erase */ false);
  selector_.add<commands::ListOrEraseMetadata>("delete metadata",
                                               /* erase */ true);
  selector_.add<commands::InfoIterators>("info iterators");
  selector_.add<commands::InfoShards>("info shards");
  selector_.add<commands::InfoSettings>("info settings");
  selector_.add<commands::InfoRecordCache>("info record_cache");
  selector_.add<commands::InfoStorageTasks>("info storage_tasks");
  selector_.add<commands::InfoStoredLogs>("info stored_logs");
  selector_.add<commands::InfoReplication>("info replication");
  selector_.add<commands::InfoShardOperationalState>("info shardopstate");

  // Admin command for querying the state of rebuilding.
  selector_.add<commands::InfoRebuildingsLegacy>("info rebuildings");
  selector_.add<commands::InfoRebuildingShards>("info rebuilding shards");
  selector_.add<commands::InfoRebuildingLogs>("info rebuilding logs");
  selector_.add<commands::InfoRebuildingChunks>("info rebuilding chunks");

  // Admin commands for rebuilding.
  selector_.add<commands::Rebuilding>(
      "rebuilding", Restriction::LOCALHOST_ONLY);

  // List all the currently running SyncSequencerRequest state machines.
  selector_.add<commands::InfoSyncSequencerRequests>(
      "info sync_sequencer_requests");

  // Deprecated commands
  deprecated("info sequencer", "info sequencers");
  deprecated("loglevel", "set loglevel");
  deprecated("stats histogram", "stats2 histogram");
  deprecated("stats latency", "stats2 histogram");
  deprecated("stats", "stats2");
  deprecated("m2m_rebuilding", "rebuilding");

  selector_.add<commands::Stats>("stats2");
  selector_.add<commands::StatsWorker>("stats worker");
  selector_.add<commands::StatsReset>("stats reset");
  selector_.add<commands::StatsRocks>("stats rocksdb");

  selector_.add<commands::StatsHistogram>("stats2 histogram");
  selector_.add<commands::TrafficShapingHistogram>("stats2 shaping");
  selector_.add<commands::StoreTimeoutHistogram>("stats2 store_timeouts");

  selector_.add<commands::StatsThroughput>("stats throughput");
  selector_.add<commands::StatsCustomCounters>("stats custom counters");

#ifdef LOGDEVICE_USING_JEMALLOC
  selector_.add<commands::StatsJemalloc>("stats jemalloc");
  selector_.add<commands::StatsJemallocFull>("stats jemalloc full");
  selector_.add<commands::StatsJemallocProfActive>(
      "stats jemalloc prof.active");
  selector_.add<commands::StatsJemallocProfDump>("stats jemalloc prof.dump");
  selector_.add<commands::JemallocGetSet>("jemalloc get", false);
  selector_.add<commands::JemallocGetSet>("jemalloc set", true);
#endif

  selector_.add<commands::Compact>("compact", Restriction::LOCALHOST_ONLY);
  selector_.add<commands::DumpQueuedMessages>(
      "dump_queued_messages", Restriction::LOCALHOST_ONLY);

  // Not restricting to localhost to be able to run from ldops and tests
  selector_.add<commands::InjectShardFault>("inject shard_fault");

  selector_.add<commands::BlockCatchupQueue>(
      "block catchup_queue", Restriction::LOCALHOST_ONLY);

  deprecated("logstoragestate", "info log_storage_state");
  selector_.add<commands::LogStorageStateCommand>("info log_storage_state");

  selector_.add<commands::TrafficShaping>(
      "traffic_shaping", Restriction::LOCALHOST_ONLY);
  selector_.add<commands::Up>("up", Restriction::LOCALHOST_ONLY);
  selector_.add<commands::Down>("down", Restriction::LOCALHOST_ONLY);
  selector_.add<commands::DeactivateSequencer>(
      "sequencer_stop", Restriction::LOCALHOST_ONLY);
  selector_.add<commands::CloseSocket>(
      "close_socket", Restriction::LOCALHOST_ONLY);
  selector_.add<commands::GossipBlacklist, bool>(
      "gossip blacklist", true, Restriction::LOCALHOST_ONLY);
  selector_.add<commands::GossipBlacklist, bool>(
      "gossip whitelist", false, Restriction::LOCALHOST_ONLY);
  selector_.add<commands::NewConnections, bool>(
      "newconnections accept", true, Restriction::LOCALHOST_ONLY);
  selector_.add<commands::NewConnections, bool>(
      "newconnections reject", false, Restriction::LOCALHOST_ONLY);
  selector_.add<commands::StartRecovery>(
      "startrecovery", Restriction::LOCALHOST_ONLY);

  selector_.add<commands::DropPartitions>(
      "logsdb drop", Restriction::LOCALHOST_ONLY);
  selector_.add<commands::FlushPartition>(
      "logsdb flush", Restriction::LOCALHOST_ONLY);
  selector_.add<commands::CompactPartition>(
      "logsdb compact", Restriction::LOCALHOST_ONLY);
  selector_.add<commands::CreatePartition>(
      "logsdb create", Restriction::LOCALHOST_ONLY);
  selector_.add<commands::PrependPartitions>(
      "logsdb prepend", Restriction::LOCALHOST_ONLY);
  selector_.add<commands::PrintLogsDBDirectories>("logsdb print_directory");
  selector_.add<commands::ApplyRetention>(
      "logsdb apply_retention_approximate", Restriction::LOCALHOST_ONLY);

  deprecated("logsdb set compact_partitions",
             "set rocksdb-partition-compactions-enabled");
  deprecated("compaction_ratelimit", "set rocksdb-compaction-ratelimit");

  selector_.add<commands::CreateCheckpoint>(
      "create_checkpoint", Restriction::LOCALHOST_ONLY);

  selector_.add<commands::Failsafe>("failsafe", Restriction::LOCALHOST_ONLY);

  selector_.add<commands::SettingSet>("set", Restriction::LOCALHOST_ONLY);
  selector_.add<commands::SettingUnset>("unset", Restriction::LOCALHOST_ONLY);

  selector_.add<commands::BoycottReset>("boycott_reset");

  // Force a Replicated State Machine to write a snapshot
  // Can be used for emergency situations when there is data loss
  // in the delta log.
  selector_.add<commands::RSMWriteSnapShot>("rsm write-snapshot");

  selector_.add<commands::PauseOrUnpauseFileEpochStore>(
      "pause_file_epoch_store", true);
  selector_.add<commands::PauseOrUnpauseFileEpochStore>(
      "unpause_file_epoch_store", false);
}

void AdminCommandFactory::deprecated(const char* old_prefix,
                                     const char* new_prefix) {
  selector_.add<commands::DeprecatedStats, std::string, std::string>(
      old_prefix, old_prefix, new_prefix);
}

std::unique_ptr<AdminCommand>
AdminCommandFactory::get(std::vector<std::string>& inout_args,
                         struct evbuffer* output) {
  return selector_.selectCommand(inout_args, output);
}

}} // namespace facebook::logdevice
