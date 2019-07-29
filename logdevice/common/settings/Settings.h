/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>
#include <string>

#include <folly/Optional.h>

#include "logdevice/common/AuthoritativeStatus.h"
#include "logdevice/common/SCDCopysetReordering.h"
#include "logdevice/common/Sockaddr.h"
#include "logdevice/common/StorageTask-enums.h"
#include "logdevice/common/configuration/NodeLocation.h"
#include "logdevice/common/protocol/MessageType.h"
#include "logdevice/common/settings/ClientReadStreamFailureDetectorSettings.h"
#include "logdevice/common/settings/Durability.h"
#include "logdevice/common/settings/SequencerBoycottingSettings.h"
#include "logdevice/common/settings/UpdateableSettings.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/Err.h"
#include "logdevice/include/debug.h"

namespace boost { namespace program_options {
class options_description;
}} // namespace boost::program_options

/**
 * @file settings global to a logdevice::Client, or to a server. Assigned
 *       values are defaults.
 */

// Shouldn't be necessary since c++14...
static_assert(sizeof(facebook::logdevice::MessageType) == sizeof(char),
              "MessageType size changed");
namespace std {
template <>
struct hash<facebook::logdevice::MessageType> {
  using MessageType = facebook::logdevice::MessageType;
  size_t operator()(const MessageType& type) const noexcept {
    return std::hash<char>()(static_cast<char>(type));
  }
};
} // namespace std

namespace facebook { namespace logdevice {

dbg::Level parse_log_level(const std::string& val);

struct StorageTaskShare {
  uint64_t share = 1;
};

struct Settings : public SettingsBundle {
  const char* getName() const override {
    return "ProcessorSettings";
  }

  void defineSettings(SettingEasyInit& init) override;

  SCDCopysetReordering scd = SCDCopysetReordering::NONE;

  // if true, the Processor with this Settings object is running in a
  // LogDevice server. If false, it's in a LogDeviceClient.
  // This isn't set by any parsed setting, but is only set directly by servers
  bool server = false;

  // if true, the Processor is only used to perform bootstrapping and will be
  // destroyed after bootstrapping is completed.
  // This isn't set by any parsed setting, but is only set directly internally
  bool bootstrapping = false;

  // number of worker threads to run
  int num_workers;

  // Time interval after which watchdog wakes up and detects stalls
  std::chrono::milliseconds watchdog_poll_interval_ms;

  // Limit on the number of backtraces printed when watchdog detects workers
  // are stalled
  rate_limit_t watchdog_bt_ratelimit;

  // If true, turn off TraceLogger by using NoopTraceLogger implementation
  bool trace_logger_disabled;

  // If false: no checksumming is done at the Protocol Layer
  // If true: 'checksumming_blacklisted_messages' is consulted
  bool checksumming_enabled;

  // Set of messages that are blacklisted for checksumming at
  // the Protocol Layer. We may need this for messages with big payload,
  // see .cpp for examples
  std::set<char> checksumming_blacklisted_messages;

  // maximum number of megabytes we can have pending in all output
  // evbuffers of all Sockets on each EventLoop managed by this
  // Processor. The total number of pending output bytes per Processor
  // is this times the number of threads the Processor runs.
  size_t outbufs_mb_max_per_thread;

  // Set SO_SNDBUF of all new TCP sockets to this number of KILOBYTES.  Note
  // that this makes Linux bypass its autotuning logic for the buffer size.
  // Importantly, it will never increase the buffer size for high-throughput
  // connections.
  //
  // If -1, system default will be used instead (setsockopt not called).
  int tcp_sendbuf_kb;

  // Set SO_RCVBUF of all new TCP sockets to this number of KILOBYTES.  Note
  // that this makes Linux bypass its autotuning logic for the buffer size.
  // Importantly, it will never increase the buffer size for high-throughput
  // connections.
  //
  // If -1, system default will be used instead (setsockopt not called).
  int tcp_rcvbuf_kb;

  // if true, enable Nagle's algorithm on all new TCP sockets. This should
  // increase the average packet size, but will increase latencies for many
  // workloads. If false (default), disable Nagle by calling
  // setsockopt(TCP_NODELAY).
  bool nagle;

  // Set outbuf_overflow_ limit of new Socket objects to this number of
  // KILOBYTES. See Socket.h.
  size_t outbuf_overflow_kb;

  // If socket is running out of buffers and it is not draining for a while
  // there is less value in maintaining it's socket buffers. This setting
  // decides how long will we allow the socket to drain before we close it.
  std::chrono::milliseconds max_time_to_allow_socket_drain;

  // How many kilobytes of RECORD messages the delivery code tries to push
  // to the client at once.  If -1, use the TCP sendbuf size.
  int output_max_records_kb;

  // How many bytes of records to read in a single StorageTask.
  // Similar to output_max_records_kb but is applied *before* filtering records.
  int64_t max_record_bytes_read_at_once;

  // Maximum execution time for reading records
  std::chrono::milliseconds max_record_read_execution_time;

  // @deprecated
  unsigned requests_from_pipe;

  // Process up to this many requests per iteration of EventLoop.
  // Increasing this improves write coalescing. Decreasing reduces the
  // probability of hitting output buffer limits too early. Capped by
  // RequestPump::MAX.
  uint32_t hi_requests_per_iteration;
  uint32_t mid_requests_per_iteration;
  uint32_t lo_requests_per_iteration;

  // Size worker request pipe to hold this many requests.
  //
  // NOTE: This currently translates to a fcntl(F_SETPIPE_SZ) call which is
  // subject to the /proc/sys/fs/pipe-max-size system limit when running as an
  // unprivileged user.  The default setting requires a 1 MB pipe, the default
  // limit on Linux.
  size_t worker_request_pipe_capacity;

  // Indicates whether prioritized queues are used with the CPU Threadpool.
  // Request and message processing priorities are honored when this is set to
  // true. Otherwise, all requests and messages are considered same priority.
  bool enable_executor_priority_queues;

  // Request Execution time(in milli-seconds) after which it is considered slow
  // and Worker stats 'worker_slow_requests' is bumped
  std::chrono::milliseconds request_execution_delay_threshold;

  // Background task execution time (in milli-seconds) after which it is
  // considered slow and we log it
  std::chrono::milliseconds slow_background_task_threshold;

  // The maximum number of incoming messages to read from an input evbuffer
  // of a Socket bufferevent before returning control to libevent.
  unsigned incoming_messages_max_per_socket;

  // The maximum number of unprocessed messages in the system.
  size_t incoming_messages_max_bytes_limit;

  // if payload of a message read from an evbuffer does not exceed this size,
  // we will malloc one buffer for both the message object and the payload
  // immediately following it. Otherwise, we will zero-copy the payload into an
  // evbuffer. This optimizes evbuffer allocation and initialization.
  size_t max_payload_inline;

  // Maximum number of StorageTask instances that one worker thread may have
  // in flight to each database shard (waiting on the storage thread pool to
  // complete them).  This is used to size pipes and queues.
  size_t max_inflight_storage_tasks;

  // Maximum number of StorageTask instances to buffer in each Worker for each
  // database shard while they wait to be handed off to the storage thread
  // pool.
  size_t per_worker_storage_task_queue_size;

  // Setting to control read I/O bandwidth throttling.
  bool enable_read_throttling;

  // A way to turn off putting nodes in graylist, to be able to revert
  // to normal copyset selection behavior.
  bool disable_graylisting;

  // Disable the worker-local outlier based graylisting
  bool disable_outlier_based_graylisting;

  // The duration through which a node need to be consistently an outlier to get
  // graylisted
  std::chrono::seconds graylisting_grace_period;

  // The duration through which a recently ungraylisted node will be monitored
  // and graylisted as soon as it becomes an outlier
  std::chrono::seconds graylisting_monitored_period;

  // The interval at which the graylists are refreshed
  std::chrono::seconds graylisting_refresh_interval;

  // Enable adaptive store timeouts. Which will use per worker histograms to
  // estimate first wave timeout.
  bool enable_adaptive_store_timeout;

  // (client-only setting) When the client loses the connection to a server,
  // it will attempt to reconnect repeatedly, with the delay increasing
  // exponentially up to this max.
  // TODO (t13314297): this setting is not modified anywhere
  std::chrono::seconds max_client_reconnect_delay{30};

  // enable caching of unclean records on the storage node. Used to avoid
  // local log store access during log recovery operations.
  bool enable_record_cache;

  // maximum size enforced for the record cache, 0 for unlimited. If positive
  // and record cache size grows more than that, it will start evicting records
  // from the cache. Also maximum total number of bytes allowed to be persisted
  // in record cache snapshots
  size_t record_cache_max_size;

  // polling interval for the record cache eviction thread for monitoring the
  // size of the record cache
  std::chrono::seconds record_cache_monitor_interval;

  // When an ld_check() fails, call abort().  If not, just continue
  // executing.  We'll log either way.
  bool abort_on_failed_check;

  // When an ld_catch() fails, call abort().  If not, just continue
  // executing.  We'll log either way.
  bool abort_on_failed_catch;

  // If true, and watchdog detects a stall, logdeviced will be aborted
  // to generate a stack trace for debugging.
  bool watchdog_abort_on_stall;

  // If true, and watchdog detects a stall, a backtrace of the
  // stalled thread(s) will be logged into the log file.
  bool watchdog_print_bt_on_stall;

  // If true, the NodeSetFinder within PurgeUncleanEpochs will use
  // only the metadata log as source for fetching historical metadata.
  // TODO: T28014582
  bool purging_use_metadata_log_only;

  // maximum concurrent purging state machines for RELEASE messages
  // per storage shard for each worker
  size_t max_concurrent_purging_for_release_per_shard;

  // An option to control if a gossip message should be sent to the
  // destination's gossip port or data port.
  // This is set to false by default for upgrades(from release where nodes
  // send to data port to release where nodes send to gossip port) to work fine.
  // In the first phase of upgrade, there is nothing special that oncall needs
  // to be aware of.
  // However, in the second phase, we need to set this option to true in the
  // commandline, and then do a rolling-restart, so that we can send to the
  // gossip port of the receiving server(which now listens on gossip port).
  bool send_to_gossip_port;

  // Enabling this option will caused the gossip port to accept only SSL
  // connections. This is currently set to false by default. The reason for
  // adding this option is to allow smooth upgrades while transitioning from
  // plaintext gossip port to SSL enabled gossip port. While transitioning:
  // 1. We will keep ssl_on_gossip_port = false, and send_to_gossip_port = false
  // 2. Then once all gossip messages are being routed through the data port,
  //    we will set ssl_on_gossip_port = true
  // 3. And finally set send_to_gossip_port = true
  bool ssl_on_gossip_port;

  // (sequencer-only setting) Limit on the number of nodes in the cluster. Used
  // to size some of the data structures Sequencer uses.
  size_t max_nodes;

  // (sequencer-only setting) threshold fraction of full nodes which triggers
  // space-based retention, if enabled. 0 means disabled
  double space_based_retention_node_threshold = 0;

  // Fraction of nodeset size upto which slow storage nodes will be treated as
  // unavailable. After crossing the threshold, the gray list can be cleared
  // to make sure that copysets can still be picked
  double gray_list_nodes_threshold;

  // Timeout after which a sequencer sends another wave of STORE messages if
  // it did not get R replies to the previous wave and >=R connections to which
  // it sent that wave are still up. TODO: revisit this once we start working
  // on fault tolerance. It may be preferrable to rely on connection status
  // rather than timeouts when initiating STORE retries.
  chrono_expbackoff_t<std::chrono::milliseconds> store_timeout;

  // Timeout after it which two nodes retry to connect when they loose a
  // a connection. Backoff for throttling socket re-connection attempts.
  chrono_expbackoff_t<std::chrono::milliseconds> connect_throttle;

  // If set, sequencer will never attempt to send STORE messages through a
  // chain.
  bool disable_chain_sending;

  // Time interval that a node health check probe is sent if there is
  // an outstanding probe from the same node in nodeset
  std::chrono::seconds node_health_check_retry_interval;

  // Min time interval for which a storage node will not be considered
  // a candidate for picking copyset, as it was slow in replying
  // to a STORE message. Internally increases exponentially up to 10x
  // of this value, decreases additively
  std::chrono::seconds slow_node_retry_interval;

  // Time interval after which a CheckNodeHealthRequest is considered as
  // timedout without a respone and deleted from worker's map
  std::chrono::seconds check_node_health_request_timeout;

  // Time interval that Appenders would retry sending STOREs to a storage node
  // after it was reported out of disk space and temporarily disabled in the
  // NodeSetState.
  std::chrono::seconds nospace_retry_interval;

  // Time interval until which space based trim check on the nodes of a nodeset
  // won't be retried
  std::chrono::seconds sbr_low_watermark_check_interval;

  // Time interval that Appenders would retry sending STOREs to a storage node
  // after it was reported unroutable and temporarily disabled in the
  // NodeSetState.
  std::chrono::seconds unroutable_retry_interval;

  // Time interval that Appenders would retry sending STOREs to a storage node
  // after it was reported overloaded (OVERLOADED bit in STORED message) and
  // temporarily disabled in the NodeSetState.
  std::chrono::seconds overloaded_retry_interval;

  // Time interval that Appenders would retry sending STOREs to a storage node
  // after it reported persistent error or rebuilding
  // (DISABLED status in STORED message) and
  // temporarily disabled in the NodeSetState.
  std::chrono::seconds disabled_retry_interval;

  // Minimum time interval between two consecutive refreshes of a NodeSetState
  std::chrono::milliseconds nodeset_state_refresh_interval;

  // Time to wait for the TCP connection to be established.
  // If set to 0, timeout is defined by OS configuration.
  std::chrono::milliseconds connect_timeout;

  // Number of allowed retries when a TCP connection times out.
  // This option has no effect if connect_timeout is set to 0.
  size_t connection_retries;

  // Multiplier for the connect_timeout that will be applied on each
  // retry.
  // NOTE: default values are set up to kick off quick retries in case a SYN
  // packet is lost (faster than the TCP stack) but also reach a large enough
  // timeout to allow geographically distant connection.
  double connect_timeout_retry_multiplier;

  // After a TCP connection is established, time to wait for the LD protocol
  // handshake to be completed before giving up. Unlimited if set to 0.
  std::chrono::milliseconds handshake_timeout;

  // Message read from socket and push into worker task queue for further
  // processing. Setting this true all messages would be processed
  // as soon as they are deserialized.
  bool inline_message_execution;

  // Maximum number of writes for a storage thread to perform in one batch.
  size_t write_batch_size;

  // Minimum number of bytes for a storage thread to write in one batch
  //   unless write_batch_size is reached first.
  size_t write_batch_bytes;

  // SLOW threadpool storage tasks go through the DRR scheduler.
  bool storage_tasks_use_drr;

  // Quanta for the DRR scheduler.
  uint64_t storage_tasks_drr_quanta = 1;

  // Shares for StorageTask principals.
  std::array<StorageTaskShare, (uint64_t)StorageTaskPrincipal::NUM_PRINCIPALS>
      storage_task_shares;

  // Maximum number of read streams clients can establish to the server, per
  // worker
  size_t max_server_read_streams;

  // Maximum number of _active_ cached digest streams on a storage node at the
  // same time
  size_t max_active_cached_digests;

  // How many kilobytes of RECORD messages can be enqueued for cached digest
  // for one client
  size_t max_cached_digest_record_queued_kb;

  // Consider worker's storage task queue overloaded if it was last dropped at
  // most this long ago.
  std::chrono::milliseconds queue_drop_overload_time;

  // Percentage of the PerWorkerStorageTaskQueue's buffer that can be used
  // before the queue is treated as being overloaded.
  int queue_size_overload_percentage;

  // How long to wait before retrying to send RELEASE messages to storage nodes.
  chrono_expbackoff_t<std::chrono::milliseconds> release_retry_interval;

  // How long to wait before broadcasting RELEASE messages to all storage nodes
  // for logs other than internal logs
  chrono_expbackoff_t<std::chrono::milliseconds> release_broadcast_interval;

  // How long to wait before broadcasting RELEASE messages to all storage nodes
  // for internal logs
  chrono_expbackoff_t<std::chrono::milliseconds>
      release_broadcast_interval_internal_logs;

  bool skip_recovery;

  // Maximum number of LogRecoveryRequests for data logs that can be running
  // at the same time. Noted that there can be an additional same amount of
  // metadata recoveries running.
  int concurrent_log_recoveries;

  // If true, purging will get the EpochRecoveryMetadata even if the epoch
  // is empty locally on the node
  bool get_erm_for_empty_epoch;

  // If true, a single E:EMPTY GET_EPOCH_RECOVERY_METADATA_REPLY response is
  // sufficient to consider an epoch as empty while purging.
  bool single_empty_erm;

  // the extra amount of time above the absolute minimum that the epoch
  // recovery procedure will wait for all nodes in the epoch nodeset to
  // participate in epoch recovery
  std::chrono::milliseconds recovery_grace_period;

  // Amount of time we wait before we report a read stream that is
  // considered stuck.
  std::chrono::milliseconds reader_stuck_threshold;

  // Amount of time we wait before we report a read stream that is
  // considered lagging.
  std::chrono::milliseconds reader_lagging_threshold;

  // If there were no rebuilding set changes in event log for this long,
  // consider the rebuilding set up to date.
  std::chrono::milliseconds event_log_grace_period;

  // if an epoch recovery procedure cannot complete in this amount of time
  // after a suitable recovery digest has been built for the epoch, the
  // procedure is restarted from scratch.
  std::chrono::seconds recovery_timeout;

  // Initial retry timeout used for checking if the latest metadata log record
  // is fully replicated during log recovery.
  chrono_expbackoff_t<std::chrono::milliseconds> recovery_seq_metadata_timeout;

  // if true, epoch recovery will insert bridge record for empty epochs of
  // data (i.e., non-metadata) logs
  bool bridge_record_in_empty_epoch;

  // Capacity of the AppenderBuffer internal queue across all workers for
  // a log, used for buffering pending Appender objects during sequencer
  // reactivation
  size_t appender_buffer_queue_cap;

  // In case AppenderBuffer is sufficiently large, we need to process Appenders
  // in batch and periodically return to libevent to prevent running out of
  // send buffers. This indicates the batch size of number of Appenders to be
  // processed before returning to the libevent loop
  size_t appender_buffer_process_batch;

  // Skip sending data to storage node from appender . So that it remains
  // in worker map to test abort or recreate appender leak scenario etc.
  bool test_appender_skip_stores;

  // The shutdown code in finishWorkAndCloseSockets tries to
  // shutdown everything and waits for everything running on the worker to
  // quiesce. At some point the work happening on the worker needs to stop so
  // that we are still within the shutdown timeout budget.
  // This value captures number of iterations that are countdown before
  // initiating force abort. This is counted twice first before starting force
  // abort of pending requests. Once pending requests go down to zero this is
  // counted down again before force closing all sockets. Currently this is
  // approximately 20seconds of wait before starting force abort.
  uint64_t time_delay_before_force_abort;

  // the extra amount of time that ClientReadStream will wait for a record to
  // be delivered (before reporting a gap) when it's suspected to be missing or
  // under-replicated (i.e. it's known that only at most f, but at least one,
  // nodes may have it)
  // If data_log_gap_grace_period is non-zero, it replaces gap_grace_period for
  // data logs. The same applies for metadata_log_gap_grace_period for metadata
  // logs.
  std::chrono::milliseconds gap_grace_period;
  std::chrono::milliseconds data_log_gap_grace_period;
  std::chrono::milliseconds metadata_log_gap_grace_period;

  // @see Settings.cpp
  std::chrono::milliseconds reader_stalled_grace_period;

  // interval between consecutive attempts to recover log state from local
  // log store and the sequencer
  std::chrono::microseconds log_state_recovery_interval;

  // how long to wait for a single node to respond to the GET_SEQ_STATE message
  std::chrono::milliseconds get_seq_state_reply_timeout;

  // Exponential Backoff Timer for GET_SEQ_STATE request.
  chrono_expbackoff_t<std::chrono::milliseconds> seq_state_backoff_time;

  // Minium timeout for a CheckSealRequest
  std::chrono::milliseconds check_seal_req_min_timeout;

  // interval for update_medatata_map_timer_; default 1 hr
  std::chrono::milliseconds update_metadata_map_interval{
      std::chrono::seconds(3600)};

  // (client-only setting) Minium timeout for a DeleteLogMetadataRequest
  std::chrono::milliseconds delete_log_metadata_request_timeout;

  // (client-only setting) Clients can query for the state of the cluster (which
  // nodes are alive) when an append() times out or to perform Single Copy
  // Delivery failover on readers. This option defines the maximum interval at
  // which the cluster state will be queried.
  std::chrono::milliseconds cluster_state_refresh_interval;

  // (client-only-setting) Enable isLogEmpty V2 for clients
  // TODO: (T46341259) Once V2 is hardened, deprecate this setting, and enable
  // V2 by default
  bool enable_is_log_empty_v2;

  // (client-only setting) If true, executes a GetClusterState at Processor
  // creation
  bool enable_initial_get_cluster_state;
  std::vector<node_index_t> test_get_cluster_state_recipients_;

  // (client-only setting) Determines how long before a request to fetch the
  // cluster state can be executed while an append() is pending.
  // TODO (t13314297): this setting is not modified anywhere
  std::chrono::milliseconds sequencer_router_internal_timeout{3000};

  // (client-only setting) Number of records to buffer in ClientReadStream.
  // A larger buffer may allow higher throughput but increases memory usage.
  // Can be overridden by specifying buffer_size in Client::createReader()
  //
  // NOTE: if changing the default also update docblock for
  // Client::createReader()
  size_t client_read_buffer_size;

  // (client-only setting) Threshold (relative to buffer size) at which
  // ClientReadStream broadcasts WINDOW messages to storage nodes.  Smaller
  // values mean more frequent broadcasting, possibly increasing throughput
  // but also wire chatter.
  double client_read_flow_control_threshold;

  // (client-only setting) maximum number of epoch metadata entries cached in
  // the client. Set it to 0 to disable epoch metadata caching
  size_t client_epoch_metadata_cache_size;

  // (client-only setting) Period for logging in logdevice_readers_flow scuba
  // table. Set it to 0 to disable feature.
  std::chrono::milliseconds client_readers_flow_tracer_period;

  // (client-only setting) Weight given to traces of unhealthy readers when
  // publishing samples (for improved debuggability).
  double client_readers_flow_tracer_unhealthy_publish_weight;

  // (client-only setting) Number of sample groups to maintain for the lagging
  // metric.
  size_t client_readers_flow_tracer_lagging_metric_num_sample_groups;

  // (client-only setting) Size of a sample group for the lagging metric.
  size_t client_readers_flow_tracer_lagging_metric_sample_group_size;

  // (client-only setting) Minimum admissible slope to consider a reader as
  // lagging.
  double client_readers_flow_tracer_lagging_slope_threshold;

  // (client-only setting) Force instantiation of StatsHolder within
  // ClientImpl even if stats publishing is disabled (via
  // `stats_collection_interval').  This allows counters to be queried and
  // tested even though they're not published.
  bool client_test_force_stats;

  // (client-only setting) After receiving responses from an f-majority of
  // nodes, wait up to this long for more nodes to chime in if there is not yet
  // consensus.
  std::chrono::milliseconds client_is_log_empty_grace_period;

  // (client-only setting) How big a checksum to include with newly appended
  // records.  Supported values are 0 (no checksum), 32 (CRC-32C) and 64
  // (SpookyHash V2).
  //
  // Use CRC-32C by default which offers decent protection, has
  // reasonable space overhead (4 bytes) and is fast with SSE4.2.
  int checksum_bits;

  // Initial timeout used during the mutation phase of recovery. If replicating
  // a record takes longer, Mutator will try to pick a few extra nodes to send
  // mutations to.
  std::chrono::milliseconds mutation_timeout;

  // DEPRECATED. Can be removed when all configs use --enable-sticky-copysets
  // and --write-copyset-index instead
  bool write_sticky_copysets_deprecated;

  // if true, StickyCopySetManager will be initialized
  bool enable_sticky_copysets;

  // if true, all records will have the copyset index written for them
  bool write_copyset_index;

  // CopySetSelector manager - sticky copyset block size
  size_t sticky_copysets_block_size;

  // CopySetSelector manager - sticky copyset block max time
  std::chrono::milliseconds sticky_copysets_block_max_time;

  // Specifies how long to keep unused cached iterators around before
  // invalidating them.
  std::chrono::milliseconds iterator_cache_ttl;

  // Maximum version of the protocol to use on this client/server.
  // Intented to be used for testing.
  uint16_t max_protocol;

  // Number of waves after which Appender will not try another wave if it times
  // out during the "Sync-leader" stage.
  // See @file in Appender for more details.
  int scd_sync_leaders_max_waves;

  // Total size in bytes of running Appenders accross all workers after which
  // we start taking measures to reduce the Appender residency time.
  size_t max_total_appenders_size_soft;

  // Total size in bytes of running Appenders accross all workers after which
  // we start rejecting new appends.
  size_t max_total_appenders_size_hard;

  // Maximum amount of memory that BufferedWriter in sequencers should use for
  // buffering writes. It will reject writes when this threshold is
  // exceeded.
  size_t max_total_buffered_append_size;

  // How long to keep executing appends for a log after an append with
  // NO_REDIRECT flag was received.
  std::chrono::milliseconds no_redirect_duration;

  // Limit on the number of sequencer reactivations per unit of time. Used on
  // sequencer nodes.
  rate_limit_t reactivation_limit;

  // (sequencer-only setting) maximum time allowed for a sequencer to drain one
  // epoch
  std::chrono::milliseconds epoch_draining_timeout;

  // (sequencer-only setting) maximum time interval for a sequencer to get
  // historical epoch metadata through reading the metadata log before retrying
  std::chrono::milliseconds read_historical_metadata_timeout;

  // (sequencer-only setting) Timeout for request by sequencer activation to
  // verify, if the epoch store is empty for the given log, that metadata log
  // is too.
  std::chrono::milliseconds check_metadata_log_empty_timeout;

  // Limit on the maximum size of payload that the client can append.
  size_t max_payload_size;

  // (server-only setting) Maximum number number of incoming connections this
  // server will accept. This is normally not set directly, but derived from
  // other settings (such as the fd limit).
  size_t max_incoming_connections{std::numeric_limits<size_t>::max()};

  // (server-only setting) Maximum number of incoming connections that have been
  // accepted by listener (have an open FD) but have not been processed by
  // workers (made logdevice protocol handshake)
  size_t connection_backlog;

  // (server-only setting) Maximum number of established incoming connections,
  // coming from outside of the cluster, with handshake completed. As above,
  // usually calculated from other settings.
  size_t max_external_connections{std::numeric_limits<size_t>::max()};

  // When true, the findTime index is written.
  bool write_find_time_index;

  // (client-only setting) When set to true on the client, this will get the
  // log configuration from the server on-demand, if it's not present in the
  // main config file.
  bool on_demand_logs_config;

  // (client-only settings) Retry delay for on-demand-logs-config
  chrono_expbackoff_t<std::chrono::milliseconds>
      on_demand_logs_config_retry_delay;

  // (client-only setting) When set to the name of a property, this settings
  // tells the logs config parser to use the specified property to expand log
  // groups, if present, instead of the regular layout property.
  std::string alternative_layout_property;

  // (client-only setting) The TTL for cache entries for the remote logs
  // config. If the logs config is not available locally and is fetched from
  // the server, this will determine how fresh the log configuration used by
  // the client will be.
  std::chrono::seconds remote_logs_config_cache_ttl;

  // (server-only setting) Override the client FindKeyAccuracy setting with
  // FindKeyAccuracy::APPROXIMATE.
  bool findtime_force_approximate;

  std::chrono::seconds initial_config_load_timeout;

  // If `true`, logdevice will create the root znodes (all znodes that are
  // parents of znodes for individual logs) if they doesn't exist. If `false`,
  // the root znodes should be created by external tooling.
  bool zk_create_root_znodes;

  // Maximum amount of memory that can be allocated by read storage tasks.
  size_t read_storage_tasks_max_mem_bytes;

  size_t append_stores_max_mem_bytes;
  size_t rebuilding_stores_max_mem_bytes;

  // Path to LD SSL-certificate
  std::string ssl_cert_path;

  // Path to CA certificate
  std::string ssl_ca_path;

  // Path to private key file
  std::string ssl_key_path;

  bool ssl_load_client_cert;

  // TTL for the cert loaded from file
  std::chrono::seconds ssl_cert_refresh_interval;

  // Sets the boundary which triggers enabling SSL. Communication that crosses
  // this boundary will be encrypted; communication that doesn't will not.
  // For instance, if set to NodeLocationScope::RACK, all cross-rack traffic
  // will be encrypted but traffic within the rack will not. The default
  // boundary ROOT means no traffic is encrypted (all nodes are in the same
  // root scope).
  NodeLocationScope ssl_boundary;

  // {client-only setting} Sets the location of the client in order to
  // determine whether the boundary set in --ssl-boundary (above) is crossed,
  // and thus to decide whether to use SSL when connecting to a certain node.
  // Also used in local SCD reading.
  folly::Optional<NodeLocation> client_location;

  // IO priority to request for slow storage threads.
  // See man ioprio_set for possible values.
  folly::Optional<std::pair<int, int>> slow_ioprio;

  // (client-only setting) Timeout after which ClientReadStream considers a
  // storage node down if it does not send any data for some time but the socket
  // to it remains open. This can happen if:
  // - the storage node OOMs;
  // - there is a bug causing the storage node to stop sending data.
  // When this timeout occurs, the reader asks other storage nodes to send the
  // data that the storage node deemed down was supposed to send. Note that when
  // this happens we remain in Single Copy Delivery mode.
  std::chrono::milliseconds scd_timeout;

  // (client-only setting) Timeout after which ClientReadStream fails over to
  // asking all storage nodes to send everything they have if it is not able to
  // make progress for some time.
  // This may happen in extremely rare situations where the reader is not able
  // to make any progress during that amount of time because all nodes have not
  // sent anything.
  // The timeout should be high enough to not create false positives due to
  // storage nodes being slow sending data. Since this timeout's purpose is to
  // guarantee eventual delivery even when suffering from very unlikely issues,
  // it is reasonable to keep it as high as 5min.
  std::chrono::milliseconds scd_all_send_all_timeout;

  // (client-only setting) Default namespace to use on the client. This will
  // be used for any client functions that take log group name as a parameter.
  std::string default_log_namespace;

  // Time interval that ConfigurationUpdatedRequest would retry sending
  // CONFIG_CHANGED messages after it got NOBUFS.
  // TODO (t13314297): this setting is not modified anywhere
  std::chrono::milliseconds configuration_update_retry_interval{5000};

  // The timeout of the Server Based Nodes Configuration Store's
  // NODES_CONFIGURATION polling round
  std::chrono::milliseconds server_based_nodes_configuration_store_timeout;

  // timeout settings for server based Nodes Configuration Store's multi-wave
  // backoff retry behavior
  chrono_expbackoff_t<std::chrono::milliseconds>
      server_based_nodes_configuration_polling_wave_timeout;

  // how many successful responses for server based Nodes Configuration Store
  // polling to wait for each round
  size_t server_based_nodes_configuration_store_polling_responses;

  // how many extra requests to send for server based Nodes Configuration Store
  // polling in addition to the required response for each wave
  size_t server_based_nodes_configuration_store_polling_extra_requests;

  // The seed string that will be used to fetch the initial nodes configuration
  // It can be in the form string:<server1>,<server2>,etc. Or you can provide an
  // smc tier via "smc:<smc_tier>". If it's empty, NCM client bootstrapping is
  // not used.
  // TODO finalize the format of the string the config string.
  std::string nodes_configuration_seed_servers;

  // timeout settings for the exponential backoff retry behavior for
  // initializing Nodes Configuration for the first time
  chrono_expbackoff_t<std::chrono::milliseconds>
      nodes_configuration_init_retry_timeout;

  // defines the maximum time allowed on the initial nodes configuration
  // fetch
  std::chrono::milliseconds nodes_configuration_init_timeout;

  // Flag indicating whether tcp keep alive should be on.
  bool use_tcp_keep_alive;

  // Time, in seconds, before the first probe will be sent.
  int tcp_keep_alive_time;

  // Interval, in seconds, between two successive probes.
  int tcp_keep_alive_intvl;

  // The number of unacknowledged probes before the connection is considered
  // broken.
  int tcp_keep_alive_probes;

  // The time in milliseconds that transmitted data may remain unacknowledged
  // before TCP will close the connection.
  // 0 for system default.
  // -1 for disable
  // default is 5min = 300000;
  int tcp_user_timeout;

  bool verify_checksum_before_replicating;

  // When true, the cluster name of the client will be included in the inital
  // handshake. The clients cluster name will be checked against the servers
  // cluster cluster name to see if there is a mismatch. If there is the
  // connection is rejected.
  bool include_cluster_name_on_handshake;

  // How often to collect and submit stats upstream.
  // Set to <=0 to disable collection of stats.
  std::chrono::seconds stats_collection_interval;

  // How long should we wait before disabling isolated sequencers.
  std::chrono::seconds isolated_sequencer_ttl;

  // How many bits to use for sequence numbers within an epoch.  LSN bits [n,
  // 32) are guaranteed to be 0.
  int esn_bits;

  // Initial delay to use when downstream rejects a record or gap.
  std::chrono::milliseconds client_initial_redelivery_delay;

  // Maximum delay to use when downstream rejects a record or gap.
  std::chrono::milliseconds client_max_redelivery_delay;

  // When true, the destination node ID of the client will be included in the
  // inital handshake. The client's actual node ID will be checked against the
  // intended destination node ID to see if there is a mismatch. If there is a
  // mismatch, the connection is rejected.
  bool include_destination_on_handshake;

  // DEPRECATED! Corresponding log attribute should be used instead.
  // When true, sequencers are fronted with a BufferedWriter instance that
  // accumulates appends from clients and batches them together to create
  // fewer records.  The usual caveats about BufferedWriter apply, most
  // notably that the same LSN will be issued to multiple appends.
  bool sequencer_batching;

  // DEPRECATED! Corresponding log attribute should be used instead.
  // Sequencer batching flushes buffered appends for a log when the oldest
  // buffered append is this old.  See BufferedWriter::Options.
  std::chrono::milliseconds sequencer_batching_time_trigger;

  // DEPRECATED! Corresponding log attribute should be used instead.
  // Sequencer batching flushes buffered appends for a log when the total
  // amount of buffered uncompressed data reaches this many bytes (if
  // positive).  See BufferedWriter::Options.
  ssize_t sequencer_batching_size_trigger;

  // DEPRECATED! Corresponding log attribute should be used instead.
  Compression sequencer_batching_compression;

  // DEPRECATED! Corresponding log attribute should be used instead.
  // Sequencer batching will pass through any appends with payload size over
  // this threshold (if positive).  This saves us a compression round trip
  // when a large batch comes in from BufferedWriter and the benefit of
  // batching and recompressing would be small.
  ssize_t sequencer_batching_passthru_threshold;

  // Number of background threads.  Currently, background threads are used by
  // BufferedWriter to construct/compress large batches.  If 0 (the default),
  // use num_workers.
  int num_processor_background_threads;

  // BufferedWriter can send batches to a background thread.  For small batches,
  // where the overhead dominates, this will just slow things down.  If the
  // total size of the batch is less than this, it will constructed / compressed
  // on the Worker thread, blocking other appends to all logs in that shard.  If
  // larger, it will be enqueued to a helper thread.
  size_t buffered_writer_bg_thread_bytes_threshold;

  // Zstd compression level to use in BufferedWriter
  size_t buffered_writer_zstd_level;

  // Maximum number of tasks we can queue to a Processor's background thread
  // pool.  A single queue is shared by all threads in a single Processor's
  // pool.
  size_t background_queue_size;

  // By default, check-seal requests are sent after receiving GET_SEQ_STATE.
  // This option prevents GET_SEQ_STATE from sending CHECK_SEAL messages.
  bool disable_check_seals;

  // GetClusterStateRequest will use these value to setup wave timer and
  // overall request timer.
  // TODO (t13314297): these settings are not modified anywhere
  std::chrono::milliseconds get_cluster_state_timeout{10000};
  std::chrono::milliseconds get_cluster_state_wave_timeout{1000};

  // Maximum duration of Sender::runFlowGroups() before yielding to the
  // event loop.
  std::chrono::microseconds flow_groups_run_yield_interval;

  // Maximum delay (plus one cycle of the event loop) between
  // a request to run FlowGroups and Sender::runFlowGroups() executing.
  std::chrono::microseconds flow_groups_run_deadline;

  // TODO T29642728, DEPRECATED and will remove finally
  // How often the sequencer sends byte offsets to storage nodes. Measured in
  // bytes. This option will be ignored if byte_offsets feature is disabled.
  uint64_t byte_offset_interval_DEPRECATED;

  // If true, cluster will keep track of logs byte offsets.
  // NOTE: this involve extra work for storage nodes during recovery.
  bool byte_offsets;

  // If true, cluster will use OffsetMap to keep track of logs offsets.
  // NOTE: this involve extra work for storage nodes during recovery.
  bool enable_offset_map;

  // If true, use the new version of timers which run on a different thread
  // and use HHWheelTimer backend.
  bool enable_hh_wheel_backed_timers;

  // If true, use the new version of timers which run on a different thread
  // and use HHWheelTimer backend.
  bool enable_store_histogram_calculations;

  // How many stores should the store histogram wait for before reporting
  // latency estimates.
  size_t store_histogram_min_samples_per_bucket;

  // With config synchronization enabled, nodes on both ends of a connection
  // will synchronize their configs if there is a mismatch in the config
  // version
  bool enable_config_synchronization;

  // If true, servers will be allowed to fetch configs from the client side of
  // a connection.
  bool client_config_fetch_allowed;

  // If true, histogram single stats will be published alongside the rate stats
  bool publish_single_histogram_stats;

  // Used to cause an error to be returned when transmitting a message;
  // either synchronously when a message is queued via Sender::sendMessage(),
  // or asynchronously when a message completion is reported via
  // Message::onSent(). The synchronous case is currently only supported
  // for the traffic shaping error code E::CBREGISTERED. All other error
  // codes will be injected via Message::onSent().
  //
  // Expressed as a percentage: 100 = 100%.
  double message_error_injection_chance_percent;

  // The error code that streams will be rewound with
  Status message_error_injection_status;

  chrono_expbackoff_t<std::chrono::milliseconds>
      sequencer_metadata_log_write_retry_delay;
  chrono_expbackoff_t<std::chrono::milliseconds>
      sequencer_epoch_store_write_retry_delay;

  // backoff time interval for sequencer retrying reading metadata logs for
  // getting historical epoch metadata
  chrono_expbackoff_t<std::chrono::milliseconds>
      sequencer_historical_metadata_retry_delay;

  // Time interval at which to check for unreleased records in storage nodes.
  // Any log which has unreleased records, and for which no records have been
  // released for two consecutive unreleased_record_detector_intervals, is
  // suspected of having a dead sequencer. Set to 0 to disable check.
  std::chrono::milliseconds unreleased_record_detector_interval;

  // Maximum number of consecutive grace periods a storage node may fail to
  // send a record or gap (if in all read all mode) before it is considered
  // disgraced and client read streams no longer wait for it. If all nodes are
  // disgraced or in GAP state, a gap record is issued. May be 0. Set to -1 to
  // disable grace counters and use simpler logic: no disgraced nodes, issue gap
  // record as soon as grace period expires.
  int grace_counter_limit;

  // Allow the event log to be snapshotted onto a snapshot log.
  bool event_log_snapshotting;

  // Use ZSTD compression to compress event log snapshots.
  bool event_log_snapshot_compression;

  // Default DSCP value for server sockets at the Sender.
  uint8_t server_dscp_default;

  // Default DSCP value for client sockets at the Sender.
  uint8_t client_dscp_default;

  // Disable trimming the event log.
  bool disable_event_log_trimming;

  // How many delta records to keep in the event log before we snapshot it.
  size_t event_log_max_delta_records;

  // How many bytes of delta records to keep in the event log before we snapshot
  // it.
  size_t event_log_max_delta_bytes;

  // If the event log is snapshotted, how long to keep a history of snapshots
  // and delta.
  std::chrono::milliseconds event_log_retention;

  // Test Options:

  // if not Status::OK, reject all HELLOs with this status code
  Status reject_hello;

  // (client-only setting) When set to true, this will get the log
  // configuration from the server on-demand, regardless of whether it is
  // available locally. Related to Settings::on_demand_logs_config
  bool force_on_demand_logs_config;

  // If set, sequencers will not automatically run recovery upon
  // activation. Recovery can be started using the 'startrecovery' admin
  // command.  Note that last released lsn won't get advanced without
  // recovery.
  bool bypass_recovery;

  // if set, we hold all STORED messages (which are replies to STORE messages),
  // until the last one comes is.  Has some race conditions and other down
  // sides, so only use in tests.  Used to ensure that all storage nodes have
  // had a chance to process the STORE messages, even if one returns PREEMPTED
  // or another error condition.
  bool hold_store_replies;

  // if valid NodeID, SequencerRouter only sends requests to this node, with
  // REDIRECT_CYCLE flag
  NodeID force_sequencer_choice;

  // if true, metadata log record writes will be synced before sending the
  // STORED reply to sequencer
  bool sync_metadata_log_writes;

  // The default storage durability contract to use for appends.
  // Durability::SYNC_WRITE is used, regardless of the value of this
  // setting, if the STORE message has STORE_Header::SYNC set.
  Durability append_store_durability;

  // The storage durability contract to use for rebuilding stores.
  Durability rebuild_store_durability;

  // Don't wait for flush callbacks during rebuilding.
  // TODO: T38362945
  bool rebuilding_dont_wait_for_flush_callbacks;

  // If true, send a normal STORE with full payload, rather than a STORE
  // with the AMEND flag, when updating the copyset of nodes that already
  // have a copy of the record. This option is used by integration tests to
  // fully divorce append content from records touched by rebuilding.
  bool rebuild_without_amends;

  // Whether we use the internal ReplicatedStateMachine or not for logsconfig
  bool enable_logsconfig_manager;
  // Grace period before populating new LogsConfigTree upon receiving RSM delta
  std::chrono::milliseconds logsconfig_manager_grace_period;

  // Enable automatic snapshotting of LogsConfig in the replicated state machine
  bool logsconfig_snapshotting;
  // Disable LogsConfig trimming
  bool disable_logsconfig_trimming;
  // How many delta records to keep in the logsconfig deltas log before
  // we snapshot it.
  size_t logsconfig_max_delta_records;
  // How many bytes of delta records to keep in the logsconfig deltas log before
  // we snapshot it.
  size_t logsconfig_max_delta_bytes;

  // Largest SCDCopysetReordering that clients may ask servers to use.  Mostly
  // available as a killswitch for copyset shuffling (0 turns off).
  int scd_copyset_reordering_max;

  bool reject_stores_based_on_copyset;

  // TODO(adri):
  // This option can be used to have ClientReadStream consider the nodes that
  // have authoritative status UNAVAILABLE be UNDERREPLICATION so that we don't
  // stall if too many nodes are down and being rebuilt. It would be better to
  // have this option be a grace period, ie how long are we willing to stall
  // readers before deciding to give up? However this would be much more work
  // to implement as we would need to somehow get a timestamp of when rebuilding
  // became non authoritative and have RebuildingSupervisor write
  // SHARD_UNRECOVERABLE messages if rebuilding is still non authoritative after
  // that grace period. The problem is that there is no easy mechanism for
  // RebuildingSupervisor to know that timestamp.
  bool read_stream_guaranteed_delivery_efficiency;

  // Max number of concurrent background sequencer activations running
  size_t max_sequencer_background_activations_in_flight;

  // This settings control the min and max time delay for postponing
  // a sequencer reactivation.
  chrono_interval_t<std::chrono::seconds> sequencer_reactivation_delay_secs;

  // An interval after which we retry processing the background sequencer
  // reactivation queue on failure
  std::chrono::milliseconds sequencer_background_activation_retry_interval;

  // TODO (#13478262): After WeightedCopySetSelector proves worthy, remove this
  //   setting, along with LinearCopySetSelector and CrossDomainCopySetSelector.
  bool weighted_copyset_selector;

  NodeLocationScope copyset_locality_min_scope;

  // Defaults to false, allows clients to opt-in to traffic shadowing
  bool traffic_shadow_enabled;

  // If there is a request to retry shadow client creation then it is retried
  // after these many seconds. See ShadowClient.cpp for details.
  std::chrono::seconds shadow_client_creation_retry_interval;

  // See .cpp
  std::chrono::milliseconds shadow_client_timeout;

  // Defaults to false, should only be set by traffic shadowing framework
  bool shadow_client;

  // enable NodesConfigurationManager on clients and servers
  bool enable_nodes_configuration_manager;

  // if true and enable_nodes_configuration_manager is set, logdevice will use
  // the nodes configuration from the NodesConfigurationManager
  bool use_nodes_configuration_manager_nodes_configuration;

  // Polling interval of NodesConfigurationManager to NodesConfigurationStore
  // to read NodesConfiguration
  std::chrono::seconds nodes_configuration_manager_store_polling_interval;

  // Timeout for proposing the transition for a shard from an intermediary state
  // to its 'destination' state
  std::chrono::seconds
      nodes_configuration_manager_intermediary_shard_state_timeout;

  // if set, the client will be used for administrative operations such as
  // emergency tooling, and it can propose changes to LD metadata such as
  // NodesConfiguration
  bool admin_client_capabilities;

  // If set, the source of truth of nodes configuration will be under this dir
  //  instead of the default (zookeeper) store. Used by integration testing.
  std::string nodes_configuration_file_store_dir;

  // If true, sequencer routing will first try to find a sequencer in the
  // location given by sequencerAffinity before looking elsewhere.
  bool use_sequencer_affinity;

  // Client only setting:

  // The following settings list logs for which certain operations should be
  // failing and the status that they should fail with
  std::unordered_set<logid_t> dont_serve_reads_logs;
  Status dont_serve_reads_status;
  std::unordered_set<logid_t> dont_serve_findtimes_logs;
  Status dont_serve_findtimes_status;
  std::unordered_set<logid_t> dont_serve_stores_logs;
  Status dont_serve_stores_status;

  chrono_expbackoff_t<std::chrono::milliseconds> reader_reconnect_delay;
  chrono_expbackoff_t<std::chrono::milliseconds> reader_started_timeout;
  chrono_expbackoff_t<std::chrono::milliseconds> reader_retry_window_delay;

  // Enable / disable real time reads.
  bool real_time_reads_enabled;

  // (server-only setting) Max size in bytes of released records that we'll keep
  // around to use for real time reads.  Includes some cache overhead, so for
  // small records, you'll store less record data than this.
  size_t real_time_max_bytes;

  // (server-only setting) When the real time buffer reaches this size, we evict
  // entries.
  size_t real_time_eviction_threshold_bytes;

  // Test Options:

  // This option should only be used in tests. This is used to linerarly
  // tranform the record's timestamp. Designed for usecase where we want
  // to write  data across a long time in a very short time.
  // The timestamp is tranformed as below:
  // timestamp = test_timestamp_linear_transform.first * now() +
  // test_timestamp_linear_transform.second
  std::pair<int64_t, int64_t> test_timestamp_linear_transform;

  // When set, serialize ShardIDs instead of node_index_t on disk.
  bool write_shard_id_in_copyset;
  // When set, new EpochMetaData is serialized using the new copyset
  // serialization format for Flexible Log Sharding.
  // TODO(T15517759): once all clusters are configured to use this option,
  // remove it.
  bool epoch_metadata_use_new_storage_set_format;

  // Simulates bad hardware flipping bits in STORE message.
  bool test_sequencer_corrupt_stores{false};

  // If true then the check to fail trim requests past
  // the tail will be disbled.
  bool disable_trim_past_tail_check;

  bool allow_reads_on_workers{true};

  std::vector<node_index_t> test_do_not_pick_in_copysets;

  std::unordered_set<MessageType> message_tracing_types;
  SockaddrSet message_tracing_peers;
  dbg::Level message_tracing_log_level;
  bool rsm_include_read_pointer_in_snapshot;
  std::chrono::milliseconds eventlog_snapshotting_period;
  std::chrono::milliseconds logsconfig_snapshotting_period;

  // polling interval for fetching trim point from historical node set
  std::chrono::seconds get_trimpoint_interval;

  folly::Optional<std::chrono::milliseconds> findkey_timeout;

  folly::Optional<std::chrono::milliseconds> append_timeout;

  folly::Optional<std::chrono::milliseconds> logsconfig_timeout;

  folly::Optional<std::chrono::milliseconds> meta_api_timeout;

  // Slow shard detection is useful when readers are in Single Copy Delivery
  // mode. It measures how fast storage nodes are and detects outliers that
  // can be blacklisted so that we read faster.
  enum class ReaderSlowShardDetectionState { DISABLED, OBSERVE_ONLY, ENABLED };
  ReaderSlowShardDetectionState reader_slow_shards_detection;
  ClientReadStreamFailureDetectorSettings reader_slow_shards_detection_settings;

  SequencerBoycottingSettings sequencer_boycotting;

  std::chrono::milliseconds nodeset_adjustment_period;
  size_t nodeset_adjustment_target_bytes_per_shard;
  double nodeset_size_adjustment_min_factor;
  std::chrono::milliseconds nodeset_adjustment_min_window;
  size_t nodeset_max_randomizations;

  // Use metadata logs in NodeSetFinder if true, otherwise use sequencers
  // (metadata logs v2) and fallback to metadata logs if needed.
  // TODO: set default to false (or remove option) when 2.35 is deployed
  // everywhere.
  bool read_streams_use_metadata_log_only;

  std::unordered_map<ShardID, AuthoritativeStatus>
      authoritative_status_overrides;

  // enforcing permissions on the given message types if supported.
  std::unordered_set<MessageType> require_permission_message_types;

  bool enable_all_read_streams_sampling;
  std::chrono::milliseconds all_read_streams_rate;

 protected:
  // Only UpdateableSettings can create this bundle to ensure defaults are
  // populated.
  Settings() {}
  friend class UpdateableSettingsRaw<Settings>;
};

}} // namespace facebook::logdevice
