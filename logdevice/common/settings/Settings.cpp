/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/settings/Settings.h"

#include <cctype>
#include <limits>
#include <utility>
#include <zstd.h>

#include <boost/algorithm/string/predicate.hpp>
#include <boost/program_options.hpp>
#include <boost/thread/thread.hpp>
#include <folly/String.h>

#include "logdevice/common/Sockaddr.h"
#include "logdevice/common/commandline_util_chrono.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/protocol/Compatibility.h"
#include "logdevice/common/protocol/MessageTypeNames.h"
#include "logdevice/common/settings/Validators.h"

using namespace facebook::logdevice::setting_validators;

namespace facebook { namespace logdevice {

static int parse_num_workers(const std::string& value) {
  // Following formats are allowed:
  // 1. Number, e.g. "16"
  // 2. Number of physical cores, "ncores" or "cores"
  // 3. Number of physical cores and multiplicator, e.g. "ncores*1.4"
  std::string num_workers = value;
  int workers;
  double multiplicator = 1.0;
  std::size_t multiplier = value.find("*");
  if (multiplier != std::string::npos) {
    num_workers = value.substr(0, multiplier);
    std::string right_operand = value.substr(multiplier + 1);
    if (num_workers != "cores" && num_workers != "ncores") {
      throw boost::program_options::error(
          std::string("Invalid value for --num-workers. Left operand must be "
                      "cores or ncores. Example: ncores*1.4"));
    }
    try {
      multiplicator = std::stod(right_operand);
    } catch (const std::invalid_argument& ex) {
      throw boost::program_options::error(
          std::string("Invalid multiplicator for --num-workers."));
    }
    if (multiplicator <= 0) {
      throw boost::program_options::error(
          std::string("Multiplicator for --num-workers must be > 0."));
    }
  }

  if (num_workers == "cores" || num_workers == "ncores") {
    workers = boost::thread::physical_concurrency();
  } else {
    try {
      workers = std::stol(num_workers, nullptr, 10);
    } catch (const std::logic_error& ex) {
      throw boost::program_options::error(
          std::string("Invalid value for --num-workers."));
    }
    validate_range<int>(1, MAX_WORKERS)("num-workers", workers);
  }

  return std::max(std::min<int>(workers * multiplicator, MAX_WORKERS), 1);
}

static std::pair<int64_t, int64_t>
parse_test_timestamp_linear_tranform(const std::string& value) {
  std::vector<std::string> test_timestamp_linear_tranformation;
  folly::split(",", value, test_timestamp_linear_tranformation, true);
  if (test_timestamp_linear_tranformation.size() != 2) {
    throw boost::program_options::error(
        std::string("Invalid value for test-timestamp-linear-tranform."));
  }
  try {
    return std::make_pair<int64_t, int64_t>(
        std::stoll(test_timestamp_linear_tranformation[0], nullptr, 10),
        std::stoll(test_timestamp_linear_tranformation[1], nullptr, 10));
  } catch (const std::logic_error& ex) {
    throw boost::program_options::error(
        std::string("Invalid value for test-timestamp-linear-tranform."));
  }
}

static std::vector<node_index_t>
parse_recipients_list(const std::string& value) {
  std::vector<std::string> recipients_tmp;
  std::vector<node_index_t> recipients;
  folly::split(",", value, recipients_tmp, true);
  try {
    for (const auto& nid : recipients_tmp) {
      recipients.push_back(std::stoi(nid, nullptr, 10));
    }
  } catch (const std::logic_error& ex) {
    throw boost::program_options::error(
        std::string("Invalid node ID in recipients list."));
  }
  return recipients;
}

static std::unordered_set<logid_t> parse_log_set(const std::string& value) {
  std::unordered_set<logid_t> res;

  std::vector<std::string> logs_tmp;
  folly::split(",", value, logs_tmp, true);
  try {
    for (const auto& str : logs_tmp) {
      auto rv = res.insert(logid_t(std::stoull(str)));
      if (!rv.second) {
        throw boost::program_options::error(
            std::string("Duplicate Log ID in the list."));
      }
    }
  } catch (const std::logic_error& ex) {
    throw boost::program_options::error(
        std::string("Invalid Log ID in the list."));
  }
  return res;
}

static Status validate_reject_hello(const std::string& value) {
  if (value == "ACCESS") {
    return E::ACCESS;
  } else if (value == "PROTONOSUPPORT") {
    return E::PROTONOSUPPORT;
  } else if (value == "INVALID_CLUSTER") {
    return E::INVALID_CLUSTER;
  } else if (value == "DESTINATION_MISMATCH") {
    return E::DESTINATION_MISMATCH;
  } else if (value == "OK") { // default
    return E::OK;             // do not reject
  } else {
    throw boost::program_options::error(
        "Invalid value for --test-reject-hello. "
        "Expected ACCESS, PROTONOSUPPORT, DESTINATION_MISMATCH, "
        "or INVALID_CLUSTER. Got " +
        value);
  }
}

static std::unordered_set<MessageType>
parse_message_types(const std::string& val) {
  std::unordered_set<MessageType> res;
  if (val == "all") {
    auto types = messageTypeNames().allValidKeys();
    res.insert(types.begin(), types.end());
    return res;
  }
  std::vector<std::string> tokens;
  folly::split(",", val, tokens, true);
  bool inverse = false;
  if (!tokens.empty() && !tokens[0].empty() && tokens[0][0] == '~') {
    inverse = true;
    tokens[0].erase(tokens[0].begin());
  }
  for (const auto& str : tokens) {
    MessageType type = messageTypeNames().reverseLookup(str);
    if (type == MessageType::INVALID) {
      throw boost::program_options::error(
          std::string("Invalid message type in the list (\"" + str + "\")."));
    }
    auto rv = res.insert(type);
    if (!rv.second) {
      throw boost::program_options::error(
          std::string("Duplicate message type in the list (\"" + str + "\")."));
    }
  }
  if (inverse) {
    auto exclude = std::move(res);
    auto types = messageTypeNames().allValidKeys();
    res = std::unordered_set<MessageType>(types.begin(), types.end());
    for (MessageType t : exclude) {
      res.erase(t);
    }
  }

  return res;
}

static SockaddrSet parse_sockaddrs(const std::string& val) {
  std::unordered_set<Sockaddr, Sockaddr::Hash> elements;
  bool anonymous_unix_socket_present = false;
  std::vector<std::string> tokens;
  folly::split(",", val, tokens, true);
  for (const auto& str : tokens) {
    folly::SocketAddress tmp;
    try {
      if (boost::starts_with(str, "unix://")) {
        if (str == "unix://") {
          anonymous_unix_socket_present = true;
          continue;
        }
        tmp.setFromPath(str.substr(7));
      } else {
        try {
          tmp.setFromIpPort(str);
        } catch (std::exception& e) {
          tmp.setFromIpPort(str + ":0");
        }
      }
    } catch (std::invalid_argument& e) {
      throw boost::program_options::error(
          std::string("Invalid socket address \"" + str + "\""));
    } catch (std::exception& e) {
      throw boost::program_options::error(
          std::string("Couldn't parse address\"" + str + "\": " + e.what()));
    }
    elements.insert(Sockaddr(std::move(tmp)));
  }
  return {elements, anonymous_unix_socket_present};
}

dbg::Level parse_log_level(const std::string& val) {
  const auto level = dbg::tryParseLoglevel(val.c_str());
  if (!level.hasValue()) {
    std::array<char, 1024> buf;
    snprintf(buf.data(),
             buf.size(),
             "Invalid value for --loglevel: %s. "
             "Expected one of: critical, error, warning, notify, "
             "info, debug, spew, none",
             val.c_str());
    throw boost::program_options::error(std::string(buf.data()));
  }
  return level.value();
}

static int parse_scd_copyset_reordering(const std::string& val) {
  if (val == "none") {
    return 0;
  } else if (val == "hash-shuffle") {
    return 1;
  } else if (val == "hash-shuffle-client-seed") {
    return 2;
  } else {
    std::array<char, 1024> buf;
    snprintf(buf.data(),
             buf.size(),
             "Invalid value for --scd-copyset-ordering-max: %s. "
             "Expected one of: none, hash-shuffle, hash-shuffle-client-seed",
             val.c_str());
    throw boost::program_options::error(std::string(buf.data()));
  }
}

std::istream& operator>>(std::istream& in, NodeLocationScope& val) {
  std::string key;
  in >> key;
  std::transform(key.begin(), key.end(), key.begin(), ::toupper);

  NodeLocationScope e;
  if (key == "NONE") {
    e = NodeLocationScope::ROOT;
  } else {
    e = NodeLocation::scopeNames().reverseLookup(key);
  }

  if (e == NodeLocationScope::INVALID) {
    in.setstate(std::ios::failbit);
    throw boost::program_options::error(
        "Invalid location scope name. Expected one of: " +
        rangeToString(NodeLocation::scopeNames().begin(),
                      NodeLocation::scopeNames().end()) +
        ", NONE");
  }

  val = e;

  return in;
}

std::istream& operator>>(std::istream& in, Status& val) {
  std::string token;
  in >> token;
  Status status = errorStrings().reverseLookup<std::string>(
      token,
      [](const std::string& s, const ErrorCodeInfo& e) { return s == e.name; });
  if (status == E::UNKNOWN) {
    in.setstate(std::ios::failbit);
    throw boost::program_options::error("Invalid status: " + token);
  }
  val = status;
  return in;
}

std::istream& operator>>(std::istream& in, Durability& val) {
  std::string token;
  in >> token;
  std::transform(token.begin(), token.end(), token.begin(), ::toupper);
  Durability durability = durabilityStrings().reverseLookup(token);
  if (durability == Durability::INVALID || durability == Durability::ALL) {
    in.setstate(std::ios::failbit);
    throw boost::program_options::error("Invalid Durability setting: " + token);
  }
  val = durability;
  return in;
}

folly::Optional<std::chrono::milliseconds>
parse_optional_chrono_option(const std::string& value) {
  folly::Optional<std::chrono::milliseconds> result;
  if (value == "") {
    return result;
  }

  std::chrono::milliseconds parsed_duration_value;
  if (parse_chrono_string(value, &parsed_duration_value) != 0) {
    throw boost::program_options::error("Invalid timeout value: " + value);
  }
  result.assign(parsed_duration_value);
  return result;
};

Compression parse_compression(const std::string& value) {
  Compression compression;
  auto rv = parseCompression(value.c_str(), &compression);
  if (rv == -1) {
    throw boost::program_options::error("Invalid compression value: " + value);
  }
  return compression;
}

std::unordered_map<ShardID, AuthoritativeStatus>
parse_authoritative_status_overrides(const std::string& value) {
  std::unordered_map<ShardID, AuthoritativeStatus> res;
  std::vector<std::string> tokens;
  folly::split(',', value, tokens, true);
  for (const std::string& tok : tokens) {
    bool ok = false;
    do { // while (false)
      // tok format: "N7:S2-5:UNDERREPLICATION" or "N7:S2:UNDERREPLICATION"
      std::string node_str;
      std::string shard_str;
      std::string status_str;
      if (!folly::split(':', tok, node_str, shard_str, status_str)) {
        break;
      }
      AuthoritativeStatus status;
      if (node_str.empty() || shard_str.empty() || node_str[0] != 'N' ||
          shard_str[0] != 'S' ||
          !parseAuthoritativeStatus(status_str, status)) {
        break;
      }
      node_str.erase(node_str.begin());
      shard_str.erase(shard_str.begin());
      try {
        std::vector<std::string> shard_tok;
        folly::split('-', shard_str, shard_tok);
        if (shard_tok.empty() || shard_tok.size() > 2) {
          break;
        }
        int min_shard = std::stoi(shard_tok[0]);
        int max_shard =
            shard_tok.size() == 2 ? std::stoi(shard_tok[1]) : min_shard;
        if (max_shard < min_shard || min_shard < 0 || max_shard >= MAX_SHARDS) {
          break;
        }
        int node = std::stoi(node_str);
        if (node < 0 || node > (int)std::numeric_limits<shard_index_t>::max()) {
          break;
        }
        for (int shard = min_shard; shard <= max_shard; ++shard) {
          ShardID shard_id((node_index_t)node, (shard_index_t)shard);
          auto ins = res.emplace(shard_id, status);
          if (!ins.second) {
            throw boost::program_options::error(
                "Duplicate authoritative status override for " +
                shard_id.toString());
          }
        }
        ok = true;
      } catch (const std::logic_error&) {
        break;
      }
    } while (false);

    if (!ok) {
      throw boost::program_options::error(
          "Invalid authoritative status override: " + tok +
          "; expected N<number>:S<number>[-<number>]:<status>, where status is "
          "one of: " +
          toString(allAuthoritativeStatusStrings()));
    }
  }
  return res;
}

void Settings::defineSettings(SettingEasyInit& init) {
  using namespace SettingFlag;

  init("server",
       &server,
       "false",
       nullptr, // no validation
       "if true, the Processor with this Settings object is running in a "
       "LogDevice server. If false, it's in a LogDeviceClient. This isn't set "
       "by "
       "any parsed setting, but is only set directly by servers",
       INTERNAL_ONLY);
  init("bootstrapping",
       &bootstrapping,
       "false",
       nullptr, // no validation
       "if true, the Processor with this Settings object is only used to "
       "perform bootstraping and will be destroyed after bootstrapping is "
       "completed. This isn't set by any parsed setting, but is only set "
       "directly internally",
       INTERNAL_ONLY);
  init(
      "max-incoming-connections",
      &max_incoming_connections,
      std::to_string(std::numeric_limits<ssize_t>::max()).c_str(),
      parse_positive<ssize_t>(),
      "(server-only setting) Maximum number number of incoming connections "
      "this "
      "server will accept. This is normally not set directly, but derived from "
      "other settings (such as the fd limit).",
      INTERNAL_ONLY);
  init(
      "connection-backlog",
      &connection_backlog,
      "2000",
      parse_positive<ssize_t>(),
      "(server-only setting) Maximum number of incoming connections that have "
      "been accepted by listener (have an open FD) but have not been processed "
      "by workers (made logdevice protocol handshake).",
      SERVER,
      SettingsCategory::Network);
  init("max-external-connections",
       &max_external_connections,
       std::to_string(std::numeric_limits<ssize_t>::max()).c_str(),
       parse_positive<ssize_t>(),
       "(server-only setting) Maximum number of established incoming "
       "connections, coming from outside of the cluster, with handshake "
       "completed. Usually calculated from other settings.",
       INTERNAL_ONLY,
       SettingsCategory::Network);
  init("num-workers",
       &num_workers,
       "cores",
       parse_num_workers,
       "number of worker threads to run, or \"cores\" for one thread "
       "per CPU core",
       SERVER | CLIENT | REQUIRES_RESTART /* used in Processor ctor */,
       SettingsCategory::Execution);
  init("msg-error-injection-chance",
       &message_error_injection_chance_percent,
       "0",
       validate_range<double>(0, 100),
       "percentage chance of a forced message error on a Socket. "
       "Used to exercise error handling paths.",
       SERVER | REQUIRES_RESTART,
       SettingsCategory::Testing);
  init("msg-error-injection-status",
       &message_error_injection_status,
       "NOBUFS",
       nullptr, // no validation
       "status that should be returned for a simulated message transmission "
       "error",
       SERVER | CLIENT | REQUIRES_RESTART,
       SettingsCategory::Testing);
  init("disable-trace-logger",
       &trace_logger_disabled,
       "false",
       nullptr, // no validation
       "If disabled, NoopTraceLogger will be used,"
       " otherwise FBTraceLogger is used",
       SERVER | CLIENT | REQUIRES_RESTART /* init'ed at startup */,
       SettingsCategory::Monitoring);
  init("outbytes-mb",
       &outbufs_mb_max_per_thread,
       "512",
       parse_positive<ssize_t>(),
       "per-thread limit on bytes pending in output evbuffers (in MB)",
       SERVER | CLIENT,
       SettingsCategory::Network);
  init("sendbuf-kb",
       &tcp_sendbuf_kb,
       "-1",
       parse_validate_lower_bound<ssize_t>(-1),
       "TCP socket sendbuf size in KB. Changing this setting on-the-fly will "
       "not "
       "apply it to existing sockets, only to newly created ones",
       SERVER | CLIENT,
       SettingsCategory::Network);
  init(
      "rcvbuf-kb",
      &tcp_rcvbuf_kb,
      "-1",
      parse_validate_lower_bound<ssize_t>(-1),
      "TCP socket rcvbuf size in KB. Changing this setting on-the-fly will not "
      "apply it to existing sockets, only to newly created ones",
      SERVER | CLIENT,
      SettingsCategory::Network);
  init(
      "nagle",
      &nagle,
      "false",
      nullptr, // no validation
      "enable Nagle's algorithm on TCP sockets. Changing this setting "
      "on-the-fly will not apply it to existing sockets, only to newly created "
      "ones",
      SERVER | CLIENT,
      SettingsCategory::Network);
  init(
      "outbuf-kb",
      &outbuf_overflow_kb,
      "32768",
      parse_positive<ssize_t>(),
      "max output buffer size (userspace extension of socket sendbuf) in KB. "
      "Changing this setting on-the-fly will not apply it to existing sockets, "
      "only to newly created ones", // TODO (t13429319): fix this
      SERVER | CLIENT,
      SettingsCategory::Network);
  init("output-max-records-kb",
       &output_max_records_kb,
       "1024",
       parse_validate_lower_bound<ssize_t>(-1),
       "amount of RECORD data to push to the client at once",
       SERVER | CLIENT,
       SettingsCategory::ReadPath);
  init("max-time-to-allow-socket-drain",
       &max_time_to_allow_socket_drain,
       "3min",
       validate_positive<ssize_t>(),
       "After hitting NOBUFS, amount of time a socket is allowed to "
       "successfully send a single message before it is closed.",
       SERVER | CLIENT,
       SettingsCategory::Network);
  init("max-cached-digest-record-queued-kb",
       &max_cached_digest_record_queued_kb,
       "256",
       parse_positive<ssize_t>(),
       "amount of RECORD data to push to the client at once for cached "
       "digesting",
       SERVER | REQUIRES_RESTART /* used in Worker ctor */,
       SettingsCategory::Recovery);
  init(
      "max-active-cached-digests",
      &max_active_cached_digests,
      "2000",
      parse_positive<ssize_t>(),
      "maximum number of active cached digest streams on a storage node at the "
      "same time",
      SERVER | REQUIRES_RESTART /* set at startup */,
      SettingsCategory::Recovery);
  init("max-record-bytes-read-at-once",
       &max_record_bytes_read_at_once,
       "1048576", // 1MB
       parse_positive<ssize_t>(),
       "amount of RECORD data to read from local log store at once",
       SERVER,
       SettingsCategory::ReadPath);
  init("max-record-read-execution-time",
       &max_record_read_execution_time,
       "1s",
       validate_positive<ssize_t>(),
       "Maximum execution time for reading records. 'max' means no limit.",
       SERVER | EXPERIMENTAL,
       SettingsCategory::ResourceManagement);
  init("read-requests",
       &requests_from_pipe,
       "128",
       parse_positive<ssize_t>(),
       "deprecated, to be removed",
       SERVER | CLIENT | DEPRECATED);
  init("execute-requests",
       &hi_requests_per_iteration,
       "13",
       parse_positive<ssize_t>(),
       "number of HI_PRI requests to process per worker event loop iteration",
       SERVER | CLIENT,
       SettingsCategory::Execution);
  init("mid_requests_per_iteration",
       &mid_requests_per_iteration,
       "2",
       parse_positive<ssize_t>(),
       "number of MID_PRI requests to process per worker event loop iteration",
       SERVER | CLIENT,
       SettingsCategory::Execution);
  init("lo_requests_per_iteration",
       &lo_requests_per_iteration,
       "1",
       parse_positive<ssize_t>(),
       "number of LO_PRI requests to process per worker event loop iteration",
       SERVER | CLIENT,
       SettingsCategory::Execution);
  init("worker-request-pipe-capacity",
       &worker_request_pipe_capacity,
       "524288",
       parse_positive<ssize_t>(),
       "size each worker request queue to hold this many requests",
       SERVER | CLIENT | REQUIRES_RESTART /* sized at startup. This is tech
           debt as an MPSCQ-based request queue can be resized at any time. */
       ,
       SettingsCategory::Execution);
  init("prioritized-task-execution",
       &enable_executor_priority_queues,
       "true",
       nullptr,
       "Enable prioritized execution of requests within CPU executor. Setting "
       "this false ignores per request and per message ExecutorPriority.",
       SERVER | CLIENT | REQUIRES_RESTART,
       SettingsCategory::Execution);
  init("request-exec-threshold",
       &request_execution_delay_threshold,
       "10ms",
       validate_positive<ssize_t>(),
       "Request Execution time beyond which it is considered slow, "
       "and 'worker_slow_requests' stat is bumped",
       SERVER | CLIENT,
       SettingsCategory::Monitoring);
  init("slow-background-task-threshold",
       &slow_background_task_threshold,
       "100ms",
       validate_positive<ssize_t>(),
       "Background task execution time beyond which it is considered slow, "
       "and we log it",
       SERVER | CLIENT,
       SettingsCategory::Monitoring);
  init("flow-groups-run-yield-interval",
       &flow_groups_run_yield_interval,
       "2ms",
       validate_positive<ssize_t>(),
       "Maximum duration of Sender::runFlowGroups() before yielding to the "
       "event loop.",
       SERVER,
       SettingsCategory::ResourceManagement);
  init("flow-groups-run-deadline",
       &flow_groups_run_deadline,
       "5ms",
       validate_positive<ssize_t>(),
       "Maximum delay (plus one cycle of the event loop) between "
       "a request to run FlowGroups and Sender::runFlowGroups() executing.",
       SERVER,
       SettingsCategory::ResourceManagement);
  init("read-messages",
       &incoming_messages_max_per_socket,
       "128",
       parse_positive<ssize_t>(),
       "read up to this many incoming messages before returning to libevent",
       SERVER | CLIENT,
       SettingsCategory::Network);
  init("incoming-messages-max-bytes-limit",
       &incoming_messages_max_bytes_limit,
       "524288000",
       parse_positive<ssize_t>(),
       "maximum byte limit of unprocessed messages within the system.",
       SERVER | CLIENT | REQUIRES_RESTART,
       SettingsCategory::Network);
  init("payload-inline",
       &max_payload_inline,
       "1024",
       parse_positive<ssize_t>(),
       "max message payload size that we store in a flat buffer after header",
       SERVER | CLIENT,
       SettingsCategory::WritePath);
  init("max-inflight-storage-tasks",
       &max_inflight_storage_tasks,
       "4096",
       parse_validate_lower_bound<ssize_t>(2),
       "max number of StorageTask instances that one worker thread may "
       "have in flight to each database shard",
       SERVER | REQUIRES_RESTART /* queues sized at startup */,
       SettingsCategory::ResourceManagement);
  init("max-concurrent-purging-for-release-per-shard",
       &max_concurrent_purging_for_release_per_shard,
       "4",
       parse_validate_lower_bound<ssize_t>(2),
       "max number of concurrently running purging state machines for RELEASE "
       "messages per each storage shard for each worker",
       SERVER | REQUIRES_RESTART /* used in PurgeScheduler ctor */,
       SettingsCategory::Recovery);
  init("enable-record-cache",
       &enable_record_cache,
       "true",
       nullptr, // no validation
       "Enable caching of unclean records on storage nodes. Used to minimize "
       "local log store access during log recovery.",
       SERVER | REQUIRES_RESTART /* used in LogStorageStateMap ctor*/,
       SettingsCategory::Recovery);
  init("record-cache-max-size",
       &record_cache_max_size,
       "4294967296", // 4GB
       parse_nonnegative<ssize_t>(),
       "Maximum size enforced for the record cache, 0 for unlimited. If "
       "positive "
       "and record cache size grows more than that, it will start evicting "
       "records from the cache. This is also the maximum total number of bytes "
       "allowed to be persisted in record cache snapshots. For snapshot limit, "
       "this is enforced per-shard with each shard having its own limit of "
       "(max_record_cache_snapshot_bytes / num_shards).",
       SERVER,
       SettingsCategory::Recovery);
  init("record-cache-monitor-interval",
       &record_cache_monitor_interval,
       // use 2s by default since we can only receive 2.5GB in 2 sec over a
       // 10Gbps link
       "2s",
       validate_positive<ssize_t>(),
       "polling interval for the record cache eviction thread for monitoring "
       "the "
       "size of the record cache.",
       SERVER,
       SettingsCategory::Recovery);

  init("abort-on-failed-check",
       &abort_on_failed_check,
       "true",  // ClientSettingsImpl overrides this default value
       nullptr, // no validation
       "When an ld_check() fails, call abort().  If not, just continue "
       "executing.  We'll log either way.",
       SERVER | CLIENT,
       SettingsCategory::Testing,
       "`false` in the client, `true` elsewhere");
  init("abort-on-failed-catch",
       &abort_on_failed_catch,
       folly::kIsDebug ? "true" : "false",
       nullptr, // no validation
       "When an ld_catch() fails, call abort().  If not, just continue "
       "executing.  We'll log either way.",
       SERVER | CLIENT,
       SettingsCategory::Testing,
       "`true` in debug builds, `false` in release builds");
  init("watchdog-poll-interval",
       &watchdog_poll_interval_ms,
       "5000ms",
       validate_positive<ssize_t>(),
       "Interval after which watchdog detects stuck workers",
       SERVER | CLIENT | REQUIRES_RESTART /* used in Processor ctor */,
       SettingsCategory::Monitoring);
  init("watchdog-abort-on-stall",
       &watchdog_abort_on_stall,
       "false",
       nullptr, // no validation
       "Should we abort logdeviced if watchdog detected stalled workers.",
       SERVER | CLIENT,
       SettingsCategory::Monitoring);
  init("watchdog-print-bt-on-stall",
       &watchdog_print_bt_on_stall,
       "true",
       nullptr, // no validation
       "Should we print backtrace of stalled workers.",
       SERVER | CLIENT,
       SettingsCategory::Monitoring);
  init("watchdog-bt-ratelimit",
       &watchdog_bt_ratelimit,
       "10/120s",
       [](const std::string& val) -> rate_limit_t {
         rate_limit_t res;
         int rv = parse_rate_limit(val.c_str(), &res);
         if (rv != 0) {
           throw boost::program_options::error(
               "Invalid value(" + val +
               ") for --watchdog-bt-ratelimit."
               "Expected format is <count>/<duration><unit>, e.g. 1/1s");
         }
         return res;
       },
       "Maximum allowed rate of printing backtraces.",
       SERVER | CLIENT | REQUIRES_RESTART /* Passed to WatchDogThread ctor */,
       SettingsCategory::Monitoring);
  init("purging-use-metadata-log-only",
       &purging_use_metadata_log_only,
       "false",
       nullptr,
       "If true, the NodeSetFinder within PurgeUncleanEpochs will use"
       "only the metadata log as source for fetching historical metadata."
       "used only for migration",
       SERVER,
       SettingsCategory::Recovery);
  init(
      "send-to-gossip-port",
      &send_to_gossip_port,
      "true",
      nullptr, // no validation
      "Send gossip messages to destination's gossip port (if one is specified) "
      "instead of data port. This is the default. Sending gossips to data port "
      "may increase gossipping delays and adversely affect the accuracy of "
      "failure detection.",
      SERVER | DEPRECATED,
      SettingsCategory::FailureDetector);
  init("ssl-on-gossip-port",
       &ssl_on_gossip_port,
       "false",
       nullptr,
       "If true, gossip port will reject all plaintext connections. Only SSL "
       "connections will be accepted. WARNING: Any change to this setting "
       "should only be performed while send-to-gossip-port = false, in order "
       "to avoid failure detection issues while the setting change propagates "
       "through the cluster.",
       SERVER,
       SettingsCategory::Security);
  init("max-nodes",
       &max_nodes,
       "512",
       parse_positive<ssize_t>(),
       "Number of preallocated nodes in the cluster. Used for sizing "
       "data structures of the failure detector.",
       SERVER | REQUIRES_RESTART,
       SettingsCategory::Core);
  init("sbr-node-threshold",
       &space_based_retention_node_threshold,
       "0",
       validate_range<double>(0, 1),
       "threshold fraction of full nodes which triggers space-based retention, "
       "if enabled (sequencer-only option), 0 means disabled",
       SERVER,
       SettingsCategory::LogsDB);
  init("gray-list-threshold",
       &gray_list_nodes_threshold,
       "0.25",
       validate_range<double>(0, 1),
       "if the number of storage nodes graylisted on the write path of a log "
       "exceeds this fraction of the log's nodeset size the gray list will be "
       "cleared to make sure that copysets can still be picked",
       SERVER,
       SettingsCategory::WritePath);
  init("store-timeout",
       &store_timeout,
       "10ms..1min",
       validate_positive<ssize_t>(),
       "timeout for attempts to store a record copy on a specific "
       "storage node. This value is used by sequencers only and is NOT the "
       "client request timeout.",
       SERVER,
       SettingsCategory::WritePath);
  init("connect-throttle",
       &connect_throttle,
       "1ms..10s",
       validate_nonnegative<ssize_t>(),
       "timeout after it which two nodes retry to connect when they loose a "
       "a connection. Used in ConnectThrottle to ensure we don't retry too  "
       "often. Needs restart to load the new values.",
       SERVER | CLIENT | REQUIRES_RESTART,
       SettingsCategory::Network);
  init("disable-chain-sending",
       &disable_chain_sending,
       "false",
       nullptr, // no validation
       "never send a wave of STORE messages through a chain",
       SERVER,
       SettingsCategory::WritePath);
  init("sbr-low-watermark-check-interval",
       &sbr_low_watermark_check_interval,
       "60s",
       validate_positive<ssize_t>(),
       "Time after which space based trim check can be done on a nodeset",
       SERVER,
       SettingsCategory::LogsDB);
  init("nospace-retry-interval",
       &nospace_retry_interval,
       "60s",
       validate_positive<ssize_t>(),
       "Time interval during which a sequencer will not route record copies "
       "to a storage node that reported an out of disk space condition.",
       SERVER,
       SettingsCategory::WritePath);
  init("node-health-check-retry-interval",
       &node_health_check_retry_interval,
       "5s",
       validate_positive<ssize_t>(),
       "Time interval during which a node health check probe will not be sent "
       "if there is an outstanding request for the same node in the nodeset",
       SERVER,
       SettingsCategory::WritePath);
  init("slow-node-retry-interval",
       &slow_node_retry_interval,
       "600s",
       validate_positive<ssize_t>(),
       "After a sequencer's request to store a record copy on a storage node "
       "times out that sequencer will graylist that node for at least this "
       "time interval. "
       "The sequencer will not pick graylisted nodes for copysets unless "
       "--gray-list-threshold is reached or no valid copyset can be selected "
       "from nodeset nodes not yet graylisted. "
       "For outlier-based graylisting increases exponentially for each "
       "new graylisting up until 10x of this value and decreases "
       "at linear rate down to this value when not graylisted",
       SERVER,
       SettingsCategory::WritePath);
  init("check-node-health-request-timeout",
       &check_node_health_request_timeout,
       "120s",
       validate_positive<ssize_t>(),
       "Timeout for health check probes that sequencers send to unresponsive "
       "storage nodes. If no reply arrives after timeout, another probe is "
       "sent.",
       SERVER,
       SettingsCategory::WritePath);
  init("unroutable-retry-interval",
       &unroutable_retry_interval,
       "60s",
       validate_positive<ssize_t>(),
       "Time interval during which a sequencer will not pick for copysets a "
       "storage node whose IP address was reported unroutable by the socket "
       "layer",
       SERVER,
       SettingsCategory::WritePath);
  init("overloaded-retry-interval",
       &overloaded_retry_interval,
       "1s",
       validate_positive<ssize_t>(),
       "Time interval during which a sequencer will not route record copies "
       "to a storage node that reported itself overloaded (storage task queue "
       "too long).",
       SERVER,
       SettingsCategory::WritePath);
  init("disabled-retry-interval",
       &disabled_retry_interval,
       "30s",
       validate_nonnegative<ssize_t>(),
       "Time interval during which a sequencer will not route record copies "
       "to a storage node that reported a permanent error.",
       SERVER,
       SettingsCategory::WritePath);
  init("nodeset-state-refresh-interval",
       &nodeset_state_refresh_interval,
       "1s",
       validate_positive<ssize_t>(),
       "Time interval that rate-limits how often a sequencer can refresh "
       "the states of nodes in the nodeset in use",
       SERVER,
       SettingsCategory::WritePath);
  init("connect-timeout",
       &connect_timeout,
       "100ms",
       validate_nonnegative<ssize_t>(),
       "connection timeout when establishing a TCP connection to a node",
       SERVER | CLIENT,
       SettingsCategory::Network);
  init("connection-retries",
       &connection_retries,
       "4",
       validate_nonnegative<ssize_t>(),
       "the number of TCP connection retries before giving up",
       SERVER | CLIENT,
       SettingsCategory::Network);
  init("connect-timeout-retry-multiplier",
       &connect_timeout_retry_multiplier,
       "3",
       validate_positive<double>(),
       "Multiplier that is applied to the connect timeout after every failed "
       "connection attempt",
       SERVER | CLIENT,
       SettingsCategory::Network);
  init("handshake-timeout",
       &handshake_timeout,
       "1s",
       validate_nonnegative<ssize_t>(),
       "LogDevice protocol handshake timeout",
       SERVER | CLIENT,
       SettingsCategory::Network);
  init("inline-message-execution",
       &inline_message_execution,
       "false",
       nullptr,
       "Indicates whether message should be processed right after "
       "deserialization. Usually within new worker model all messages are "
       "processed after posting them into the work context. This option works "
       "only when worker context is run with previous eventloop architecture.",
       SERVER | CLIENT | REQUIRES_RESTART,
       SettingsCategory::Network);

  init("per-worker-storage-task-queue-size",
       &per_worker_storage_task_queue_size,
       "1",
       parse_positive<ssize_t>(),
       "max number of StorageTask instances to buffer in each Worker for "
       "each local log store shard",
       SERVER | REQUIRES_RESTART /* queue is sized in Worker's ctor*/,
       SettingsCategory::ResourceManagement);
  init("disable-graylisting",
       &disable_graylisting,
       "false",
       nullptr, // no validation
       "setting this to true disables graylisting nodes by sequencers "
       "in the write path",
       SERVER,
       SettingsCategory::WritePath);
  init("disable-outlier-based-graylisting",
       &disable_outlier_based_graylisting,
       "true",
       nullptr, // no validation
       "setting this to true disables the outlier based graylisting nodes by "
       "sequencers in the write path",
       SERVER | EXPERIMENTAL,
       SettingsCategory::WritePath);
  init("graylisting-grace-period",
       &graylisting_grace_period,
       "300s",
       nullptr, // no validation
       "The duration through which a node need to be consistently an outlier to"
       " get graylisted",
       SERVER,
       SettingsCategory::WritePath);
  init("graylisting-monitored-period",
       &graylisting_monitored_period,
       "120s",
       nullptr, // no validation
       "The duration through which a recently ungraylisted node will be "
       "monitored and graylisted as soon as it becomes an outlier",
       SERVER,
       SettingsCategory::WritePath);
  init("graylisting-refresh-interval",
       &graylisting_refresh_interval,
       "30s",
       nullptr, // no validation
       "The interval at which the graylists are refreshed",
       SERVER,
       SettingsCategory::WritePath);
  init("enable-read-throttling",
       &enable_read_throttling,
       "false",
       nullptr, // no validation
       "Throttle Disk I/O due to log read streams",
       SERVER,
       SettingsCategory::ReadPath);
  init("enable-adaptive-store-timeout",
       &enable_adaptive_store_timeout,
       "false",
       nullptr, // no validation
       "decides whether to enable an adaptive store timeout",
       SERVER | EXPERIMENTAL,
       SettingsCategory::WritePath);
  init("write-batch-size",
       &write_batch_size,
       "1024",
       parse_positive<ssize_t>(),
       "max number of records for a storage thread to write in one batch",
       SERVER,
       SettingsCategory::Storage);
  init("write-batch-bytes",
       &write_batch_bytes,
       "1048576", // 1MB
       parse_positive<ssize_t>(),
       "min number of payload bytes for a storage thread to write in one batch "
       "unless write-batch-size is reached first",
       SERVER,
       SettingsCategory::Storage);
  init("storage-tasks-use-drr",
       &storage_tasks_use_drr,
       "false",
       nullptr,
       "Use DRR for scheduling read IO's.",
       SERVER | REQUIRES_RESTART,
       SettingsCategory::Storage);
  init("storage-tasks-drr-quanta",
       &storage_tasks_drr_quanta,
       "1",
       parse_positive<uint64_t>(),
       "Default quanta per-principal. 1 implies request based scheduling. "
       "Use something like 1MB for byte based scheduling.",
       SERVER,
       SettingsCategory::Storage);

#define STORAGE_TASK_PRINCIPAL(name, key, shareVal)                      \
  init("storage-task-" #key "-share",                                    \
       &storage_task_shares[(uint64_t)StorageTaskPrincipal::name].share, \
       shareVal,                                                         \
       parse_positive<uint64_t>(),                                       \
       "The share for principal " #key " in the DRR scheduler.",         \
       SERVER,                                                           \
       SettingsCategory::Storage);
#include "logdevice/common/storage_task_principals.inc"
#undef STORAGE_TASK_PRINCIPAL

  init("max-server-read-streams",
       &max_server_read_streams,
       "150000",
       parse_nonnegative<ssize_t>(),
       "max number of read streams clients can establish to the server, "
       "per worker",
       SERVER,
       SettingsCategory::ResourceManagement);
  init("queue-drop-overload-time",
       &queue_drop_overload_time,
       "1s",
       validate_positive<ssize_t>(),
       "max time after worker's storage task queue is dropped "
       "before it stops being considered overloaded",
       SERVER,
       SettingsCategory::ResourceManagement);
  init("queue-size-overload-percentage",
       &queue_size_overload_percentage,
       "50",
       validate_range<ssize_t>(0, 100),
       "percentage of per-worker-storage-task-queue-size that can be buffered "
       "before the queue is considered overloaded",
       SERVER,
       SettingsCategory::ResourceManagement);
  init("concurrent-log-recoveries",
       &concurrent_log_recoveries,
       "400",
       parse_positive<ssize_t>(),
       "limit on the number of logs that can be in recovery at the same time",
       SERVER,
       SettingsCategory::Recovery);
  init("appender-buffer-queue-cap",
       &appender_buffer_queue_cap,
       "10000",
       parse_nonnegative<ssize_t>(),
       "capacity of per-log queue of pending writes while sequencer "
       " is initializing or activating",
       SERVER | REQUIRES_RESTART /* queue sized in
                                    Worker/AppenderBuffer ctors */
       ,
       SettingsCategory::WritePath);
  init("appender-buffer-process-batch",
       &appender_buffer_process_batch,
       "20",
       parse_positive<ssize_t>(),
       "batch size for processing per-log queue of pending writes",
       SERVER,
       SettingsCategory::WritePath);

  init("test-appender-skip-stores",
       &test_appender_skip_stores,
       "false",
       nullptr,
       "Allow appenders to skip sending data to storage node. Currently used"
       " in tests to make sure an appender state machine is running",
       SERVER,
       SettingsCategory::Testing);

  init("time-delay-before-force-abort",
       &time_delay_before_force_abort,
       "400",
       nullptr,
       "Time delay before force abort of remaining work is attempted during "
       "shutdown. The value is in 50ms time periods. The quiescence condition "
       "is checked once every 50ms time period. When the timer expires for "
       "the first time, all pending requests are aborted and the timer is "
       "restarted. On second expiration all remaining TCP connections are "
       "reset (RST packets sent).",
       SERVER,
       SettingsCategory::Core);

  init("client-read-buffer-size",
       &client_read_buffer_size,
       "512",
       parse_positive<ssize_t>(),
       "number of records to buffer per read stream in the client object while "
       "reading. If this setting is changed on-the-fly, the change will only "
       "apply to new reader instances",
       SERVER | CLIENT,
       SettingsCategory::ReadPath);
  init("client-read-flow-control-threshold",
       &client_read_flow_control_threshold,
       "0.7",
       validate_range<double>(std::numeric_limits<double>::min(), 1),
       "threshold (relative to buffer size) at which the client broadcasts "
       "window update messages (less means more often)",
       CLIENT | SERVER /* for event log reads */,
       SettingsCategory::ReadPath);
  init("client-epoch-metadata-cache-size",
       &client_epoch_metadata_cache_size,
       "50000",
       parse_nonnegative<ssize_t>(),
       "maximum number of entries in the client-side epoch metadata cache. "
       "Set it to 0 to disable the epoch metadata cache.",
       CLIENT | REQUIRES_RESTART,
       SettingsCategory::ReadPath);
  init("client-readers-flow-tracer-period",
       &client_readers_flow_tracer_period,
       "0s",
       validate_nonnegative<ssize_t>(),
       "Period for logging in logdevice_readers_flow scuba table and for "
       "triggering certain sampling actions for monitoring. Set it to 0 to "
       "disable feature.",
       CLIENT,
       SettingsCategory::Monitoring);
  init("client-readers-flow-tracer-unhealthy-publish-weight",
       &client_readers_flow_tracer_unhealthy_publish_weight,
       "5.0",
       validate_positive<ssize_t>(),
       "Weight given to traces of unhealthy readers when publishing samples "
       "(for improved debuggability).",
       CLIENT,
       SettingsCategory::Monitoring);
  init("client-readers-flow-tracer-lagging-metric-num-sample-groups",
       &client_readers_flow_tracer_lagging_metric_num_sample_groups,
       "3",
       validate_nonnegative<ssize_t>(),
       "Maximum number of samples that are kept by ClientReadersFlowTracer for "
       "computing relative reading speed in relation to writing speed. See "
       "client_readers_flow_tracer_lagging_slope_threshold.",
       CLIENT,
       SettingsCategory::Monitoring);
  init("client-readers-flow-tracer-lagging-metric-sample-group-size",
       &client_readers_flow_tracer_lagging_metric_sample_group_size,
       "20",
       validate_nonnegative<ssize_t>(),
       "Number of samples in ClientReadersFlowTracer that are aggregated and "
       "recorded as one entry. See "
       "client-readers-flow-tracer-lagging-metric-sample-group-size.",
       CLIENT,
       SettingsCategory::Monitoring);
  init(
      "client-readers-flow-tracer-lagging-slope-threshold",
      &client_readers_flow_tracer_lagging_slope_threshold,
      "-0.3",
      validate_range<double>(-100, 100),
      "If a reader's lag increase at at least this rate, the reader is "
      "considered lagging (rate given as variation of time lag per time unit). "
      "If the desired read ratio needs to be x\% of the write ratio, set this "
      "threshold to be (1 - x / 100).",
      CLIENT,
      SettingsCategory::Monitoring);
  init("client-test-force-stats",
       &client_test_force_stats,
       "false",
       nullptr, // no validation
       "force instantiation of StatsHolder within ClientImpl even if stats "
       "publishing is disabled",
       CLIENT | REQUIRES_RESTART,
       SettingsCategory::Testing);
  init(
      "client-is-log-empty-grace-period",
      &client_is_log_empty_grace_period,
      "5s",
      validate_nonnegative<ssize_t>(),
      "After receiving responses to an isLogEmpty() request from an f-majority "
      "of nodes, wait up to this long for more nodes to chime in if there is "
      "not yet consensus.",
      CLIENT | EXPERIMENTAL,
      SettingsCategory::ReadPath);
  init("release-retry-interval",
       &release_retry_interval,
       "20s",
       validate_positive<ssize_t>(),
       "RELEASE message retry period",
       SERVER,
       SettingsCategory::WritePath);
  init("release-broadcast-interval",
       &release_broadcast_interval,
       "300s",
       validate_positive<ssize_t>(),
       "the time interval for periodic broadcasts of RELEASE "
       "messages by sequencers of regular logs. Such broadcasts are not "
       "essential for correct cluster operation. They are used as the last "
       "line of defence to make sure storage nodes deliver all records "
       "eventually even if a regular (point-to-point) RELEASE message "
       "is lost due to a TCP connection failure. See also "
       "--release-broadcast-interval-internal-logs.",
       SERVER,
       SettingsCategory::WritePath);
  init("release-broadcast-interval-internal-logs",
       &release_broadcast_interval_internal_logs,
       "5s",
       validate_positive<ssize_t>(),
       "Same as --release-broadcast-interval but instead applies to internal "
       "logs, currently the event logs and logsconfig logs",
       SERVER,
       SettingsCategory::WritePath);
  init("recovery-grace-period",
       &recovery_grace_period,
       "100ms",
       validate_nonnegative<ssize_t>(),
       "Grace period time used by epoch recovery after it acquires an "
       "authoritative incomplete digest but wants to wait more time for "
       "an authoritative complete digest. Millisecond granularity. Can be 0. ",
       SERVER,
       SettingsCategory::Recovery);
  init("event-log-grace-period",
       &event_log_grace_period,
       "10s",
       validate_nonnegative<ssize_t>(),
       "grace period before considering event log caught up",
       SERVER,
       SettingsCategory::Rebuilding);
  init("recovery-timeout",
       &recovery_timeout,
       "120s",
       validate_positive<ssize_t>(),
       "epoch recovery timeout. Millisecond granularity.",
       SERVER,
       SettingsCategory::Recovery);
  init("gap-grace-period",
       &gap_grace_period,
       "100ms",
       validate_nonnegative<ssize_t>(),
       "gap detection grace period for all logs, including data logs, "
       "metadata logs, and internal state machine logs. Millisecond "
       "granularity. "
       "Can be 0.",
       SERVER | CLIENT,
       SettingsCategory::ReadPath);
  init("data-log-gap-grace-period",
       &data_log_gap_grace_period,
       "0ms",
       validate_nonnegative<ssize_t>(),
       "When non-zero, replaces gap-grace-period for data logs.",
       SERVER | CLIENT,
       SettingsCategory::ReadPath);
  init("metadata-log-gap-grace-period",
       &metadata_log_gap_grace_period,
       "0ms",
       validate_nonnegative<ssize_t>(),
       "When non-zero, replaces gap-grace-period for metadata logs.",
       SERVER | CLIENT,
       SettingsCategory::ReadPath);
  init("reader-stalled-grace-period",
       &reader_stalled_grace_period,
       "30s",
       validate_nonnegative<ssize_t>(),
       "Amount of time we wait before declaring a reader stalled because we "
       "can't read the metadata or data log. "
       "When this grace period expires, the client "
       "stat \"read_streams_stalled\" is bumped and record to scuba ",
       SERVER | CLIENT,
       SettingsCategory::Monitoring);
  init("reader-stuck-threshold",
       &reader_stuck_threshold,
       "121s", // 2 min + 1 sec
       validate_nonnegative<ssize_t>(),
       "Amount of time we wait before we report a read stream that is "
       "considered stuck.",
       SERVER | CLIENT,
       SettingsCategory::Monitoring);
  init("reader-lagging-threshold",
       &reader_lagging_threshold,
       "2min",
       validate_nonnegative<ssize_t>(),
       "Amount of time we wait before we report a read stream that is "
       "considered lagging.",
       SERVER | CLIENT,
       SettingsCategory::Monitoring);
  init(
      "log-state-recovery-interval",
      &log_state_recovery_interval,
      "500ms",
      validate_nonnegative<ssize_t>(),
      "interval between consecutive attempts by a storage node to "
      "obtain the attributes of a log residing on that storage node "
      "Such 'log state recovery' is performed independently for each log upon "
      "the first request to start delivery of records of that log. "
      "The attributes to be recovered include the LSN of the last cumulatively "
      "released record in the log, which may have to be requested from the "
      "log's sequencer over the network.",
      SERVER | REQUIRES_RESTART /* init'ed with this in Procesor's ctor */,
      SettingsCategory::ReadPath);
  init("seq-state-reply-timeout",
       &get_seq_state_reply_timeout,
       "2s",
       validate_positive<ssize_t>(),
       "how long to wait for a reply to a 'get sequencer state' request before "
       "retrying (usually to a different node)",
       SERVER | CLIENT,
       SettingsCategory::Sequencer);
  init(
      "seq-state-backoff-time",
      &seq_state_backoff_time,
      "1s..10s",
      validate_positive<ssize_t>(),
      "how long to wait before resending a 'get sequencer state' request after "
      "a timeout.",
      SERVER | CLIENT,
      SettingsCategory::Sequencer);
  init("check-seal-req-min-timeout",
       &check_seal_req_min_timeout,
       "500ms",
       validate_positive<ssize_t>(),
       "before a sequencer returns its state in response to a 'get "
       "sequencer state' request the sequencer checks that it is the most "
       "recent (highest numbered) sequencer for the log. It performs the check "
       "by sending a 'check seal' request to a valid copyset of nodes in the "
       "nodeset of the sequencer's epoch. The 'check seal' request looks "
       "for a seal record placed by a higher-numbered sequencer. This setting "
       "sets the timeout for 'check seal' requests. The timeout is set to the "
       "smaller of this value and half the value of --seq-state-reply-timeout.",
       SERVER,
       SettingsCategory::Sequencer);
  init("update-metadata-map-interval",
       &update_metadata_map_interval,
       "1h",
       nullptr,
       "Sequencer has a timer for periodically reading metadata logs and "
       "refreshing the in memory metadata_map_. This setting specifies the "
       "interval for this timer",
       SERVER,
       SettingsCategory::Sequencer);
  init("delete_log_metadata_request_timeout",
       &delete_log_metadata_request_timeout,
       "30000ms",
       validate_positive<ssize_t>(),
       "A timeout to wait for DELETE_LOG_METADATA_REPLY messages after a"
       "DELETE_LOG_METADATA message.",
       CLIENT | INTERNAL_ONLY | EXPERIMENTAL);
  init(
      "cluster-state-refresh-interval",
      &cluster_state_refresh_interval,
      "10s",
      validate_positive<ssize_t>(),
      "how frequently to search for the sequencer in case of an append timeout",
      CLIENT,
      SettingsCategory::FailureDetector);
  init("enable-is-log-empty-v2",
       &enable_is_log_empty_v2,
       "false",
       nullptr,
       "When enabled, the V2 implementation will be used to process all "
       "isLogEmpty requests.",
       CLIENT,
       SettingsCategory::Core);
  init(
      "enable-initial-get-cluster-state",
      &enable_initial_get_cluster_state,
      "true",
      nullptr,
      "Enable executing a GetClusterState request to retrieve the state of the "
      "cluster as soon as the client is created",
      CLIENT,
      SettingsCategory::FailureDetector);
  init("test-get-cluster-state-recipients",
       &test_get_cluster_state_recipients_,
       "",
       parse_recipients_list,
       "Force get-cluster-state recipients as a comma-separated list of node "
       "ids",
       CLIENT,
       SettingsCategory::Testing);
  init(
      "checksum-bits",
      &checksum_bits,
      "32",
      checksum_bits_notifier,
      "how big a checksum to include with newly appended records (0, 32 or 64)",
      SERVER | CLIENT,
      SettingsCategory::WritePath);
  init(
      "mutation-timeout",
      &mutation_timeout,
      "500ms",
      validate_positive<ssize_t>(),
      "initial timeout used during the mutation phase of log recovery to store "
      "enough copies of a record or a hole plug",
      SERVER,
      SettingsCategory::Recovery);
  init("write-sticky-copysets",
       &write_sticky_copysets_deprecated,
       "true",
       nullptr, // no validation
       "DEPRECATED. Instead, use --enable-sticky-copysets to enable copyset "
       "stickiness and --write-copyset-index to write the copyset index. If "
       "set to `false`, has the same effect as --enable-sticky-copysets=false "
       "--write-copyset-index=false. Otherwise has no effect.",
       SERVER | DEPRECATED | REQUIRES_RESTART /* Used in CopySetManager ctor */,
       SettingsCategory::WritePath);
  init("enable-sticky-copysets",
       &enable_sticky_copysets,
       "true",
       nullptr, // no validation
       "If set, sequencers will enable sticky copysets. Doesn't affect the "
       "copyset index.",
       SERVER | REQUIRES_RESTART /* Used in CopySetManager ctor */,
       SettingsCategory::WritePath);
  init("sticky-copysets-block-size",
       &sticky_copysets_block_size,
       "33554432", // 32MB
       parse_positive<ssize_t>(),
       "The total size of processed appends (in bytes), after which the sticky "
       "copyset manager will start a new block.",
       SERVER | REQUIRES_RESTART /* Used in CopySetManager ctor */,
       SettingsCategory::WritePath);
  init(
      "sticky-copysets-block-max-time",
      &sticky_copysets_block_max_time,
      "10min",
      validate_positive<ssize_t>(),
      "The time since starting the last block, after which the copyset manager "
      "will consider it expired and start a new one.",
      SERVER | REQUIRES_RESTART /* Used in CopySetManager ctor */,
      SettingsCategory::WritePath);
  init("write-copyset-index",
       &write_copyset_index,
       "true",
       nullptr, // no validation
       "If set, storage nodes will write the copyset index for all records. "
       "This must be set before --rocksdb-use-copyset-index is enabled. "
       "Doesn't affect copyset stickiness",
       SERVER,
       SettingsCategory::WritePath);
  init("iterator-cache-ttl",
       &iterator_cache_ttl,
       "20s",
       validate_positive<ssize_t>(),
       "expiration time of idle RocksDB iterators in the iterator cache.",
       SERVER,
       SettingsCategory::RocksDB);
  init("max-protocol",
       &max_protocol,
       std::to_string(Compatibility::MAX_PROTOCOL_SUPPORTED).c_str(),
       validate_range<ssize_t>(Compatibility::MIN_PROTOCOL_SUPPORTED,
                               Compatibility::MAX_PROTOCOL_SUPPORTED),
       "maximum version of LogDevice protocol that the server/client will "
       "accept",
       SERVER | CLIENT,
       SettingsCategory::Network);
  init("max-total-appenders-size-soft",
       &max_total_appenders_size_soft,
       "524288000", // 500MB
       parse_positive<ssize_t>(),
       "Total size in bytes of running Appenders accross all workers after "
       "which "
       "we start taking measures to reduce the Appender residency time.",
       SERVER,
       SettingsCategory::ResourceManagement);
  init("max-total-appenders-size-hard",
       &max_total_appenders_size_hard,
       "629145600", // 600MB
       parse_positive<ssize_t>(),
       "Total size in bytes of running Appenders accross all workers after "
       "which "
       "we start rejecting new appends.",
       SERVER,
       SettingsCategory::ResourceManagement);
  init("max-total-buffered-append-size",
       &max_total_buffered_append_size,
       "1073741824", // 1GB
       parse_positive<ssize_t>(),
       "Total size in bytes of payloads buffered in BufferedWriters in "
       "sequencers for server-side batching and compression. Appends will "
       "be rejected when this threshold is significantly exceeded.",
       SERVER,
       SettingsCategory::ResourceManagement);
  init("no-redirect-duration",
       &no_redirect_duration,
       "5s",
       validate_positive<ssize_t>(),
       "when a sequencer activates upon request from a client, it does not "
       "redirect its clients to a different sequencer node for this amount of "
       "time (even if for instance the primary sequencer just started up and "
       "an older sequencer may be up and running)",
       SERVER,
       SettingsCategory::WritePath);
  init(
      "reactivation-limit",
      &reactivation_limit,
      "5/1s",
      [](const std::string& val) -> rate_limit_t {
        rate_limit_t res;
        int rv = parse_rate_limit(val.c_str(), &res);
        if (rv != 0) {
          throw boost::program_options::error(
              "Invalid value for --reactivation-limit. Expected format is "
              "<count>/<duration><unit>, e.g. 1/1s");
        }
        return res;
      },
      "Maximum allowed rate of sequencer reactivations. When exceeded, further "
      "appends will fail.",
      SERVER | REQUIRES_RESTART /* Passed to Sequencer ctor */,
      SettingsCategory::Sequencer);
  init("epoch-draining-timeout",
       &epoch_draining_timeout,
       "2s",
       validate_positive<ssize_t>(),
       "Maximum time allowed for sequencer to drain one epoch. Sequencer "
       "will abort draining the epoch if it takes longer than the timeout. "
       "A sequencer 'drains' its epoch (waits for all appenders to complete) "
       "while reactivating to serve a higher epoch.",
       SERVER,
       SettingsCategory::Sequencer);
  init("read-historical-metadata-timeout",
       &read_historical_metadata_timeout,
       "10s",
       validate_positive<ssize_t>(),
       "maximum time interval for a sequencer to get historical epoch metadata "
       "through reading the metadata log before retrying.",
       SERVER,
       SettingsCategory::Sequencer);
  init("check-metadata-log-empty-timeout",
       &check_metadata_log_empty_timeout,
       "300s",
       validate_positive<ssize_t>(),
       "Timeout for request that verifies that a metadata log does not already "
       "exist for a log that is presumed new and whose metadata provisioning "
       "has been initiated by a sequencer activation",
       SERVER,
       SettingsCategory::Configuration);
  init("max-payload-size",
       &max_payload_size,
       "1048576", // 1MB
       parse_validate_range<ssize_t>(16, MAX_PAYLOAD_SIZE_PUBLIC),
       ("The maximum payload size that will be accepted by the client library "
        "or the server. Can't be larger than " +
        folly::to<std::string>(MAX_PAYLOAD_SIZE_PUBLIC) + " bytes.")
           .c_str(),
       SERVER | CLIENT /* Exposed via Client::getMaxPayloadSize() */,
       SettingsCategory::ResourceManagement);
  init("write-find-time-index",
       &write_find_time_index,
       "false",
       nullptr, // no validation
       "Set this to true if you want findTime index to be written. "
       "A findTime index speeds up findTime() requests by maintaining an index "
       "from timestamps to LSNs in LogsDB data partitions.",
       SERVER,
       SettingsCategory::Performance);
  init("on-demand-logs-config",
       &on_demand_logs_config,
       "false",
       nullptr, // no validation
       "Set this to true if you want the client to get log configuration on "
       "demand from the server when log configuration is not included in the "
       "main config file.",
       CLIENT | REQUIRES_RESTART /* used in ClientImpl::create() */,
       SettingsCategory::Configuration);
  init("on-demand-logs-config-retry-delay",
       &on_demand_logs_config_retry_delay,
       "5ms..1s",
       validate_nonnegative<ssize_t>(),
       "When a client's attempt to get log configuration information from "
       "server "
       "on demand fails, the client waits this much before retrying.",
       CLIENT,
       SettingsCategory::Configuration);
  init("remote-logs-config-cache-ttl",
       &remote_logs_config_cache_ttl,
       "60s",
       validate_nonnegative<ssize_t>(),
       "The TTL for cache entries for the remote logs config. If the logs "
       "config is not available locally and is fetched from the server, this "
       "will determine how fresh the log configuration used by the client will "
       "be.",
       CLIENT | REQUIRES_RESTART /* used in ClientImpl::create() */,
       SettingsCategory::Configuration);
  init("alternative-layout-property",
       &alternative_layout_property,
       "",
       nullptr, // no validation
       "Set this to the name of an alternate layout property if you want the "
       "client to use this property of log configuration instead of standard "
       "layout. This is deprecated and designed to support specific Facebook "
       "use cases. Do not use.",
       CLIENT | REQUIRES_RESTART /* used in ClientImpl::create() */
           | DEPRECATED);
  init(
      "findtime-force-approximate",
      &findtime_force_approximate,
      "false",
      nullptr, // no validation
      "(server-only setting) Override the client-supplied FindKeyAccuracy with "
      "FindKeyAccuracy::APPROXIMATE. This makes the resource requirements of "
      "FindKey requests small and predictable, at the expense of accuracy",
      SERVER,
      SettingsCategory::Performance);
  init("read-storage-tasks-max-mem-bytes",
       &read_storage_tasks_max_mem_bytes,
       "16106127360", // 15GB
       parse_positive<size_t>(),
       "Maximum amount of memory that can be allocated by read storage tasks.",
       SERVER,
       SettingsCategory::ResourceManagement);
  init("append-stores-max-mem-bytes",
       &append_stores_max_mem_bytes,
       "2G",
       parse_positive<size_t>(),
       "Maximum total size of in-flight StoreStorageTasks from appenders and "
       "recoveries. Evenly divided among shards.",
       SERVER,
       SettingsCategory::ResourceManagement);
  init("rebuilding-stores-max-mem-bytes",
       &rebuilding_stores_max_mem_bytes,
       "2G",
       parse_positive<size_t>(),
       "Maxumun total size of in-flight StoreStorageTasks from rebuilding. "
       "Evenly divided among shards.",
       SERVER,
       SettingsCategory::ResourceManagement);
  init("initial-config-load-timeout",
       &initial_config_load_timeout,
       "15s",
       validate_positive<ssize_t>(),
       "maximum time to wait for initial server configuration until giving up",
       SERVER | REQUIRES_RESTART | CLI_ONLY,
       SettingsCategory::Configuration);
  init(
      "zk-create-root-znodes",
      &zk_create_root_znodes,
      "true",
      nullptr, // no validation
      "If \"false\", the root znodes for a tier should be pre-created "
      "externally before logdevice can do any ZooKeeper epoch store operations",
      SERVER | EXPERIMENTAL,
      SettingsCategory::Core);
  init("ssl-load-client-cert",
       &ssl_load_client_cert,
       "false",
       nullptr, // no validation
       "Set to include client certificate for mutual ssl authenticaiton",
       CLIENT | SERVER,
       SettingsCategory::Security);
  init("ssl-cert-path",
       &ssl_cert_path,
       "",
       nullptr, // no validation
       "Path to LogDevice SSL certificate.",
       SERVER | CLIENT | REQUIRES_RESTART /* used in Worker ctor */,
       SettingsCategory::Security);
  init("ssl-ca-path",
       &ssl_ca_path,
       "",
       nullptr, // no validation
       "Path to CA certificate.",
       SERVER | CLIENT | REQUIRES_RESTART /* used in Worker ctor */,
       SettingsCategory::Security);
  init("ssl-key-path",
       &ssl_key_path,
       "",
       nullptr, // no validation
       "Path to LogDevice SSL key.",
       SERVER | CLIENT | REQUIRES_RESTART /* used in Worker ctor */,
       SettingsCategory::Security);
  init("ssl-cert-refresh-interval",
       &ssl_cert_refresh_interval,
       "300s",
       validate_positive<ssize_t>(),
       "TTL for an SSL certificate that we have loaded from disk.",
       SERVER | CLIENT | REQUIRES_RESTART /* used in Worker ctor */,
       SettingsCategory::Security);
  init("ssl-boundary",
       &ssl_boundary,
       "none",
       nullptr, // no validation
       "Enable SSL in cross-X traffic, where X is the setting. Example: if set "
       "to \"rack\", all cross-rack traffic will be sent over SSL. Can be one "
       "of "
       "\"none\", \"node\", \"rack\", \"row\", \"cluster\", \"data_center\" or "
       "\"region\". If a value other than \"none\" or \"node\" is specified on "
       "the client, --my-location has to be specified as well.",
       SERVER | CLIENT,
       SettingsCategory::Security);
  init("my-location",
       &client_location,
       "",
       [](const std::string& val) -> folly::Optional<NodeLocation> {
         folly::Optional<NodeLocation> res;
         if (val.empty()) {
           return res;
         }
         res.assign(NodeLocation());
         if (res->fromDomainString(val) != 0) {
           throw boost::program_options::error(
               "Invalid value for --my-location. Expecting valid location "
               "string: \"{region}.{dc}.{cluster}.{row}.{rack}\"");
         }
         return res;
       },
       "{client-only setting}. Specifies the location of the machine running "
       "the "
       "client. Used for determining whether to use SSL based on "
       "--ssl-boundary. Also used in local SCD reading. "
       "Format: \"{region}.{dc}.{cluster}.{row}.{rack}\".",
       CLIENT | REQUIRES_RESTART /* saved in Sender::initMyLocation() */,
       SettingsCategory::Core);
  init("slow-ioprio",
       &slow_ioprio,
       "",
       [](const std::string& val) -> folly::Optional<std::pair<int, int>> {
         folly::Optional<std::pair<int, int>> res;
         if (parse_ioprio(val, &res) != 0) {
           throw boost::program_options::error(
               "value of --low-ioprio must be of the form "
               "<class>,<data> e.g. 2,6; " +
               val + " given.");
         }
         return res;
       },
       "IO priority to request for 'slow' storage threads. "
       "Storage threads in the 'slow' thread pool handle high-latency RocksDB "
       "IO requests,  primarily data reads. "
       "Not all kernel IO schedulers supports IO priorities."
       "See man ioprio_set for possible values."
       "\"any\" or \"\" to keep the default.",
       SERVER | REQUIRES_RESTART /* used once when ExecStorageThread starts */,
       SettingsCategory::ResourceManagement);

  init("checksumming-enabled",
       &checksumming_enabled,
       "false",
       nullptr, // no validation
       "A switch to turn on/off checksumming for all LogDevice protocol "
       "messages."
       " If false: no checksumming is done, "
       "If true: checksumming-blacklisted-messages is consulted.",
       SERVER | CLIENT | EXPERIMENTAL,
       SettingsCategory::Network);

  init(
      "checksumming-blacklisted-messages",
      &checksumming_blacklisted_messages,
      // see message_types.inc for message mnemonics
      // We can potentially leave out the following messages for performance:
      // APPEND, APPENDED, STORE, STORED, LOGS_CONFIG_API, LOGS_CONFIG_API_REPLY
      "",
      [](const std::string& val) -> std::set<char> {
        std::set<char> res;
        for (char c : val) {
          res.insert(c);
        }
        return res;
      },
      "Used to control what messages shouldn't be checksummed at "
      "the protocol layer",
      SERVER | CLIENT | REQUIRES_RESTART | EXPERIMENTAL,
      SettingsCategory::Network);

  init("scd-timeout",
       &scd_timeout,
       "300s",
       validate_nonnegative<ssize_t>(),
       "Timeout after which ClientReadStream considers a storage node down if "
       "it does not send any data for some time but the socket to it remains "
       "open.",
       SERVER /* for event log */ | CLIENT,
       SettingsCategory::ReaderFailover);
  init("scd-all-send-all-timeout",
       &scd_all_send_all_timeout,
       "600s",
       validate_nonnegative<ssize_t>(),
       "Timeout after which ClientReadStream fails over to asking all storage "
       "nodes to send everything they have if it is not able to make progress "
       "for some time",
       SERVER /* for event log */ | CLIENT,
       SettingsCategory::ReaderFailover);
  init(
      "verify-checksum-before-replicating",
      &verify_checksum_before_replicating,
      "true",
      nullptr, // no validation
      "If set, sequencers and rebuilding will verify checksums of records that "
      "have checksums. If there is a mismatch, sequencer will reject the "
      "append. Note that this setting doesn't make storage nodes verify "
      "checksums. Note that if not set, and "
      "--rocksdb-verify-checksum-during-store is set, a corrupted record kills "
      "write-availability for that log, as the appender keeps retrying and "
      "storage nodes reject the record.",
      SERVER,
      SettingsCategory::WritePath);
  init("default-log-namespace",
       &default_log_namespace,
       "",
       nullptr, // no validation
       "Default log namespace to use on the client.",
       CLIENT | DEPRECATED,
       SettingsCategory::Configuration);
  init("server-based-nodes-configuration-store-timeout",
       &server_based_nodes_configuration_store_timeout,
       "60s",
       validate_nonnegative<ssize_t>(),
       "The timeout of the Server Based Nodes Configuration Store's "
       "NODES_CONFIGURATION polling round.",
       CLIENT | SERVER,
       SettingsCategory::Configuration);
  init(
      "server-based-nodes-configuration-polling-wave-timeout",
      &server_based_nodes_configuration_polling_wave_timeout,
      "500ms..10s",
      validate_positive<ssize_t>(),
      "timeout settings for server based Nodes Configuration Store's multi-wave"
      "backoff retry behavior",
      CLIENT | SERVER,
      SettingsCategory::Configuration);
  init(
      "server-based-nodes-configuration-store-polling-responses",
      &server_based_nodes_configuration_store_polling_responses,
      "2",
      parse_positive<ssize_t>(),
      "how many successful responses for server based Nodes Configuration Store"
      "polling to wait for each round",
      CLIENT | SERVER,
      SettingsCategory::Configuration);
  init("server_based_nodes_configuration_store_polling_extra_requests",
       &server_based_nodes_configuration_store_polling_extra_requests,
       "1",
       parse_nonnegative<ssize_t>(),
       "how many extra requests to send for server based Nodes Configuration "
       "Store polling in addition to the required response for each wave",
       CLIENT | SERVER,
       SettingsCategory::Configuration);
  init("nodes-configuration-seed-servers",
       &nodes_configuration_seed_servers,
       "",
       nullptr, // no validation
       "The seed string that will be used to fetch the initial nodes "
       "configuration. It can be in the form string:<server1>,<server2>,etc. "
       "Or you can provide an smc tier via 'smc:<smc_tier>'. If it's empty, "
       "NCM client bootstraping is not used.",
       CLIENT,
       SettingsCategory::Configuration);
  init("nodes-configuration-init-retry-timeout",
       &nodes_configuration_init_retry_timeout,
       "500ms..5s",
       validate_positive<ssize_t>(),
       "timeout settings for the exponential backoff retry behavior for "
       "initializing Nodes Configuration for the first time",
       CLIENT | SERVER,
       SettingsCategory::Configuration);
  init("nodes-configuration-init-timeout",
       &nodes_configuration_init_timeout,
       "60s",
       validate_positive<ssize_t>(),
       "defines the maximum time allowed on the initial nodes configuration "
       "fetch.",
       CLIENT | SERVER,
       SettingsCategory::Configuration);
  init("use-tcp-keep-alive",
       &use_tcp_keep_alive,
       "true",
       nullptr, // no validation
       "Enable TCP keepalive for all connections",
       SERVER | CLIENT,
       SettingsCategory::Network);
  init("tcp-keep-alive-time",
       &tcp_keep_alive_time,
       "-1",
       nullptr, // no validation
       "TCP keepalive time. This is the time, in seconds, before the first "
       "probe will be sent. If negative the OS default will be used.",
       SERVER | CLIENT,
       SettingsCategory::Network);
  init("tcp-keep-alive-intvl",
       &tcp_keep_alive_intvl,
       "-1",
       nullptr, // no validation
       "TCP keepalive interval. The interval between successive probes."
       "If negative the OS default will be used.",
       SERVER | CLIENT,
       SettingsCategory::Network);
  init("tcp-keep-alive-probes",
       &tcp_keep_alive_probes,
       "-1",
       nullptr, // no validation
       "TCP keepalive probes. How many unacknowledged probes before the "
       "connection is considered broken. "
       "If negative the OS default will be used.",
       SERVER | CLIENT,
       SettingsCategory::Network);
  init("tcp-user-timeout",
       &tcp_user_timeout,
       "300000", // 5 min
       nullptr,  // no validation
       "The time in miliseconds that transmitted data may remain unacknowledged"
       "before TCP will close the connection. "
       "0 for system default. "
       "-1 to disable. "
       "default is 5min = 300000",
       SERVER | CLIENT,
       SettingsCategory::Network);
  init(
      "include-cluster-name-on-handshake",
      &include_cluster_name_on_handshake,
      "true",
      nullptr, // no validation
      "The cluster name of the connection initiator will be included in the "
      "LogDevice protocol handshake. If the cluster name of the initiator does "
      "not match the actual cluster name of the destination, the connection is "
      "terminated. We don't know of any good reasons to disable this option. "
      "If you disable it and move some hosts from one cluster to another, you "
      "may have a bad time: some clients or servers may not pick up the update "
      "and keep talking to the hosts as if they weren't moved; this may "
      "corrupt "
      "metadata. Used for testing and internally created connections only.",
      SERVER | CLIENT,
      SettingsCategory::Testing);
  init("isolated-sequencer-ttl",
       &isolated_sequencer_ttl,
       "1200s",
       nullptr, // no validation
       "How long we wait before disabling isolated sequencers. "
       "A sequencer is declared isolated if nodes outside of the innermost "
       "failure domain of the sequencer's epoch appear unreachable "
       "to the failure detector. For example, a sequencer of a rack-replicated "
       "log epoch is declared isolated if the failure detector can't reach "
       "any nodes outside of that sequencer node's rack. A disabled sequencer "
       "rejects all append requests.",
       SERVER,
       SettingsCategory::WritePath);
  init("stats-collection-interval",
       &stats_collection_interval,
       "60s",
       nullptr, // no validation
       "How often to collect and submit stats upstream.  "
       "Set to <=0 to disable collection of stats.",
       SERVER | CLIENT | REQUIRES_RESTART /* passed to ctor of
                                             StatsCollectionThread */
       ,
       SettingsCategory::Monitoring);
  init(
      "esn-bits",
      &esn_bits,
      "32",
      validate_range<ssize_t>(2, 32),
      "How many bits to use for sequence numbers within an epoch.  LSN bits [n,"
      " 32) are guaranteed to be 0. Used for testing ESN exhaustion.",
      SERVER | REQUIRES_RESTART /* passed to Sequencer ctor in AllSequencers */,
      SettingsCategory::Testing);
  init("client-initial-redelivery-delay",
       &client_initial_redelivery_delay,
       "1s",
       validate_positive<ssize_t>(),
       "Initial delay to use when reader application rejects a record or gap",
       SERVER /* event log */ | CLIENT,
       SettingsCategory::ReadPath);
  init("client-max-redelivery-delay",
       &client_max_redelivery_delay,
       "30s",
       validate_positive<ssize_t>(),
       "Maximum delay to use when reader application rejects a record or gap",
       SERVER /* event log */ | CLIENT,
       SettingsCategory::ReadPath);
  init("include-destination-on-handshake",
       &include_destination_on_handshake,
       "true",
       nullptr, // no validation
       "Include the destination node ID in the LogDevice protocol handshake. "
       "If the actual node ID of the connection target does not match the "
       "intended destination ID, the connection is terminated.",
       SERVER | CLIENT,
       SettingsCategory::Network);
  init("sequencer-batching",
       &sequencer_batching,
       "false",
       nullptr, // no validation
       "Accumulate appends from clients and batch them together to create "
       "fewer records in the system. This setting is only used when the log "
       "group doesn't override it",
       SERVER,
       SettingsCategory::Batching);
  init("sequencer-batching-time-trigger",
       &sequencer_batching_time_trigger,
       "1s",
       nullptr, // no validation
       "Sequencer batching (if used) flushes buffered appends for a log when "
       "the oldest buffered append is this old. When enabled, this gets "
       "applied to the first new batch. This setting is only used when the log "
       "group doesn't override it",
       SERVER,
       SettingsCategory::Batching);
  init("sequencer-batching-size-trigger",
       &sequencer_batching_size_trigger,
       "-1",
       parse_validate_lower_bound<ssize_t>(-1),
       "Sequencer batching (if used) flushes buffered appends for a log when "
       "the total amount of buffered uncompressed data reaches this many bytes "
       "(if positive). When enabled, this gets applied to the first new batch. "
       "This setting is only used when the log group doesn't override it",
       SERVER,
       SettingsCategory::Batching);
  init("sequencer-batching-compression",
       &sequencer_batching_compression,
       "zstd",
       parse_compression,
       "Compression setting for sequencer batching (if used). It can be 'none' "
       "for no compression; 'zstd' for ZSTD; 'lz4' for LZ4; or lz4_hc for LZ4 "
       "High Compression. The default is ZSTD. When enabled, this gets applied "
       "to the first new batch. This setting is only used when the log group "
       "doesn't override it",
       SERVER,
       SettingsCategory::Batching);
  init(
      "sequencer-batching-passthru-threshold",
      &sequencer_batching_passthru_threshold,
      "-1",
      parse_validate_lower_bound<ssize_t>(-1),
      "Sequencer batching (if used) will pass through any appends with payload "
      "size over this threshold (if positive).  This saves us a compression "
      "round trip when a large batch comes in from BufferedWriter and the "
      "benefit of batching and recompressing would be small.",
      SERVER,
      SettingsCategory::Batching);
  init("num-processor-background-threads",
       &num_processor_background_threads,
       "0",
       nullptr, // no validation
       "Number of threads in Processor's background thread pool. Background "
       "threads are used by, e.g., BufferedWriter to construct/compress "
       "large batches.  If 0 (default), use num-workers.",
       SERVER | CLIENT | REQUIRES_RESTART,
       SettingsCategory::Execution);
  init("buffered-writer-bg-thread-bytes-threshold",
       &buffered_writer_bg_thread_bytes_threshold,
       "4096",
       parse_nonnegative<ssize_t>(),
       "BufferedWriter can send batches to a background thread.  For small "
       "batches, where the overhead dominates, this will just slow things "
       "down.  If the total size of the batch is less than this, it will "
       "constructed / compressed on the Worker thread, blocking other appends "
       "to all logs in that shard.  If larger, it will be enqueued to a helper "
       "thread.",
       SERVER | CLIENT,
       SettingsCategory::Batching);
  init("buffered-writer-zstd-level",
       &buffered_writer_zstd_level,
       "1",
       parse_validate_range<int>(1, ZSTD_maxCLevel()),
       "Zstd compression level to use in BufferedWriter.",
       SERVER | CLIENT,
       SettingsCategory::Batching);
  init("background-queue-size",
       &background_queue_size,
       "100000",
       parse_positive<ssize_t>(),
       "Maximum number of events we can queue to background thread.  A single "
       "queue is shared by all threads in a process.",
       SERVER | CLIENT | REQUIRES_RESTART,
       SettingsCategory::Execution);
  init(
      "skip-recovery",
      &skip_recovery,
      "false",
      nullptr, // no validation
      "Skip recovery. For tests only. When this option is enabled, recovery "
      "does not recover any data but instead immediately marks all epochs as "
      "clean in the epoch store and purging immediately marks all epochs as "
      "clean in the local log store. This feature should be used as a last "
      "resort if a cluster's availability is hurt by recovery and it is "
      "important to quickly restore availability at the cost of some "
      "inconsistencies. On-the-fly changes of this setting will only apply to "
      "new LogRecoveryRequests and will not affect recoveries that are already "
      "in progress.",
      SERVER,
      SettingsCategory::Testing);
  init("single-empty-erm",
       &single_empty_erm,
       "true",
       nullptr, // no validation
       "A single E:EMPTY response for an epoch is sufficient for "
       "GetEpochRecoveryMetadataRequest to consider the epoch as "
       "empty if this option is set.",
       SERVER | EXPERIMENTAL,
       SettingsCategory::Recovery);
  init(
      "disable-check-seals",
      &disable_check_seals,
      "false",
      nullptr, // no validation
      "if true, 'get sequencer state' requests will not be sending 'check "
      "seal' "
      "requests that they normally do in order to confirm that this sequencer "
      "is the most recent one for the log. This saves network and CPU, but may "
      "cause getSequencerState() calls to return stale results. Intended for "
      "use in production emergencies only.",
      SERVER,
      SettingsCategory::Performance);
  init("recovery-seq-metadata-timeout",
       &recovery_seq_metadata_timeout,
       "2s..60s",
       validate_positive<ssize_t>(),
       "Retry backoff timeout used for checking if the latest metadata log "
       "record is fully replicated during log recovery.",
       SERVER,
       SettingsCategory::Recovery);
  init("bridge-record-in-empty-epoch",
       &bridge_record_in_empty_epoch,
       "true",
       nullptr, // no validation
       "epoch recovery will insert bridge records for empty epoch for data "
       "logs. This helps with read availability and efficiency during epoch "
       "transitions.",
       SERVER | DEPRECATED,
       SettingsCategory::Recovery);
  init(
      "byte-offset-interval",
      &byte_offset_interval_DEPRECATED,
      "1",
      parse_positive<ssize_t>(),
      "DEPRECATED! How often the sequencer sends byte offsets to storage nodes."
      "Measured in bytes.",
      SERVER | REQUIRES_RESTART | DEPRECATED /* passed to Sequencer ctor */);
  init("byte-offsets",
       &byte_offsets,
       "false",
       nullptr, // no validation
       "Enables the server-side byte offset calculation feature."
       "NOTE: There is no guarantee of byte offsets result correctness if "
       "feature"
       "was switched on->off->on in period shorter than retention value for"
       "logs.",
       SERVER,
       SettingsCategory::WritePath);
  init("enable-config-synchronization",
       &enable_config_synchronization,
       "true",
       nullptr, // no validation
       "With config synchronization enabled, nodes on both ends of a connection"
       "will synchronize their configs if there is a mismatch in the config"
       "version.",
       SERVER | CLIENT | DEPRECATED,
       SettingsCategory::Configuration);
  init("get-erm-for-empty-epoch",
       &get_erm_for_empty_epoch,
       "true",
       nullptr,
       "If true, Purging will get the EpochRecoveryMetadata "
       "even if the epoch is empty locally",
       SERVER | EXPERIMENTAL,
       SettingsCategory::Recovery);
  init(
      "enable-logsconfig-manager",
      &enable_logsconfig_manager,
      "true",
      nullptr,
      "If true, logdeviced will load the logs configuration from the internal "
      "replicated storage and will ignore the logs section in the config file. "
      "This also enables the remote management API for logs config.",
      SERVER | CLIENT,
      SettingsCategory::Configuration);
  init(
      "logsconfig-manager-grace-period",
      &logsconfig_manager_grace_period,
      "0ms",
      validate_nonnegative<ssize_t>(),
      "Grace period before making a change to the logs config available to the "
      "server.",
      SERVER | CLIENT,
      SettingsCategory::Configuration);
  init("logsconfig-snapshotting",
       &logsconfig_snapshotting,
       "true",
       nullptr,
       "Allow logsconfig to be snapsthotted onto a snapshot log.",
       SERVER | DEPRECATED,
       SettingsCategory::Configuration);
  init("disable-logsconfig-trimming",
       &disable_logsconfig_trimming,
       "false",
       nullptr,
       "Disable the trimming of logsconfig delta log. Used for testing only.",
       SERVER,
       SettingsCategory::Testing);
  init("logsconfig-max-delta-records",
       &logsconfig_max_delta_records,
       "4000",
       nullptr,
       "How many delta records to keep in the logsconfig deltas log before we "
       "snapshot it.",
       SERVER,
       SettingsCategory::Configuration);
  init(
      "logsconfig-max-delta-bytes",
      &logsconfig_max_delta_bytes,
      "10485760",
      nullptr,
      "How many bytes of deltas to keep in the logsconfig deltas log before we "
      "snapshot it.",
      SERVER,
      SettingsCategory::Configuration);
  init("client-config-fetch-allowed",
       &client_config_fetch_allowed,
       "true",
       nullptr, // no validation
       "If true, servers will be allowed to fetch configs from the client side "
       "of a connection during config synchronization.",
       SERVER,
       SettingsCategory::Configuration);

  init("unreleased-record-detector-interval",
       &unreleased_record_detector_interval,
       "30s",
       validate_nonnegative<ssize_t>(),
       "Time interval at which to check for unreleased records in storage "
       "nodes. Any log which has unreleased records, and for which no records "
       "have been released for two consecutive "
       "unreleased-record-detector-intervals, is suspected of having a dead "
       "sequencer. Set to 0 to disable check.",
       SERVER,
       SettingsCategory::ReadPath);
  init(
      "grace-counter-limit",
      &grace_counter_limit,
      "2", // 3 strikes and you're out
      validate_lower_bound<int>(-1),
      "Maximum number of consecutive grace periods a storage node may fail to "
      "send a record or gap (if in all read all mode) before it is considered "
      "disgraced and client read streams no longer wait for it. If all nodes "
      "are disgraced or in GAP state, a gap record is issued. May be 0. Set to "
      "-1 to disable grace counters and use simpler logic: no disgraced nodes, "
      "issue gap record as soon as grace period expires.",
      SERVER | CLIENT,
      SettingsCategory::ReadPath);
  init("test-reject-hello",
       &reject_hello,
       "OK",
       validate_reject_hello,
       "if set to the name of an error code, reject all HELLOs "
       "with the specified error code. Currently supported values are ACCESS "
       "and PROTONOSUPPORT. Used for testing.",
       SERVER | REQUIRES_RESTART,
       SettingsCategory::Testing);

  init("force-on-demand-logs-config",
       &force_on_demand_logs_config,
       "false",
       nullptr,
       "Set this to true if you want the client to get log configuration on "
       "demand from the server even when log configuration is present in the "
       "main config file.",
       SERVER | CLIENT | REQUIRES_RESTART | DEPRECATED,
       SettingsCategory::Configuration);

  init("test-bypass-recovery",
       &bypass_recovery,
       "false",
       nullptr,
       "If set, sequencers will not automatically run recovery upon "
       "activation. Recovery can be started using the 'startrecovery' admin "
       "command.  Note that last released lsn won't get advanced without "
       "recovery.",
       SERVER | REQUIRES_RESTART,
       SettingsCategory::Testing);

  init("hold-store-replies",
       &hold_store_replies,
       "false",
       nullptr,
       "If set, we hold all STORED messages (which are replies to STORE "
       "messages), until the last one comes is.  Has some race conditions and "
       "other down sides, so only use in tests.  Used to ensure that all "
       "storage nodes have had a chance to process the STORE messages, even if "
       "one returns PREEMPTED or another error condition.",
       SERVER | REQUIRES_RESTART,
       SettingsCategory::Testing);

  init("sync-metadata-log-writes",
       &sync_metadata_log_writes,
       "true",
       nullptr,
       "If set, storage nodes will wait for wal sync of metadata log "
       "writes before sending the STORED ack.",
       SERVER);
  init("publish-single-histogram-stats",
       &publish_single_histogram_stats,
       "false",
       nullptr, // no validation
       "If true, single histogram values will be published alongside the rate "
       "values.",
       SERVER | CLIENT,
       SettingsCategory::Monitoring);

  init("event-log-snapshotting",
       &event_log_snapshotting,
       "true",
       nullptr,
       "Allow the event log to be snapshotted onto a snapshot log. This "
       "requires the event log group to contain two logs, the first one being "
       "the snapshot log and the second one being the delta log.",
       SERVER | CLIENT | REQUIRES_RESTART,
       SettingsCategory::Rebuilding);

  init("event-log-snapshot-compression",
       &event_log_snapshot_compression,
       "true",
       nullptr,
       "Use ZSTD compression to compress event log snapshots",
       SERVER | CLIENT,
       SettingsCategory::Rebuilding);

  init("server-default-dscp",
       &server_dscp_default,
       "0",
       parse_validate_range<uint8_t>(0, 63),
       "Use default DSCP to setup to server sockets at Sender."
       "Range was defined by https://tools.ietf.org/html/rfc4594#section-1.4.4",
       SERVER | REQUIRES_RESTART,
       SettingsCategory::Configuration);

  init("client-default-dscp",
       &client_dscp_default,
       "0",
       parse_validate_range<uint8_t>(0, 63),
       "Use default DSCP to setup to client sockets at Sender."
       "Range was defined by https://tools.ietf.org/html/rfc4594#section-1.4.4",
       SERVER | CLIENT | REQUIRES_RESTART,
       SettingsCategory::Configuration);

  init("disable-event-log-trimming",
       &disable_event_log_trimming,
       "false",
       nullptr,
       "Disable trimming of the event log (for tests only)",
       SERVER,
       SettingsCategory::Testing);

  init("event-log-max-delta-records",
       &event_log_max_delta_records,
       "100",
       nullptr,
       "How many delta records to keep in the event log before we "
       "snapshot it.",
       SERVER,
       SettingsCategory::Rebuilding);

  init("event-log-max-delta-bytes",
       &event_log_max_delta_bytes,
       "10485760",
       parse_nonnegative<ssize_t>(),
       "How many bytes of deltas to keep in the event log before "
       "we snapshot it.",
       SERVER,
       SettingsCategory::Rebuilding);

  init("event-log-retention",
       &event_log_retention,
       "14d",
       nullptr,
       "How long to keep a history of snapshots and deltas for "
       "the event log. "
       "Unused if the event log has never been snapshotted or "
       "if event log "
       "trimming is disabled with disable-event-log-trimming.",
       SERVER,
       SettingsCategory::Rebuilding);

  init(
      "append-store-durability",
      &append_store_durability,
      "async_write",
      nullptr, // no validation
      "The minimum guaranteed durablity of record copies before a storage node "
      "confirms the STORE as successful. Can be one of \"memory\" if record "
      "is to be stored in a RocksDB memtable only (logdeviced memory), "
      "\"async_write\" if record is to be additionally written to the RocksDB "
      "WAL file (kernel memory, frequently synced to disk), or \"sync_write\" "
      "if the record is to be written to the memtable and WAL, and the STORE "
      "acknowledged only after the WAL is synced to disk by a separate WAL "
      "syncing thread using fdatasync(3).",
      SERVER,
      SettingsCategory::WritePath);

  init("rebuild-store-durability",
       &rebuild_store_durability,
       "async_write",
       nullptr, // no validation
       "The minimum guaranteed durablity of rebuilding writes before a storage "
       "node will confirm the STORE as successful. Can be one of \"memory\", "
       "\"async_write\", or \"sync_write\". See --append-store-durability for "
       "a description of these options.",
       SERVER,
       SettingsCategory::Rebuilding);

  init("rebuilding-dont-wait-for-flush-callbacks",
       &rebuilding_dont_wait_for_flush_callbacks,
       "false",
       nullptr, // no validation
       "Regardless of the value of 'rebuild-store-durability', assume "
       "any successfully completed store is durable without waiting for "
       "flush notifications. NOTE: Use of this setting will lead to silent "
       "under-replication when 'rebuild-store-durability' is set to 'MEMORY'. "
       "Use for testing and I/O characterization only.",
       SERVER | REQUIRES_RESTART,
       SettingsCategory::Rebuilding);

  init("rebuild-without-amends",
       &rebuild_without_amends,
       "false",
       nullptr, // no validation
       "During rebuilding, send a normal STORE rather than a STORE with "
       "the "
       "AMEND flag, when updating the copyset of nodes that already have a "
       "copy "
       "of the record. This option is used by integration tests to fully "
       "divorce "
       "append content from records touched by rebuilding.",
       SERVER,
       SettingsCategory::Testing);

  init("scd-copyset-reordering-max",
       &scd_copyset_reordering_max,
       "hash-shuffle",
       parse_scd_copyset_reordering,
       "SCDCopysetReordering values that clients may ask servers to "
       "use.  "
       "Currently available options: "
       "none, hash-shuffle (default), hash-shuffle-client-seed. "
       "hash-shuffle results in only one storage node reading a record "
       "block "
       "from disk, and then serving it to multiple readers from the "
       "cache. "
       "hash-shuffle-client-seed enables multiple storage nodes to "
       "participate "
       "in reading the log, which can be benefit non-disk-bound "
       "workloads.",
       SERVER | CLIENT,
       SettingsCategory::ReadPath);

  init("sequencer-metadata-log-write-retry-delay",
       &sequencer_metadata_log_write_retry_delay,
       "500ms..30s-2x",
       nullptr, // no validation
       "The retry delay for sequencer writing into its own "
       "metadata log "
       "during log reconfiguration.",
       SERVER,
       SettingsCategory::Configuration);

  init("sequencer-epoch-store-write-retry-delay",
       &sequencer_epoch_store_write_retry_delay,
       "5s..1min-2x",
       nullptr, // no validation
       "The retry delay for sequencer writing log metadata "
       "into the epoch store "
       "during log reconfiguration.",
       SERVER,
       SettingsCategory::Configuration);
  init("sequencer-historical-metadata-retry-delay",
       &sequencer_historical_metadata_retry_delay,
       "5s..1min-2x",
       nullptr, // no validation
       "The retry delay for sequencer reading metadata log "
       "for historical "
       "epoch metadata during log reconfiguration.",
       SERVER,
       SettingsCategory::Configuration);

  init("weighted-copyset-selector",
       &weighted_copyset_selector,
       "true",
       nullptr, // no validation
       "If true, the shiny new copyset selector will "
       "be used for everything. "
       "If false, legacy copyset selector will be used "
       "when possible. "
       "There should be no reason to disable it, "
       "unless it's broken in some way.",
       SERVER | DEPRECATED,
       SettingsCategory::WritePath);

  init("copyset-locality-min-scope",
       &copyset_locality_min_scope,
       "rack",
       nullptr, // no validation
       "Tl;dr: if you experience data distribution "
       "imbalance caused by hot "
       "logs, and you have plenty of unused "
       "cross-rack/cross-region bandwidth, "
       "try changing this setting to \"root\"; "
       "otherwise the default \"rack\" "
       "is just fine.  More details: let X be the "
       "value of this setting, and "
       "let Y be the biggest scope in log's "
       "replicateAcross property; if Y < X, "
       "nothing happens; if Y >= X, at least one "
       "copy of each record will be "
       "stored in sequencer's domain of scope Y "
       "(not X), when it's possible "
       "without affecting average data "
       "distribution. This, combined with "
       "chain-sending, typically reduces the "
       "number of cross-Y hops by one per "
       "record.",
       SERVER,
       SettingsCategory::WritePath);

  init("test-do-not-pick-in-copysets",
       &test_do_not_pick_in_copysets,
       "",
       parse_recipients_list,
       "Copyset selectors won't pick these "
       "nodes. Comma-separated list of node "
       "indexes, e.g. '1,2,3'. Used in tests.",
       SERVER,
       SettingsCategory::Testing);

  init("traffic-shadow-enabled",
       &traffic_shadow_enabled,
       "false", // opt-in: defaults to false
       nullptr, // no custom validation necessary
       "Controls the traffic shadowing feature. Defaults to false to disable "
       "shadowing on all clients writing to a cluster. Must be set to true to "
       "allow traffic shadowing, which will then be controlled on a per-log "
       "basic through parameters in LogsConfig.",
       CLIENT,
       SettingsCategory::Monitoring);

  init("shadow-client-creation-retry-interval",
       &shadow_client_creation_retry_interval,
       "60s",
       validate_nonnegative<ssize_t>(),
       "Failed shadow appends because shadow client was not available, "
       "enqueue a client recreation request. The retry mechanism retries "
       "the enqueued attempt after these many seconds. See ShadowClient.cpp "
       "for a detailed explanation. 0 disables the retry feature. 1 silently "
       "drops all client creations so that they only get created from the "
       "retry path.",
       CLIENT,
       SettingsCategory::WritePath);

  init("shadow-client-timeout",
       &shadow_client_timeout,
       "30s",
       validate_positive<ssize_t>(),
       "Timeout to use for shadow clients. See traffic-shadow-enabled.",
       CLIENT,
       SettingsCategory::Monitoring);

  init("enable-nodes-configuration-manager",
       &enable_nodes_configuration_manager,
       "false", // defaults to false
       nullptr, // no custom validation necessary
       "If set, NodesConfigurationManager and its workflow will be enabled.",
       CLIENT | SERVER | REQUIRES_RESTART,
       SettingsCategory::Configuration);

  init("use-nodes-configuration-manager-nodes-configuration",
       &use_nodes_configuration_manager_nodes_configuration,
       "false", // defaults to false
       nullptr, // no custom validation necessary
       "If true and enable_nodes_configuration_manager is set, logdevice will "
       "use the nodes configuration from the NodesConfigurationManager.",
       CLIENT | SERVER | REQUIRES_RESTART,
       SettingsCategory::Configuration);

  init("nodes-configuration-manager-store-polling-interval",
       &nodes_configuration_manager_store_polling_interval,
       "3s",
       validate_positive<ssize_t>(),
       "Polling interval of NodesConfigurationManager to "
       "NodesConfigurationStore to read NodesConfiguration",
       CLIENT | SERVER,
       SettingsCategory::Configuration);

  init("nodes-configuration-manager-intermediary-shard-state-timeout",
       &nodes_configuration_manager_intermediary_shard_state_timeout,
       "180s", // 3 minutes
       validate_positive<ssize_t>(),
       "Timeout for proposing the transition for a shard from an intermediary "
       "state to its 'destination' state",
       CLIENT | SERVER, // available on the clients for tooling
       SettingsCategory::Configuration);

  init("admin-client-capabilities",
       &admin_client_capabilities,
       "false", // defaults to false
       nullptr, // no custom validation necessary
       "If set, the client will have the capabilities for administrative "
       "operations such as changing NodesConfiguration. Usually used by "
       "emergency tooling. Beware that admin clients use a different "
       "NodesConfigurationStore that may not support a large fan-out, so "
       "this settings shouldn't be applied to large number of clients "
       "(e.g., through client_settings in settings config).",
       CLIENT,
       SettingsCategory::Configuration);

  init("nodes-configuration-file-store-dir",
       &nodes_configuration_file_store_dir,
       "", // defaults to empty
       nullptr,
       "If set, the source of truth of nodes configuration will be under this "
       "dir instead of the default (zookeeper) store. Only effective when "
       "--enable-nodes-configuration-manager=true; Used by "
       "integration testing.",
       CLIENT | SERVER | REQUIRES_RESTART,
       SettingsCategory::Testing);

  init("shadow-client",
       &shadow_client,
       "false",
       nullptr,
       "Indicates if the Client object being created is a shadow client, "
       "i.e. "
       "a client used specifically to perform traffic shadowing. This "
       "setting "
       "allows the client constructor to disable initialization for members "
       "that may not be necessary for shadow clients.",
       CLIENT | INTERNAL_ONLY);

  init("real-time-reads-enabled",
       &real_time_reads_enabled,
       "false", // default
       nullptr, // no custom validation necessary
       "Turns on the experimental real time reads feature.",
       SERVER | EXPERIMENTAL,
       SettingsCategory::ReadPath);

  init("reject-stores-based-on-copyset",
       &reject_stores_based_on_copyset,
       "true",
       nullptr,
       "If true, logdevice will prevent writes to nodes that are being drained "
       "(rebuilt in RELOCATE mode). Not recommended to set to false unless "
       "you're having a production issue.",
       SERVER,
       SettingsCategory::Rebuilding);

  init("read-stream-guaranteed-delivery-efficiency",
       &read_stream_guaranteed_delivery_efficiency,
       "false",
       nullptr, // no validation
       "In this mode, readers will prioritize making progress by "
       "issuing DATALOSS gaps instead of stalling when too many nodes are "
       "unavailable or taking actions that increase network bandwidth. "
       "Using this mode will make readers less robust against silent "
       "underreplication and copyset inconsistencies.",
       CLIENT | DEPRECATED,
       SettingsCategory::ReaderFailover);

  init("read-streams-use-metadata-log-only",
       &read_streams_use_metadata_log_only,
       "true",
       nullptr,
       "If true, the NodeSetFinder within ClientReadStream will use "
       "only the metadata log rather than the sequencer as source for fetching "
       "historical metadata of the log. This option is used only for migration "
       "and will be removed at some point.",
       CLIENT | SERVER | DEPRECATED);

  init("max-sequencer-background-activations-in-flight",
       &max_sequencer_background_activations_in_flight,
       "20",
       nullptr, // no validation
       "Max number of concurrent background sequencer activations to run. "
       "Background sequencer activations perform log metadata changes "
       "(reprovisioning) when the configuration attributes of a log change.",
       SERVER,
       SettingsCategory::Configuration);
  init(
      "sequencer-reactivation-delay-secs",
      &sequencer_reactivation_delay_secs,
      "60s..3600s",
      validate_nonnegative<ssize_t>(),
      "Some sequencer reactivations may be postponed when the changes that "
      "triggered the reactivation are not important enough to be propogated "
      "immediately. E.g., changes to replication factor or window size, need "
      "to be made immediately visible on the other hand changes changes to the "
      "nodeset due to say the 'exclude_from_nodeset' flag being set as part "
      "of a passive drain can be postponed. If the reactivations can be "
      "postponed then the delay is chosen to be a radnom delay seconds "
      "between the above range. If 0 then don't postpone ",
      SERVER,
      SettingsCategory::Configuration);
  init("sequencer-background-activation-retry-interval",
       &sequencer_background_activation_retry_interval,
       "500ms",
       nullptr, // no validation
       "Retry interval on failures while processing background sequencer "
       "activations for reprovisioning.",
       SERVER,
       SettingsCategory::Configuration);
  init(
      "use-sequencer-affinity",
      &use_sequencer_affinity,
      "false",
      nullptr, // no validation
      "If true, the routing of append requests to sequencers will first try to "
      "find a sequencer in the location given by sequencerAffinity() before "
      "looking elsewhere.",
      SERVER | CLIENT,
      SettingsCategory::WritePath);
  init("real-time-max-bytes",
       &real_time_max_bytes,
       "100000000",
       nullptr, // no validation
       "Max size (in bytes) of released records that we'll keep around to use "
       "for real time reads.  Includes some cache overhead, so for "
       "small records, you'll store less record data than this.",
       SERVER | REQUIRES_RESTART | EXPERIMENTAL,
       SettingsCategory::ReadPath);
  init("real-time-eviction-threshold-bytes",
       &real_time_eviction_threshold_bytes,
       "80000000",
       nullptr, // no validation
       "When the real time buffer reaches this size, we evict entries.",
       SERVER | REQUIRES_RESTART | EXPERIMENTAL,
       SettingsCategory::ReadPath);

  init("test-timestamp-linear-transform",
       &test_timestamp_linear_transform,
       "1,0",
       parse_test_timestamp_linear_tranform,
       "Coefficents for tranforming the timestamp of records for test. "
       "The value should contain two integrs sperated by ','. For example"
       "'m,c'. Records timestamp is tranformed as m * now() + c."
       "A default value of '1,0' makes the timestamp = now() which is expected"
       "for all the normal use cases.",
       SERVER | REQUIRES_RESTART,
       SettingsCategory::Testing);

  init("reader-reconnect-delay",
       &reader_reconnect_delay,
       "10ms..30s",
       validate_positive<ssize_t>(),
       "When a reader client loses a connection to a storage node, delay after "
       "which it tries reconnecting.",
       CLIENT,
       SettingsCategory::ReadPath);
  init("reader-started-timeout",
       &reader_started_timeout,
       "30s..5min",
       validate_positive<ssize_t>(),
       "How long a reader client waits for a STARTED reply from a storage node "
       "before sending a new START message.",
       CLIENT,
       SettingsCategory::ReadPath);
  init("reader-retry-window-delay",
       &reader_retry_window_delay,
       "10ms..30s",
       validate_positive<ssize_t>(),
       "When a reader client fails to send a WINDOW message, delay after which "
       "it retries sending it.",
       CLIENT,
       SettingsCategory::ReadPath);

#define DEF_SETTING(requests)                                 \
  init("dont-serve-" #requests "-for-logs",                   \
       &dont_serve_##requests##_logs,                         \
       "",                                                    \
       parse_log_set,                                         \
       "Logs for which " #requests " will not be served",     \
       SERVER,                                                \
       SettingsCategory::Testing);                            \
  init("dont-serve-" #requests "-status",                     \
       &dont_serve_##requests##_status,                       \
       "FAILED",                                              \
       nullptr,                                               \
       "status that should be returned for logs that are in " \
       "\"dont-serve-" #requests "-for-logs\"",               \
       SERVER,                                                \
       SettingsCategory::Testing)
  DEF_SETTING(reads);
  DEF_SETTING(findtimes);
  DEF_SETTING(stores);
#undef DEF_SETTING

  init("write-shard-id-in-copyset",
       &write_shard_id_in_copyset,
       "false",
       nullptr,
       "Serialize copysets using ShardIDs instead of node_index_t on disk. "
       "TODO(T15517759): enable by default once Flexible Log Sharding is fully "
       "implemented and this has been thoroughly tested.",
       SERVER | EXPERIMENTAL,
       SettingsCategory::WritePath);

  init("epoch-metadata-use-new-storage-set-format",
       &epoch_metadata_use_new_storage_set_format,
       "false",
       nullptr,
       "Serialize copysets using ShardIDs instead of node_index_t inside "
       "EpochMetaData. TODO(T15517759): enable by default once Flexible Log "
       "Sharding is fully implemented and this has been thoroughly tested.",
       SERVER | CLIENT | EXPERIMENTAL,
       SettingsCategory::WritePath);

  init("test-sequencer-corrupt-stores",
       &test_sequencer_corrupt_stores,
       "false",
       nullptr,
       "Simulates bad hardware flipping a bit in the payload of a STORE "
       "message.",
       SERVER,
       SettingsCategory::Testing);

  init("message-tracing-types",
       &message_tracing_types,
       "",
       parse_message_types,
       "Emit a log line for each sent/received message of the type(s) "
       "specified. Separate different types with a comma. 'all' to trace all "
       "messages. Prefix the value with '~' to trace all types except the "
       "given ones, e.g. '~WINDOW,RELEASE' will trace messages of all types "
       "except WINDOW and RELEASE.",
       SERVER | CLIENT,
       SettingsCategory::Monitoring);

  init("message-tracing-peers",
       &message_tracing_peers,
       "",
       parse_sockaddrs,
       "Emit a log line for each sent/received message to/from the specified "
       "address(es). Separate different addresses with a comma, prefix unix "
       "socket paths with 'unix://'. An empty unix path will match all unix "
       "paths",
       SERVER | CLIENT,
       SettingsCategory::Monitoring);

  init(
      "message-tracing-log-level",
      &message_tracing_log_level,
      "info",
      parse_log_level,
      "For messages that pass the message tracing filters, emit a log line at "
      "this level. One of: critical, error, warning, notify, info, debug, spew",
      SERVER | CLIENT,
      SettingsCategory::Monitoring);

  init("reader-slow-shards-detection",
       &reader_slow_shards_detection,
       "disabled",
       [](const std::string& val) {
         if (val == "disabled") {
           return ReaderSlowShardDetectionState::DISABLED;
         } else if (val == "observe-only") {
           return ReaderSlowShardDetectionState::OBSERVE_ONLY;
         } else if (val == "enabled") {
           return ReaderSlowShardDetectionState::ENABLED;
         } else {
           char buf[1024];
           snprintf(
               buf,
               sizeof(buf),
               "Invalid value for --reader-slow-shard-detection: %s. "
               "Must be one of \"disabled\", \"observe-only\", \"enabled\"",
               val.c_str());
           throw boost::program_options::error(buf);
         }
       },
       "If true, readers in SCD mode will detect shards that are very slow and"
       "may ask the other storage shards to filter them out",
       CLIENT,
       SettingsCategory::ReaderFailover);

  init("reader-slow-shards-detection-moving-avg-duration",
       &reader_slow_shards_detection_settings.moving_avg_duration,
       "30min",
       nullptr,
       "When slow shards detection is enabled, duration to use for the moving "
       "average",
       CLIENT,
       SettingsCategory::ReaderFailover);

  init("reader-slow-shards-detection-required-margin",
       &reader_slow_shards_detection_settings.required_margin,
       "10.0",
       nullptr,
       "When slow shards detection is enabled, sensitivity of the outlier "
       "detection algorithm. For instance, if set to 3.0, only consider an "
       "outlier a shard that is 300% slower than the others. The required "
       "margin is adaptive and may increase or decrease but will be capped "
       "at a minimum defined by this setting.",
       CLIENT,
       SettingsCategory::ReaderFailover);

  init("reader-slow-shards-detection-required-margin-decrease-rate",
       &reader_slow_shards_detection_settings.required_margin_decrease_rate,
       "0.25",
       nullptr,
       "Rate at which we decrease the required margin when we are "
       "healthy. If the value is 0.25 for instance, we will reduce the "
       "required margin by 0.25 for every second spent reading.",
       CLIENT,
       SettingsCategory::ReaderFailover);

  init("reader-slow-shards-detection-outlier-duration",
       &reader_slow_shards_detection_settings.outlier_duration,
       "1min..30min",
       validate_positive<ssize_t>(),
       "When slow shards detection is enabled, amount of time that "
       "we'll "
       "consider a shard an outlier if it is slow.",
       CLIENT,
       SettingsCategory::ReaderFailover);

  init("reader-slow-shards-detection-outlier-duration-decrease-"
       "rate",
       &reader_slow_shards_detection_settings.outlier_duration_decrease_rate,
       "0.25",
       nullptr,
       "When slow shards detection is enabled, rate at which "
       "we decrease the "
       "time after which we'll try to reinstate an outlier in "
       "the read "
       "set. If the value is 0.25, for each second of healthy "
       "reading we will "
       "decrease that time by 0.25s.",
       CLIENT,
       SettingsCategory::ReaderFailover);

  init("rsm-include-read-pointer-in-snapshot",
       &rsm_include_read_pointer_in_snapshot,
       "false",
       nullptr,
       "Allow inclusion of read pointer in RSM snapshots. Note that if this is "
       "set to true IT IS UNSAFE TO CHANGE IT BACK TO FALSE!",
       SERVER | CLIENT,
       SettingsCategory::Core);

  init("eventlog-snapshotting-period",
       &eventlog_snapshotting_period,
       "1h",
       validate_positive<ssize_t>(),
       "Controls time based snapshotting. New eventlog snapshot will be "
       "created after this period if there are new deltas",
       SERVER,
       SettingsCategory::Rebuilding);

  init("logsconfig-snapshotting-period",
       &logsconfig_snapshotting_period,
       "1h",
       validate_positive<ssize_t>(),
       "Controls time based snapshotting. New logsconfig snapshot will be "
       "created after this period if there are new log configuration deltas",
       SERVER,
       SettingsCategory::Configuration);

  init("get-trimpoint-interval",
       &get_trimpoint_interval,
       "600s",
       validate_positive<ssize_t>(),
       "polling interval for the sequencer getting the trim point from all "
       "storage nodes",
       SERVER,
       SettingsCategory::Sequencer);
  init("disable-trim-past-tail-check",
       &disable_trim_past_tail_check,
       "false",
       nullptr, // no validation
       "Disable check for trim past tail. Used for testing log trimming.",
       CLIENT,
       SettingsCategory::Testing);
  init("allow-reads-on-workers",
       &allow_reads_on_workers,
       "true",
       nullptr,
       "If false, all rocksdb reads are done from storage threads. If true, "
       "a cache-only reading attempt is made from worker thread first, and a "
       "storage thread task is scheduled only if the cache wasn't enough to "
       "fulfill the read. Disabling this can be used for: working around "
       "rocksdb bugs; working around latency spikes caused by cache-only reads "
       "being slow sometimes",
       SERVER | EXPERIMENTAL,
       SettingsCategory::Performance);
  init("findkey-timeout",
       &findkey_timeout,
       "",
       parse_optional_chrono_option,
       "Findkey API call timeout. If omitted the client timeout will be used.",
       CLIENT,
       SettingsCategory::Core);
  init("append-timeout",
       &append_timeout,
       "",
       parse_optional_chrono_option,
       "Timeout for appends. If omitted the client timeout will be used.",
       CLIENT,
       SettingsCategory::Core);
  init("logsconfig-timeout",
       &logsconfig_timeout,
       "",
       parse_optional_chrono_option,
       "Timeout for LogsConfig API requests. "
       "If omitted the client timeout will be used.",
       CLIENT,
       SettingsCategory::Core);
  init("meta-api-timeout",
       &meta_api_timeout,
       "",
       parse_optional_chrono_option,
       "Timeout for trims/isLogEmpty/tailLSN/datasize API/etc. "
       "If omitted the client timeout will be used.",
       CLIENT,
       SettingsCategory::Core);
  init("enable-offset-map",
       &enable_offset_map,
       "false",
       nullptr, // no validation
       "Enables the server-side OffsetMap calculation feature."
       "NOTE: There is no guarantee of byte offsets result correctness if "
       "feature"
       "was switched on->off->on in period shorter than retention value for"
       "logs.",
       SERVER,
       SettingsCategory::WritePath);
  init("enable-hh-wheel-backed-timers",
       &enable_hh_wheel_backed_timers,
       "true",
       nullptr, // no validation
       "Enables the new version of timers which run on a different thread"
       "and use HHWheelTimer backend.",
       SERVER | CLIENT | REQUIRES_RESTART,
       SettingsCategory::Core);
  init("enable-store-histograms-calculations",
       &enable_store_histogram_calculations,
       "false",
       nullptr, // no validation
       "Enables estimation of store timeouts per worker per node.",
       SERVER,
       SettingsCategory::Core);
  init("store-histogram-min-samples-per-bucket",
       &store_histogram_min_samples_per_bucket,
       "30",
       parse_positive<size_t>(),
       "How many stores should the store histogram wait for before reporting "
       "latency estimates",
       SERVER,
       SettingsCategory::Core);
  init(
      "authoritative-status-overrides",
      &authoritative_status_overrides,
      "",
      parse_authoritative_status_overrides,
      "Force the given authoritative statuses for the given shards. "
      "Comma-separated list of overrides, each override of form "
      "'N<node>S<shard>:<status>' or 'N<node>S<shard1>-<shard2>:<status>'. "
      "E.g. 'N7:S0-15:UNDERREPLICATION,N8:S2:UNDERREPLICATION' will set status "
      "of shards 0-15 of node 7 and shard 2 of node 8 to UNDERREPLICATION. "
      "This is useful for recovering from situations where internal logs or "
      "metadata logs are unreadable because too many nodes are unavailable or "
      "lost their data. In such situation, use this setting to temporarily "
      "override the state of shards that are unavailable (not running "
      "logdeviced) to UNDERREPLICATION, then, optionally, write "
      "SHARD_UNRECOVERABLE events for the same shards to event log.",
      SERVER,
      SettingsCategory::ReadPath);

  init("nodeset-adjustment-period",
       &nodeset_adjustment_period,
       "6h",
       validate_nonnegative<ssize_t>(),
       "If not zero, nodeset size for each log will be periodically adjusted "
       "based on logs's measured throughput. This settings controls how often "
       "such adjustments will be considered. The nodeset size is chosen "
       "proportionally to throughput, replication factor and backlog duration. "
       "The nodeset_size log attribute acts as the minimim allowed nodeset "
       "size, used for low-throughput logs and logs with infinite backlog "
       "duration. If --nodeset-adjustment-period is changed from nonzero to "
       "zero, all adjusted nodesets get immediately updated back to normal "
       "size.",
       SERVER,
       SettingsCategory::Sequencer);

  init("nodeset-adjustment-target-bytes-per-shard",
       &nodeset_adjustment_target_bytes_per_shard,
       "10G",
       parse_nonnegative<size_t>(),
       "When automatic nodeset size adjustment is enabled, "
       "(--nodeset-adjustment-period), this setting controls the size of the "
       "chosen nodesets. The size is chosen so that each log takes around this "
       "much space on each shard. More precisely, "
       "`nodeset_size = append_bytes_per_sec * backlog_duration * "
       "replication_factor / nodeset_adjustment_target_bytes_per_shard`. "
       "Appropriate value for this setting is around 0.1% - 1% of disk size.",
       SERVER,
       SettingsCategory::Sequencer);

  init("nodeset-size-adjustment-min-factor",
       &nodeset_size_adjustment_min_factor,
       "2",
       validate_nonnegative<double>(),
       "When automatic nodeset size adjustment is enabled, we skip adjustments "
       "that are smaller than this factor. E.g. if this setting is set to 2, "
       "we won't bother updating nodeset if its size would increase or "
       "decrease by less than a factor of 2. If set to 0, nodesets will be "
       "unconditionally updated every --nodeset-adjustment-period, and will "
       "also be randomized each time, as opposed to using consistent hashing.",
       SERVER,
       SettingsCategory::Sequencer);

  init("nodeset-adjustment-min-window",
       &nodeset_adjustment_min_window,
       "1h",
       validate_positive<ssize_t>(),
       "When automatic nodeset size adjustment is enabled, only do the "
       "adjustment if we've got append throughput information for at least this"
       "period of time. More details: we choose nodeset size based on log's "
       "average append throughput in a moving window of "
       "size --nodeset-adjustment-period. The average is maintained by the "
       "sequencer. If the sequencer was activated recently, we may not have a "
       "good estimate of log's append throughput. This setting says how long "
       "to wait after sequencer activation before allowing adjusting nodeset "
       "size based on that sequencer's throughput.",
       SERVER,
       SettingsCategory::Sequencer);

  init("nodeset-max-randomizations",
       &nodeset_max_randomizations,
       "4",
       validate_positive<ssize_t>(),
       "When automatic nodeset size adjustment wants to enlarge nodeset "
       "to unreasonably big size N > 127, we instead set nodeset size to 127 "
       "but re-randomize the nodeset min(N/127, nodeset_max_randomizations) "
       "times during retention period. If you make it too big, the union of "
       "historical nodesets will get big (127 * n), and findTime, isLogEmpty "
       "etc may become expensive. If you set it too small, and the cluster has "
       "high-throughput high-retention logs, space usage may be not very "
       "balanced.",
       SERVER,
       SettingsCategory::Sequencer);

  sequencer_boycotting.defineSettings(init);

  init("require-permission-message-types",
       &require_permission_message_types,
       "START",
       parse_message_types,
       "Check permissions only for the received message of the type(s) "
       "specified. Separate different types with a comma. 'all' to apply to "
       "all messages. Prefix the value with '~' to include all types except "
       "the given ones, e.g. '~WINDOW,RELEASE' will check permssions for "
       "messages of all types except WINDOW and RELEASE.",
       SERVER,
       SettingsCategory::Security);

  init("enable-all-read-streams-sampling",
       &enable_all_read_streams_sampling,
       "false",
       nullptr, // no validation
       "Enables sampling of debug info from client's all read "
       "streams.",
       CLIENT,
       SettingsCategory::ReadPath);

  init("all-read-streams-rate",
       &all_read_streams_rate,
       "100ms",
       validate_positive<ssize_t>(),
       "Rate of sampling all client read streams debug info to Scuba",
       CLIENT,
       SettingsCategory::ReadPath);
}
}} // namespace facebook::logdevice
