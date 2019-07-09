/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <signal.h>
#include <unordered_map>
#include <unordered_set>

#include <boost/program_options.hpp>
#include <boost/tokenizer.hpp>
#include <folly/Bits.h>
#include <folly/Random.h>
#include <folly/Range.h>
#include <folly/Singleton.h>
#include <folly/Varint.h>
#include <folly/dynamic.h>
#include <folly/json.h>
#include <sys/prctl.h>

#include "logdevice/common/Checksum.h"
#include "logdevice/common/CopySet.h"
#include "logdevice/common/DataRecordOwnsPayload.h"
#include "logdevice/common/LocalLogStoreRecordFormat.h"
#include "logdevice/common/MetaDataLog.h"
#include "logdevice/common/NoopTraceLogger.h"
#include "logdevice/common/PermissionChecker.h"
#include "logdevice/common/PrincipalParser.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/SequencerLocator.h"
#include "logdevice/common/SingleEvent.h"
#include "logdevice/common/StopReadingRequest.h"
#include "logdevice/common/SyncSequencerRequest.h"
#include "logdevice/common/Timer.h"
#include "logdevice/common/Timestamp.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/WorkerCallbackHelper.h"
#include "logdevice/common/client_read_stream/AllClientReadStreams.h"
#include "logdevice/common/client_read_stream/ClientReadStream.h"
#include "logdevice/common/client_read_stream/ClientReadStreamBufferFactory.h"
#include "logdevice/common/commandline_util.h"
#include "logdevice/common/commandline_util_chrono.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/LocalLogsConfig.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/common/util.h"
#include "logdevice/include/Client.h"
#include "logdevice/include/Err.h"
#include "logdevice/include/LogTailAttributes.h"
#include "logdevice/include/types.h"
#include "logdevice/lib/ClientImpl.h"
#include "logdevice/lib/ClientSettingsImpl.h"
#include "logdevice/replication_checker/LogErrorTracker.h"
#include "logdevice/replication_checker/ReplicationCheckerSettings.h"

// Signal handlers.
void print_stats_signal(int);
void print_stats_and_die_signal(int);

using namespace facebook::logdevice;
using namespace facebook::logdevice::configuration;
using namespace facebook::logdevice::MetaDataLog;

using RecordLevelError = LogErrorTracker::RecordLevelError;
using LogLevelError = LogErrorTracker::LogLevelError;
using ReportErrorsMode = ReplicationCheckerSettings::ReportErrorsMode;

struct LogCheckRequest {
  logid_t log_id;
  size_t latest_replication_factor;
  lsn_t start_lsn;
  lsn_t until_lsn;
};

enum class ShardAuthoritativenessSituation {
  FULLY_AUTHORITATIVE,
  NOT_IN_NODESET,
  AUTHORITATIVE_EMPTY,
  UNDERREPLICATION,
  // ClientReadStreamSenderState::GapState::UNDER_REPLICATED
  GAP_UNDER_REPLICATED,
};

using ShardAuthoritativenessSituationMap =
    std::unordered_map<ShardID, ShardAuthoritativenessSituation>;

std::unique_ptr<ClientSettingsImpl> client_settings;
UpdateableSettings<Settings> settings;
std::shared_ptr<const ReplicationCheckerSettings> checker_settings;
RecordLevelError errors_to_ignore;
std::shared_ptr<UpdateableConfig> config;
std::shared_ptr<Client> ldclient;
Processor* processor;

std::mutex mutex;
std::vector<LogCheckRequest> logs_to_check;

std::chrono::steady_clock::time_point start_time;
size_t logs_to_check_initial_count;
std::atomic<size_t> num_failures_to_stop{0};

class PerWorkerCoordinatorRequest;
std::vector<std::unique_ptr<PerWorkerCoordinatorRequest>> worker_coordinators;

static void output(dbg::Level level, const char* format, ...)
    __attribute__((__format__(__printf__, 2, 3)));

// If json == true, log the given message at severity `level`. If json == false,
// write the message to stdout.
static void output(dbg::Level level, const char* format, ...) {
  va_list ap;
  char record[16384];
  va_start(ap, format);
  int reclen = vsnprintf(record, sizeof(record), format, ap);
  va_end(ap);
  if (reclen < 0) { // invalid format
    reclen = 0;
  }
  if (reclen >= sizeof(record) - 1) { // unlikely
    reclen = sizeof(record) - 2;
  }
  record[reclen++] = '\n';
  record[reclen++] = '\0';
  if (checker_settings->json || checker_settings->json_continuous) {
    // We are printing a json summary to stdout at the end. Do not polute stdout
    // with other stuff.
    ld_log(level, "%s", record);
  } else {
    std::cout << record;
    std::cout.flush();
  }
}

const unsigned line_length = 80;
const unsigned min_description_length = 60;

void check_no_option_conflicts(
    const boost::program_options::variables_map& variables,
    const std::vector<std::string>& options,
    bool require_once) {
  std::vector<std::string> conflicting;
  for (auto option : options) {
    if (variables.count(option) && !variables[option].defaulted()) {
      conflicting.push_back(option);
    }
  }
  if (conflicting.size() > 1) {
    std::stringstream error;
    error
        << "Mutually exclusive options specified! You should only set one of:";
    for (auto option : options) {
      error << " " << option << ",";
    }
    error.seekp(-1, error.cur);
    error << ".";
    throw boost::program_options::error(error.str());
  }
  if (conflicting.empty() && require_once) {
    std::stringstream error;
    error << "Required option missing! You should set one of:";
    for (auto option : options) {
      error << " " << option << ",";
    }
    error.seekp(-1, error.cur);
    error << ".";
    throw boost::program_options::error(error.str());
  }
}

static ShardAuthoritativenessSituation characterizeShardAuthoritativeness(
    ShardID shard,
    const ClientReadStream::GapFailureDomain& auth_info) {
  if (!auth_info.containsShard(shard)) {
    return ShardAuthoritativenessSituation::NOT_IN_NODESET;
  }
  AuthoritativeStatus st = auth_info.getShardAuthoritativeStatus(shard);
  if (st == AuthoritativeStatus::AUTHORITATIVE_EMPTY) {
    return ShardAuthoritativenessSituation::AUTHORITATIVE_EMPTY;
  }
  if (st == AuthoritativeStatus::UNDERREPLICATION) {
    return ShardAuthoritativenessSituation::UNDERREPLICATION;
  }

  ClientReadStreamSenderState::GapState gap_state;
  int rv = auth_info.getShardAttribute(shard, &gap_state);
  ld_check_eq(0, rv); // ClientReadStream sets all attributes
  return gap_state == ClientReadStreamSenderState::GapState::UNDER_REPLICATED
      ? ShardAuthoritativenessSituation::GAP_UNDER_REPLICATED
      : ShardAuthoritativenessSituation::FULLY_AUTHORITATIVE;
}

static std::string formatShardIDWithAuthoritativeness(
    ShardID shard,
    const ShardAuthoritativenessSituationMap& auths) {
  if (!auths.count(shard)) {
    return shard.toString();
  }
  std::string str = shard.toString();
  str += "(";
  switch (auths.at(shard)) {
    case ShardAuthoritativenessSituation::FULLY_AUTHORITATIVE:
      ld_check(false);
      break;
    case ShardAuthoritativenessSituation::NOT_IN_NODESET:
      str += "not in nodeset";
      break;
    case ShardAuthoritativenessSituation::AUTHORITATIVE_EMPTY:
      str += "AE";
      break;
    case ShardAuthoritativenessSituation::UNDERREPLICATION:
      str += "UR";
      break;
    case ShardAuthoritativenessSituation::GAP_UNDER_REPLICATED:
      str += "gap U_R";
      break;
  }
  str += ")";
  return str;
}

namespace {

struct RecordCopy {
  uint32_t wave;
  uint32_t payload_hash;
  copyset_t copyset;
  std::chrono::milliseconds timestamp;
  size_t payload_size;
  // The interesting flags are BUFFERED_WRITER_BLOB and HOLE.
  RECORD_flags_t flags;

  bool sameData(const RecordCopy& rhs) const {
    auto f = [](const RecordCopy& r) {
      const RECORD_flags_t flag_mask =
          RECORD_Header::BUFFERED_WRITER_BLOB | RECORD_Header::HOLE;
      return std::make_tuple(
          r.timestamp, r.payload_hash, r.payload_size, r.flags & flag_mask);
    };
    return f(*this) == f(rhs);
  }

  void parsePayload(Payload payload) {
    // Try to guess whether PAYLOAD_HASH_ONLY is supported on server.
    if (payload.size() == 8) {
      static const uint32_t empty_hash = checksum_32bit(Slice("", 0));
      std::pair<uint32_t, uint32_t> h;
      static_assert(sizeof(h) == 8, "Weird");
      memcpy(&h, payload.data(), sizeof(h));
      if (h.first <= Payload::maxSize() &&
          (h.first > 0 || h.second == empty_hash)) {
        payload_size = h.first;
        payload_hash = h.second;
        return;
      }
    }
    if (!checker_settings->csi_data_only || payload.size() != 0) {
      RATELIMIT_WARNING(std::chrono::seconds(10),
                        1,
                        "Looks like at least some storage nodes don't support "
                        "PAYLOAD_HASH_ONLY.");
    }
    payload_size = payload.size();
    payload_hash = checksum_32bit(Slice(payload));
  }

  std::string
  copysetToString(const ShardAuthoritativenessSituationMap& auths) const {
    std::string str = "{";
    for (size_t i = 0; i < copyset.size(); ++i) {
      if (i) {
        str += ", ";
      }
      str += formatShardIDWithAuthoritativeness(copyset[i], auths);
    }
    str += "}";
    return str;
  }

  std::string toString(const ShardAuthoritativenessSituationMap& auths) const {
    std::stringstream ss;
    ss << "wave: " << wave << ", timestamp: " << format_time(timestamp)
       << ", copyset: " << copysetToString(auths)
       << ", payload size: " << payload_size
       << ", payload hash: " << payload_hash
       << ", flags: " << RECORD_Message::flagsToString(flags);
    return ss.str();
  }

  folly::dynamic
  toDynamic(const ShardAuthoritativenessSituationMap& auths) const {
    folly::dynamic d = folly::dynamic::object();
    d["wave"] = wave;
    d["timestamp"] = timestamp.count();
    d["copyset"] = copysetToString(auths);
    d["payload_size"] = payload_size;
    d["payload_hash"] = payload_hash;
    d["flags"] = RECORD_Message::flagsToString(flags);
    return d;
  }
};

std::string
formatRecordCopies(const std::map<ShardID, RecordCopy>& copies,
                   const ShardAuthoritativenessSituationMap& auths) {
  std::stringstream ss;
  for (const auto& it : copies) {
    ss << "  shard: " << it.first.toString() << ", "
       << it.second.toString(auths) << '\n';
  }
  return ss.str();
}

struct ErrorCategory {
  RecordLevelError error_type;
  bool metadata_log;

  ErrorCategory(RecordLevelError error_type, bool metadata_log)
      : error_type(error_type), metadata_log(metadata_log) {}

  bool operator<(const ErrorCategory& rhs) const {
    if (error_type != rhs.error_type) {
      return error_type < rhs.error_type;
    }
    return metadata_log < rhs.metadata_log;
  }
};
struct Sampler {
  size_t idx = 0;
  size_t stride = 1;

  // Called for each record, returns true if we should print this record.
  // Print records number:
  //   1, 2, 3, ..., 9,
  //   10, 20, 30, ..., 90,
  //   100, 200, 300, ..., 900,
  //   ...
  bool sample() {
    ++idx;
    if (idx / stride >= 10) {
      ld_check(idx == stride * 10);
      stride *= 10;
    }
    return idx % stride == 0;
  }
};
using sampler_map = std::map<ErrorCategory, Sampler>;
// Called for each record we encounter.
void maybeReportRecord(logid_t log,
                       lsn_t lsn,
                       const std::map<ShardID, RecordCopy>& copies,
                       const ShardAuthoritativenessSituationMap& auths,
                       RecordLevelError errors,
                       folly::dynamic& dynamic,
                       sampler_map& samplers) {
  if (checker_settings->report_errors == ReportErrorsMode::NONE) {
    return;
  }

  // Ignore errors listed in --dont-fail-on.
  errors = errors & (~errors_to_ignore);

  if (errors == RecordLevelError::NONE &&
      checker_settings->report_errors != ReportErrorsMode::EVEN_IF_NO_ERRORS) {
    return;
  }

  if (checker_settings->report_errors == ReportErrorsMode::SAMPLE) {
    bool pick = false;
    for (auto i = RecordLevelError::NONE; i < RecordLevelError::MAX; ++i) {
      if ((i & errors) == RecordLevelError::NONE) {
        continue;
      }
      ErrorCategory cat(i, MetaDataLog::isMetaDataLog(log));
      pick |= samplers[cat].sample();
    }
    if (!pick) {
      return;
    }
  }

  std::string lsn_s = lsn_to_string(lsn);
  std::stringstream ss;
  ss << "log " << log.val_ << ": record " << lsn_s << " has errors: ";
  ss << LogErrorTracker::recordLevelErrorsToString(errors);
  ss << "; copies:\n" << formatRecordCopies(copies, auths);
  output(dbg::Level::ERROR, "%s", ss.str().c_str());

  if (checker_settings->json || checker_settings->json_continuous) {
    dynamic[lsn_s] = folly::dynamic::object();
    dynamic[lsn_s]["error_types"] = folly::dynamic::array();
    for (auto i = RecordLevelError::NONE; i < RecordLevelError::MAX; ++i) {
      if ((i & errors) == RecordLevelError::NONE) {
        continue;
      }
      dynamic[lsn_s]["error_types"].push_back(
          LogErrorTracker::recordLevelErrorsToString(i));
    }
    dynamic[lsn_s]["copies"] = folly::dynamic::array();
    for (const auto& it : copies) {
      auto copy = it.second.toDynamic(auths);
      copy["shard"] = formatShardIDWithAuthoritativeness(it.first, auths);
      dynamic[lsn_s]["copies"].push_back(std::move(copy));
    }
  }
  // Too many failures, killing the process.
  if (checker_settings->stop_after_num_errors > 0 &&
      num_failures_to_stop.fetch_sub(1) == 0) {
    ld_info("Ran into %lu errors. Shutting down replication checker.",
            checker_settings->stop_after_num_errors);
    print_stats_and_die_signal(0);
  }
}

// We maintain one such object per log inside the LogChecker class as well as
// one such object for aggregated data per replication factor.
struct CheckStats {
  std::map<size_t, size_t> by_num_copies;
  std::array<size_t,
             folly::findLastSet(static_cast<uint32_t>(RecordLevelError::MAX))>
      by_errors{{0}};

  size_t& getErrorCounter(RecordLevelError err) {
    // TODO(t23409524) move findLastSet as a helper in LogErrorTracker
    auto idx = folly::findLastSet(static_cast<uint32_t>(err));
    ld_check(idx < by_errors.size());
    return by_errors[idx];
  }
  const size_t& getErrorCounter(RecordLevelError err) const {
    auto idx = folly::findLastSet(static_cast<uint32_t>(err));
    ld_check(idx < by_errors.size());
    return by_errors[idx];
  }

  void bumpForError(RecordLevelError err, size_t /*val*/ = 1) {
    ++getErrorCounter(err);
  }

#define FIELDS(field)                                                         \
  field(logs) field(logs_failed) field(num_records_seen) field(bridge_record) \
      field(multiple_waves) field(big_copyset) field(hole) field(bytes)       \
          field(trimmed_bytes) field(records_above_bridge)                    \
              field(copies_on_authoritative_empty_nodes)                      \
                  field(authoritative_empty_in_copyset)                       \
                      field(records_rebuilding_pending)

#define DECLARE_FIELD(name) size_t name = 0;
  FIELDS(DECLARE_FIELD)
#undef DECLARE_FIELD

  // Returns true if there were no non-bridge records.
  bool noRealRecords() const {
    ld_check(num_records_seen >= bridge_record);
    return getErrorCounter(RecordLevelError::DATALOSS) == 0 &&
        num_records_seen == bridge_record;
  }

  void merge(const CheckStats& rhs) {
#define MERGE_FIELD(name) name += rhs.name;
    FIELDS(MERGE_FIELD)
#undef MERGE_FIELD

    for (auto& it : rhs.by_num_copies) {
      by_num_copies[it.first] += it.second;
    }

    for (auto i = RecordLevelError::NONE; i < RecordLevelError::MAX; ++i) {
      getErrorCounter(i) += rhs.getErrorCounter(i);
    }
  }

  std::string toString(std::string indent) const {
    std::stringstream ss;

#define PUT_SUB(name, cnt)                       \
  do {                                           \
    size_t x = (cnt);                            \
    if (x)                                       \
      ss << indent << name << ": " << x << "\n"; \
  } while (false);
#define PUT(name) PUT_SUB(#name, name)
    FIELDS(PUT)

    for (auto& it : by_num_copies) {
      PUT_SUB(it.first << "_copies", it.second);
    }
    for (auto i = RecordLevelError::NONE; i < RecordLevelError::MAX; ++i) {
      std::string name = LogErrorTracker::recordLevelErrorsToString(i);
      PUT_SUB(name, getErrorCounter(i));
    }

    return ss.str();
#undef PUT
#undef PUT_SUB
  }

  folly::dynamic toDynamic() const {
    folly::dynamic res = folly::dynamic::object;
#define PUT(name) res[#name] = name;
    FIELDS(PUT)

    for (auto& it : by_num_copies) {
      res[folly::to<std::string>(it.first) + "_copies"] = it.second;
    }
    for (auto i = RecordLevelError::NONE; i < RecordLevelError::MAX; ++i) {
      std::string name = LogErrorTracker::recordLevelErrorsToString(i);
      auto counter_value = getErrorCounter(i);
      if (!counter_value) {
        continue;
      }
      res[name] = counter_value;
    }
    return res;
#undef PUT
  }

  bool hasFailures() const {
    // There is a failure if we found an incorrectly replicated record...
    for (auto i = RecordLevelError::NONE; i < RecordLevelError::MAX; ++i) {
      if (getErrorCounter(i) > 0 &&
          ((errors_to_ignore & i) == RecordLevelError::NONE)) {
        return true;
      }
    }
    // Or if we could not read a log or we found an empty metadata log.
    return logs_failed;
  }
};

struct GlobalStats {
  std::map<size_t, CheckStats> by_replication_factor;

  void merge(const GlobalStats& rhs) {
    for (auto& it : rhs.by_replication_factor) {
      by_replication_factor[it.first].merge(it.second);
    }
  }

  CheckStats aggregate() const {
    CheckStats res;
    for (auto& it : by_replication_factor) {
      res.merge(it.second);
    }
    return res;
  }

  std::string toString(std::string indent) const {
    std::stringstream ss;
    for (auto& it : by_replication_factor) {
      ss << indent << "r = " << it.first << ":\n"
         << it.second.toString(indent + "  ");
    }
    ss << indent << "total:\n" << aggregate().toString(indent + "  ");
    return ss.str();
  }

  folly::dynamic toDynamic() const {
    folly::dynamic r = folly::dynamic::object;
    for (auto& it : by_replication_factor) {
      auto obj = it.second.toDynamic();
      r[folly::to<std::string>(it.first)] = std::move(obj);
    }
    folly::dynamic res = folly::dynamic::object;
    res["by_replication_factor"] = std::move(r);
    res["total"] = aggregate().toDynamic();
    return res;
  }

  bool hasFailures() const {
    for (auto& it : by_replication_factor) {
      if (it.second.hasFailures()) {
        return true;
      }
    }
    return false;
  }
};

struct PerfStats {
  std::atomic<uint64_t> ncopies_received{0};
  std::atomic<uint64_t> payload_bytes{0};
  std::atomic<uint64_t> nrecords_processed{0};
  std::atomic<uint64_t> ngaps_processed{0};
  std::atomic<uint64_t> finished_logs{0};
  std::atomic<uint64_t> total_logs{0};
  std::mutex mutex;
  std::set<logid_t> logs_in_flight;
  std::atomic<uint64_t> sync_seq_requests_in_flight{0};
};

class LogChecker : public std::enable_shared_from_this<LogChecker> {
 public:
  LogChecker(ClientImpl& client_impl,
             LogCheckRequest rq,
             std::shared_ptr<PerfStats> perf_stats,
             std::function<void(std::shared_ptr<LogChecker>)> done_callback)
      : client_impl_(client_impl),
        did_findtime_(false),
        log_id_(rq.log_id),
        done_callback_(done_callback),
        start_lsn_(std::max(1ul, rq.start_lsn)),
        until_lsn_(std::min(LSN_MAX, rq.until_lsn)),
        latest_replication_factor_(rq.latest_replication_factor),
        cfg_(config->get()),
        perf_stats_(perf_stats),
        callbackHelper_(this) {}

  std::string getError() const {
    return error_;
  }

  CheckStats getStats() const {
    return stats_;
  }

  void finishIfIdle() {
    auto* stream = Worker::onThisThread()->clientReadStreams().getStream(rsid_);
    if (stream && stream->isStreamStuckFor(checker_settings->idle_timeout)) {
      finish("Stream has been stuck for too long: it is idle");
    }
  }

  folly::dynamic getRecordErrors() const {
    return std::move(record_errors);
  }

  logid_t getLogID() const {
    return log_id_;
  }

  size_t getLatestReplicationFactor() const {
    return latest_replication_factor_;
  }

  std::set<copyset_size_t> getReplicationFactors() const {
    return replication_factors_;
  }

  void start() {
    if (!did_findtime_) {
      std::lock_guard<std::mutex> lock(perf_stats_->mutex);
      ld_check(!perf_stats_->logs_in_flight.count(log_id_));
      perf_stats_->logs_in_flight.insert(log_id_);
    }

    auto callback_ticket = callbackHelper_.ticket();
    // Invoke findtime to get the starting lsn for read. FindTime does not work
    // for metadata logs. Hence, read them completely hoping that they are quick
    // to read.
    if (checker_settings->read_starting_point.count() > 0 &&
        !MetaDataLog::isMetaDataLog(log_id_) && did_findtime_ == false) {
      auto timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch() -
          checker_settings->read_starting_point);
      int rv = client_impl_.findTime(
          log_id_,
          timestamp,
          [callback_ticket](Status rv, lsn_t result) {
            callback_ticket.postCallbackRequest([=](LogChecker* checker) {
              if (!checker) {
                return;
              }
              if (rv != Status::OK) {
                ld_error("Find time failed for log: %lu with %s",
                         checker->log_id_.val(),
                         error_description(rv));
                checker->finish(error_description(rv));
                return;
              }
              checker->start_lsn_ = result;
              checker->did_findtime_ = true;
              checker->start();
            });
          },
          FindKeyAccuracy::STRICT);
      if (rv != 0) {
        finish("Find time failed with NOBUFS");
      }
      return;
    }
    std::weak_ptr<LogChecker> self_weak = shared_from_this();
    read_duration_timer_ = std::make_unique<Timer>([self_weak] {
      // The user requires to not spend more time reading this log. Finish
      // successfully.
      if (auto self = self_weak.lock()) {
        self->finish("");
      }
    });
    auto elapsed_time = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now() - start_time);
    auto execution_time_left =
        checker_settings->max_execution_time - elapsed_time;
    auto timer_duration =
        std::min(checker_settings->read_duration, execution_time_left);

    read_duration_timer_->activate(timer_duration);

    throttle_timer_ = std::make_unique<Timer>([self_weak] {
      if (auto self = self_weak.lock()) {
        self->onThrottleTimerTick();
      }
    });

    if (until_lsn_ != LSN_MAX) {
      startReading();
      return;
    }

    auto cb = [=](Status st,
                  NodeID /*seq*/,
                  lsn_t next_lsn,
                  std::unique_ptr<LogTailAttributes> /*tail_attributes*/,
                  std::shared_ptr<const EpochMetaDataMap> /*metadata_map*/,
                  std::shared_ptr<TailRecord> /*tail_record*/,
                  folly::Optional<bool> /*is_log_empty*/) {
      callback_ticket.postCallbackRequest([=](LogChecker* checker) {
        if (!checker) {
          ld_error("SyncSequencerRequest finished after"
                   " LogChecker was destroyed");
          return;
        }
        if (st != E::OK) {
          --perf_stats_->sync_seq_requests_in_flight;
          char buf[128];
          snprintf(
              buf, 128, "SyncSequencerRequest timed out: %s", error_name(st));
          checker->finish(buf);
          return;
        }
        checker->onGotNextLSN(next_lsn);
      });
    };
    std::unique_ptr<Request> rq = std::make_unique<SyncSequencerRequest>(
        log_id_,
        0,
        cb,
        GetSeqStateRequest::Context::SYNC_SEQUENCER,
        checker_settings->sync_sequencer_timeout);
    ld_debug("Posting a new SyncSequencerRequest(id:%" PRIu64 ") for log:%lu",
             (uint64_t)rq->id_,
             log_id_.val_);
    Worker::onThisThread()->processor_->postWithRetrying(rq);
    ++perf_stats_->sync_seq_requests_in_flight;
  }

 private:
  ClientImpl& client_impl_;
  bool did_findtime_{false};
  sampler_map samplers;
  const logid_t log_id_;
  const std::function<void(std::shared_ptr<LogChecker>)> done_callback_;
  lsn_t start_lsn_;
  lsn_t until_lsn_;
  size_t latest_replication_factor_;
  read_stream_id_t rsid_{READ_STREAM_ID_INVALID};
  std::shared_ptr<Configuration> cfg_;
  std::shared_ptr<PerfStats> perf_stats_;

  CheckStats stats_;
  std::string error_;
  std::set<copyset_size_t> replication_factors_;
  WorkerCallbackHelper<LogChecker> callbackHelper_;

  ClientReadStream* stream_ = nullptr; // owned by AllClientReadStreams
  std::map<lsn_t, std::map<ShardID, RecordCopy>> records_;
  lsn_t last_seen_bridge_;
  epoch_t current_epoch_ = EPOCH_INVALID;
  std::unique_ptr<FailureDomainNodeSet<uint64_t>> replication_checker_;
  uint64_t replication_check_counter_ = 1;

  folly::dynamic record_errors = folly::dynamic::object(); // Used if json==true

  std::unique_ptr<Timer> read_duration_timer_;
  std::unique_ptr<Timer> throttle_timer_;

  // Used in order to throttle read throughput.
  bool throttled_ = false;
  bool finished_{false};
  size_t bytes_ = 0;

  void onThrottleTimerTick() {
    bytes_ = 0;
    if (throttled_) {
      throttled_ = false;
      stream_->resumeReading();
    }
    throttle_timer_->activate(std::chrono::milliseconds{100});
  }

  void onGotNextLSN(lsn_t next_lsn) {
    --perf_stats_->sync_seq_requests_in_flight;
    until_lsn_ = std::max(LSN_OLDEST + 1, next_lsn) - 1;
    ld_info("Reading log %lu until LSN %s",
            log_id_.val_,
            lsn_to_string(until_lsn_).c_str());
    startReading();
  }

  void startReading() {
    if (until_lsn_ < start_lsn_) {
      finish("");
      return;
    }

    std::weak_ptr<LogChecker> self_weak = shared_from_this();
    rsid_ = processor->issueReadStreamID();
    auto callback_ticket = callbackHelper_.ticket();
    auto stream = std::make_unique<ClientReadStream>(
        rsid_,
        log_id_,
        start_lsn_,
        until_lsn_,
        0.5, // flow_control_threshold
        ClientReadStreamBufferType::CIRCULAR,
        settings->client_read_buffer_size,
        std::make_unique<ClientReadStreamDependencies>(
            rsid_,
            log_id_,
            "",
            [self_weak](std::unique_ptr<DataRecord>& record) {
              if (auto self = self_weak.lock()) {
                if (!self->gotAllCopiesOfRecord(record->attrs.lsn)) {
                  return false;
                }
                record.reset();
                return true;
              }
              return false;
            },
            [self_weak](const GapRecord& gap) {
              if (auto self = self_weak.lock()) {
                self->gotGap(gap);
              }
              return true;
            },
            [self_weak](logid_t) {
              if (auto self = self_weak.lock()) {
                self->finish("");
              }
            },
            nullptr, // metadata_cache
            nullptr,
            [self_weak](ShardID from, const DataRecordOwnsPayload* record) {
              if (auto self = self_weak.lock()) {
                self->gotRecordCopy(from, record);
              }
            }),
        config);
    stream->requireFullReadSet();
    stream->forceNoSingleCopyDelivery();
    stream->shipPseudorecords();
    stream->waitForAllCopies();
    START_flags_t flags = START_Header::INCLUDE_EXTRA_METADATA |
        START_Header::DIRECT | START_Header::PAYLOAD_HASH_ONLY;
    if (checker_settings->csi_data_only) {
      flags |= START_Header::CSI_DATA_ONLY;
    }
    stream->addStartFlags(flags);

    if (MetaDataLog::isMetaDataLog(log_id_)) {
      // Releases are not reliable for metadata logs. Just ignore them.
      // This may cause false positive errors if there's a running Appender
      // for LSN=until_lsn_.
      // TODO (#9815085): Probably the right way to fix this would be to
      // wait for WRITTEN_IN_METADATALOG flag to appear in epoch store.
      stream->ignoreReleasedStatus();
    }

    stream_ = stream.get();
    Worker::onThisThread()->clientReadStreams().insertAndStart(
        std::move(stream));

    throttle_timer_->activate(std::chrono::milliseconds{100});
  }

  void gotRecordCopy(ShardID from, const DataRecordOwnsPayload* record) {
    lsn_t lsn = record->attrs.lsn;

    ld_debug("log %lu: got record from %s with lsn %s",
             log_id_.val_,
             from.toString().c_str(),
             lsn_to_string(record->attrs.lsn).c_str());

    if (!record->extra_metadata_) {
      ld_error("log %lu: got record without extra metadata from %s with "
               "lsn %s; ignoring",
               log_id_.val_,
               from.toString().c_str(),
               lsn_to_string(lsn).c_str());
      records_[lsn]; // acknowledge that we've seen this LSN
      return;
    }

    RecordCopy rec = {
        record->extra_metadata_->header.wave,
        0, // payload hash
        record->extra_metadata_->copyset,
        record->attrs.timestamp,
        0, // payload size
        record->flags_,
    };
    rec.parsePayload(record->payload);

    ++perf_stats_->ncopies_received;
    perf_stats_->payload_bytes += rec.payload_size;

    // NOTE: You may be tempted to assert that records_[lsn] doesn't have a copy
    //       from this node, because ClientReadStream should reject duplicates,
    //       Nope, we may get duplicates if ClientReadStream rewound following
    //       a data loss.
    records_[lsn].emplace(from, rec);
  }

  void refreshEpochMetaData(lsn_t lsn) {
    ld_check(stream_);
    if (lsn_to_epoch(lsn) == current_epoch_) {
      return;
    }
    current_epoch_ = lsn_to_epoch(lsn);
    const auto meta = stream_->getCurrentEpochMetadata();
    ld_check(meta);
    replication_checker_ = std::make_unique<FailureDomainNodeSet<lsn_t>>(
        meta->shards,
        *cfg_->serverConfig()->getNodesConfigurationFromServerConfigSource(),
        meta->replication);
  }

  bool gotAllCopiesOfRecord(lsn_t lsn) {
    auto g = std::chrono::milliseconds{1000} / std::chrono::milliseconds{100};
    if (bytes_ >= checker_settings->per_log_max_bps / g) {
      throttled_ = true;
      return false;
    }

    ld_check(!records_.empty());
    ++perf_stats_->nrecords_processed;
    lsn_t trim_point = stream_->getTrimPointLowerBound();
    while (records_.begin()->first <= trim_point) {
      stats_.trimmed_bytes +=
          records_.begin()->second.begin()->second.payload_size;
      records_.erase(records_.begin());
      if (records_.empty()) {
        return true;
      }
    }
    if (lsn <= trim_point) {
      return true;
    }
    if (lsn_to_epoch(last_seen_bridge_) != EPOCH_INVALID) {
      // If there are records above bridge, ClientReadStream may call
      // gotRecordCopy() for some of their copies but won't call
      // gotAllRecordCopies() for them. Discard such copies.
      while (records_.begin()->first < lsn &&
             lsn_to_epoch(records_.begin()->first) ==
                 lsn_to_epoch(last_seen_bridge_) &&
             records_.begin()->first > last_seen_bridge_) {
        ld_debug("Discarding record %s of log %lu because it's above bridge "
                 "record %s",
                 lsn_to_string(records_.begin()->first).c_str(),
                 log_id_.val(),
                 lsn_to_string(last_seen_bridge_).c_str());
        ++stats_.records_above_bridge;

        // Only holes are expected above bridge record.
        bool all_holes = true;
        for (auto& copy : records_.begin()->second) {
          if (!(copy.second.flags & RECORD_Header::HOLE)) {
            all_holes = false;
            break;
          }
        }
        if (!all_holes) {
          auto e = RecordLevelError::BRIDGE_RECORD_CONFLICT;
          stats_.bumpForError(e);
          maybeReportRecord(log_id_,
                            records_.begin()->first,
                            records_.begin()->second,
                            ShardAuthoritativenessSituationMap(),
                            e,
                            record_errors,
                            samplers);
        }

        records_.erase(records_.begin());
        ld_check(!records_.empty());
      }
    }
    ld_check(records_.begin()->first == lsn);

    const auto& copies = records_.begin()->second;

    if (copies.empty()) {
      ld_error("log %lu: all copies of record %s didn't have extra metadata"
               "; ignoring",
               log_id_.val_,
               lsn_to_string(lsn).c_str());
      records_.erase(records_.begin());
      return true;
    }

    bytes_ += copies.begin()->second.payload_size;
    Slice s;
    folly::Range<const ShardID*> copyset(copies.begin()->second.copyset.begin(),
                                         copies.begin()->second.copyset.end());
    bytes_ += LocalLogStoreRecordFormat::recordHeaderSizeEstimate(
        copies.begin()->second.flags, copyset.size(), Slice(), OffsetMap());

    refreshEpochMetaData(lsn);
    copyset_size_t replication_factor =
        stream_->getCurrentEpochMetadata()->replication.getReplicationFactor();

    // Look at all copies and see if anything is weird.

    ++stats_.by_num_copies[copies.size()];
    ++stats_.num_records_seen;
    stats_.bytes += copies.begin()->second.payload_size;
    // (wave | (written_by_recovery << 32))  -> copyset -> nodes
    std::map<uint64_t, std::map<copyset_t, std::vector<ShardID>>> wave_copysets;
    // Union of copysets and shards that sent us a copy.
    std::vector<ShardID> relevant_shards;
    bool different_data = false;
    bool copyset_without_self = false;
    bool copyset_not_in_nodeset = false;
    bool small_copyset = false;
    bool duplicates_in_copyset = false;
    // "Pending rebuilding" means that the set of copies we've received is
    // underreplicated, but if we add the set of shards that are known
    // to have lost records (AuthoritativeStatus::UNDERREPLICATION and,
    // GapState::UNDER_REPLICATED), the set becomes fully replicated.
    bool pending_rebuilding = false;
    int hole_mask = 0;
    int bridge_mask = 0;
    RecordLevelError errors{RecordLevelError::NONE};

    auto on_error = [&](RecordLevelError err) {
      errors = errors | err;
      stats_.bumpForError(err);
    };

    for (auto& it : copies) {
      if (!(it.second.flags & facebook::logdevice::RECORD_Header::DRAINED) &&
          std::find(it.second.copyset.begin(),
                    it.second.copyset.end(),
                    it.first) == it.second.copyset.end()) {
        // The copyset doesn't contain the node that sent us this copyset.
        copyset_without_self = true;
        relevant_shards.push_back(it.first);
      }
      for (ShardID s : it.second.copyset) {
        relevant_shards.push_back(s);
        if (!replication_checker_->containsShard(s)) {
          copyset_not_in_nodeset = true;
        }
      }
      if (it.second.copyset.size() < replication_factor) {
        small_copyset = true;
      }
      copyset_t sorted_copyset = it.second.copyset;
      std::sort(sorted_copyset.begin(), sorted_copyset.end());
      if (std::unique(sorted_copyset.begin(), sorted_copyset.end()) !=
          sorted_copyset.end()) {
        duplicates_in_copyset = true;
      }

      // wave_index = (wave | (written_by_recovery << 32))
      // This ensures that records that were written by recovery are considered
      // to be higher waves than records written by appenders
      uint64_t recovery_flag =
          !!(it.second.flags & RECORD_Header::WRITTEN_BY_RECOVERY);
      uint64_t wave_index = it.second.wave | (recovery_flag << 32);
      wave_copysets[wave_index][it.second.copyset].push_back(it.first);
      if (!it.second.sameData(copies.begin()->second)) {
        different_data = true;
      }
      hole_mask |= 1 << !!(it.second.flags & RECORD_Header::HOLE);
      bridge_mask |= 1 << !!(it.second.flags & RECORD_Header::BRIDGE);
    }

    std::sort(relevant_shards.begin(), relevant_shards.end());
    relevant_shards.erase(
        std::unique(relevant_shards.begin(), relevant_shards.end()),
        relevant_shards.end());

    // Look at relevant shards and find which ones have something unusual about
    // them: not in nodeset, authoritative empty, etc.
    ShardAuthoritativenessSituationMap auths;
    {
      auto& auth_info = stream_->getGapFailureDomain();
      for (ShardID s : relevant_shards) {
        auto situation = characterizeShardAuthoritativeness(s, auth_info);
        if (situation != ShardAuthoritativenessSituation::FULLY_AUTHORITATIVE) {
          auths[s] = situation;
        }
      }
    }

    auto check_can_replicate = [&](uint64_t check_id) {
      if (replication_checker_->canReplicate(check_id)) {
        return true;
      }
      for (auto p : auths) {
        if (p.second == ShardAuthoritativenessSituation::UNDERREPLICATION ||
            p.second == ShardAuthoritativenessSituation::GAP_UNDER_REPLICATED) {
          replication_checker_->setShardAttribute(p.first, check_id);
        }
      }
      if (replication_checker_->canReplicate(check_id)) {
        pending_rebuilding = true;
        return true;
      }
      return false;
    };

    for (auto& it : copies) {
      if (auths.count(it.first) &&
          auths.at(it.first) ==
              ShardAuthoritativenessSituation::AUTHORITATIVE_EMPTY) {
        // It's possible but suspicious that an AUTHORITATIVE_EMPTY node would
        // send us a record. Normally if an AUTHORITATIVE_EMPTY node is able to
        // send records it transitions to FULLY_AUTHORITATIVE.
        // Check such copies for hole/record conflicts and data mismatch, but
        // skip them when checking for proper replication.
        ++stats_.copies_on_authoritative_empty_nodes;
      }
    }

    if (copyset_without_self) {
      on_error(RecordLevelError::COPYSET_WITHOUT_SELF);
    }
    // TODO(T21641471) report for metadata logs after known issue gets resolved
    if (copyset_not_in_nodeset && !isMetaDataLog(log_id_)) {
      on_error(RecordLevelError::COPYSET_NOT_IN_NODESET);
    }
    ld_debug("log %lu: record %s copies:\n%s",
             log_id_.val_,
             lsn_to_string(lsn).c_str(),
             formatRecordCopies(copies, auths).c_str());
    if (hole_mask == 3) {
      // Some copies are hole plugs while others have payload.
      on_error(RecordLevelError::HOLE_RECORD_CONFLICT);
    }
    if (bridge_mask == 3) {
      on_error(RecordLevelError::BRIDGE_RECORD_CONFLICT);
    }
    if (hole_mask != 3 && bridge_mask != 3 && different_data) {
      // Different copies have different payload or attributes, except
      // if it's a hole/bridge-record conflict.
      on_error(RecordLevelError::DATA_MISMATCH);
    }
    if (hole_mask & 2) {
      // At least one copy is a hole.
      ++stats_.hole;
    }
    if (bridge_mask & 2) {
      // At least one copy is a bridge.
      ++stats_.bridge_record;
      last_seen_bridge_ = lsn;
    }

    ld_check(wave_copysets.size() >= 1);
    if (wave_copysets.size() > 1) {
      // Copies have at least two different waves (including 0).
      ++stats_.multiple_waves;
    }
    // Look at last wave, or wave 0 (hole plug) if it exists.
    auto copysets = wave_copysets.begin()->first
        ? wave_copysets.rbegin()->second
        : wave_copysets.begin()->second;
    ld_check(copysets.size() >= 1);
    bool count_record =
        (bridge_mask == 1 || !checker_settings->dont_count_bridge_records);
    if (!count_record) {
      records_.erase(records_.begin());
      return true;
    }
    if (copysets.size() > 1) {
      // Last wave copies have at least two different copysets.
      on_error(RecordLevelError::DIFFERENT_COPYSET_IN_LATEST_WAVE);
    }
    bool big = false;
    bool accurate = false;
    bool good_replication = false;
    for (auto& it : copysets) {
      if (it.first.size() > replication_factor) {
        big = true;
      }
      if (it.second.size() >=
          std::min(it.first.size(), (size_t)replication_factor)) {
        bool contains_authoritative_empty = false;
        uint64_t check_id = ++replication_check_counter_;
        for (auto n : it.second) {
          if (auths.count(n) &&
              auths.at(n) ==
                  ShardAuthoritativenessSituation::AUTHORITATIVE_EMPTY) {
            // Copyset containing an authoritative empty node is no good.
            // Don't count it as accurate or properly replicated.
            // But don't raise an error either: such copysets can legitimately
            // exist if a node was unavailable during rebuilding of another
            // node, then came back with data.
            contains_authoritative_empty = true;
            break;
          }
          // `n` is guaranteed to be contained in the nodeset of this epoch
          // because it's a node from which ClientReadStream has accepted
          // a copy of this record.
          replication_checker_->setShardAttribute(n, check_id);
        }
        if (contains_authoritative_empty) {
          ++stats_.authoritative_empty_in_copyset;
        } else {
          accurate = true;
          if (check_can_replicate(check_id)) {
            good_replication = true;
          }
        }
      }
    }
    if (small_copyset) {
      // Some of the copies have copyset smaller than r.
      on_error(RecordLevelError::SMALL_COPYSET);
    }
    if (duplicates_in_copyset) {
      // Some shard id occurs multiple times in the same copyset.
      on_error(RecordLevelError::DUPLICATES_IN_COPYSET);
    }
    if (big) {
      // Some of the copies have copyset bigger than r.
      ++stats_.big_copyset;
    }
    if (!accurate) {
      // There's no copyset such that at least r of its nodes have a last wave
      // record with this copyset.
      on_error(RecordLevelError::NO_ACCURATE_COPYSET);
    }
    if (!good_replication) {
      // None of the accurate copysets is properly distributed across failure
      // domains.
      on_error(RecordLevelError::BAD_REPLICATION_LAST_WAVE);

      uint64_t check_id = ++replication_check_counter_;
      for (auto& it : copies) {
        if (auths.count(it.first) &&
            auths.at(it.first) ==
                ShardAuthoritativenessSituation::AUTHORITATIVE_EMPTY) {
          // Copies on AUTHORITATIVE_EMPTY nodes are best-effort.
          // Don't count them.
          continue;
        }
        replication_checker_->setShardAttribute(it.first, check_id);
      }
      if (!check_can_replicate(check_id)) {
        // Even if we consider all copies of this record, they are not properly
        // distributed across failure domains.
        on_error(RecordLevelError::BAD_REPLICATION);
      }
    }

    if (pending_rebuilding) {
      ++stats_.records_rebuilding_pending;
    }

    maybeReportRecord(
        log_id_, lsn, copies, auths, errors, record_errors, samplers);

    ld_debug(
        "log %lu: checked record %s", log_id_.val_, lsn_to_string(lsn).c_str());

    records_.erase(records_.begin());
    return true;
  }

  void gotGap(const GapRecord& gap) {
    ++perf_stats_->ngaps_processed;
    switch (gap.type) {
      case GapType::UNKNOWN:
      case GapType::HOLE:
      case GapType::MAX:
        ld_check(false);
        break;
      case GapType::BRIDGE:
        break;
      case GapType::TRIM:
        break;
      case GapType::DATALOSS: {
        output(dbg::Level::ERROR,
               "log %lu: data loss %s - %s",
               gap.logid.val_,
               lsn_to_string(gap.lo).c_str(),
               lsn_to_string(gap.hi).c_str());
        stats_.bumpForError(RecordLevelError::DATALOSS, gap.hi - gap.lo + 1);
        // Give a sample only for the boundaries of the gap. We don't want to
        // iterate on all lsns in [gap.lo, gap.hi] as this gap may span epochs.
        maybeReportRecord(log_id_,
                          gap.lo,
                          {},
                          {},
                          RecordLevelError::DATALOSS,
                          record_errors,
                          samplers);
        maybeReportRecord(log_id_,
                          gap.hi,
                          {},
                          {},
                          RecordLevelError::DATALOSS,
                          record_errors,
                          samplers);
        break;
      }
      case GapType::ACCESS:
        output(dbg::Level::ERROR,
               "log %lu: access denied %s -  %s",
               gap.logid.val_,
               lsn_to_string(gap.lo).c_str(),
               lsn_to_string(gap.hi).c_str());
        break;
      case GapType::NOTINCONFIG:
        output(dbg::Level::ERROR,
               "log %lu was removed from the config",
               gap.logid.val_);
        break;
      case GapType::FILTERED_OUT:
        output(dbg::Level::ERROR,
               "log %lu: Record filtered out. From %s to %s",
               gap.logid.val_,
               lsn_to_string(gap.lo).c_str(),
               lsn_to_string(gap.hi).c_str());
        break;
    }
  }

  void finish(std::string error) {
    if (finished_) {
      return;
    }
    finished_ = true;
    {
      std::lock_guard<std::mutex> lock(perf_stats_->mutex);
      ld_check(perf_stats_->logs_in_flight.count(log_id_));
      perf_stats_->logs_in_flight.erase(log_id_);
    }
    error_ = error;
    stats_.logs = 1;
    stats_.logs_failed = !error_.empty();
    if (!stats_.logs_failed && stats_.noRealRecords() &&
        MetaDataLog::isMetaDataLog(log_id_)) {
      // ToDO: stats_.bumpForError(ReplicationError::EMPTY_METADATA_LOG);
    }
    auto done_callback = done_callback_;
    auto self = shared_from_this();
    if (rsid_.val_) {
      // finish() can be called from ClientReadStream callback, so we can't
      // deallocate it right here. Instead post a StopReadingRequest.
      std::unique_ptr<Request> rq = std::make_unique<StopReadingRequest>(
          (ReadingHandle){Worker::onThisThread()->idx_, rsid_},
          [self]() mutable { self.reset(); });
      processor->postWithRetrying(rq);
    }

    done_callback_(self);
  }
};
} // namespace
// After everything is done, calls done_callback and doesn't delete itself.
// You need to delete it either inside the callback or afterwards.
class PerWorkerCoordinatorRequest : public Request {
 public:
  PerWorkerCoordinatorRequest(int thread_idx,
                              std::shared_ptr<PerfStats> perf_stats,
                              std::function<void()> done_callback,
                              ClientImpl& client_impl)
      : Request(RequestType::CHECKER_PER_WORKER_COORDINATOR),
        thread_idx_(thread_idx),
        client_impl_(client_impl),
        done_callback_(done_callback),
        perf_stats_(perf_stats) {}

  Execution execute() override {
    if (checker_settings->idle_timeout != std::chrono::seconds{0}) {
      idle_timer_ = std::make_unique<Timer>([this] { checkIdleLogCheckers(); });
      idle_timer_->activate(checker_settings->idle_timeout);
    }
    startMoreWork();
    if (in_flight_.empty()) {
      done_callback_();
      // Return Execution::CONTINUE anyway because `worker_coordinators` is
      // the owner of PerWorkerCoordinatorRequest.
    }
    return Execution::CONTINUE;
  }

  int getThreadAffinity(int /*nthreads*/) override {
    return thread_idx_;
  }

  GlobalStats getStats() const {
    GlobalStats res = stats_;
    for (auto c : in_flight_) {
      mergeStats(res, c);
    }
    return res;
  }

  folly::dynamic getPerLogStatsDynamic() const {
    return per_log_stats;
  }

 private:
  int thread_idx_;
  ClientImpl& client_impl_;
  std::function<void()> done_callback_;
  std::shared_ptr<PerfStats> perf_stats_;
  GlobalStats stats_;
  struct LogCheckerPtrComp {
    bool operator()(const std::shared_ptr<LogChecker>& l,
                    const std::shared_ptr<LogChecker>& r) const {
      return l.get() < r.get();
    }
  };
  std::set<std::shared_ptr<LogChecker>, LogCheckerPtrComp> in_flight_;
  folly::dynamic per_log_stats = folly::dynamic::object(); // Used if json==true
  std::unique_ptr<Timer> idle_timer_;

  void checkIdleLogCheckers() {
    auto log_checkers_in_flight = in_flight_;
    for (auto& lc : log_checkers_in_flight) {
      lc->finishIfIdle();
    }
    idle_timer_->activate(checker_settings->idle_timeout);
  }

  void startMoreWork() {
    while (in_flight_.size() < checker_settings->logs_in_flight_per_worker) {
      LogCheckRequest rq = {LOGID_INVALID, 0, 0, 0};
      size_t logs_left = 0;
      {
        std::lock_guard<std::mutex> lock(mutex);
        if (!logs_to_check.empty()) {
          rq = logs_to_check.back();
          logs_to_check.pop_back();
          logs_left = logs_to_check.size();
        }
      }
      auto current_time = std::chrono::steady_clock::now();
      auto elapsed_time = std::chrono::duration_cast<std::chrono::microseconds>(
          current_time - start_time);
      if (rq.log_id == LOGID_INVALID ||
          elapsed_time >= checker_settings->max_execution_time) {
        break;
      }
      if (rq.start_lsn == LSN_INVALID) {
        continue;
      }
      auto checker = std::make_shared<LogChecker>(
          client_impl_,
          rq,
          perf_stats_,
          [this, rq](std::shared_ptr<LogChecker> c) { onLogDone(rq, c); });
      in_flight_.insert(checker);
      ld_info("starting log %lu, %lu in flight, %lu / %lu left",
              rq.log_id.val_,
              in_flight_.size(),
              logs_left,
              logs_to_check_initial_count);
      checker->start();
    }
  }

  // Merges stats from `c` into `stats`.
  static void mergeStats(GlobalStats& stats, std::shared_ptr<LogChecker> c) {
    auto st = c->getStats();
    auto rs = c->getReplicationFactors();
    size_t r = c->getLatestReplicationFactor();
    // I bet no one will ever see these warnings, but if you did:
    // TODO: instead of logging these warnings just aggregate different epochs
    //       into different `by_replication_factor` stats.
    if (rs.size() > 1) {
      output(dbg::Level::WARNING,
             "warning: log %lu: epochs have different replication factors, may "
             "be aggregated into wrong bucket of total stats; individual "
             "record checks are correct though",
             c->getLogID().val_);
    }
    if (!rs.empty() && !rs.count(r)) {
      output(dbg::Level::WARNING,
             "warning: log %lu: all epochs have replication factors different "
             "from the one in config, may be aggregated into wrong bucket of "
             "total stats; individual record checks are correct though",
             c->getLogID().val_);
    }
    stats.by_replication_factor[r].merge(st);
  }

  void onLogDone(LogCheckRequest rq, std::shared_ptr<LogChecker> c) {
    ++perf_stats_->finished_logs;
    ld_check(in_flight_.count(c));
    in_flight_.erase(c);
    ld_info("finished log %lu%s, %lu in flight",
            rq.log_id.val_,
            (c->getError().empty() ? std::string()
                                   : " with error \"" + c->getError() + "\"")
                .c_str(),
            in_flight_.size());
    if (!c->getError().empty()) {
      output(dbg::Level::ERROR,
             "error: log %lu failed: %s",
             rq.log_id.val_,
             c->getError().c_str());
    }
    auto st = c->getStats();
    folly::dynamic d = folly::dynamic::object();
    if (checker_settings->json_continuous || checker_settings->json) {
      std::string logid = folly::to<std::string>(rq.log_id.val_);
      if (st.num_records_seen > 0) {
        d["stats"] = st.toDynamic();
        d["record_errors"] = c->getRecordErrors();
        if (checker_settings->json_continuous) {
          folly::dynamic data = folly::dynamic::object();
          data["log_id"] = logid;
          data["is_metadata_log"] = MetaDataLog::isMetaDataLog(rq.log_id);
          data["total_logs"] = perf_stats_->total_logs.load();
          data["finished_logs"] = perf_stats_->finished_logs.load();
          data["log_results"] = d;
          folly::json::serialization_opts opts;
          auto json = folly::json::serialize(data, opts);
          {
            std::lock_guard<std::mutex> lock(mutex);
            std::cout << json << std::endl;
            std::cout.flush();
          }
        }
      }
    }
    if (c->getError().empty() && st.hasFailures()) {
      output(dbg::Level::INFO,
             "log %lu stats:\n%s",
             rq.log_id.val_,
             st.toString("  ").c_str());
      if (checker_settings->json) {
        std::string logid = folly::to<std::string>(rq.log_id.val_);
        per_log_stats[logid] = std::move(d);
      }
    }
    mergeStats(stats_, c);
    startMoreWork();
    if (in_flight_.empty()) {
      done_callback_();
    }
  }
};

class StatThread {
 public:
  explicit StatThread(std::shared_ptr<PerfStats> perf_stats)
      : perf_stats_(perf_stats) {}

  void stop() {
    shutdown_.signal();
  }

  void operator()() {
    using std::chrono::steady_clock;

    auto tstart = steady_clock::now();
    auto last_update_time = tstart;

    uint64_t last_ncopies_received = 0;
    uint64_t last_payload_bytes = 0;

    while (!shutdown_.waitFor(std::chrono::seconds(1))) {
      auto tnow = steady_clock::now();

      uint64_t ncopies_received = perf_stats_->ncopies_received.load();
      uint64_t payload_bytes = perf_stats_->payload_bytes.load();
      uint64_t nrecords_processed = perf_stats_->nrecords_processed.load();
      uint64_t ngaps_processed = perf_stats_->ngaps_processed.load();
      uint64_t sync_seq_requests_in_flight =
          perf_stats_->sync_seq_requests_in_flight.load();

      std::stringstream logs_in_flight_str;
      {
        std::lock_guard<std::mutex> lock(perf_stats_->mutex);
        logs_in_flight_str << perf_stats_->logs_in_flight.size();
        if (!perf_stats_->logs_in_flight.empty()) {
          logs_in_flight_str << " [";
          const size_t limit = 10;
          size_t i = 0;
          for (auto log : perf_stats_->logs_in_flight) {
            if (i > 0) {
              logs_in_flight_str << ", ";
            }
            if (i >= limit) {
              logs_in_flight_str << "...";
              break;
            }
            logs_in_flight_str << log.val_;
            ++i;
          }
          logs_in_flight_str << "]";
        }
      }

      double runtime =
          std::chrono::duration_cast<std::chrono::duration<double>>(tnow -
                                                                    tstart)
              .count();
      double since_last =
          std::max(1e-3,
                   std::chrono::duration_cast<std::chrono::duration<double>>(
                       tnow - last_update_time)
                       .count());
      ld_info("ncopies_received = %lu, payload_bytes = %lu, "
              "nrecords_processed = %lu, ngaps_processed = %lu, "
              "copies/s = %.0f, payload_bytes/s = %.0f, logs_in_flight = %s, "
              "sync_seq_requests_in_flight = %lu, elapsed time = %.1fs",
              ncopies_received,
              payload_bytes,
              nrecords_processed,
              ngaps_processed,
              (ncopies_received - last_ncopies_received) / since_last,
              (payload_bytes - last_payload_bytes) / since_last,
              logs_in_flight_str.str().c_str(),
              sync_seq_requests_in_flight,
              runtime);

      last_update_time = tnow;

      last_ncopies_received = ncopies_received;
      last_payload_bytes = payload_bytes;
    }
  }

 private:
  SingleEvent shutdown_;
  std::shared_ptr<PerfStats> perf_stats_;
};

class CurrentStatsRequest : public Request {
 public:
  explicit CurrentStatsRequest(std::function<void()> callback)
      : Request(RequestType::CHECKER_CURRENT_STATS), callback_(callback) {}

  int getThreadAffinity(int) override {
    return 0;
  }

  Execution execute() override {
    responses_remaining_ = processor->getWorkerCount(WorkerType::GENERAL);
    for (int i = 0; i < responses_remaining_; ++i) {
      std::unique_ptr<Request> rq = std::make_unique<WorkerRequest>(this, i);
      processor->postWithRetrying(rq);
    }
    return Execution::CONTINUE;
  }

 private:
  class WorkerResponse : public Request {
   public:
    WorkerResponse(CurrentStatsRequest* owner, GlobalStats stats)
        : Request(RequestType::CHECKER_WORKER_RESPONSE),
          owner_(owner),
          stats_(stats) {}

    int getThreadAffinity(int) override {
      return 0;
    }

    Execution execute() override {
      owner_->gotWorkerStats(stats_);
      return Execution::COMPLETE;
    }

   private:
    CurrentStatsRequest* owner_;
    GlobalStats stats_;
  };

  class WorkerRequest : public Request {
   public:
    WorkerRequest(CurrentStatsRequest* owner, int worker_idx)
        : Request(RequestType::CHECKER_WORKER_REQUEST),
          owner_(owner),
          worker_idx_(worker_idx) {}

    int getThreadAffinity(int) override {
      return worker_idx_;
    }

    Execution execute() override {
      GlobalStats stats;
      PerWorkerCoordinatorRequest* c = worker_coordinators[worker_idx_].get();
      if (c) {
        stats = c->getStats();
      }
      std::unique_ptr<Request> rq =
          std::make_unique<WorkerResponse>(owner_, stats);
      processor->postWithRetrying(rq);
      return Execution::COMPLETE;
    }

   private:
    CurrentStatsRequest* owner_;
    int worker_idx_;
  };

  std::function<void()> callback_;
  GlobalStats stats_;
  int responses_remaining_;

  void gotWorkerStats(const GlobalStats& stats) {
    stats_.merge(stats);
    --responses_remaining_;

    if (!responses_remaining_) {
      output(dbg::Level::INFO,
             "current overall stats:\n%s",
             stats_.toString("  ").c_str());
      callback_();
      delete this;
    }
  }
};

void setup_signal_handler(int signum, void (*handler)(int), int flags = 0) {
  struct sigaction sa;
  sa.sa_handler = handler;
  sigemptyset(&sa.sa_mask);
  sa.sa_flags = flags;
  int rv = sigaction(signum, &sa, nullptr);
  ld_check(rv == 0);
}

void print_stats_signal(int) {
  ld_info("Received signal to print current stats.");
  std::unique_ptr<Request> rq = std::make_unique<CurrentStatsRequest>([] {});
  processor->postWithRetrying(rq);
}

void print_stats_and_die_signal(int) {
  ld_info("Received termination signal. Printing current stats. Hit control-C "
          "again to terminate immediately");
  std::unique_ptr<Request> rq =
      std::make_unique<CurrentStatsRequest>([] { _exit(130); });
  processor->postWithRetrying(rq);
}

/**
 * Setup a callback for the USR2 signal which causes us to dump the state of all
 * ClientReadStream objects on all workers.
 */
void setup_dump_state_trigger() {
  if (!processor) {
    return;
  }
  struct sigaction sa;
  sa.sa_handler = [](int /*sig*/) {
    std::string data = AllClientReadStreams::getAllReadStreamsDebugInfo(
        true, false, *processor);
    ld_info("\n%s", data.c_str());
  };
  sigemptyset(&sa.sa_mask);
  sa.sa_flags = 0;
  const int rv = sigaction(SIGUSR2, &sa, nullptr);
  ld_check(rv == 0);
}

int main(int argc, const char** argv) {
  // These parameters are used to split the work of checking between multiple
  // checker instances. When the checker process is spawned it is passed the
  // number of checker instances that will run (numTasks) and the ID of this
  // instance (taskId). Based on this, the instance can pick the logs it will
  // be responsible for. The default is one checker instance.
  int numTasks = 1;
  int taskId = 0;

  logdeviceInit();

  client_settings.reset(new ClientSettingsImpl);
  auto settings_updater = client_settings->getSettingsUpdater();
  UpdateableSettings<ReplicationCheckerSettings> checker_settings_;
  settings_updater->registerSettings(checker_settings_);

  std::string config_path;
  std::vector<logid_t> logids_to_check;
  auto fallback_fn = [&](int ac, const char* av[]) {
    namespace style = boost::program_options::command_line_style;
    boost::program_options::options_description opts(
        "Replication checker settings", line_length, min_description_length);
    // clang-format off
      opts.add_options()
      ("help,h", "produce this help message and exit")
      ("verbose,v", "by default --help only prints replication checker settings"
       ", use this option to print all settings")
      ("tier,t",
       boost::program_options::value<std::string>()
         ->default_value("")
         ->notifier([&](const std::string& val) -> void {
             if (val.empty()) {
               return;
             }
             config_path.clear();
             config_path += "configerator:logdevice/";
             config_path += val;
             config_path += ".conf";
             std::cout << config_path << std::endl;
           }
         ),
       "tier name to use in constructing configerator config path"
      )
      ("config-path",
       boost::program_options::value<std::string>()
         ->default_value("")
         ->notifier(
           [&](const std::string& val) {
             if (val.empty()) {
               return;
             }
             config_path = val;
           }
         ),
       "location of the cluster config file to use. Format: "
       "[file:]<path-to-config-file> or configerator:<configerator-path>"
      )
      ("num-tasks,n",
       boost::program_options::value<int>(&numTasks)
       ->default_value(numTasks)
       ->notifier([](int val) -> void {
          if (val < 1) {
            throw boost::program_options::error(
            "Invalid value for --num-tasks. Expected >= 1");
          }
         }),
       "number of checker processes running in paralel against the same tier. "
       "If > 1 then it's passed in to this tier so that in can be used by this "
       "instance, in conjunction with --task-id, to determine which logs to check "
       "from this task instance."
      )
      ("task-id,n",
       boost::program_options::value<int>(&taskId)
       ->default_value(taskId)
       ->notifier([](int val) -> void {
          if (val < 0) {
            throw boost::program_options::error(
            "Invalid value for --task-id. Expected >= 0");
          }
         }),
       "Task ID of this instance. Used in conjunction with --num-tasks "
       "to decide which logs to check from this task."
      )
      ("logs,l",
       boost::program_options::value<std::string>()
       ->notifier(
         [&logids_to_check = logids_to_check](const std::string& val) -> void {
           if (parse_logid_intervals(val.c_str(), &logids_to_check) != 0) {
             throw boost::program_options::error(
               "invalid value of --logs (-l) option. "
               "expected a single log id, a range of ids lo..hi or a "
               "comma-separated list of logs or disjoint ranges), "
               "got " + val);
           }
         }
       ),
       "check log(s) with the specified id(s); comma-separated list of ranges, "
       "e.g. 10..20,42,2..2,3..5"
      )
      ("loglevel",
       boost::program_options::value<std::string>()
         ->default_value("info")
         ->notifier(dbg::parseLoglevelOption),
       "One of the following: critical, error, warning, info, debug, spew"
      );
    // clang-format on

    boost::program_options::variables_map parsed =
        program_options_parse_no_positional(ac, av, opts);

    if (parsed.count("help")) {
      folly::Optional<std::string> bundle;
      if (!parsed.count("verbose")) {
        bundle = checker_settings_->getName();
      }
      boost::program_options::options_description errors_description(
          "Description of each replication error type",
          line_length,
          min_description_length);

      for (auto err = LogLevelError::NONE; err < LogLevelError::MAX; ++err) {
        errors_description.add_options()(
            LogErrorTracker::logLevelErrorsToString(err).c_str(),
            LogErrorTracker::describeLogError(err).c_str());
      }
      for (auto err = RecordLevelError::NONE; err < RecordLevelError::MAX;
           ++err) {
        errors_description.add_options()(
            LogErrorTracker::recordLevelErrorsToString(err).c_str(),
            LogErrorTracker::describeRecordError(err).c_str());
      };
      std::cout << opts << std::endl
                << std::endl
                << settings_updater->help(SettingFlag::CLIENT,
                                          bundle,
                                          line_length,
                                          min_description_length)
                << std::endl
                << std::endl
                << errors_description << std::endl
                << std::endl;
      exit(0);
    }
    std::vector<std::string> mutually_exclusive_options;
    mutually_exclusive_options.push_back("config-path");
    mutually_exclusive_options.push_back("tier");
    check_no_option_conflicts(parsed, mutually_exclusive_options, true);
    // Surface any errors
    boost::program_options::notify(parsed);
  };
  try {
    settings_updater->parseFromCLI(
        argc, argv, &SettingsUpdater::mustBeClientOption, fallback_fn);
    // Copying UpdateableSettings out of ClientSettings
    settings = client_settings->getSettings();
  } catch (const boost::program_options::error& ex) {
    std::cerr << argv[0] << ": " << ex.what() << '\n';
    exit(1);
  }
  checker_settings = checker_settings_.get();
  if (checker_settings->num_logs_to_check >= 0 && !logids_to_check.empty()) {
    std::cerr << "--num-logs-to-check and --logs are incompatible" << std::endl;
    exit(2);
  }
  if (numTasks > 1 && !logids_to_check.empty()) {
    std::cerr << "--numTasks > 1 and --logs are incompatible" << std::endl;
    exit(2);
  }
  errors_to_ignore = checker_settings->dont_fail_on_errors;
  if (!checker_settings->enable_noisy_errors) {
    const RecordLevelError noisy_errors =
        RecordLevelError::DIFFERENT_COPYSET_IN_LATEST_WAVE |
        RecordLevelError::NO_ACCURATE_COPYSET |
        RecordLevelError::BAD_REPLICATION_LAST_WAVE;
    errors_to_ignore |= noisy_errors;
  }
  start_time = std::chrono::steady_clock::now();
  ldclient = ClientFactory()
                 .setClientSettings(std::move(client_settings))
                 .setTimeout(checker_settings->client_timeout)
                 .create(config_path);

  if (!ldclient) {
    ld_error("Could not create client: %s", error_description(err));
    return 1;
  }

  ClientImpl* client_impl = static_cast<ClientImpl*>(ldclient.get());
  config = client_impl->getConfig();

  auto cfg = config->get();
  auto logs_config = cfg->localLogsConfig();
  std::set<logid_t> logids_to_check_set(
      logids_to_check.begin(), logids_to_check.end());
  for (auto it = logs_config->logsBegin(); it != logs_config->logsEnd(); ++it) {
    auto add_log = [&](logid_t log_id, size_t replication_factor) {
      if (!logids_to_check_set.empty() && !logids_to_check_set.count(log_id)) {
        return;
      }

      if ((log_id.val_ % numTasks) == taskId) {
        ld_debug("Queueing log %lu for checking", log_id.val_);
        logs_to_check.push_back({
            log_id,
            replication_factor,
            1,       // start_lsn
            LSN_MAX, // until_lsn
        });
      } else {
        ld_debug("Skipping log %lu due to instance filter", log_id.val_);
      }
    };
    // Add log and its metadata log.
    if (!checker_settings->only_metadata_logs) {
      add_log(
          logid_t(it->first),
          ReplicationProperty::fromLogAttributes(it->second.log_group->attrs())
              .getReplicationFactor());
    }
    if (!checker_settings->only_data_logs) {
      add_log(metaDataLogID(logid_t(it->first)),
              ReplicationProperty::fromLogAttributes(
                  cfg->serverConfig()->getMetaDataLogGroup()->attrs())
                  .getReplicationFactor());
    }
  }
  std::shuffle(
      logs_to_check.begin(), logs_to_check.end(), folly::ThreadLocalPRNG());
  if (checker_settings->num_logs_to_check >= 0) {
    size_t n = checker_settings->num_logs_to_check >= 1
        ? (size_t)checker_settings->num_logs_to_check
        : (size_t)(checker_settings->num_logs_to_check * logs_to_check.size() +
                   .5);
    n = std::max(1ul, std::min(logs_to_check.size(), n));
    logs_to_check.resize(n);
  }
  logs_to_check_initial_count = logs_to_check.size();

  processor = &(client_impl->getProcessor());

  setup_dump_state_trigger();

  num_failures_to_stop.store(checker_settings->stop_after_num_errors);

  ld_info("will check %lu logs, %lu logs at a time on each of the %d worker "
          "threads %s",
          logs_to_check.size(),
          checker_settings->logs_in_flight_per_worker,
          processor->getWorkerCount(WorkerType::GENERAL),
          folly::sformat(
              ". will stop process on {} errors", num_failures_to_stop.load())
              .c_str());

  auto perf_stats = std::make_shared<PerfStats>();
  perf_stats->total_logs = logs_to_check.size();
  StatThread stat_thread_obj(perf_stats);
  std::thread stat_thread(std::ref(stat_thread_obj));

  const int nworkers = processor->getWorkerCount(WorkerType::GENERAL);
  Semaphore sem;
  worker_coordinators.resize(nworkers);

  int requests_posted = 0;
  int rv = 0;
  for (int i = 0; i < nworkers; ++i) {
    PerWorkerCoordinatorRequest* rq_raw = new PerWorkerCoordinatorRequest(
        i, perf_stats, [&sem]() { sem.post(); }, *client_impl);
    std::unique_ptr<Request> rq(rq_raw);
    rv = processor->postRequest(rq);
    if (rv == 0) {
      ++requests_posted;
      // Create another unique_ptr from the same raw pointer. This is ok
      // because PerWorkerCoordinatorRequest::execute will return
      // Execution::CONTINUE, making Processor release the ownership.
      worker_coordinators[i].reset(rq_raw);
    } else {
      ld_error("failed to post PerWorkerCoordinatorRequest %d", i);
    }
  }

  // SA_RESETHAND resets the handler to default after the first signal.
  // This makes hitting control-C for the second time kill the process.
  setup_signal_handler(SIGINT, print_stats_and_die_signal, SA_RESETHAND);
  setup_signal_handler(SIGTERM, print_stats_and_die_signal, SA_RESETHAND);
  setup_signal_handler(SIGUSR1, print_stats_signal);

  for (int i = 0; i < requests_posted; ++i) {
    sem.wait();
    ld_info("%d workers done", i + 1);
  }

  setup_signal_handler(SIGINT, SIG_DFL);
  setup_signal_handler(SIGTERM, SIG_DFL);
  setup_signal_handler(SIGUSR1, SIG_DFL);

  stat_thread_obj.stop();

  GlobalStats st;
  for (const auto& rq : worker_coordinators) {
    st.merge(rq->getStats());
  }

  ld_info("all done");
  output(dbg::Level::INFO, "done; total stats:\n%s", st.toString("  ").c_str());
  if (checker_settings->json && !checker_settings->json_continuous) {
    folly::dynamic per_log_stats = folly::dynamic::object();
    for (const auto& rq : worker_coordinators) {
      per_log_stats.update(rq->getPerLogStatsDynamic());
    }
    folly::dynamic data = folly::dynamic::object();
    if (!checker_settings->summary_only) {
      data["per_log"] = per_log_stats;
    }
    data["summary"] = st.toDynamic();
    folly::json::serialization_opts opts;
    opts.pretty_formatting = true;
    std::string json = folly::json::serialize(data, opts);
    std::cout << json << std::endl;
  }

  worker_coordinators.clear();
  stat_thread.join();
  return st.hasFailures() ? 1 : 0;
}
