/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cmath>
#include <type_traits>

#include <boost/format.hpp>
#include <boost/python.hpp>
#include <folly/container/Array.h>

#include "logdevice/clients/python/logdevice_logsconfig.h"
#include "logdevice/clients/python/util/util.h"
#include "logdevice/common/Semaphore.h"
#include "logdevice/common/ZookeeperClient.h"
#include "logdevice/common/commandline_util_chrono.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/toString.h"
#include "logdevice/common/util.h"
#include "logdevice/include/Client.h"
#include "logdevice/include/debug.h"

namespace facebook { namespace logdevice {
std::ostream& operator<<(std::ostream& os, logid_t logid) {
  return os << toString(logid);
}
}} // namespace facebook::logdevice

using namespace boost::python;
using namespace facebook::logdevice;
using namespace facebook::logdevice::logsconfig;

object errorClass;
static PyObject* loggingCallback = nullptr;

void throw_logdevice_exception(boost::python::object extra) {
  ld_check(!errorClass.is_none());

  // I'm oddly uncomfortable with this, despite the fact that it is kind of
  // defined as "never fails, ever" in the LogDevice code.  Old habits die
  // hard, but not as hard as my SIGSEGV server if it fails, I guess.
  auto info = errorStrings()[err];
  object name(info.name != nullptr ? info.name : "Unknown");
  object description(info.description != nullptr
                         ? info.description
                         : "An unknown error has occurred.");

  object args = extra.is_none() ? make_tuple(err, name, description)
                                : make_tuple(err, name, description, extra);

  throw_python_exception(errorClass, args);
}

auto const logLevelLetters =
    folly::make_array('-', 'C', 'E', 'W', 'N', 'I', 'D', 'S');

void logging_cb(const char* cluster,
                const char* file,
                const char* fn,
                const int line,
                dbg::Level lvl,
                struct timeval created,
                pid_t tid,
                const char* thread_name,
                const char* msg) {
  if (loggingCallback == nullptr) {
    return;
  }

  if (lvl == dbg::Level::SPEW || lvl == dbg::Level::NONE) {
    // ignore spew level
    return;
  }

  struct tm now_tm;
  ::localtime_r(&created.tv_sec, &now_tm);
  char record[2048];
  int hdrlen = 0;
  hdrlen = snprintf(record,
                    sizeof(record),
                    "%c%02d%02d %02d:%02d:%02d.%06lu %7d [%s] %s:%d] %s() %s",
                    logLevelLetters[(unsigned)lvl],
                    now_tm.tm_mon + 1,
                    now_tm.tm_mday,
                    now_tm.tm_hour,
                    now_tm.tm_min,
                    now_tm.tm_sec,
                    (unsigned long)created.tv_usec,
                    tid,
                    thread_name,
                    file,
                    line,
                    fn,
                    msg);

  const char* level = nullptr;
  switch (lvl) {
    case dbg::Level::CRITICAL:
      level = "critical";
      break;
    case dbg::Level::ERROR:
      level = "error";
      break;
    case dbg::Level::WARNING:
      level = "warning";
      break;
    case dbg::Level::NOTIFY:
    case dbg::Level::INFO:
      level = "info";
      break;
    case dbg::Level::DEBUG:
      level = "debug";
      break;
    case dbg::Level::SPEW:
    case dbg::Level::NONE:
      break;
  }

  if (PyInterpreterState_Head() == NULL) {
    return;
  }
  PyGILState_STATE gstate;
  gstate = PyGILState_Ensure();

  call<void>(loggingCallback, cluster, level, record);

  /* Release the thread. No Python API allowed beyond this point. */
  PyGILState_Release(gstate);
}

void logdevice_use_python_logging(object cb) {
  if (loggingCallback != nullptr) {
    throw_python_exception(
        PyExc_RuntimeError,
        ("Logging callback is already set. use_python_logging() can only "
         "be called once per application"));
  }
  incref(cb.ptr());
  loggingCallback = cb.ptr();
  dbg::useCallback(logging_cb);
  ld_info("Using python logging");
}

/**
 * Create a new LogDevice client, and return a boost shared_ptr to it, which
 * makes it work sensibly with boost::python.
 */
boost::shared_ptr<Client> logdevice_make_client(object name,
                                                object config,
                                                object timeout_seconds,
                                                dict settings,
                                                object credentials,
                                                object csid) {
  folly::Optional<std::chrono::milliseconds> timeout;

  if (!timeout_seconds.is_none()) {
    double extracted_timeout = extract_double(timeout_seconds, "timeout", true);
    timeout.assign(std::chrono::milliseconds(lround(extracted_timeout * 1000)));
  }

  std::unique_ptr<ClientSettings> client_settings(ClientSettings::create());

  // unfortunately there are no nice helpers for iterating over a Python
  // dictionary in boost::python, so this is the cleanest thing I could figure
  // out to do here.
  stl_input_iterator<object> begin(settings.keys()), end;
  std::for_each(begin, end, [&](object key) {
    auto string = extract_string(key, "settings key");
    auto value = settings.get(key);

    int result = -1;

    // try treating the value as an int first.
    extract<int64_t> value_as_int64_t(value);
    if (value_as_int64_t.check()) {
      result = client_settings->set(string.c_str(), value_as_int64_t());
    } else {
      // this will throw if it isn't a string or unicode
      result = client_settings->set(
          string.c_str(), extract_string(value, "settings value").c_str());
    }

    if (result != 0)
      throw_logdevice_exception(key);
  });

  std::string client_name = extract_string(name, "name");
  std::string config_path = extract_string(config, "config");
  std::string credentials_str = extract_string(credentials, "credentials");
  std::string csid_str = extract_string(csid, "csid");
  std::shared_ptr<Client> client;
  {
    gil_release_and_guard guard;
    ClientFactory factory;
    factory.setClusterName(std::move(client_name))
        .setCredentials(std::move(credentials_str))
        .setClientSettings(std::move(client_settings))
        .setCSID(std::move(csid_str));
    if (timeout.hasValue()) {
      factory.setTimeout(timeout.value());
    }
    client = factory.create(std::move(config_path));
  }
  if (client) {
    return make_boost_shared_ptr_from_std_shared_ptr(client);
  }
  throw_logdevice_exception();
  throw std::runtime_error("impossible, the line above always throws!");
}

void logdevice_set_timeout(Client& self, double timeout_seconds) {
  auto timeout = std::chrono::milliseconds(lround(timeout_seconds * 1000));
  self.setTimeout(timeout);
}

std::string get_directory_delimiter(Client& self) {
  return self.getLogNamespaceDelimiter();
}

tuple logdevice_get_log_range_by_name(Client& self, object name) {
  logid_range_t range = self.getLogRangeByName(extract_string(name, "name"));
  if (range.first != LOGID_INVALID) {
    return make_tuple(range.first.val_, range.second.val_);
  }
  throw_logdevice_exception();
  throw std::runtime_error("impossible, the line above always throws!");
}

lsn_t logdevice_get_tail_lsn(Client& self, logid_t logid) {
  lsn_t lsn;
  {
    gil_release_and_guard guard;
    lsn = self.getTailLSNSync(logid);
  }
  if (lsn != LSN_INVALID) {
    return lsn;
  }
  throw_logdevice_exception();
  throw std::runtime_error("impossible, the line above always throws!");
}

tuple logdevice_get_tail_attributes(Client& self, logid_t logid) {
  std::unique_ptr<LogTailAttributes> attributes;
  {
    gil_release_and_guard guard;
    attributes = self.getTailAttributesSync(logid);
  }
  if (!attributes) {
    throw_logdevice_exception();
    throw std::runtime_error("impossible, the line above always throws!");
  } else {
    return make_tuple(attributes->last_released_real_lsn,
                      attributes->last_timestamp.count(),
                      attributes->offsets.getCounter(BYTE_OFFSET));
  }
}

tuple logdevice_get_head_attributes(Client& self, logid_t logid) {
  std::unique_ptr<LogHeadAttributes> attributes;
  {
    gil_release_and_guard guard;
    attributes = self.getHeadAttributesSync(logid);
  }
  if (!attributes) {
    throw_logdevice_exception();
    throw std::runtime_error("impossible, the line above always throws!");
  } else {
    return make_tuple(
        attributes->trim_point, attributes->trim_point_timestamp.count());
  }
}

lsn_t logdevice_append(Client& self, logid_t logid, object data) {
  lsn_t lsn;
  std::string pl = extract_string(data, "data");
  {
    gil_release_and_guard guard;
    lsn = self.appendSync(logid, std::move(pl));
  }
  if (lsn != LSN_INVALID)
    return lsn;
  throw_logdevice_exception();
  throw std::runtime_error("impossible, the line above always throws!");
}

lsn_t logdevice_find_time(Client& self, logid_t logid, double seconds) {
  auto ts = std::chrono::milliseconds(lround(seconds * 1000));
  lsn_t lsn;
  Status status;
  {
    gil_release_and_guard guard;
    lsn = self.findTimeSync(logid, ts, &status);
  }

  // We do not want to return partial results
  if (status == E::OK) {
    return lsn;
  }
  throw_logdevice_exception();
  throw std::runtime_error("impossible, the line above always throws!");
}

tuple logdevice_find_key(Client& self, logid_t logid, std::string key) {
  FindKeyResult result;
  {
    gil_release_and_guard guard;
    result = self.findKeySync(logid, std::move(key));
  }
  if (result.lo != LSN_INVALID || result.hi != LSN_INVALID) {
    return make_tuple(result.lo, result.hi);
  }
  throw_logdevice_exception();
  throw std::runtime_error("impossible, the line above always throws!");
}

void logdevice_trim(Client& self, logid_t logid, lsn_t lsn) {
  int res;
  {
    gil_release_and_guard guard;
    res = self.trimSync(logid, lsn);
  }
  if (res != 0) {
    throw_logdevice_exception();
    throw std::runtime_error("impossible, the line above always throws!");
  }
}

bool logdevice_is_log_empty(Client& self, logid_t logid) {
  bool empty;
  int rv;
  {
    gil_release_and_guard guard;
    rv = self.isLogEmptySync(logid, &empty);
  }
  if (rv != 0) {
    throw_logdevice_exception();
  }
  return empty;
}

size_t logdevice_data_size(Client& self,
                           logid_t logid,
                           double start_sec = -1,
                           double end_sec = -1) {
  size_t result_size;
  int rv;
  std::chrono::milliseconds start_ts = start_sec == -1
      ? std::chrono::milliseconds::min()
      : std::chrono::milliseconds(lround(start_sec * 1000));
  std::chrono::milliseconds end_ts = end_sec == -1
      ? std::chrono::milliseconds::max()
      : std::chrono::milliseconds(lround(end_sec * 1000));

  {
    gil_release_and_guard guard;
    rv = self.dataSizeSync(
        logid, start_ts, end_ts, DataSizeAccuracy::APPROXIMATE, &result_size);
  }
  if (rv != 0) {
    throw_logdevice_exception();
  }
  return result_size;
}

size_t logdevice_get_max_payload_size(Client& self) {
  auto maxSize = self.getMaxPayloadSize();
  return maxSize;
}

int64_t timestr_to_seconds(object timestring) {
  std::chrono::seconds out;
  int rv = parse_chrono_string(extract_string(timestring, "data"), &out);
  if (rv != 0) {
    throw_python_exception(
        PyExc_ValueError, "Could not convert the time string!");
  }
  return out.count();
}

const char* SMOKE_TEST_PAYLOAD_FMT = "Payload message # %1%";

lsn_t smoke_test_for_log(Client& client, const logid_t& logid, int count) {
  object logging = import("logging");
  object logger = logging.attr("getLogger")("LogDeviceSmokeTest");
  Semaphore sem;
  int passed = 0;
  int failed = 0;
  lsn_t first_lsn(LSN_MAX);

  auto cb = [&](Status st, const DataRecord& r) {
    if (st != E::OK) {
      failed++;
      logger.attr("error")(
          str((boost::format("Append failed for LogID %1%: %2% %3%") %
               logid.val() % error_name(st) % error_description(st))
                  .str()));
    } else {
      // Storing first LSN
      if (first_lsn > r.attrs.lsn) {
        first_lsn = r.attrs.lsn;
      }
      passed++;
    }
    if ((failed + passed) >= count) {
      sem.post();
    }
  };

  for (auto i = 0; i < count; ++i) {
    std::string payload = (boost::format(SMOKE_TEST_PAYLOAD_FMT) % i).str();
    int rv = client.append(logid, payload, cb);
    if (rv != 0) {
      logger.attr("error")(str((boost::format("Append call failed for LogID"
                                              "%1%: %2% (err: %3%)") %
                                logid.val() % rv % err)
                                   .str()));
      failed++;
    }
  }

  // Did all of them fail?
  if ((failed + passed) >= count) {
    sem.post();
  }

  sem.wait();

  if (failed > 0) {
    // Some appends failed in the middle
    throw_logdevice_exception(
        str((boost::format("Some append failed for %1%: only %2% out of %3% "
                           "appends have been successful so far!") %
             logid % passed % (passed + failed))
                .str()));
  }
  return first_lsn;
}

dict smoke_test_write(Client& client, list log_ids, int count) {
  object logging = import("logging");
  object logger = logging.attr("getLogger")("LogDeviceSmokeTestWrite");

  logger.attr("info")(str("Appending to logs"));

  dict starting_points;

  // Append it to different logs
  stl_input_iterator<object> begin(log_ids), end;
  std::for_each(begin, end, [&](object logid_obj) {
    logid_t id(convert_or_throw<uint64_t>(logid_obj, ""));
    logger.attr("debug")(
        str((boost::format("Appending, LogID: %1%") % id.val()).str()));
    lsn_t lsn = smoke_test_for_log(client, id, count);
    logger.attr("debug")((boost::format("First LSN: %1%") % lsn).str());
    // Building LogID -> LSN map
    starting_points[id.val()] = static_cast<uint64_t>(lsn);
  });

  logger.attr("info")(str("All appends finished"));
  return starting_points;
}

void smoke_test_read(Client& client, dict starting_points, int count) {
  object logging = import("logging");
  object logger = logging.attr("getLogger")("LogDeviceSmokeTestRead");

  std::unique_ptr<Reader> reader = client.createReader(len(starting_points));
  ld_check(reader != nullptr);
  reader->setTimeout(std::chrono::milliseconds(60 * 1000));

  logger.attr("info")(str("Reading from logs"));
  stl_input_iterator<object> begin(starting_points.keys()), end;
  std::for_each(begin, end, [&](object log_id_obj) {
    logid_t log_id(convert_or_throw<uint64_t>(log_id_obj, ""));
    lsn_t lsn(convert_or_throw<uint64_t>(starting_points.get(log_id_obj), ""));
    logger.attr("debug")(
        (boost::format("Started reading from LSN %1%, LogID: %2%") % lsn %
         log_id)
            .str());
    reader->startReading(log_id, lsn);
  });

  size_t records_expected = len(starting_points) * count;

  std::vector<std::unique_ptr<DataRecord>> records;
  GapRecord gap;
  logger.attr("info")(
      (boost::format("Waiting for %1% records") % records_expected).str());

  while ((records.size() < records_expected) && reader->isReadingAny()) {
    ssize_t records_left = records_expected - records.size();
    ssize_t nread = reader->read(records_left, &records, &gap);

    if (nread == -1) {
      logger.attr("info")(
          (boost::format("Got a gap for log %1% in lsn %2% of type %3%") %
           gap.logid % gap.lo % int32_t(gap.type))
              .str());
    } else if (nread > 0) {
      records_left -= nread;
      logger.attr("info")((boost::format("Got %1% records") % nread).str());
    } else {
      logger.attr("info")(str("Got no more records"));
      break;
    }

    if (records_left > 0) {
      logger.attr("info")(
          (boost::format("Still missing %1% records out of %2%") %
           records_left % records_expected)
              .str());
    }
  }

  if (records.size() < records_expected) {
    throw_logdevice_exception(
        str((boost::format("We have received only %1% records out of %2%") %
             records.size() % records_expected)
                .str()));
  }

  // Validate it - at last
  std::map<logid_t, size_t> match_pos; // next expected record ID
  for (auto& it : records) {
    auto map_iter = match_pos.find(it->logid);
    size_t index = 0;
    if (map_iter != match_pos.end()) {
      index = map_iter->second;
    }

    if (index >= count) {
      logger.attr("debug")(str("All records read"));
      break;
    }

    std::string expected_payload =
        (boost::format(SMOKE_TEST_PAYLOAD_FMT) % (index)).str();
    match_pos[it->logid] = index + 1;

    std::string received_payload{it->payload.toString()};
    if (received_payload != expected_payload) {
      throw_logdevice_exception(
          str("Payload did not match: " + received_payload +
              "!= " + expected_payload));
    }
  }
}

void smoke_test(Client& client, list log_ids, int count) {
  object logging = import("logging");
  object logger = logging.attr("getLogger")("LogDeviceSmokeTest");

  logger.attr("info")(str("Smoke test started"));
  dict logid_to_lsn = smoke_test_write(client, log_ids, count);
  smoke_test_read(client, logid_to_lsn, count);
  logger.attr("info")(str("Smoke test successfull"));
}

std::string seconds_to_timestr(int64_t seconds) {
  std::chrono::seconds input(seconds);
  return format_chrono_string(input);
}

int64_t timestr_to_milliseconds(object timestring) {
  std::chrono::milliseconds out;
  int rv = parse_chrono_string(extract_string(timestring, "data"), &out);
  if (rv != 0) {
    throw_python_exception(
        PyExc_ValueError, "Could not convert the time string!");
  }
  return out.count();
}

std::string milliseconds_to_timestr(int64_t millis) {
  std::chrono::milliseconds input(millis);
  return format_chrono_string(input);
}

object logdevice_create_reader(Client& self, size_t max_logs) {
  return wrap_logdevice_reader(self.createReader(max_logs));
}

void set_logging_level(dbg::Level level) {
  dbg::currentLevel = level;
  ZookeeperClient::setDebugLevel(level);
}

dbg::Level get_logging_level() {
  return dbg::currentLevel;
}

bool validate_json(object main, object logs, bool verbose) {
  dbg::Level lvl = get_logging_level();
  // Force loglevel to info because we want to see the validation errors
  if (verbose && lvl < dbg::Level::INFO) {
    set_logging_level(dbg::Level::INFO);
  }
  int ret = Configuration::validateJson(extract_string(main, "main").c_str(),
                                        extract_string(logs, "logs").c_str());

  set_logging_level(lvl);
  return (ret == 0);
}

std::string normalize_json(object main, object logs, bool verbose) {
  dbg::Level lvl = get_logging_level();
  if (verbose && lvl < dbg::Level::INFO) {
    set_logging_level(dbg::Level::INFO);
  }

  std::string res =
      Configuration::normalizeJson(extract_string(main, "main").c_str(),
                                   extract_string(logs, "logs").c_str());

  set_logging_level(lvl);

  if (res.empty()) {
    throw_logdevice_exception(str("invalid config"));
  }

  return res;
}

str get_setting(ClientSettings& self, str key) {
  std::string s_key = extract_string(key, "key");
  auto val = self.get(s_key);
  if (!val) {
    throw_python_exception(PyExc_KeyError, "Invalid key");
  }
  return str(*val);
}

int set_setting(ClientSettings& self, str key, object value) {
  int result = -1;
  std::string s_key = extract_string(key, "key");
  extract<int64_t> value_as_int64_t(value);
  if (value_as_int64_t.check()) {
    result = self.set(s_key.c_str(), value_as_int64_t());
  } else {
    std::string s_value = extract_string(value, "value");
    result = self.set(s_key.c_str(), s_value.c_str());
  }
  if (result != 0) {
    throw_logdevice_exception();
  }
  return result;
}

// set() is overloaded in ClientSettings
int set_settings(ClientSettings& self, dict settings) {
  stl_input_iterator<str> begin(settings.keys()), end;
  std::for_each(begin, end, [&](str key) {
    auto value = settings.get(key);
    set_setting(self, key, value);
  });
  return 0;
}

BOOST_PYTHON_MODULE(client) {
  // register our error class to handle exceptions
  errorClass = createExceptionClass("LogDeviceError", R"DOC(
A LogDevice error occurred.  Details vary by type.

The `args` contain the tuple (code, name, description, extra)

CODE is the error code number.
NAME is the short (string) name of the error.
DESCRIPTION is the long description of the error.

EXTRA is optional, but if present contains an extra object
that can be used to better understand the failure.

At this point only data loss gaps use EXTRA, and they return
the gap descriptor object.
)DOC");

  register_logsconfig_types();

  class_<ClientSettings, boost::noncopyable>("ClientSettings",
                                             R"DOC(
ClientSettings object used to expose an api for get() and set() to the
python client)DOC",
                                             no_init)
      .def("get", &get_setting)
      .def("set", &set_setting)
      .def("set", &set_settings);

  class_<Client,
         boost::shared_ptr<facebook::logdevice::Client>,
         boost::noncopyable>("Client",
                             R"DOC(
LogDevice client object.  This only exposes synchronous operations to
Python at this stage, and raises when errors happen.
)DOC",
                             no_init)

      .def("__init__",
           make_constructor(&logdevice_make_client,
                            default_call_policies(),
                            (arg("name"),
                             arg("config"),
                             arg("timeout") = object(), // None
                             arg("settings") = dict(),
                             arg("credentials") = "",
                             arg("csid") = "")),
           R"DOC(
Initialize a Client instance, setting up to be ready to communicate with the
cluster when required.  This does not directly connect to, or communicate
with, the LogDevice cluster until required.

NAME is the cluster name to connect to.

CONFIG is the URL to identify the configuration source;
The supported formats are:
 * file:<path-to-configuration-file>
 * configerator:<configerator-path>

TIMEOUT is the timeout (in seconds) for construction, and also the default
timeout for other methods (such as append) on this instance.

SETTINGS is an (optional) dictionary containing settings for the Client.
See Settings::addOptions() in fbcode/logdevice/common/Settings.cpp for details
of what settings are available.

CREDENTIALS is credentials to present to the LogDevice cluster.
Used for self-identification.

CSID (Client Session ID).  Used for logging to uniquely identify session.
If CSID is empty, random one is generated.
)DOC")
      .add_property(
          "settings",
          make_function(&Client::settings, return_internal_reference<>()),
          "Client settings property")
      .def("set_timeout",
           &logdevice_set_timeout,
           args("self", "timeout"),
           R"DOC(
Sets the client timeout, in seconds
)DOC")
      .def("get_directory_delimiter",
           &get_directory_delimiter,
           args("self"),
           R"DOC(
Returns the delimiter used for LogsConfig paths
)DOC")
      .def("get_log_range_by_name",
           &logdevice_get_log_range_by_name,
           args("self", "name"),
           R"DOC(
Looks up the boundaries of a log range by its name as specified.

If there's a range with the given name, returns a tuple containing the lowest
and highest log ids in the range (this may be the same id for log ranges of size
1).
Raises an exception if the range does not exist.

)DOC")

      .def("get_tail_lsn",
           &logdevice_get_tail_lsn,
           args("self", "name"),
           R"DOC(
Return the log sequence number that points to the tail of log `logid`. The
returned LSN is guaranteed to be higher or equal than the LSN of any record
that was successfully acknowledged as appended prior to this call.

Can also be used to ensure that there is a activate sequencer up and running
for a particular log.

Raises an exception if there is an error in contacting or bringing up the
sequencer.
)DOC")

      .def("get_tail_attributes",
           &logdevice_get_tail_attributes,
           args("self", "logid"),
           R"DOC(
Returns current attributes of the tail of the log by sending request to the
sequencer.
Return value is tuple with values:
last_released_real_lsn    Sequence number of last written and released for
                          delivery record of the log.
last_timestamp  Estimated timestamp of record with last_released_real_lsn
                sequence number. It may be slightly larger than real
                timestamp of a record with last_released_real_lsn lsn.
byte_offset     Amount of data in bytes written from the beginning of the log
                up to the end.
)DOC")

      .def("get_head_attributes",
           &logdevice_get_head_attributes,
           args("self", "logid"),
           R"DOC(
Return current attributes of the head of the log. See LogHeadAttributes.h docs
about possible head attributes. The timestamp of the next record after trim
point may be approximate.
Return value is tuple with values:
trim_point              Trim point of the log. Set to LSN_INVALID if log was
                        never trimmed.
trim_point_timestamp    Approximate timestamp of the next record after trim
                        point. Set to std::chrono::milliseconds::max() if there
                        is no records bigger than trim point.
)DOC")

      .def("append",
           &logdevice_append,
           args("self", "logid", "data"),
           R"DOC(
Append a record to log LOGID containing DATA.

DATA can be a bytes object, which is appended as-is,
or a str/Unicode object which are appended as utf-8 encoded data.
)DOC")

      .def("find_time",
           &logdevice_find_time,
           args("self", "logid", "seconds"),
           R"DOC(
Looks for the sequence number that the log was at at the given time.  The
most common use case is to read all records since that time, by
subsequently calling startReading(result_lsn).

More precisely, this attempts to find the first LSN at or after the given
time.  However, if we cannot get a conclusive answer (system issues
prevent us from getting answers from part of the cluster), this may
return a slightly earlier LSN (with an appropriate status as documented
below).  Note that even in that case startReading(result_lsn) will read
all records at the given timestamp or later, but it may also read some
earlier records.

If the given timestamp is earlier than all records in the log, this returns
the LSN after the point to which the log was trimmed.

If the given timestamp is later than all records in the log, this returns
the next sequence number to be issued.  Calling startReading(result_lsn)
will read newly written records.

If the log is empty, this returns LSN_OLDEST.

All of the above assumes that records in the log have increasing
timestamps.  If timestamps are not monotonic, the accuracy of this API
may be affected.  This may be the case if the sequencer's system clock is
changed, or if the sequencer moves and the clocks are not in sync.

LOGID is the ID of log to query

TIMESTAMP is the Unix timestamp to find the first record at or after, in
seconds.  Fractional (floating point) seconds may be specified.

This will raise on error, or return the LSN in question.
)DOC")

      .def("find_key",
           &logdevice_find_key,
           args("self", "logid", "key"),
           R"DOC(
Looks for the sequence number corresponding to the record with the given key
for the log.
)DOC")

      .def("trim",
           &logdevice_trim,
           args("self", "logid", "lsn"),
           R"DOC(
Trims the log, deleting all records with lsn less than or equal to the provided
lsn. In case of failure, an exception is thrown (see corresponding C++
documentation for error conditions)..
)DOC")

      .def("is_log_empty",
           &logdevice_is_log_empty,
           args("self", "logid"),
           R"DOC(
Return if a particular log is empty or not
)DOC")

      .def("data_size",
           &logdevice_data_size,
           args("self", "logid", "start_sec", "end_sec"),
           R"DOC(
Estimate size of data in the given time range for a particular log.
)DOC")

      .def("get_max_payload_size",
           &logdevice_get_max_payload_size,
           args("self"),
           R"DOC(
Return the maximum permitted payload size for this client. The default
is 1MB, but this can be increased via changing the max-payload-size
setting.
)DOC")

      .def("create_reader",
           &logdevice_create_reader,
           args("self", "max_logs"),
           R"DOC(
Return a reader object that can be used to read up to MAX_LOGS logs
concurrently.  Each possible log consumed a fixed about of memory on
the client, so you should avoid specifying too large a number.
)DOC")
      .def("make_log_group",
           &make_log_group,
           args("self",
                "path",
                "start_id",
                "end_id",
                "attrs",
                "mk_intermediate_dirs"),
           R"DOC(
Returns LogGroup object if creation of the LogGroup was successful, throws an
exception if operation has failed.
      )DOC")
      .def("make_directory",
           &make_directory,
           args("self", "path", "mk_intermediate_dirs", "attrs"),
           R"DOC(
Returns Directory object if creation was successful, throws an
exception if operation has failed.
      )DOC")
      .def("get_directory",
           &get_directory,
           args("self", "path"),
           R"DOC(
Returns a Directory if exists, otherwise throws an exception.
      )DOC")
      .def("get_log_group_by_name",
           &get_log_group_by_name,
           args("self", "path"),
           R"DOC(
Returns a LogGroup if exists, otherwise throws an exception.
      )DOC")
      .def("get_log_group_by_id",
           &get_log_group_by_id,
           args("self", "id"),
           R"DOC(
Returns a LogGroup if exists, otherwise throws an exception.
      )DOC")
      .def("remove_directory",
           &remove_directory,
           args("self", "path", "recursive"),
           R"DOC(
Removes a directory from the LogsConfig tree, if recursive is True, this will
delete all the children as well. Throws an exception if failed. Returns
LogsConfig version.
  E::ACCESS you don't have permissions to mutate the logs configuration.
  E::EXISTS Directory already exists.
  E::TIMEDOUT Operation timed out.
  E::NOTFOUND the directory was not found and thus couldn't be deleted.
      )DOC")
      .def("remove_log_group",
           &remove_log_group,
           args("self", "path"),
           R"DOC(
Removes a log group from the LogsConfig tree. Throws an exception if failed.
Returns LogsConfig version.
  E::ACCESS you don't have permissions to mutate the logs configuration.
  E::TIMEDOUT Operation timed out.
  E::NOTFOUND the log-group was not found and thus couldn't be deleted.
      )DOC")
      .def("rename",
           &rename_path,
           args("self", "old_path", "new_path"),
           R"DOC(
Rename the leaf of the supplied path. This does not move entities in the
tree it only renames the last token in the path supplies. Returns LogsConfig
version.

The new path is the full path of the destination, it must not exist,
otherwise you will receive status of E::EXISTS
  E::INVALID_PARAM if paths are invalid, a common
  example is that source or destination are the root
  path. or that the source and destination are the
  same.
  E::NOTFOUND source path doesn't exist.
  E::EXISTS the destination path already exists!
  E::TIMEDOUT Operation timed out.
  E::ACCESS you don't have permissions to mutate the logs configuration.
      )DOC")
      .def("set_attributes",
           &set_attributes,
           args("self", "path", "attrs"),
           R"DOC(
This sets either a LogGroup or LogsDirectory attributes to the supplied
attributes object. If the path refers to directory, all child directories
and log groups will be updated accordingly. Throws an exception if operation
has failed. Returns LogsConfig version.
  E::ID_CLASH          the supplied ID clashes with an existing log group
  E::INVALID_ATTRIBUTES After applying the parent
                       attributes and the supplied
                       attributes, the resulting
                       attributes are not valid.
  E::NOTFOUND the path supplied doesn't exist.
  E::TIMEDOUT Operation timed out.
  E::ACCESS you don't have permissions to mutate the logs configuration.
      )DOC")
      .def("set_log_group_range",
           &set_log_group_range,
           args("self", "path", "from", "to"),
           R"DOC(
This sets the log group range to the supplied new range. Throws an exception
if failed. Returns LogsConfig version.
  E::NOTFOUND if the path doesn't exist or it's pointing to a directory
  E::INVALID_ATTRIBUTES the range you supplied is
     invalid or reserved for system-logs.
  E::TIMEDOUT Operation timed out.
  E::ACCESS you don't have permissions to mutate the logs configuration.
      )DOC")
      .def("sync_logsconfig_version",
           &sync_logsconfig_version,
           args("self", "version"),
           R"DOC(
      This will block until the client gets the passed version of LogsConfig.
      Returns False on Timeout.
      )DOC")
      .def("smoke_test",
           &smoke_test,
           args("self", "log_ids", "count"),
           R"DOC(

      )DOC")
      .def("smoke_test_read",
           &smoke_test_read,
           args("self", "starting_points", "count"),
           R"DOC(

      )DOC")
      .def("smoke_test_write",
           &smoke_test_write,
           args("self", "log_ids", "count"),
           R"DOC(

      )DOC");

  // Implicit conversions for integer types to strong C++ types, because
  // that makes function arguments work much better coming from the Python
  // universe into C++.
  //
  // note: this only influences the casting behaviour of boost::python, not
  // any C++ code in this scope or whatever.
  implicitly_convertible<uint64_t, logid_t>();

  // expose some constants to Python
  scope().attr("BYTE_OFFSET_INVALID") =
      static_cast<uint64_t>(BYTE_OFFSET_INVALID);
  scope().attr("LOGID_INVALID") = static_cast<uint64_t>(LOGID_INVALID);
  scope().attr("LOGID_MAX") = static_cast<uint64_t>(LOGID_MAX);

  scope().attr("LSN_INVALID") = static_cast<uint64_t>(LSN_INVALID);
  scope().attr("LSN_OLDEST") = static_cast<uint64_t>(LSN_OLDEST);
  scope().attr("LSN_MAX") = static_cast<uint64_t>(LSN_MAX);

  def("timestr_to_seconds", &timestr_to_seconds, args("time"), R"DOC(
different units are accepted and converted if necessary.
For example, all these are acceptable
  100ms
  '100 ms'
  1s
  0.1s
  1min
  0.5hr

 Recognized suffixes: ns, us, ms, s, min, mins, h, hr, hrs, d, day, days.

 Parsed values are truncated if storing into a coarser unit, e.g. "1500ms"
 gets parsed as 1 second.  As a special case, if the value would truncate to
 zero (e.g. "1ms" as seconds), parsing will fail.

 min and max values for the DurationClass can be passed:
   min
   max
)DOC");
  def("timestr_to_milliseconds",
      &timestr_to_milliseconds,
      args("time"),
      R"DOC(
different units are accepted and converted if necessary.
For example, all these are acceptable
  100ms
  '100 ms'
  1s
  0.1s
  1min
  0.5hr

 Recognized suffixes: ns, us, ms, s, min, mins, h, hr, hrs, d, day, days.

 Parsed values are truncated if storing into a coarser unit, e.g. "1500ms"
 gets parsed as 1 second.  As a special case, if the value would truncate to
 zero (e.g. "1ms" as seconds), parsing will fail.

 min and max values for the DurationClass can be passed:
   min
   max
)DOC");
  def("seconds_to_timestr", &seconds_to_timestr, args("seconds"));
  def("milliseconds_to_timestr", &milliseconds_to_timestr, args("millis"));

  // register the error enum.  since they are not enumerable, we use
  // the include file to generate the relevant code.
  auto status = enum_<Status>("status");
#define ERROR_CODE(slot, _a, _b) status.value(error_name(E::slot), E::slot);
#include "logdevice/include/errors.inc"
#undef ERROR_CODE

  // register object wrappers from other components
  register_logdevice_reader();
  register_logdevice_record();

  enum_<dbg::Level>("LoggingLevel")
      .value("NONE", dbg::Level::NONE)
      .value("CRITICAL", dbg::Level::CRITICAL)
      .value("ERROR", dbg::Level::ERROR)
      .value("WARNING", dbg::Level::WARNING)
      .value("NOTIFY", dbg::Level::NOTIFY)
      .value("INFO", dbg::Level::INFO)
      .value("DEBUG", dbg::Level::DEBUG)
      .value("SPEW", dbg::Level::SPEW);
  def("getLoggingLevel", &get_logging_level);
  def("setLoggingLevel", &set_logging_level);
  def("lsn_to_string", lsn_to_string);
  def("validate_json", validate_json);
  def("normalize_json", normalize_json);
  def("parse_log_level", &dbg::parseLoglevel, args("value"));
  def("use_python_logging", &logdevice_use_python_logging, args("callback"));
  def("set_log_fd", &dbg::useFD, args("fd"));
}
