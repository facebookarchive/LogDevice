/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cmath>
#include <type_traits>
#include <variant>

#include <boost/format.hpp>
#include <folly/container/Array.h>

#include "logdevice/clients/c/ld_c_client.h"
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
std::ostream& operator<<(std::ostream& os, ldc_logid_t logid) {
  return os << toString((logid_t)logid);
}
}} // namespace facebook::logdevice

using namespace facebook::logdevice;
using namespace facebook::logdevice::logsconfig;

// object errorClass;
// static PyObject* loggingCallback = nullptr;

// void throw_logdevice_exception(boost::python::object extra) {
//   ld_check(!errorClass.is_none());

//   // I'm oddly uncomfortable with this, despite the fact that it is kind of
//   // defined as "never fails, ever" in the LogDevice code.  Old habits die
//   // hard, but not as hard as my SIGSEGV server if it fails, I guess.
//   auto info = errorStrings()[err];
//   object name(info.name != nullptr ? info.name : "Unknown");
//   object description(info.description != nullptr
//                          ? info.description
//                          : "An unknown error has occurred.");

//   object args = extra.is_none() ? make_tuple(err, name, description)
//                                 : make_tuple(err, name, description, extra);

//   throw_python_exception(errorClass, args);
// }

// auto const logLevelLetters =
//     folly::make_array('-', 'C', 'E', 'W', 'N', 'I', 'D', 'S');

// void logging_cb(const char* cluster,
//                 const char* file,
//                 const char* fn,
//                 const int line,
//                 dbg::Level lvl,
//                 struct timeval created,
//                 pid_t tid,
//                 const char* thread_name,
//                 const char* msg) {
//   if (loggingCallback == nullptr) {
//     return;
//   }

//   if (lvl == dbg::Level::SPEW || lvl == dbg::Level::NONE) {
//     // ignore spew level
//     return;
//   }

//   struct tm now_tm;
//   ::localtime_r(&created.tv_sec, &now_tm);
//   char record[2048];
//   int hdrlen = 0;
//   hdrlen = snprintf(record,
//                     sizeof(record),
//                     "%c%02d%02d %02d:%02d:%02d.%06lu %7d [%s] %s:%d] %s() %s",
//                     logLevelLetters[(unsigned)lvl],
//                     now_tm.tm_mon + 1,
//                     now_tm.tm_mday,
//                     now_tm.tm_hour,
//                     now_tm.tm_min,
//                     now_tm.tm_sec,
//                     (unsigned long)created.tv_usec,
//                     tid,
//                     thread_name,
//                     file,
//                     line,
//                     fn,
//                     msg);

//   const char* level = nullptr;
//   switch (lvl) {
//     case dbg::Level::CRITICAL:
//       level = "critical";
//       break;
//     case dbg::Level::ERROR:
//       level = "error";
//       break;
//     case dbg::Level::WARNING:
//       level = "warning";
//       break;
//     case dbg::Level::NOTIFY:
//     case dbg::Level::INFO:
//       level = "info";
//       break;
//     case dbg::Level::DEBUG:
//       level = "debug";
//       break;
//     case dbg::Level::SPEW:
//     case dbg::Level::NONE:
//       break;
//   }

//   if (PyInterpreterState_Head() == NULL) {
//     return;
//   }
//   PyGILState_STATE gstate;
//   gstate = PyGILState_Ensure();

//   call<void>(loggingCallback, cluster, level, record);

//   /* Release the thread. No Python API allowed beyond this point. */
//   PyGILState_Release(gstate);
// }

// void logdevice_use_python_logging(object cb) {
//   if (loggingCallback != nullptr) {
//     throw_python_exception(
//         PyExc_RuntimeError,
//         ("Logging callback is already set. use_python_logging() can only "
//          "be called once per application"));
//   }
//   incref(cb.ptr());
//   loggingCallback = cb.ptr();
//   dbg::useCallback(logging_cb);
//   ld_info("Using python logging");
// }

using ClientSPtr = std::shared_ptr<facebook::logdevice::Client>;
using ClientSettingsSPtr = std::shared_ptr<facebook::logdevice::ClientSettings>;

#define DEREF_CLNT(PSP) (*(ClientSPtr*)PSP)
#define DEREF_CLNT_SETTINGS(PSP) (*(ClientSettingsSPtr*)PSP)

ld_err ldc_make_client(const char* name,
                        const char* config,
                        double timeout_seconds,
                        PStr2IntOrStrMap settings,
                        const char* credentials,
                        const char* csid,
                        PClientSPtr* pclient) {
  std::string ldclient_name(name);
  std::string ldclient_config(config);


  folly::Optional<std::chrono::milliseconds> timeout;
  timeout.assign(std::chrono::milliseconds(lround(timeout_seconds * 1000)));

  std::unique_ptr<ClientSettings> client_settings(ClientSettings::create());
  auto psettings =
    reinterpret_cast<
      std::map<std::string,
               std::variant<int64_t, std::string>>*> (settings);

  for(auto it = psettings->begin(); it != psettings->end(); it++) {
    auto key = it->first;
    auto val = it->second;

    int result = LD_ERR_UNKNOWN;

    if (val.index() == 0) {
      result = client_settings->set(key.c_str(), std::get<int64_t>(val));
    } else if (val.index() == 1) {
      result = client_settings->set(key.c_str(), std::get<std::string>(val));
    } else {
      return LD_ERR_UNKNOWN;
    }

    if (result != 0) {
      return (ld_err)err;
    }
  };

  std::string client_name(name);
  std::string config_path(config);
  std::string credentials_str(credentials);
  std::string csid_str(csid);
  ClientSPtr client;
  {
    // gil_release_and_guard guard;
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
    *pclient = (PClientSPtr)(new ClientSPtr(client));
    return LD_ERR_OK;
  }
  return LD_ERR_UNKNOWN;
  // throw_logdevice_exception();
}

ld_err ldc_free_client(PClientSPtr client) {
  if (client != NULL) {
    delete (ClientSPtr*)client;
  }

  return LD_ERR_OK;
}


ld_err ldc_set_timeout(PClientSPtr pclient_sp, double timeout_seconds) {
  auto timeout = std::chrono::milliseconds(lround(timeout_seconds * 1000));
  DEREF_CLNT(pclient_sp)->setTimeout(timeout);
  return LD_ERR_OK;
}

std::string ldc_get_directory_delimiter(PClientSPtr pclient_sp) {
  return DEREF_CLNT(pclient_sp)->getLogNamespaceDelimiter();
}

ld_err ldc_get_log_range_by_name(PClientSPtr pclient_sp,
                                 const char* name,
                                 ldc_logid_t* from, ldc_logid_t* to) {
  logid_range_t range = DEREF_CLNT(pclient_sp)->getLogRangeByName(std::string(name));
  if (range.first != LOGID_INVALID) {
    *from = range.first.val_;
    *to = range.second.val_;
    return LD_ERR_OK;
  }
  return LD_ERR_UNKNOWN;
}

ld_err ldc_get_tail_lsn(PClientSPtr pclient_sp, ldc_logid_t logid, ldc_lsn_t* plsn) {
  ldc_lsn_t lsn;
  {
    // gil_release_and_guard guard;
    lsn = DEREF_CLNT(pclient_sp)->getTailLSNSync((logid_t)logid);
  }
  if (lsn != LSN_INVALID) {
    *plsn = lsn;
    return LD_ERR_OK;
  }
  return LD_ERR_UNKNOWN;
}

ld_err ldc_get_tail_attributes(PClientSPtr pclient_sp, ldc_logid_t logid,
                               ldc_lsn_t* p_last_released_real_lsn,
                               uint64_t* p_last_timestamp_count,
                               uint64_t* p_offsets_counter) {

  std::unique_ptr<LogTailAttributes> attributes;
  {
    // gil_release_and_guard guard;
    attributes = DEREF_CLNT(pclient_sp)->getTailAttributesSync((logid_t)logid);
  }
  if (!attributes) {
    return LD_ERR_UNKNOWN;
    // throw_logdevice_exception();
    // throw std::runtime_error("impossible, the line above always throws!");
  } else {
    *p_last_released_real_lsn = attributes->last_released_real_lsn;
    *p_last_timestamp_count = attributes->last_timestamp.count();
    *p_offsets_counter = attributes->offsets.getCounter(BYTE_OFFSET);
    return LD_ERR_OK;
  }
}

ld_err ldc_get_head_attributes(PClientSPtr pclient_sp,
                               ldc_logid_t logid,
                               ldc_lsn_t* trim_point,
                               uint64_t* trim_point_timestamp_count) {
  std::unique_ptr<LogHeadAttributes> attributes;
  {
    // gil_release_and_guard guard;
    attributes = DEREF_CLNT(pclient_sp)->getHeadAttributesSync((logid_t)logid);
  }
  if (!attributes) {
    // throw_logdevice_exception();
    // throw std::runtime_error("impossible, the line above always throws!");
    return LD_ERR_UNKNOWN;
  } else {
    *trim_point = attributes->trim_point;
    *trim_point_timestamp_count = attributes->trim_point_timestamp.count();
    return LD_ERR_OK;
  }
}

ld_err ldc_append(PClientSPtr pclient_sp,
                     ldc_logid_t logid,
                     const char* data,
                     uint64_t len,
                     ldc_lsn_t* plsn) {
  ldc_lsn_t lsn;
  std::string pl(data, len);
  {
    // gil_release_and_guard guard;
    lsn = DEREF_CLNT(pclient_sp)->appendSync((logid_t)logid, std::move(pl));
  }
  if (lsn != LSN_INVALID) {
    *plsn = (ldc_lsn_t)lsn;
    return LD_ERR_OK;
  }

  return LD_ERR_UNKNOWN;

  // throw_logdevice_exception();
  // throw std::runtime_error("impossible, the line above always throws!");
}

ld_err logdevice_find_time(PClientSPtr pclient_sp,
                           ldc_logid_t logid,
                           double seconds,
                           ldc_lsn_t* plsn) {

  auto ts = std::chrono::milliseconds(lround(seconds * 1000));
  ldc_lsn_t lsn;
  Status status;
  {
    // gil_release_and_guard guard;
    lsn = DEREF_CLNT(pclient_sp)->findTimeSync((logid_t)logid, ts, &status);
  }

  // We do not want to return partial results
  if (status == E::OK) {
    *plsn = (ldc_lsn_t)lsn;
    return LD_ERR_OK;
  }
  return LD_ERR_UNKNOWN;
  // throw_logdevice_exception();
  // throw std::runtime_error("impossible, the line above always throws!");
}

ld_err ldc_find_key(PClientSPtr pclient_sp,
                    ldc_logid_t logid,
                    const char* key,
                    uint64_t key_len,
                    ldc_lsn_t* lo,
                    ldc_lsn_t* hi) {
  FindKeyResult result;
  {
    // gil_release_and_guard guard;
    result = DEREF_CLNT(pclient_sp)->findKeySync((logid_t)logid, std::move(key));
  }
  if (result.lo != LSN_INVALID || result.hi != LSN_INVALID) {
    *lo = (ldc_lsn_t)result.lo;
    *hi = (ldc_lsn_t)result.hi;
    return LD_ERR_OK;
  }
  return LD_ERR_UNKNOWN;
}

ld_err ldc_trim(PClientSPtr pclient_sp, ldc_logid_t logid, ldc_lsn_t lsn) {
  int res;
  {
    // gil_release_and_guard guard;
    res = DEREF_CLNT(pclient_sp)->trimSync((logid_t)logid, lsn);
  }
  if (res != 0) {
    return LD_ERR_UNKNOWN;
  } else {
    return LD_ERR_OK;
  }
}

ld_err ldc_is_log_empty(PClientSPtr pclient_sp, ldc_logid_t logid, bool* pempty) {
  int rv;
  {
    // gil_release_and_guard guard;
    rv = DEREF_CLNT(pclient_sp)->isLogEmptySync((logid_t)logid, pempty);
  }
  if (rv != 0) {
    return LD_ERR_UNKNOWN;
  } {
    return LD_ERR_OK;
  }
}

ld_err ldc_data_size(PClientSPtr pclient_sp,
                     ldc_logid_t logid,
                     double start_sec,
                     double end_sec,
                     size_t* result_size) {
  int rv;
  std::chrono::milliseconds start_ts = start_sec == -1
      ? std::chrono::milliseconds::min()
      : std::chrono::milliseconds(lround(start_sec * 1000));
  std::chrono::milliseconds end_ts = end_sec == -1
      ? std::chrono::milliseconds::max()
      : std::chrono::milliseconds(lround(end_sec * 1000));

  {
    // gil_release_and_guard guard;
    rv = DEREF_CLNT(pclient_sp)->dataSizeSync(
      (logid_t)logid, start_ts, end_ts,
      DataSizeAccuracy::APPROXIMATE, result_size);
  }
  return rv != 0 ? LD_ERR_UNKNOWN : LD_ERR_OK;
}

ld_err ldc_get_max_payload_size(PClientSPtr pclient_sp,
                              size_t* max_size) {
  *max_size = DEREF_CLNT(pclient_sp)->getMaxPayloadSize();
  return LD_ERR_OK;
}

ld_err timestr_to_seconds(const char* timestring, uint64_t len, int64_t* seconds) {
  std::chrono::seconds out;
  int rv = parse_chrono_string(std::string(timestring, len), &out);
  if (rv != 0) {
    return LD_ERR_UNKNOWN;
    // throw_python_exception(
    //     PyExc_ValueError, "Could not convert the time string!");
  }
  *seconds = out.count();
  return LD_ERR_OK;
}

// const char* SMOKE_TEST_PAYLOAD_FMT = "Payload message # %1%";

ld_err ldc_get_setting(PClientSettingsSPtr pclient_setting_sp,
                       const char* key,
                       char* setting,
                       uint64_t max_len) {
  std::string s_key(key);
  auto val = DEREF_CLNT_SETTINGS(pclient_setting_sp)->get(s_key);

  if (!val) {
    return LD_ERR_UNKNOWN;
    // throw_python_exception(PyExc_KeyError, "Invalid key");
  } else {
    size_t setting_len = val->size() + 1;
    if (setting_len > max_len) {
      return LD_ERR_NOBUFS;
    } else {
      strncpy(setting, val->c_str(), setting_len);
      return LD_ERR_OK;
    }
  }

  return LD_ERR_UNKNOWN;
}

ld_err ldc_set_setting(PClientSettingsSPtr pclient_setting_sp,
                      const char* key,
                      const char* value) {
  int result = -1;
  std::string s_key(key);

  result =
    DEREF_CLNT_SETTINGS(pclient_setting_sp)->set(key, value);
  if (result != 0) {
    return LD_ERR_UNKNOWN;
  } else {
    return LD_ERR_OK;
  }
}

ld_err ldc_make_reader(PClientSPtr pclient_sp,
                        size_t max_logs,
                        PReaderSPtr* ppreader_sp) {

  std::unique_ptr<Reader> reader = DEREF_CLNT(pclient_sp)->createReader(max_logs);
  *ppreader_sp = (PReaderSPtr)new std::shared_ptr<Reader>(std::move(reader));
  return LD_ERR_OK;
}

ld_err ldc_free_reader(PReaderSPtr preader_sp) {
  delete (std::shared_ptr<Reader>*)preader_sp;
  return LD_ERR_OK;
}
