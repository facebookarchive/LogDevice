/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/util.h"

#include <cctype>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <errno.h>
#include <limits.h>
#include <pwd.h>
#include <signal.h>

#include <folly/Format.h>
#include <folly/Singleton.h>
#include <sys/prctl.h>
#include <sys/syscall.h>

#include "logdevice/common/MetaDataLog.h"
#include "logdevice/common/commandline_util_chrono.h"
#include "logdevice/common/debug.h"

#pragma GCC diagnostic ignored "-Wchar-subscripts"

namespace facebook { namespace logdevice {

// see .h
const char* commaprint_r(unsigned long n, char* buf, int size) {
  static const char comma = ',';
  char* p = buf + size - 1;
  int i = 0;

  *p = '\0';

  do {
    if (p <= buf) {
      errno = ENOBUFS;
      return nullptr;
    }
    if (i % 3 == 0 && i != 0) {
      *--p = comma;
    }
    *--p = '0' + n % 10;
    n /= 10;
    i++;
  } while (n != 0);

  return p;
}

// see .h
int parse_scaled_int(const char* s, uint64_t* l) {
  static uint64_t multiplier[256];

  char* eptr;

  if (!multiplier[static_cast<uint8_t>('K')]) {
    multiplier[static_cast<uint8_t>('K')] = 1000;
    multiplier[static_cast<uint8_t>('M')] = 1000000;
    multiplier[static_cast<uint8_t>('G')] = 1000000000;
    multiplier[static_cast<uint8_t>('T')] = 1000000000000;
  }

  errno = 0;

  *l = strtoull(s, &eptr, 10);

  if (*l == ULONG_LONG_MAX && errno == ERANGE) {
    return -1;
  }

  if (*eptr) {
    if (multiplier[(int)*eptr] && *(eptr + 1) == '\0') {
      uint64_t m = multiplier[(int)*eptr];
      if (*l > std::numeric_limits<uint64_t>::max() / m) {
        // Multiplication would overflow.
        return -1;
      }
      *l *= m;
    } else {
      return -1;
    }
  }

  return 0;
}

// see .h
int parse_interval(const char* s, interval_t* out) {
  char lostr[64], histr[64], sep[64];
  int nmatched;

  int consumed;
  nmatched = sscanf(s, "%[^.]%[^0-9]%s%n", lostr, sep, histr, &consumed);

  if (nmatched <= 0) {
    return -1;
  }

  if (parse_scaled_int(lostr, &out->lo) != 0) {
    return -1;
  }

  if (nmatched == 1) {
    out->hi = out->lo;
  } else if (nmatched < 3 || s[consumed] != '\0' || strcmp(sep, "..") != 0 ||
             parse_scaled_int(histr, &out->hi) != 0 || out->lo > out->hi) {
    return -1;
  }

  return 0;
}

// see .h
int parse_logid_intervals(const char* s, log_ranges_t* out_log_ranges) {
  std::vector<std::string> tokens;
  folly::split(',', s, tokens);
  ld_check(out_log_ranges);
  out_log_ranges->clear();
  for (const std::string& token : tokens) {
    interval_t interval;
    int rv = parse_interval(token.c_str(), &interval);
    if (rv != 0) {
      return rv;
    }
    out_log_ranges->insert(
        log_ranges_t::interval_type(interval.lo, interval.hi + 1));
  }
  if (out_log_ranges->empty()) {
    return -1;
  }
  return 0;
}

int parse_logid_intervals(const char* s, std::vector<logid_t>* out_logs_ids) {
  log_ranges_t ranges;
  int rv = parse_logid_intervals(s, &ranges);
  if (rv != 0) {
    return rv;
  }
  ld_check(out_logs_ids);
  out_logs_ids->clear();
  for (const auto& interval : ranges) {
    for (logid_t::raw_type id = interval.lower(); id < interval.upper(); ++id) {
      out_logs_ids->push_back(logid_t(id));
    }
  }
  return 0;
}

int parse_rate_limit(const char* s, rate_limit_t* r) {
  if (strcmp(s, "unlimited") == 0) {
    *r = RATE_UNLIMITED;
    return 0;
  }

  std::vector<std::string> pieces;
  uint64_t count;
  std::chrono::milliseconds duration;

  folly::split("/", s, pieces);
  if (pieces.size() != 2 || parse_scaled_int(pieces[0].c_str(), &count) != 0 ||
      parse_chrono_string(pieces[1], &duration) != 0 || duration.count() < 0 ||
      (duration.count() == 0 && count == 0)) {
    err = E::INVALID_PARAM;
    return -1;
  }

  *r = rate_limit_t(count, duration);
  return 0;
}

int parse_ioprio(const std::string& val,
                 folly::Optional<std::pair<int, int>>* out_prio) {
  ld_check(out_prio);
  if (val == "" || val == "any") {
    out_prio->clear();
    return 0;
  }

  int a, b;
  bool success = false;
  try {
    success = folly::split(',', val, a, b);
  } catch (std::range_error&) {
    return -1;
  }
  if (!success) {
    return -1;
  }
  *out_prio = std::make_pair(a, b);
  return 0;
}

int parse_compaction_schedule(
    const std::string& val,
    folly::Optional<std::vector<std::chrono::seconds>>& out) {
  if (val == "disabled" || val == "none") {
    return 0;
  }

  if (val == "automatic" || val == "auto" || val == "") {
    // Empty vector means we will use all the backlog durations.
    out.assign(std::vector<std::chrono::seconds>());
    return 0;
  }

  std::vector<std::chrono::seconds> res;

  std::vector<std::string> tokens;
  folly::split(',', val, tokens, true /* ignoreEmpty */);
  for (const auto& token : tokens) {
    std::chrono::seconds duration;
    int rv = parse_chrono_string(token, &duration);
    if (rv != 0 || duration.count() <= 0) {
      err = E::INVALID_PARAM;
      return -1;
    }
    res.push_back(duration);
  }

  std::sort(res.begin(), res.end());
  res.erase(std::unique(res.begin(), res.end()), res.end());

  out.assign(res);
  return 0;
}

namespace {
bool validatePort(folly::StringPiece port) {
  return port.size() > 0 && std::all_of(port.begin(), port.end(), ::isdigit);
}

bool validateIpV4(folly::StringPiece ip) {
  return ip.size() > 0 && std::all_of(ip.begin(), ip.end(), [](const char c) {
           return ::isdigit(c) || c == '.';
         });
}

bool validateIpV6(folly::StringPiece ip) {
  return ip.size() > 0 && std::all_of(ip.begin(), ip.end(), [](const char c) {
           return ::isxdigit(c) || c == '.' || c == ':';
         });
}
} // namespace

std::pair<std::string, std::string> parse_ip_port(const std::string& hostStr) {
  auto delim_pos = hostStr.rfind(":");
  if (delim_pos == std::string::npos) {
    return {};
  }

  // try to parse port
  folly::StringPiece port =
      folly::StringPiece{hostStr, /* startFrom */ delim_pos + 1};
  if (!validatePort(port)) {
    return {};
  }

  folly::StringPiece ip;
  if (hostStr.at(0) == '[' && hostStr.at(delim_pos - 1) == ']') {
    // try parsing as IPv6, and strip the brackets
    ld_check(delim_pos >= 2);
    ip = folly::StringPiece{
        hostStr, /* startFrom */ 1, /* size */ delim_pos - 2};
    if (!validateIpV6(ip)) {
      return {};
    }
  } else {
    ip = folly::StringPiece{hostStr, /* startFrom */ 0, /* size */ delim_pos};
    if (!validateIpV4(ip)) {
      return {};
    }
  }

  return std::make_pair(ip.str(), port.str());
}

std::string lsn_to_string(lsn_t lsn) {
  if (lsn == LSN_INVALID) {
    return "LSN_INVALID";
  }
  if (lsn == LSN_MAX) {
    return "LSN_MAX";
  }

  char buf[128];

  snprintf(
      buf, sizeof(buf), "e%un%u", lsn_to_epoch(lsn).val_, lsn_to_esn(lsn).val_);

  return std::string(buf);
}

int string_to_lsn(std::string in, lsn_t& out) {
  if (in.empty()) {
    return -1;
  }

  in = lowerCase(in);

  uint64_t lsn;
  uint32_t epoch, esn;
  if (sscanf(in.c_str(), "e%un%u", &epoch, &esn) == 2) {
    out = compose_lsn(epoch_t(epoch), esn_t(esn));
    return 0;
  } else if (sscanf(in.c_str(), "%lu", &lsn) == 1) {
    out = lsn;
    return 0;
  } else if (in == "lsn_invalid") {
    out = LSN_INVALID;
    return 0;
  } else if (in == "lsn_oldest") {
    out = LSN_OLDEST;
    return 0;
  } else if (in == "lsn_max") {
    out = LSN_MAX;
    return 0;
  }

  return -1;
}

std::string toString(const logid_t logid) {
  switch (logid.val()) {
    case LOGID_INVALID.val():
      return "LOGID_INVALID";

    case LOGID_INVALID2.val():
      return "LOGID_INVALID2";

    default:
      return folly::to<std::string>(
          MetaDataLog::isMetaDataLog(logid) ? 'M' : 'L',
          MetaDataLog::dataLogID(logid).val());
  }
}

std::string toString(const Severity& sev) {
  switch (sev) {
    case Severity::CRITICAL:
      return "CRITICAL";
    case Severity::ERROR:
      return "ERROR";
    case Severity::WARNING:
      return "WARNING";
    case Severity::NOTICE:
      return "NOTICE";
    case Severity::INFO:
      return "INFO";
    case Severity::DEBUG:
      return "DEBUG";
  };
  return "UNKNOWN";
}

std::string toString(const StorageSet& storage_set) {
  if (storage_set.empty()) {
    return "";
  }
  return toString(&storage_set[0], storage_set.size());
}

std::string toString(const ShardID* copyset, size_t size) {
  std::string res = "{";
  for (size_t i = 0; i < size; ++i) {
    if (i) {
      res += ",";
    }
    res += copyset[i].toString();
  }
  res += "}";
  return res;
}

std::string toString(const KeyType& type) {
  switch (type) {
    case KeyType::FINDKEY:
      return "FINDKEY";
    case KeyType::FILTERABLE:
      return "FILTERABLE";
    case KeyType::MAX:
      return "NUM_KEYS";
    case KeyType::UNDEFINED:
      return "UNDEFINED";
  }
  return "UNEXPECTED_KEYTYPE";
}

std::string compressionToString(Compression c) {
  switch (c) {
    case Compression::NONE:
      return "none";
    case Compression::ZSTD:
      return "zstd";
    case Compression::LZ4:
      return "lz4";
    case Compression::LZ4_HC:
      return "lz4_hc";
  }
  ld_check(false);
  return "(internal error)";
}

int parseCompression(const char* str, Compression* out_c) {
  Compression c;
  if (!strcmp(str, "none")) {
    c = Compression::NONE;
  } else if (!strcmp(str, "zstd")) {
    c = Compression::ZSTD;
  } else if (!strcmp(str, "lz4")) {
    c = Compression::LZ4;
  } else if (!strcmp(str, "lz4_hc")) {
    c = Compression::LZ4_HC;
  } else {
    return -1;
  }
  if (out_c) {
    *out_c = c;
  }
  return 0;
}

std::string safe_print(const char* data, size_t buf_size) {
  std::string ret;
  for (size_t i = 0; i < buf_size; ++i) {
    const char c = data[i];
    if (c == '\0') {
      break;
    }
    ret += std::isprint(static_cast<unsigned char>(c)) ? c : '.';
  }
  return ret;
};

std::string sanitize_string(std::string s, int max_len) {
  if (!std::all_of(
          s.begin(), s.end(), [](char c) { return std::isprint(c); })) {
    std::string t;
    for (char c : s) {
      if (std::isprint(c)) {
        t += c;
        continue;
      }
      t += "\\x";
      t += hexdump_buf(&c, 1, 2);
    }
    s = std::move(t);
  }
  if (max_len >= 0 && (int)s.size() > max_len) {
    std::string suffix = "...[" + std::to_string(s.size()) + " bytes]";
    s = s.substr(0, max_len - std::min(max_len, (int)suffix.size())) + suffix;
  }
  return s;
}

std::string markdown_sanitize(const std::string& str) {
  std::ostringstream out;

  for (char c : str) {
    switch (c) {
      case '|':  // delimits table cells
      case '_':  // italics
      case '\\': // escapes
        out << '\\' << c;
        break;
      case '\n':    // implicitly terminates the bottom right cell
        out << ' '; // (the righmost pipe | terminator is optional). Substitute.
        break;
      default:
        out << c;
    }
  }

  return out.str();
}

char* error_text_sanitize(char* str) {
  for (char* p = str; *p; p++) {
    if (!isprint(*p)) {
      *p = ' ';
    }
  }

  return str;
}

std::string hexdump_buf(const void* buf, size_t size, size_t max_output_size) {
  if (size == 0) {
    return "(empty)";
  }
  const size_t suffix_size =
      strlen("...[ bytes]") + std::to_string(size).size();
  max_output_size = std::max(max_output_size, suffix_size + 2);
  ld_check(buf);
  std::string out;
  const size_t bytes_to_print =
      size * 2 < max_output_size ? size : (max_output_size - suffix_size) / 2;
  ld_check(bytes_to_print <= size);
  for (size_t i = 0; i < bytes_to_print; ++i) {
    out += folly::sformat(
        "{:02X}", reinterpret_cast<const unsigned char*>(buf)[i]);
  }
  if (bytes_to_print < size) {
    out += "...[" + std::to_string(size) + " bytes]";
  }
  ld_check(out.size() <= max_output_size);
  ld_check(out.size() == size * 2 || out.size() >= max_output_size - 1);
  return out;
}

int set_io_priority_of_this_thread(std::pair<int, int> prio) {
  int rv =
      syscall(SYS_ioprio_set,
              1,                               // IOPRIO_WHO_PROCESS
              0,                               // current thread
              (prio.first << 13) | prio.second // IOPRIO_PRIO_VALUE(class, data)
      );
  if (rv != 0) {
    ld_error("ioprio_set() failed: %s", strerror(errno));
  }
  return rv;
}

int get_io_priority_of_this_thread(std::pair<int, int>* out_prio) {
  int rv = syscall(SYS_ioprio_get,
                   1, // IOPRIO_WHO_PROCESS
                   0  // current thread
  );
  if (rv < 0) {
    ld_error("ioprio_get() failed: %s", strerror(errno));
    return rv;
  } else if (out_prio) {
    *out_prio = std::make_pair(rv >> 13, rv & ((1 << 13) - 1));
  }
  return 0;
}

std::string lowerCase(std::string s) {
  std::transform(s.begin(), s.end(), s.begin(), ::tolower);
  return s;
}

std::string get_username(bool effective) {
  uid_t uid = effective ? geteuid() : getuid();
  auto pw = getpwuid(uid);
  if (!pw || !pw->pw_name) {
    return std::string();
  }
  return std::string(pw->pw_name);
}

void logdeviceInit() {
  folly::SingletonVault::singleton()->registrationComplete();

  // Ignore SIGPIPE.
  struct sigaction sa;
  sa.sa_handler = SIG_IGN;
  sa.sa_flags = 0;
  sigemptyset(&sa.sa_mask);
  int rv = sigaction(SIGPIPE, &sa, nullptr);
  ld_check(rv == 0);

  // Die when parent process dies.
  prctl(PR_SET_PDEATHSIG, SIGKILL, 0, 0, 0);
}

}} // namespace facebook::logdevice
