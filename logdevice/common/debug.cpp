/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/debug.h"

#include <chrono>
#include <cstdarg>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <mutex>
#include <thread>
#include <time.h>
#include <unistd.h>

#include <boost/program_options.hpp>
#include <folly/Format.h>
#include <folly/ScopeGuard.h>
#include <folly/Singleton.h>
#include <folly/container/Array.h>
#include <folly/synchronization/LifoSem.h>
#include <sys/time.h>
#include <sys/types.h>

#include "logdevice/common/MPSCQueue.h"
#include "logdevice/common/ThreadID.h"
#include "logdevice/common/plugin/Logger.h"

namespace facebook { namespace logdevice { namespace dbg {

auto const logLevelNames = folly::make_array("none",
                                             "critical",
                                             "error",
                                             "warning",
                                             "notify",
                                             "info",
                                             "debug",
                                             "spew");

auto const logLevelLetters =
    folly::make_array('-', 'C', 'E', 'W', 'N', 'I', 'D', 'S');

// see include/debug.h
std::atomic<Level> currentLevel(Level::INFO);

std::atomic<Level> externalLoggerLogLevel(Level::NONE);

std::atomic<size_t> maxBufferedLogMsg(10000);

// If this is true, a call to dd_assert() may trigger a call to assert(false)
// if the condition fails.
std::atomic<bool> assertOnData(false);

// If this is true, then when an ld_check*() fails, we'll abort().
std::atomic<bool> abortOnFailedCheck{true};
// If this is true, then when an ld_catch*() fails, we'll abort().
std::atomic<bool> abortOnFailedCatch{folly::kIsDebug};

// If it's available than we have linked logger plugin.
std::shared_ptr<Logger> external_logger_plugin{nullptr};

// see common/debug.h
bump_error_counter_fn_t bumpErrorCounterFn = nullptr;

// fd to log to. By default log to stderr. Setting this to a negative
// value disables logging.
static int logFD = STDERR_FILENO; // from unistd.h

// How many messages in a row we failed to write() to logFD.
// If it's nonzero, every subsequent write tries to report and clear it.
static std::atomic<size_t> writesFailedWouldblock{0};
static std::atomic<size_t> writesFailedOther{0};

// Callback to be called for each log entry.
logging_fn_t customLogFn = nullptr;

static const char* component_to_file(const char* component);

namespace {
struct LogMsg {
  LogMsg(std::string _cluster,
         const char* _file,
         const char* _function,
         int _line,
         Level _level,
         struct timeval _created,
         pid_t _tid,
         std::string _name,
         std::string _record)
      : cluster(std::move(_cluster)),
        file(_file),
        function(_function),
        line(_line),
        level(_level),
        created(std::move(_created)),
        tid(_tid),
        name(std::move(_name)),
        record(std::move(_record)) {}

  std::string cluster;
  const char* file;
  const char* function;
  int line;
  Level level;
  struct timeval created;
  pid_t tid;
  std::string name;
  std::string record;

  folly::AtomicIntrusiveLinkedListHook<LogMsg> msg_hook_;
};

class BackgroundLogger {
 public:
  explicit BackgroundLogger(size_t batchSize) : batchSize_(batchSize) {
    thread_ = std::thread(&BackgroundLogger::mainLoop, this);
    running_.store(true);
  }

  ~BackgroundLogger() {
    shutdown();
  }

  void push(std::unique_ptr<LogMsg> rec) {
    if (running_.load()) {
      if (sem_.valueGuess() < maxBufferedLogMsg.load()) {
        size_t dropped = dropped_.exchange(0, std::memory_order_relaxed);
        if (dropped > 0) {
          forcePush(makeDroppedMsg(dropped));
        }
        forcePush(std::move(rec));
      } else {
        ++dropped_;
      }
    }
  }

  void shutdown() {
    sem_.shutdown();
    thread_.join();
  }

  static std::shared_ptr<BackgroundLogger> getInstance();

 protected:
  void forcePush(std::unique_ptr<LogMsg> rec) {
    queue_.push(std::move(rec));
    sem_.post();
  }

  std::unique_ptr<LogMsg> makeDroppedMsg(size_t dropped) {
    struct timeval now;
    gettimeofday(&now, nullptr);
    return std::make_unique<LogMsg>(
        thisThreadClusterName(),
        component_to_file(__FILE__),
        __FUNCTION__,
        __LINE__,
        Level::INFO,
        now,
        ThreadID::getId(),
        ThreadID::getName(),
        folly::format("Dropped {} log messages because the queue of "
                      "the background logger was greater than {}",
                      dropped,
                      maxBufferedLogMsg.load())
            .str());
  }

  void mainLoop() {
    while (true) {
      try {
        sem_.wait();
      } catch (const folly::ShutdownSemError&) {
        break;
      }

      size_t n = sem_.tryWait(batchSize_) + 1;
      for (size_t i = 0; i < n; i++) {
        auto rec = queue_.pop();
        customLogFn(rec->cluster.c_str(),
                    rec->file,
                    rec->function,
                    rec->line,
                    rec->level,
                    rec->created,
                    rec->tid,
                    rec->name.c_str(),
                    rec->record.c_str());
      }
    }
    running_.store(false);
  }

 private:
  std::thread thread_;
  MPSCQueue<LogMsg, &LogMsg::msg_hook_> queue_;
  folly::LifoSem sem_;
  std::atomic_bool running_{false};
  std::atomic<size_t> dropped_{0};
  size_t batchSize_;
};

struct BackgroundLoggerTag {};

} // namespace

static folly::Singleton<BackgroundLogger, BackgroundLoggerTag> the_logger([]() {
  return new BackgroundLogger(100);
});
std::shared_ptr<BackgroundLogger> BackgroundLogger::getInstance() {
  return the_logger.try_get();
}

std::string& thisThreadClusterName() {
  static thread_local std::string name;
  return name;
}

/**
 * @return a pointer to the file name in @param component
 */
static const char* component_to_file(const char* component) {
  const char* file;

  if ((file = strrchr(component, '/'))) {
    file++;
  } else if ((file = strrchr(component, '\\'))) {
    file++;
  } else {
    file = component;
  }

  return file;
}

static void setLogLevelOverrideImpl(Module& mod, Level level) {
  Level was = mod.setLogLevel(level);
  if (was == level) {
    return;
  }
  if (was == Level::NONE) {
    ld_info("Overriding log level for module '%s' to %s (current: %s)",
            mod.getName().c_str(),
            logLevelNames[(size_t)level],
            logLevelNames[(size_t)currentLevel.load()]);
  } else if (level == Level::NONE) {
    ld_info("Removing log level override %s for module '%s' (current: %s)",
            logLevelNames[(size_t)was],
            mod.getName().c_str(),
            logLevelNames[(size_t)currentLevel.load()]);
  } else {
    ld_info("Changing log level override for module '%s' from %s to %s "
            "(current: %s)",
            mod.getName().c_str(),
            logLevelNames[(size_t)was],
            logLevelNames[(size_t)level],
            logLevelNames[(size_t)currentLevel.load()]);
  }
}

void addLogLevelOverrides(const LogLevelMap& map) {
  auto& reg = ModuleRegistry::instance();
  for (const auto& it : map) {
    setLogLevelOverrideImpl(*reg.createOrGet(it.first), it.second);
  }
}

void setLogLevelOverrides(LogLevelMap map) {
  auto& reg = ModuleRegistry::instance();
  std::vector<std::pair<Module*, Level>> changes;
  reg.applyToAll([&](Module& mod) {
    auto it = map.find(mod.getName());

    // Can't call setLogLevelOverrideImpl() from here because we're holding
    // ModuleRegistry's mutex, and setLogLevelOverrideImpl()'s ld_info() calls
    // will try to grab that mutex too. Instead, enqueue the needed change and
    // execute it after releasing the mutex. It is safe to do so because
    // modules are never destroyed and removed from the registry.
    if (it == map.end()) {
      changes.emplace_back(&mod, Level::NONE);
    } else {
      changes.emplace_back(&mod, it->second);
      map.erase(it);
    }
  });

  for (const auto& p : changes) {
    setLogLevelOverrideImpl(*p.first, p.second);
  }

  for (const auto& it : map) {
    setLogLevelOverrideImpl(*reg.createOrGet(it.first), it.second);
  }
}

void clearLogLevelOverrides() {
  setLogLevelOverrides({});
}

Module* getModuleFromFile(const char* file) {
  const std::string modname = ModuleRegistry::moduleNameFromFilename(file);
  return ModuleRegistry::instance().createOrGet(modname);
}

void enableNonblockingPipe() {
  int fd = logFD;
  if (fd < 0) {
    return;
  }

  // Add O_NONBLOCK flag. Note that it only works if fd is a pipe. If fd
  // points to a file, this flag has no effect, and write() can still block
  // waiting for writeback.

  int flags = fcntl(fd, F_GETFL);
  if (flags < 0) {
    ld_error("Failed to get flags of file descriptor %d: %s",
             logFD,
             strerror(errno));
    return;
  }
  flags |= O_NONBLOCK;
  int rv = fcntl(fd, F_SETFL, flags);
  if (rv < 0) {
    if (errno == EBADF) {
      // EBADF likely means that fd is not a pipe but a file. Ignore it.
      ld_debug("Got EBADF error trying to set flags of file descriptor %d. "
               "Probably it's not a pipe.",
               fd);
      // Not a pipe, nothing to do here.
      return;
    } else {
      ld_error(
          "Failed to set flags of file descriptor %d: %s", fd, strerror(errno));
    }
  } else {
    ld_debug("Added O_NONBLOCK flag to file descriptor %d. It only has any "
             "effect if it's a pipe.",
             fd);
  }

  // Increase pipe size from the default 64 KiB to min(32 MiB, limit).

  auto set_pipe_size = [fd](int size) {
    int err = fcntl(fd, F_SETPIPE_SZ, size);
    if (err < 0) {
      err = errno;
      if (err == EBADF) {
        ld_debug("Got EBADF error trying to set pipe %d size to %d. "
                 "Probably it's not a pipe.",
                 fd,
                 size);
      } else if (err != EPERM) {
        ld_error(
            "Failed to set pipe %d size to %d: %s", fd, size, strerror(err));
      }
    } else {
      ld_info("Set log pipe %d size to %d", fd, size);
    }
    return err;
  };

  // Try increasing pipe size to 32 MiB.
  int size = 32 << 20; // 32 MiB.
  rv = set_pipe_size(size);

  if (rv != EPERM) {
    return;
  }

  // Got EPERM - we don't have permissions to make pipe this big.
  // As of kernel 4.6 only root can set pipe size to more than 1 MB.
  // Let's try a smaller size. Typical pipe-max-size is 1 MiB.
  {
    std::ifstream in("/proc/sys/fs/pipe-max-size");
    int max_size;
    if (in >> max_size) {
      size = std::min(size, max_size);
    } else {
      ld_info("Failed to read /proc/sys/fs/pipe-max-size");
      // Let's guess 1 MiB.
      size = 1 << 20;
    }
  }

  rv = set_pipe_size(size);
  if (rv == EPERM) {
    ld_info("Got EPERM error trying to set pipe %d size to %d.", fd, size);
  }
}

// see include/debug.h
int useFD(int fd) {
  int prev_fd = logFD;
  logFD = fd;
  return prev_fd;
}

// see common/debug.h
int getFD() {
  return logFD;
}

void useCallback(logging_fn_t fn) {
  // makes sure the singleton gets created
  BackgroundLogger::getInstance();
  customLogFn = fn;
}

void log(const char* cluster,
         const char* component,
         const char* function,
         const int line,
         Level level,
         const char* format,
         ...) {
  // Save errno and restore on return since it's reasonable for callers not to
  // expect a logging macro to clobber it.
  auto errno_stashed = errno;
  SCOPE_EXIT {
    errno = errno_stashed;
  };

  // If true, we're in a recursive log() call and shouldn't do any more
  // recursive calls.
  static __thread bool nested_call = false;
  // The nested call sets this to false if write() fails.
  static __thread bool nested_ok;

  // If some previous calls to log() failed, try to report it.

  if (!nested_call) {
    auto report = [&](std::atomic<size_t>& counter, const char* reason) {
      size_t cnt = counter.exchange(0, std::memory_order_relaxed);
      if (cnt == 0) {
        return;
      }
      nested_call = true;
      ld_info("Missed %lu log messages because %s.", cnt, reason);
      nested_call = false;
      if (!nested_ok) {
        // Failed to write, put it back.
        counter += cnt;
      }
    };
    report(writesFailedWouldblock, "log pipe was full");
    report(writesFailedOther, "write() failed");
  }

  // Prepare the message.

  auto start_time = std::chrono::steady_clock::now();

  va_list ap;
  char record[2048]; // fully formed error log record, including all headers
  int hdrlen = 0;    // length of record header (timestamp, file name, line num)
  int reclen = 0;    // length of the whole record, including header

  if (logFD < 0 || level > Level::SPEW) {
    nested_ok = true;
    return;
  }

  va_start(ap, format);

  struct timeval now;
  struct tm now_tm;

  gettimeofday(&now, nullptr);
  localtime_r(&now.tv_sec, &now_tm);

  pid_t tid = ThreadID::getId();
  const char* name = ThreadID::getName();
  const char* file = component_to_file(component);

  // [SDINWEC]mmdd hh:mm:ss.uuuuuu tid [threadname] file:line] function() msg
  hdrlen = snprintf(record,
                    sizeof(record),
                    "%c%02d%02d %02d:%02d:%02d.%06lu %7d [%s] %s:%d] %s() ",
                    logLevelLetters[(unsigned)level],
                    now_tm.tm_mon + 1,
                    now_tm.tm_mday,
                    now_tm.tm_hour,
                    now_tm.tm_min,
                    now_tm.tm_sec,
                    (unsigned long)now.tv_usec,
                    tid,
                    name,
                    file,
                    line,
                    function);
  ld_check(hdrlen > 0);

  if (hdrlen > sizeof(record)) {
    hdrlen = sizeof(record);
  }

  reclen = vsnprintf(record + hdrlen, sizeof(record) - hdrlen, format, ap);

  if (reclen < 0) { // invalid format
    reclen = hdrlen;
  } else {
    reclen += hdrlen;
  }

  if (reclen < 0 || reclen >= sizeof(record)) { // unlikely
    record[sizeof(record) - 1] = '\n';
    reclen = sizeof(record);
  } else {
    record[reclen++] = '\n';
  }

  va_end(ap);

  // Write to an external logger.
  if (external_logger_plugin && level <= externalLoggerLogLevel) {
    folly::StringPiece log_line(record, reclen);
    external_logger_plugin->log(cluster, static_cast<int>(level), log_line);
  }

  // Write the message to logFD.
  auto before_write_time = std::chrono::steady_clock::now();

  const char* ptr = record;
  int rv;
  while (true) {
    rv = write(logFD, ptr, reclen);
    if (rv == -1 || rv >= reclen) {
      break;
    }
    ptr += rv;
    reclen -= rv;
  }
  if (rv < 0 && !nested_call) {
    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      ++writesFailedWouldblock;
    } else {
      ++writesFailedOther;
    }
  }

  auto after_write_time = std::chrono::steady_clock::now();

  // If this call to log() took a while, complain about it in the log.

  if (!nested_call &&
      after_write_time - start_time > std::chrono::milliseconds(50)) {
    // We're going to call log() recursively.
    // Prevent further recursive calls if that call happens to be slow too.
    nested_call = true;

    ld_info("Slow log() call: it took %.3fs to prepare, %.3fs to write().",
            std::chrono::duration_cast<std::chrono::duration<double>>(
                before_write_time - start_time)
                .count(),
            std::chrono::duration_cast<std::chrono::duration<double>>(
                after_write_time - before_write_time)
                .count());

    nested_call = false;
  }

  nested_ok = rv >= 0;
}

void logDefer(const char* cluster,
              const char* component,
              const char* function,
              const int line,
              Level level,
              const char* format,
              ...) {
  // Prepare the message.

  struct timeval now;
  gettimeofday(&now, nullptr);

  va_list ap;
  std::string record(2048, '\0'); // error message without header
  int reclen = 0;

  va_start(ap, format);

  pid_t tid = ThreadID::getId();
  const char* name = ThreadID::getName();
  const char* file = component_to_file(component);

  reclen = vsnprintf(
      const_cast<char*>(record.data()), record.capacity(), format, ap);
  ld_check(reclen >= 0);
  record.resize(std::min(reclen, 2048));

  va_end(ap);

  auto logger = BackgroundLogger::getInstance();
  if (logger == nullptr) {
    // fall back to directly calling the callback
    customLogFn(
        cluster, file, function, line, level, now, tid, name, record.c_str());
  } else {
    auto rec = std::make_unique<LogMsg>(cluster,
                                        file,
                                        function,
                                        line,
                                        level,
                                        now,
                                        tid,
                                        name,
                                        std::move(record));

    logger->push(std::move(rec));
  }
}

void ld_check_fail_impl(CheckType type,
                        const char* expr,
                        const char* component,
                        const char* function,
                        const int line) {
  const char* check_type_str = type == CheckType::CHECK ? "Check" : "Assertion";
  ld_log_impl(component,
              function,
              line,
              Level::CRITICAL,
              "%s failed: %s",
              check_type_str,
              expr);
  bool should_abort = true;
  switch (type) {
    case CheckType::CHECK:
      should_abort = abortOnFailedCheck.load();
      break;
    case CheckType::ASSERT:
      should_abort = true;
      break;
  }
  if (should_abort) {
    std::abort();
  }
}

// @deprecated because it doesn't parse dbg::Level::NONE, use tryParseLoglevel
Level parseLoglevel(const char* value) {
  static_assert((unsigned)Level::NONE == 0, "dbg::Level::NONE must be 0");
  for (unsigned i = 0; i < sizeof(logLevelNames) / sizeof(logLevelNames[0]);
       i++) {
    if (strcmp(value, logLevelNames[i]) == 0) {
      return (Level)i;
    }
  }

  return Level::NONE;
}

folly::Optional<Level> tryParseLoglevel(const char* value) {
  static_assert((unsigned)Level::NONE == 0, "dbg::Level::NONE must be 0");
  for (int i = 0; i < logLevelNames.size(); ++i) {
    if (strcmp(value, logLevelNames[i]) == 0) {
      return static_cast<Level>(i);
    }
  }
  return folly::none;
}

const char* loglevelToString(Level loglevel) {
  static_assert((unsigned)Level::SPEW + 1 ==
                    sizeof(logLevelNames) / sizeof(logLevelNames[0]),
                "logLevelNames[] must be indexable by dbg::Level constants");
  if (loglevel >= Level::CRITICAL && loglevel <= Level::SPEW) {
    return logLevelNames[(unsigned)loglevel];
  }

  return "unknown";
}

void parseLoglevelOption(const std::string& value) {
  Level level = parseLoglevel(value.c_str());
  if (level == dbg::Level::NONE) {
    char buf[1024];
    snprintf(buf,
             sizeof(buf),
             "Invalid value for --loglevel: %s. "
             "Expected one of: critical, error, warning, notify, "
             "info, debug, spew",
             value.c_str());
    throw boost::program_options::error(buf);
  }
  currentLevel = level;
}

void parseAssertOnDataOption(bool value) {
  assertOnData = value;
}
}}} // namespace facebook::logdevice::dbg
