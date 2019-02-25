/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <atomic>
#include <chrono>
#include <cstdarg>
#include <unordered_map>

#include <folly/Likely.h>
#include <folly/Optional.h>

#include "logdevice/common/EBRateLimiter.h"
#include "logdevice/common/ModuleRegistry.h"
#include "logdevice/common/checks.h"
#include "logdevice/common/toString.h"
#include "logdevice/include/debug.h"

namespace facebook { namespace logdevice {

class Logger;

namespace dbg {

/**
 * @file Internal LogDevice debug logging infrastructure.
 *       See logdevice/include/debug.h for client-visible parts.
 */

/**
 * This function will be called for every log message of level >= INFO.
 * It's called even for calls that were discarded by rate limiter (RATELIMIT_*).
 * If this is nullptr (e.g., in client processes), error counting is disabled.
 */
using bump_error_counter_fn_t = void (*)(Level level);
extern bump_error_counter_fn_t bumpErrorCounterFn;

extern logging_fn_t customLogFn;

// Threads can set this value to the name of the cluster.
// This can be used to differentiate logs between several Client instances for
// different clusters.
std::string& thisThreadClusterName();

/**
 * If the output file descriptor is a pipe, makes it nonblocking and increases
 * its size. This prevents log() from stalling if the receiving end of the pipe
 * doesn't consume fast enough.
 */
void enableNonblockingPipe();

/**
 * Default logging implementation, used if useCallback() was not called by the
 * client.  Writes a message to the error file descriptor if logging is
 * enabled (the descriptor is non-negative).
 *
 * The level is assumed to have been already checked by the logging macros
 * (but passed in to be included on the logged line).
 */
void log(const char* cluster,
         const char* component,
         const char* function,
         const int line,
         Level level,
         const char* format,
         ...)
    __attribute__((__format__(__printf__, 6, 7))); /* tell gcc to
                                                      check printf args */

void logDefer(const char* cluster,
              const char* component,
              const char* function,
              const int line,
              Level level,
              const char* format,
              ...)
    __attribute__((__format__(__printf__, 6, 7))); /* tell gcc to
                                                      check printf args */

/**
 * Expects @param value to be one of: critical, error, warning, info, debug,
 * spew. Returns the corresponding loglevel, or Level::NONE if @param value
 * is invalid.
 */
Level parseLoglevel(const char* value);

folly::Optional<Level> tryParseLoglevel(const char* value);

/**
 * Returns a string description of @param loglevel (one of "critical", "error",
 * "warn", etc.)
 */
const char* loglevelToString(Level loglevel);

/**
 * This is a convenience function intended for use with boost::progam_options
 * library for parsing --loglevel style command line options. It sets
 * dbg::currentLevel to the value corresponding to the log level string
 * passed in as @param value.
 *
 * @param  value   one of: critical, error, warning, info, debug, spew
 *
 * @throws boost::program_options::error if the string in @param value is
 *         invalid
 */
void parseLoglevelOption(const std::string& value);

/**
 * This is a convenience function for use with boost::program_options It sets
 * dbg::assertOnData to the passed value
 */
void parseAssertOnDataOption(bool value);

/**
 * Wrapper around bumpErrorCounter() that only does a non-inline function
 * call if level <= INFO
 *
 * @param level  the level at which this error was reported.
 */
inline void noteError(Level level) {
  if (level <= Level::INFO && bumpErrorCounterFn) {
    bumpErrorCounterFn(level);
  }
}

/**
 * Return the logging FD so it can be reused by other components, such as the
 * Zookeeper client
 */
int getFD();

using LogLevelMap = std::unordered_map<std::string, Level>;

/**
 * Set log level of specific modules (source file), without removing existing
 * overrides in other modules.
 */
void addLogLevelOverrides(const LogLevelMap& map);

/**
 * Set log level of specific modules (source file) and remove log level
 * overrides in all other modules.
 */
void setLogLevelOverrides(LogLevelMap map);

/**
 * Remove all log level overrides. Same as setLogLevelOverrides({}).
 */
void clearLogLevelOverrides();

/**
 * Helper function to get the module object from file name
 */
Module* getModuleFromFile(const char* file);

/**
 * Main macro for logging error/debug messages in LogDevice code.
 */
#define ld_emit_logline(file, function, line, level, fmt, args...)   \
  do {                                                               \
    if (facebook::logdevice::dbg::customLogFn == nullptr) {          \
      facebook::logdevice::dbg::log(                                 \
          facebook::logdevice::dbg::thisThreadClusterName().c_str(), \
          (file),                                                    \
          (function),                                                \
          (line),                                                    \
          (level),                                                   \
          (fmt),                                                     \
          ##args);                                                   \
    } else {                                                         \
      facebook::logdevice::dbg::logDefer(                            \
          facebook::logdevice::dbg::thisThreadClusterName().c_str(), \
          (file),                                                    \
          (function),                                                \
          (line),                                                    \
          (level),                                                   \
          (fmt),                                                     \
          ##args);                                                   \
    }                                                                \
  } while (0)

#define ld_log_impl(file, function, line, level, fmt, args...)              \
  do {                                                                      \
    const facebook::logdevice::dbg::Level _level = (level);                 \
    facebook::logdevice::dbg::noteError(_level);                            \
    static facebook::logdevice::Module* _module =                           \
        facebook::logdevice::dbg::getModuleFromFile((file));                \
    if (_level <= _module->getLogLevel()) {                                 \
      ld_emit_logline((file), (function), (line), (_level), (fmt), ##args); \
    }                                                                       \
  } while (0)

#define ld_log(l, f, args...) \
  ld_log_impl(__FILE__, __FUNCTION__, __LINE__, l, f, ##args)

#define ld_critical(f, args...) \
  ld_log(facebook::logdevice::dbg::Level::CRITICAL, f, ##args)
#define ld_error(f, args...) \
  ld_log(facebook::logdevice::dbg::Level::ERROR, f, ##args)
#define ld_warning(f, args...) \
  ld_log(facebook::logdevice::dbg::Level::WARNING, f, ##args)
#define ld_notify(f, args...) \
  ld_log(facebook::logdevice::dbg::Level::NOTIFY, f, ##args)
#define ld_info(f, args...) \
  ld_log(facebook::logdevice::dbg::Level::INFO, f, ##args)
#define ld_debug(f, args...) \
  ld_log(facebook::logdevice::dbg::Level::DEBUG, f, ##args)
#define ld_spew(f, args...) \
  ld_log(facebook::logdevice::dbg::Level::SPEW, f, ##args)

//////////  Asserts, checks and catches.
extern std::atomic<bool> abortOnFailedCheck;
extern std::atomic<bool> abortOnFailedCatch;
extern std::atomic<bool> assertOnData;
extern std::atomic<Level> externalLoggerLogLevel;
extern std::shared_ptr<facebook::logdevice::Logger> external_logger_plugin;

//////////  Catch: Conditions that abort in debug, but return true / false in
//////////  non-debug.

// Use this to check for bugs or other things that should halt tests, but that
// in production we explicitly want to handle.  For things that depend on data
// from disk or network, use dd_assert().

// An expression that returns "false" if expr is evaluates to false, and also
// prints out the given message.  If expr evaluates to true, or isn't evaluated
// at all (because precheck is false), returns true and doesn't print anything.
#define ld_catch(expr, fmt, ...)                           \
  (LIKELY((bool)(expr)) || ({                              \
     RATELIMIT_CRITICAL(::std::chrono::seconds(5),         \
                        10,                                \
                        "Assertion `%s' failed: " fmt,     \
                        #expr,                             \
                        ##__VA_ARGS__);                    \
     if (::facebook::logdevice::dbg::abortOnFailedCatch) { \
       ::abort();                                          \
     }                                                     \
     false;                                                \
   }))

/**
 * Output a log message unconditionally, tagged at the specified level.
 */
#define ld_log_always(l, f, args...) \
  ld_emit_logline(__FILE__, __FUNCTION__, __LINE__, (l), (f), ##args);

/**
 * Call ld_log_impl(file, function, line, level, fmt, args...) not more than a
 * certain number of times per time interval every time the number of skipped
 * messages reaches a new order of magnitude. The limit is approximate.
 *
 * @param file      source code filename of the caller
 * @param function  caller function
 * @param line      source code line number of the call
 * @param level     log level (a value of type Level)
 * @param duration  time interval expressed as a std::chrono::duration type.
 *                  Converted to milliseconds internally.
 * @param samples   the maximum number of times ld_error() should be called
 *                  per time interval (approximate)
 * @param fmt       format of debug message as expected by fprintf(3)
 * @param args...   arguments for the format string
 */
#define RATELIMIT_LEVEL_IMPL(                                            \
    file, function, line, level, duration, samples, fmt, args...)        \
  do {                                                                   \
    static_assert((long)(samples) > 0, "samples must be positive");      \
    /* library-local */ static EBRateLimiter limiter(samples, duration); \
    /* library-local */ static std::atomic<bool> lock_;                  \
                                                                         \
    /* If someone is holding the lock, we don't print a message and  */  \
    /* don't even call the rate limiter. The message won't therefore */  \
    /* be accounted for the number of skipped messages. This would   */  \
    /* increase overhead considerably because of cache contention,   */  \
    /* and is the reason the limit is approximate.                   */  \
    if (lock_.load(std::memory_order_acquire) ||                         \
        lock_.exchange(true, std::memory_order_acquire)) {               \
      facebook::logdevice::dbg::noteError(level);                        \
      break;                                                             \
    }                                                                    \
    size_t skipped;                                                      \
    if (limiter.isAllowed(skipped)) {                                    \
      if (skipped) {                                                     \
        ld_log((level), "skipped at least %zu log entries", skipped);    \
      }                                                                  \
      ld_log_impl((file), (function), (line), (level), (fmt), ##args);   \
    } else {                                                             \
      facebook::logdevice::dbg::noteError((level));                      \
    }                                                                    \
    lock_.store(false);                                                  \
  } while (0)

#define RATELIMIT_LEVEL(level, duration, samples, fmt, args...) \
  RATELIMIT_LEVEL_IMPL(__FILE__,                                \
                       __FUNCTION__,                            \
                       __LINE__,                                \
                       (level),                                 \
                       (duration),                              \
                       (samples),                               \
                       (fmt),                                   \
                       ##args)

#define RATELIMIT_CRITICAL(duration, samples, fmt, args...)  \
  RATELIMIT_LEVEL(facebook::logdevice::dbg::Level::CRITICAL, \
                  (duration),                                \
                  (samples),                                 \
                  (fmt),                                     \
                  ##args)

#define RATELIMIT_ERROR(duration, samples, fmt, args...)  \
  RATELIMIT_LEVEL(facebook::logdevice::dbg::Level::ERROR, \
                  (duration),                             \
                  (samples),                              \
                  (fmt),                                  \
                  ##args)

#define RATELIMIT_WARNING(duration, samples, fmt, args...)  \
  RATELIMIT_LEVEL(facebook::logdevice::dbg::Level::WARNING, \
                  (duration),                               \
                  (samples),                                \
                  (fmt),                                    \
                  ##args)

#define RATELIMIT_INFO(duration, samples, fmt, args...)  \
  RATELIMIT_LEVEL(facebook::logdevice::dbg::Level::INFO, \
                  (duration),                            \
                  (samples),                             \
                  (fmt),                                 \
                  ##args)

#define RATELIMIT_DEBUG(duration, samples, fmt, args...)  \
  RATELIMIT_LEVEL(facebook::logdevice::dbg::Level::DEBUG, \
                  (duration),                             \
                  (samples),                              \
                  (fmt),                                  \
                  ##args)

// data-dependent assert. Use this instead of an assert if you are asserting
// on something that depends on data on disk and/or network. Will return true
// if the condition is satisfied. If not, logs an error and assert()'s if
// NDEBUG is not set and dbg::assertOnData is true, Returns false otherwise.
//
// Use it at any point where you would use an ld_assert() if the following
// assumption were always true:
// The data that LD wrote to disk or to the network did not get messed with
// by anyone or anything except for LD code.
// Example valid uses:
// - dd_asserting that the data you read is written in the way you'd expect LD
//   code (including older versions) to write it.
// - dd_asserting that different pieces of data are mutually consistent.
// - dd_asserting that configuration is consistent between nodes if we expect
//   it to be.
// Example invalid use cases:
// - dd_asserting that there are no I/O errors while reading.
// - dd_asserting that the data is in the format that the new LD version would
//   write, even though it might've been written by a previous version. This
//   applies to LD client/server running different versions too (or even
//   different servers running different versions or having connections from
//   different versions of the client).

#define dd_assert(condition, f, args...)               \
  ((condition) ||                                      \
   facebook::logdevice::dbg::DD_Assert_Trigger<        \
       facebook::logdevice::dbg::const_hash(__FILE__), \
       __LINE__>::impl(__FILE__,                       \
                       __FUNCTION__,                   \
                       __LINE__,                       \
                       #condition,                     \
                       f,                              \
                       ##args))

constexpr uint64_t const_hash(const char* a) {
  return *a ? *a + 257 * const_hash(a + 1) : 0;
}

// this actually triggers the assert. The only reason to have this is to skip
// having all the template params of DD_AssertTrigger in the log when the actual
// assert fires
inline void dd_assert_trigger() {
  ld_check(false);
}

// Implements logging the error and triggering the assert if dd_assert()'s
// condition fails. Should not be called directly. Always returns false.
// The templating here is to have a separate rate limiter (which is initialized
// as a static var) for each __FILE__/__LINE__ pair.
template <uint64_t FilenameHash, uint64_t LineNo>
class DD_Assert_Trigger {
 public:
  static bool impl(const char* file,
                   const char* function,
                   int line,
                   const char* condition,
                   const char* fmt,
                   ...)
      // Tell gcc to check printf args
      __attribute__((__format__(__printf__, 5, 6))) {
    va_list ap;
    va_start(ap, fmt);
    char buf[2048];

    int rv = vsnprintf(buf, sizeof(buf), fmt, ap);
    if (rv < 0) {
      snprintf(buf, sizeof(buf), "Invalid printf() format");
    }
    va_end(ap);

    RATELIMIT_LEVEL_IMPL(file,
                         function,
                         line,
                         facebook::logdevice::dbg::Level::CRITICAL,
                         std::chrono::seconds(10),
                         10,
                         "Data-dependent assertion `%s` failed. %s",
                         condition,
                         buf);
    if (facebook::logdevice::dbg::assertOnData) {
      dd_assert_trigger();
    }
    return false;
  }
};

} // namespace dbg
}} // namespace facebook::logdevice
