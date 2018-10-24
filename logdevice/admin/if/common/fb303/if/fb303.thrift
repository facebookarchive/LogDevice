/*
 * fb303.thrift
 *
 * Copyright (c) 2006- Facebook
 * Distributed under the Thrift Software License
 *
 * See accompanying file LICENSE or visit the Thrift site at:
 * http://developers.facebook.com/thrift/
 *
 *
 * Definition of common Facebook data types and status reporting mechanisms
 * common to all Facebook services. In some cases, these methods are
 * provided in the base implementation, and in other cases they simply define
 * methods that inheriting applications should implement (i.e. status report)
 *
 * @author Mark Slee <mcslee@facebook.com>
 */
namespace java com.facebook.fbcode.fb303
namespace java.swift com.facebook.swift.fb303
namespace cpp facebook.fb303
namespace d facebook.fb303
namespace py.asyncio fb303_asyncio.fb303
namespace py.twisted fb303_twisted.fb303
namespace perl fb303
namespace php fb303
namespace node_module fb303
namespace go common.fb303.if.fb303

/**
 * Common status reporting mechanism across all services
 */
enum fb_status {
  DEAD = 0,
  STARTING = 1,
  ALIVE = 2,
  STOPPING = 3,
  STOPPED = 4,
  WARNING = 5,
}

/**
 * Structure for holding counters information.
 */
struct CountersInformation {
  1: map<string, i64> data,
}

struct CpuProfileOptions {
  1: i32 durationSecs,

  // If "selective" is set, we only profile sections of code where profiling
  // is explicitly enabled by calling Perftools::startSelectiveCpuProfiling()
  // and Perftools::stopSelectiveCpuProfiling() from each thread that
  // should be profiled. You must likely instrument your server to do this.
  2: bool selective,
}

struct PcapLoggingConfig {
  // Is logging enabled?
  1: bool enabled,

  // Filename will be /tmp/{prefix}_{date}. Prefix can only be alphanumeric,
  // or contain - (minus) and _ (underscore).
  2: string prefix,

  // Turn off logging after this timeout. If 0, don't turn off automaticlaly
  3: i32 timeoutMs,

  // Only log the first snaplen bytes of a message. spanlen only includes
  // Thrift header and message bytes, not IP or TCP headers.
  4: i32 snaplen,

  // Number of messages to log at the start of a connection. A connection open
  // (the TCP handshake), and all security handshakes (TLS) count as
  // 1 message. So, e.g. 1 will only log a connection establishment,
  // while 2 will do that and log 1 user message. 0 means don't log
  // connection establishment. -1 means log all messages (in which case,
  // numMessagesConnEnd is ignored).
  5: i32 numMessagesConnStart,

  // Number of last messages to log before a connection is closed.
  // 0 ignores closes. 1 logs the connection termination message
  // (even if more that 1 message is involved as with SSL).
  // If N (>1), logs N-1 last user messages and the close message.
  // Ignored if numMessagesConnStart == -1.
  // Note that if numMessagesConnEnd > 0, then numMessagesConnEnd - 1
  // messages are buffered in memory for every connection.
  6: i32 numMessagesConnEnd,

  // Only log on a random sample (sampleConnectionPct %) of connections.
  7: i32 sampleConnectionPct,

  // Rotate output file once it grows beyond this many MB.
  // No rotation happens if -1.
  8: i32 rotateAfterMB,
}

exception PcapLoggingConfigException {
  1: string message,
} (message = 'message')

/**
 * Standard base service
 *
 * Those methods with priority level IMPORTANT run in a dedicated thread pool
 * in C++. If you need to change one of the rest to be IMPORTANT, make sure it
 * runs fast so it won't block other IMPORTANT methods.
 */
service FacebookService {

  /**
   * Returns a descriptive name of the service
   */
  string getName(),

  /**
   * Returns the version of the service
   */
  string getVersion(),

  /**
   * Gets the status of this service
   */
  fb_status getStatus() (priority='IMPORTANT'),

  /**
   * User friendly description of status, such as why the service is in
   * the dead or warning state, or what is being started or stopped.
   */
  string getStatusDetails() (priority = 'IMPORTANT'),

  /**
   * Gets the counters for this service
   */
  map<string, i64> getCounters(),

  /**
   * Gets a subset of counters which match a
   * Perl Compatible Regular Expression for this service
   */
  map<string, i64> getRegexCounters(1: string regex),

  /**
   * Gets a subset of counters which match a
   * Perl Compatible Regular Expression for this service.
   * Puts them into a CountersInformation struct
   * serialized with TBinaryProtocol and compressed with zlib.
   */
  binary getRegexCountersCompressed(1: string regex),

  /**
   * Gets the counters for this service into a CountersInformation struct
   * serialized with TBinaryProtocol and compressed with zlib.
   */
  binary getCountersCompressed(),

  /**
   * Get counter values for a specific list of keys.  Returns a map from
   * key to counter value; if a requested counter doesn't exist, it won't
   * be in the returned map.
   */
  map<string, i64> getSelectedCounters(1: list<string> keys)
    (priority = 'IMPORTANT'),

  /**
   * Gets the value of a single counter
   */
  i64 getCounter(1: string key) (priority = 'IMPORTANT'),

  /**
   * Gets the exported string values for this service
   */
  map<string, string> getExportedValues() (priority = 'IMPORTANT'),

  /**
   * Get exported strings for a specific list of keys.  Returns a map from
   * key to string value; if a requested key doesn't exist, it won't
   * be in the returned map.
   */
  map<string, string> getSelectedExportedValues(1: list<string> keys)
    (priority = 'IMPORTANT'),

  /**
   * Gets a subset of exported values which match a
   * Perl Compatible Regular Expression for this service
   */
  map<string, string> getRegexExportedValues(1: string regex),

  /**
   * Gets the value of a single exported string
   */
  string getExportedValue(1: string key) (priority = 'IMPORTANT'),

  /**
   * Sets an option
   */
  void setOption(1: string key, 2: string value),

  /**
   * Gets an option
   */
  string getOption(1: string key),

  /**
   * Gets all options
   */
  map<string, string> getOptions(),

  /**
   * Returns a CPU profile over the given time interval (client and server
   * must agree on the profile format).
   *
   */
  string getCpuProfile(1: i32 profileDurationInSec) (thread='eb'),

  /**
   * Returns a CPU profile, specifying options (see the struct definition).
   */
  string getCpuProfileWithOptions(1: CpuProfileOptions options) (thread='eb'),

  /**
   * Returns a heap profile over the given time interval (client and server
   * must agree on the profile format).
   */
  string getHeapProfile(1: i32 profileDurationInSec) (thread='eb'),

  /**
   * Returns a WallTime Profiler data over the given time
   * interval (client and server must agree on the profile format).
   */
  string getWallTimeProfile(1: i32 profileDurationInSec),

  /**
   * Returns the current memory usage (RSS) of this process in bytes.
   */
  i64 getMemoryUsage(),

  /**
   * Returns the 1-minute load average on the system (i.e. the load may not
   * all be coming from the current process).
   */
  double getLoad() (priority = 'IMPORTANT'),

  /**
   * Returns the pid of the process
   */
  i64 getPid(),

  /**
   * Returns the command line used to execute this process.
   */
  string getCommandLine(),

  /**
   * Returns the unix time that the server has been running since
   */
  i64 aliveSince() (priority = 'IMPORTANT'),

  /**
   * Tell the server to reload its configuration, reopen log files, etc
   */
  oneway void reinitialize(),

  /**
   * Suggest a shutdown to the server
   */
  oneway void shutdown(),

  /**
   * Translate frame pointers to file name and line pairs.
   */
  list<string> translateFrames(1: list<i64> pointers),

  PcapLoggingConfig getPcapLoggingConfig(),

  void setPcapLoggingConfig(1: PcapLoggingConfig config) throws
    (1: PcapLoggingConfigException e),

}
