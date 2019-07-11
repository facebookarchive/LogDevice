/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <utility>

#include "logdevice/include/AsyncReader.h"
#include "logdevice/include/ClientFactory.h"
#include "logdevice/include/ClientSettings.h"
#include "logdevice/include/ClusterAttributes.h"
#include "logdevice/include/ConfigSubscriptionHandle.h"
#include "logdevice/include/Err.h"
#include "logdevice/include/LogHeadAttributes.h"
#include "logdevice/include/LogTailAttributes.h"
#include "logdevice/include/LogsConfigTypes.h"
#include "logdevice/include/Reader.h"
#include "logdevice/include/Record.h"
#include "logdevice/include/types.h"

/**
 * @file LogDevice client class. "Client" is a generic name, we expect
 *       application code to use namespaces, possibly aliasing
 *       facebook::logdevice to something shorter, like ld.
 *       See also README.
 */

namespace facebook { namespace logdevice {

// Enum which is used to determine accuracy of findTime or findKey in order to
// get best accuracy-speed trade off for each specific use case. See
// findTimeSync() or findKeySync() for documentation about each accuracy mode.
enum class FindKeyAccuracy { STRICT, APPROXIMATE };

// Struct which is used to provide the result of findKey. See findKey() for
// documentation about the returned values.
struct FindKeyResult {
  Status status;
  lsn_t lo;
  lsn_t hi;
};

// Enum which is used to determine accuracy of dataSize -- only approximate
// mode is currently supported.
enum class DataSizeAccuracy { APPROXIMATE, COUNT };

/**
 * Type of callback that is called when a non-blocking append completes.
 *
 * @param st   E::OK on success. On failure this will be one of the error
 *             codes defined for Client::appendSync().
 *
 * @param r    contains the log id and payload passed to the async append
 *             call. If the operation succeeded (st==E::OK), it will also
 *             contain the LSN and timestamp assigned to the new record.
 *             If the operation failed, the LSN will be set to LSN_INVALID,
 *             timestamp to the time the record was accepted for delivery.
 */
typedef std::function<void(Status st, const DataRecord& r)> append_callback_t;

/**
 * Type of callback that is called when a non-blocking findTime() request
 * completes.
 *
 * See findTime() and findTimeSync() for docs.
 */
typedef std::function<void(Status, lsn_t result)> find_time_callback_t;

/**
 * Type of callback that is called when a non-blocking findKey() request
 * completes.
 *
 * See findKey() and findKeySync() for docs.
 */
typedef std::function<void(FindKeyResult result)> find_key_callback_t;

/**
 * Type of callback that is called when a non-blocking ByteOffset() request
 * completes.
 *
 * See ByteOffset() and ByteOffsetSync() for docs.
 */
typedef std::function<void(Status, uint64_t result)> byte_offset_callback_t;

/**
 * Type of callback that is called when a non-blocking trim() request
 * completes.
 *
 * See trim() and trimSync() for docs.
 */
typedef std::function<void(Status)> trim_callback_t;

/**
 * Type of callback that is called when a non-blocking isLogEmpty() request
 * completes.
 *
 * See isLogEmpty() and isLogEmptySync() for docs.
 */
typedef std::function<void(Status status, bool empty)> is_empty_callback_t;

/**
 * Type of callback that is called when a non-blocking dataSize() request
 * completes.
 *
 * See dataSize() and dataSizeSync() for docs.
 */
typedef std::function<void(Status status, size_t size)> data_size_callback_t;

/**
 * Type of callback that is called when a non-blocking getTailLSN() request
 * completes.
 *
 * See getTailLSN() and getTailLSNSync() for docs.
 */
typedef std::function<void(Status status, lsn_t)> get_tail_lsn_callback_t;

/**
 * Type of callback that is called when a non-blocking getLogRangeByName()
 * request completes.
 *
 * See getLogRangeByName() for docs.
 */
typedef std::function<void(Status status, std::pair<logid_t, logid_t>)>
    get_log_range_by_name_callback_t;

/**
 * Type of callback that is called when a non-blocking getLogRangesByNamespace()
 * request completes.
 *
 * See getLogRangesByNamespace() for docs.
 */
typedef std::function<void(Status status, std::map<std::string, logid_range_t>)>
    get_log_ranges_by_namespace_callback_t;

/**
 * Type of callback that is called when LogDevice config is updated.
 *
 * see subscribeToConfigUpdates() for docs.
 */
typedef std::function<void()> config_update_callback_t;

/**
 * Type of callback that is called when a non-blocking getLogGroup()
 * request completes.
 *
 * See getLogGroup() for docs.
 */

typedef std::function<void(Status status, std::unique_ptr<client::LogGroup>)>
    get_log_group_callback_t;

typedef std::function<void(Status status,
                           std::unique_ptr<client::LogGroup>,
                           const std::string& failure_reason)>
    make_log_group_callback_t;

typedef std::function<void(Status status,
                           std::unique_ptr<client::LogAttributes>)>
    get_log_attributes_callback_t;

typedef std::function<void(Status status, std::unique_ptr<client::Directory>)>
    get_directory_callback_t;

typedef std::function<void(Status status,
                           std::unique_ptr<client::Directory>,
                           const std::string& failure_reason)>
    make_directory_callback_t;

typedef std::function<
    void(Status, uint64_t version, const std::string& failure_reason)>
    logsconfig_status_callback_t;

/**
 * Type of callback that is called when a non-blocking getTailAttributes()
 * request completes.
 *
 * See getTailAttributes() and getTailAttributesSync() for docs.
 */
typedef std::function<void(Status status, std::unique_ptr<LogTailAttributes>)>
    get_tail_attributes_callback_t;

/**
 * Type of callback that is called when a non-blocking getHeadAttributes()
 * request completes.
 *
 * See getHeadAttributes() and getHeadAttributesSync() for docs.
 */
typedef std::function<void(Status status, std::unique_ptr<LogHeadAttributes>)>
    get_head_attributes_callback_t;

class Client {
 public:
  /**
   * This interface is deprecated. Please use ClientFactory to create clients
   */
  [[deprecated("Replaced by ClientFactory")]] static std::shared_ptr<Client>
  create(std::string cluster_name,
         std::string config_url,
         std::string credentials,
         std::chrono::milliseconds timeout,
         std::unique_ptr<ClientSettings> settings,
         std::string csid = "") noexcept;

  /**
  No default constructor will issue a compilation error, since ClientImpl
  need to initialize the Client object at first.
  */
  Client() = default;

  /**
   * ClientFactory::create() actually returns pointers to objects of class
   * ClientImpl that inherits from Client. The destructor must be virtual in
   * order to work correctly.
   */
  virtual ~Client() = default;

  /**
   * Appends a new record to the log. Blocks until operation completes.
   * The delivery of a signal does not interrupt the wait.
   *
   * @param logid     unique id of the log to which to append a new record
   *
   * @param payload   record payload
   *
   * @param attrs     additional append attributes. See AppendAttributes
   *
   * @param timestamp Timestamp set and stored with the record by the
   *                  LogDevice cluster.
   *
   * @return on success the sequence number (LSN) of new record is returned.
   *         On failure LSN_INVALID is returned and logdevice::err is set to
   *         one of:
   *    TIMEDOUT       timeout expired before operation status was known. The
   *                   record may or may not be appended. The timeout
   *                   used is from this Client object.
   *    NOTFOUND       The logid was not found in the config.
   *
   *    NOTINSERVERCONFIG  The logid was not found in the config of the
   *                       seqeuncer node
   *
   *    NOSEQUENCER    The client has been unable to locate a sequencer for
   *                   this log. For example, the server that was previously
   *                   sequencing this log has crashed or is shutting down,
   *                   and a replacement has not yet been brought up, or its
   *                   identity has not yet been communicated to this client.
   *    CONNFAILED     Failed to connect to sequencer. Request was not sent.
   *                   Possible reasons:
   *                    - invalid address in cluster config
   *                    - logdeviced running the sequencer is down or
   *                      unreachable
   *                    - mismatching cluster name between client and sequencer
   *                    - mismatching destination and receiving node ids
   *    PEER_CLOSED    Sequencer closed connection after we sent the append
   *                   request but before we got a reply. Record may or
   *                   may not be appended.
   *    TOOBIG         Payload is too big (see Client::getMaxPayloadSize())
   *    NOBUFS         request could not be enqueued because a buffer
   *                   space limit was reached in this Client object. Request
   *                   was not sent
   *    SYSLIMIT       client process has reached a system limit on resources,
   *                   such as file descriptors, ephemeral ports, or memory.
   *                   Request was not sent.
   *    SEQNOBUFS      sequencer is out of buffer space for this log. Record
   *                   was not appended.
   *    SEQSYSLIMIT    sequencer has reached a file descriptor limit,
   *                   the maximum number of ephemeral ports, or some other
   *                   system limit. Record may or may not be appended.
   *    NOSPC          too many nodes on the storage cluster have run out of
   *                   free disk space. Record was not appended.
   *    OVERLOADED     too many nodes on the storage cluster are
   *                   overloaded. Record was not appended.
   *    DISABLED       too many nodes on the storage cluster are in error state
   *                   or rebuilding. Record was not appended.
   *    ACCESS         the service denied access to this client based on
   *                   credentials presented
   *    SHUTDOWN       the logdevice::Client instance was destroyed. Request
   *                   was not sent.
   *    INTERNAL       an internal error has been detected, check logs
   *    INVALID_PARAM  logid is invalid
   *    BADPAYLOAD     the checksum bits do not correspond with the Payload
   *                   data. In this case the record is not appended.
   *    CANCELLED      the appender cancelled the request due to some conditions
   *                   on server side, such as for instance it detected that
   *                   the record may have been stored in a previous epoch
   *                   (a.k.a silent duplicate)
   *    PEER_UNAVAILABLE  the sequencer died or became unresponsive. the
   *                      record may or may not have been appended.
   */
  virtual lsn_t
  appendSync(logid_t logid,
             std::string payload,
             AppendAttributes attrs = AppendAttributes(),
             std::chrono::milliseconds* timestamp = nullptr) noexcept = 0;

  /**
   * Appends a new record to the log. Blocks until operation completes.
   * The delivery of a signal does not interrupt the wait.
   *
   * @param logid     unique id of the log to which to append a new record
   *
   * @param payload   record payload, see Record.h. The function does not
   *                  make an internal copy of payload. Other threads of the
   *                  caller must not modify payload data until the call
   *                  returns.
   *
   * @param attrs     additional append attributes. See AppendAttributes
   *
   * @param timestamp Timestamp set and stored with the record by the
   *                  LogDevice cluster.
   *
   * See appendSync(logid_t, const Payload&) for a description of return
   * values.
   */
  virtual lsn_t
  appendSync(logid_t logid,
             const Payload& payload,
             AppendAttributes attrs = AppendAttributes(),
             std::chrono::milliseconds* timestamp = nullptr) noexcept = 0;

  /**
   * Appends a new record to the log without blocking. The function returns
   * control to caller as soon as the append request is put on a delivery
   * queue in this process' address space. The LogDevice client library will
   * call a callback on an unspecified thread when the operation completes.
   *
   * NOTE: records appended to the same log by calling append() method of the
   *       same Client object on the same thread are guaranteed to receive
   *       sequence numbers in the order the append() calls were made. That is,
   *       if both appends succeed, the sequence number assigned to the record
   *       sent earlier will be smaller than the sequence number assigned to
   *       the later record.
   *
   *       This is not always true for a pair of append() calls on the same
   *       log made by _different_ threads or through _different_ Client
   *       objects. In those cases internal buffering in various LogDevice
   *       client and server components may result in the record in an earlier
   *       append() call to receive a higher sequence number than the one
   *       submitted by a later append() call made by a different thread or
   *       process, or made through a different logdevice::Client object.
   *
   * @param logid     unique id of the log to which to append a new record
   *
   * @param payload   record payload.
   *
   * @param cb        the callback to call
   *
   * @param attrs     additional append attributes. See AppendAttributes
   *
   * @return  0 is returned if the request was successfully enqueued for
   *          delivery. On failure -1 is returned and logdevice::err is set to
   *             TOOBIG      if payload is too big (see
   *                         Client::getMaxPayloadSize())
   *             NOBUFS      if request could not be enqueued because a buffer
   *                         space limit was reached
   *      INVALID_PARAM      logid is invalid
   */
  virtual int append(logid_t logid,
                     std::string payload,
                     append_callback_t cb,
                     AppendAttributes attrs = AppendAttributes()) noexcept = 0;

  /**
   * Appends a new record to the log without blocking. This version doesn't
   * transfer the ownership of the payload to LogDevice and assumes that the
   * caller will be responsible for destroying it.
   *
   *  IMPORTANT: for performance reasons this function does not make
   *  an internal copy of payload.  It just passes payload.data
   *  pointer and payload.size value to the LogDevice client thread
   *  pool. The caller MUST make sure that the payload is not free'd
   *  or modified, or its memory is otherwise reused until the
   *  callback cb() is called with the same payload as its argument. A
   *  common pattern for sending a payload that's on the stack is to
   *  memcpy() it into a malloc'ed buffer, then call free() on
   *  payload.data pointer passed to cb().
   */
  virtual int append(logid_t logid,
                     const Payload& payload,
                     append_callback_t cb,
                     AppendAttributes attrs = AppendAttributes()) noexcept = 0;

  /**
   * Creates a Reader object that can be used to read from one or more logs.
   *
   * Approximate memory usage when reading is:
   *   max_logs * client_read_buffer_size * (24*F + C + avg_record_size) bytes
   *
   * The constant F is between 1 and 2 depending on the
   * client_read_flow_control_threshold setting.  The constant C is
   * ClientReadStream overhead, probably a few pointers.
   *
   * When reading many logs, or when memory is important, the client read
   * buffer size can be reduced (before creating the Reader) from the default
   * 4096:
   *
   *   int rv = client->settings().set("client-read-buffer-size", 128);
   *   assert(rv == 0);
   *
   * The client can also set its individual buffer size via the optional
   * buffer_size parameter
   *
   * @param max_logs maximum number of logs that can be read from this Reader
   *                 at the same time
   * @param buffer_size specify the read buffer size for this client, fallback
   *                 to the value in settings if it is -1 or omitted
   */
  virtual std::unique_ptr<Reader>
  createReader(size_t max_logs, ssize_t buffer_size = -1) noexcept = 0;

  /**
   * Creates an AsyncReader object that can be used to read from one or more
   * logs via callbacks.
   */
  virtual std::unique_ptr<AsyncReader>
  createAsyncReader(ssize_t buffer_size = -1) noexcept = 0;

  /**
   * Overrides the timeout value passed to ClientFactory::create() everywhere
   * that timeout is used.
   */
  virtual void setTimeout(std::chrono::milliseconds timeout) noexcept = 0;

  /**
   * Ask LogDevice cluster to trim the log up to and including the specified
   * LSN. After the operation successfully completes records with LSNs up to
   * 'lsn' are no longer accessible to LogDevice clients.
   *
   * This method is synchronous -- it blocks until all storage nodes
   * acknowledge the trim command, the timeout occurs, or the provided
   * credentials are invalid.
   *
   * @param logid ID of log to trim
   * @param lsn   Trim the log up to this LSN (inclusive), should not be larger
   *              than the LSN of the most recent record available to readers
   * @return      Returns 0 if the request was successfully acknowledged
   *              by all nodes. Otherwise, returns -1 with logdevice::err set to
   *
   *    E::NOTFOUND        There is no log with such logid.
   *    E::FAILED          Failed to even start trimming
   *    E::PARTIAL         Got replies from all storage nodes, but some of them
   *                       were unsuccessful. The trimming operation is only
   *                       partially complete: if you read the log now, you may
   *                       see it trimmed or untrimmed, and it may change from
   *                       reader to reader.
   *    E::TIMEDOUT        The operation timed out before reaching all storage
   *                       nodes. The trimming may be partially complete,
   *                       just like with E::PARTIAL.
   *    E::ACCESS          Client has invalid credentials or client does not
   *                       have the correct permissions to perform the trim
   *                       operation.
   *    E::TOOBIG          The trim LSN is beyond the tail of the log.
   *    E::NOBUFS          The Client is overloaded.
   */
  virtual int trimSync(logid_t logid, lsn_t lsn) noexcept = 0;

  /**
   * A non-blocking version of trimSync().
   *
   * @return If the request was successfully submitted for processing, returns
   * 0.  In that case, the supplied callback is guaranteed to be called at a
   * later time with the outcome of the request.  See trimSync() for
   * documentation for the result.  Otherwise, returns -1.
   */
  virtual int trim(logid_t logid, lsn_t lsn, trim_callback_t cb) noexcept = 0;

  /**
   * Supply a write token.  Without this, writes to any logs configured to
   * require a write token will fail.
   *
   * Write tokens are a safety feature intended to reduce the risk of
   * accidentally writing into the wrong log, particularly in multitenant
   * deployments.
   */
  virtual void addWriteToken(std::string) noexcept = 0;

  /**
   * Looks for the sequence number that the log was at at the given time.  The
   * most common use case is to read all records since that time, by
   * subsequently calling startReading(result_lsn).
   *
   * The result lsn can be smaller than biggest lsn which timestamp is <= given
   * timestamp. With accuracy parameter set to APPROXIMATE this error can be
   * several minutes.
   * Note that even in that case startReading(result_lsn) will read
   * all records at the given timestamp or later, but it may also read some
   * earlier records.
   *
   * If the given timestamp is earlier than all records in the log, this returns
   * the LSN after the point to which the log was trimmed.
   *
   * If the given timestamp is later than all records in the log, this returns
   * the next sequence number to be issued.  Calling startReading(result_lsn)
   * will read newly written records.
   *
   * If the log is empty, this returns LSN_OLDEST.
   *
   * All of the above assumes that records in the log have increasing
   * timestamps.  If timestamps are not monotonic, the accuracy of this API
   * may be affected.  This may be the case if the sequencer's system clock is
   * changed, or if the sequencer moves and the clocks are not in sync.
   *
   * The delivery of a signal does not interrupt the wait.
   *
   * @param logid       ID of log to query
   * @param timestamp   select the oldest record in this log whose
   *                    timestamp is greater or equal to _timestamp_.
   * @param status_out  if this argument is nullptr, it is ignored. Otherwise,
   *                    *status_out will hold the outcome of the request as
   *                    described below.
   * @param accuracy   Accuracy option specify how accurate the result of
   *                   findTime() has to be. It allows to choose best
   *                   accuracy-speed trade off for each specific use case.
   *                   Accuracy can be:
   *  STRICT               In this case findTime() will do binary search over
   *                       partitions in memory + binary search inside partition
   *                       on disk. Result will be accurate but execution is
   *                       slower than in APPROXIMATE mode.
   *                       More precisely, this attempts to find the first LSN
   *                       at or after the given time. However, if we cannot get
   *                       a conclusive answer (system issues prevent us from
   *                       getting answers from part of the cluster), this may
   *                       return a slightly earlier LSN (with an appropriate
   *                       status as documented below).
   *                       STRICT options is recommended if cluster has Flash
   *                       disks set up.
   *  APPROXIMATE          findTime() will only perform binary search on the
   *                       partition directory in order to find the newest
   *                       partition whose timestamp in the directory is
   *                       <= given timestamp. Then it will return first lsn of
   *                       given log_id in this partition. The result lsn can be
   *                       several minutes earlier than biggest lsn which
   *                       timestamp is <= given timestamp but execution will be
   *                       faster than in STRICT mode.
   *                       APPROXIMATE options is recommended if cluster has HDD
   *                       disks set up.
   *
   * @return
   * Returns LSN_INVALID on complete failure or an LSN as described above.  If
   * status_out is not null, *status_out can be inspected to determine the
   * accuracy of the result:
   * - E::OK: Enough of the cluster responded to produce a conclusive answer.
   *   Assuming monotonic timestamps, the returned LSN is exactly the first
   *   record at or after the given time.
   * - E::INVALID_PARAM: `logid` is invalid or doesn't exist.
   * - E::ACCESS   Permission to access the log was denied
   * - E::PARTIAL: Only part of the cluster responded and we only got an
   *   approximate answer.  Assuming monotonic timestamps, the returned LSN is
   *   no later than any record at or after the given time.
   * - E::FAILED: No storage nodes responded, or another critical failure.
   * - E::SHUTDOWN: Client was destroyed while the request was processing.
   */
  virtual lsn_t
  findTimeSync(logid_t logid,
               std::chrono::milliseconds timestamp,
               Status* status_out = nullptr,
               FindKeyAccuracy accuracy = FindKeyAccuracy::STRICT) noexcept = 0;

  /**
   * Looks for the sequence number corresponding to the record with the given
   * key for the log.
   *
   * The result provides two LSNs: the first one, lo, is the highest LSN with
   * key smaller than the given key, the second one, hi, is the lowest LSN with
   * key equal or greater than the given key. With accuracy parameter set to
   * APPROXIMATE, the first LSN can be underestimated and the second LSN can be
   * overestimated by a few minutes, in terms of record timestamps.
   *
   * It is assumed that keys within the same log are monotonically
   * non-decreasing (when compared lexicographically). If this is not true, the
   * accuracy of this API may be affected.
   *
   * The delivery of a signal does not interrupt the wait.
   *
   * @param logid       ID of log to query
   * @param key         select the oldest record in this log whose
   *                    key is greater or equal to _key_, for upper bound of
   *                    result; select the newest record in this log whose key
   *                    is smaller than _key_, for lower bound.
   * @param status_out  if this argument is nullptr, it is ignored. Otherwise,
   *                    *status_out will hold the outcome of the request as
   *                    described below.
   * @param accuracy    Accuracy option specifies how accurate the result of
   *                    findKey() has to be. It allows to choose best
   *                    accuracy-speed trade off for each specific use case.
   *                    Accuracy can be:
   *  STRICT               In this case findKey() will do binary search over
   *                       partitions in memory + search inside partition
   *                       on disk. Result will be accurate but execution is
   *                       slower than in APPROXIMATE mode.
   *                       More precisely, this attempts to find the last LSN
   *                       before the given key and the first LSN at or after
   *                       the given key. However, if we cannot get a conclusive
   *                       answer (system issues prevent us from getting answers
   *                       from part of the cluster), this may return a slightly
   *                       larger range (with an appropriate status as
   *                       documented below).
   *                       STRICT options is recommended if cluster has Flash
   *                       disks set up.
   *  APPROXIMATE          findKey() will only perform binary search on the
   *                       partition directory in order to find the partition
   *                       whose minimum key in the directory is
   *                       <= given key and the partition after this one whose
   *                       minimum key is >= given key. Then it will return the
   *                       corresponding LSNs of the two records. The record
   *                       corresponding to the lower LSN can be several minutes
   *                       older than the record given by the upper LSN, but
   *                       execution will be faster than in STRICT mode.
   *                       APPROXIMATE options is recommended if cluster has HDD
   *                       disks set up.
   *
   * @return
   * Returns a FindKeyResult struct with both lo and hi set to LSN_INVALID on
   * complete failure or set to the values described above. The value of status
   * can be inspected to determine the accuracy of the result:
   * - E::INVALID_PARAM: logid was invalid
   * - E::OK: Enough of the cluster responded to produce a conclusive answer.
   *   Assuming monotonic keys, the result provides the LSNs lo and hi as
   *   described above.
   * - E::ACCESS   Permission to access the log was denied
   * - E::PARTIAL: Only part of the cluster responded and we only got an
   *   approximate answer. Assuming monotonic keys, the result contains two LSNs
   *   representing a larger range than in the E::OK case.
   * - E::FAILED: No storage nodes responded, or another critical failure.
   * - E::SHUTDOWN: Client was destroyed while the request was processing.
   */
  virtual FindKeyResult
  findKeySync(logid_t logid,
              std::string key,
              FindKeyAccuracy accuracy = FindKeyAccuracy::STRICT) noexcept = 0;

  /**
   * A non-blocking version of findTimeSync().
   *
   * @return If the request was successfully submitted for processing, returns
   * 0.  In that case, the supplied callback is guaranteed to be called at a
   * later time with the outcome of the request. See findTimeSync() for
   * documentation for the result. Otherwise, returns -1 with the error:
   * - E::NOBUFS: Too many requests were pending to be delivered to Workers.
   */
  virtual int
  findTime(logid_t logid,
           std::chrono::milliseconds timestamp,
           find_time_callback_t cb,
           FindKeyAccuracy accuracy = FindKeyAccuracy::STRICT) noexcept = 0;

  /**
   * A non-blocking version of findKeySync().
   *
   * @return If the request was successfully submitted for processing, returns
   * 0. In that case, the supplied callback is guaranteed to be called at a
   * later time with the outcome of the request.  See findKeySync() for
   * documentation for the result.  Otherwise, returns -1.
   */
  virtual int
  findKey(logid_t logid,
          std::string key,
          find_key_callback_t cb,
          FindKeyAccuracy accuracy = FindKeyAccuracy::STRICT) noexcept = 0;

  /**
   * NOTE: This API call translates to isLogEmptyV2Sync call, if client setting
   * --enable-is-log-empty-v2 is set to true. If you intend to use this, please
   * refer to the comment section of isLogEmptyV2Sync for possbile error codes
   * as they are slightly different from V1.
   *
   * Checks wether a particular log is empty. This method is blocking until the
   * state can be determined or an error occurred.
   *
   * @param logid is the ID of the log to check
   * @param empty will be set by this method to either true or false depending
   *        on the responses received by storage nodes.
   * @return 0 if the request was successful, -1 otherwise and sets
   *         logdevice::err to:
   *     INVALID_PARAM  if the log ID is invalid,
   *     PARTIAL        if we can't answer with certainty. The log is probably
   *                    either empty or almost empty, and any records are
   *                    probably not very recent.
   *     ACCESS         if permission to access the log was denied
   *     FAILED         if the state of the log could not be determined.
   *     NOBUFS         if too many requests are pending to be delivered to
   *                    Workers
   *     SHUTDOWN       Processor is shutting down
   *     INTERNAL       if attempt to write into the request pipe of a
   *                    Worker failed
   *     TIMEDOUT       None of the above happened before the client timeout
   *                    ran out.
   */
  virtual int isLogEmptySync(logid_t logid, bool* empty) noexcept = 0;

  /**
   * NOTE: This API call translates to isLogEmptyV2 call, if client setting
   * --enable-is-log-empty-v2 is set to true. If you intend to use this, please
   * refer to the comment section of isLogEmptyV2Sync for possbile error codes
   * as they are slightly different from V1.
   *
   * A non-blocking version of isLogEmptySync().
   *
   * @param logid is the ID of the log to check
   * @param cb will be called once the state of the log is determined or an
   *        error occurred. The possible status values are the same as for
   *        isLogEmptySync().
   * @return 0 if the request was successfuly scheduled, -1 otherwise.
   */
  virtual int isLogEmpty(logid_t logid, is_empty_callback_t cb) noexcept = 0;

  /**
   * Like isLogEmptySync, but instead of asking all the nodes, we ask the
   * sequencer to compare the trim point to the tail record of the log.
   *
   * @param logid is the ID of the log to check
   * @param empty will be set by this method to either true or false depending
   *        on the responses received by storage nodes.
   * @return 0 if the request was successful, -1 otherwise and sets
   *         logdevice::err to:
   *     INVALID_PARAM  if the log ID is a metadata log,
   *     NOSEQUENCER    if the log ID was not found and static sequencer
   *                    placement is used,
   *     NOTFOUND       if the log ID was not found and dynamic sequencer
   *                    placement is used,
   *     ACCESS         if permission to access the log was denied,
   *     AGAIN          if the sequencer is currently doing recovery, and thus
   *                    does not yet know what the tail record is,
   *     NOBUFS         if too many requests are pending to be delivered to
   *                    Workers,
   *     SHUTDOWN       Processor is shutting down,
   *     FAILED         if the sequencer node does not support isLogEmptyV2,
   *     INTERNAL       if attempt to write into the request pipe of a
   *                    Worker failed,
   *     TIMEDOUT       None of the above happened before the client timeout
   *                    ran out.
   */
  virtual int isLogEmptyV2Sync(logid_t logid, bool* empty) noexcept = 0;

  /**
   * A non-blocking version of isLogEmptyV2Sync().
   *
   * @param logid is the ID of the log to check
   * @param cb will be called once the state of the log is determined or an
   *        error occurred. The possible status values are the same as for
   *        isLogEmptyV2Sync().
   * @return 0 if the request was successfuly scheduled, -1 otherwise.
   */
  virtual int isLogEmptyV2(logid_t logid, is_empty_callback_t cb) noexcept = 0;

  /**
   * Finds the size of stored data for the given log in the given time range,
   * with accuracy as requested. Please note: this is post-batching and
   * compression; the size will likely be larger to a reader. This method is
   * blocking until the size has been determined or an error occurred.
   *
   * @param logid     ID of the log to examine
   * @param start     start of the range we want to find the size of
   * @param end       end of the range we want to find the size of
   * @param accuracy  specifies the desired accuracy; higher accuracy means
   *                  the operation will be slower and less efficient.
   *                  Accuracy can be:
   *  APPROXIMATE       In this case, uses findTime to get the LSN boundaries
   *                    of the range we want to find the size of. On any
   *                    partitions which are partially covered by this range,
   *                    dataSize will approximate the size usage by linear
   *                    interpolation. The size of partitions is affected
   *                    largely by partition-duration and partition-size-limit.
   * @return
   * Returns 0 and sets size appropriately if the operation succeeded; if an
   * error was encountered, returns -1 and sets err to one of the following:
   * - E::ACCESS   Permission to access the log was denied
   * - E::PARTIAL: Only part of the cluster responded and we only got an
   *   approximate answer. The size of the data in the range should be expected
   *   to be greater than the answer given.
   * - E::FAILED: No storage nodes responded, or another critical failure.
   * - E::SHUTDOWN: Client was destroyed while the request was processing.
   *
   * NOTE While this is a dummy API, possible err values are subject to change.
   */
  virtual int dataSizeSync(logid_t logid,
                           std::chrono::milliseconds start,
                           std::chrono::milliseconds end,
                           DataSizeAccuracy accuracy,
                           size_t* size) noexcept = 0;
  /**
   * NOTE: this is currently a dummy API that will always return 0;
   * please refrain from using other than for testing.
   *
   * A non-blocking version of dataSizeSync().
   * Calllback will be called with one of the error codes mentioned regarding
   * return in dataSizeSync, with the addition of E::OK for successful
   * requests.
   *
   * @return If the request was successfully submitted for processing, returns
   * 0. In that case, the supplied callback is guaranteed to be called at a
   * later time with the outcome of the request. See dataSizeSync() for
   * documentation of possible results. Otherwise, returns -1 with the error:
   * - E::NOBUFS: Too many requests were pending to be delivered to Workers.
   */
  virtual int dataSize(logid_t logid,
                       std::chrono::milliseconds start,
                       std::chrono::milliseconds end,
                       DataSizeAccuracy accuracy,
                       data_size_callback_t cb) noexcept = 0;

  /**
   * Return the sequence number that points to the tail of log `logid`. The
   * returned LSN is guaranteed to be higher or equal than the LSN of any record
   * that was successfully acknowledged as appended prior to this call.
   *
   * Note that there can be benign gaps in the numbering sequence of a log. As
   * such, it is not guaranteed that a record was assigned the returned
   * sequencer number.
   *
   * One can read the full content of a log by creating a reader to read from
   * LSN_OLDEST until the LSN returned by this method. Note that it is not
   * guaranteed that the full content of the log is immediately available for
   * reading.
   *
   * This method is blocking until the tail LSN could be determined, the timeout
   * occurs, or an error occurred. The timeout is specified in the
   * `ClientFactory::create()` method and can be overridden with `setTimeout()`.
   *
   * @param logid is the ID of the log for which to find the tail LSN;
   * @return tail LSN issued by the sequencer of log `logid` or LSN_INVALID on
   *              error and err is set to:
   *     E::TIMEDOUT    We could not get a reply from a sequencer in
   *                    time;
   *     E::CONNFAILED  Unable to reach a sequencer node;
   *     E::NOSEQUENCER Failed to determine which node runs the sequencer;
   *     E::FAILED      Sequencer activation failed for some other
   *                    reason e.g. due to E::SYSLIMIT, E::NOBUFS,
   *                    E::TOOMANY(too many activations), E::NOTFOUND(log-id not
   *                    found);
   *     E::NOBUFS      if too many requests are pending to be delivered to
   *                    Workers;
   *     E::SHUTDOWN    Processor is shutting down;
   *     E::INTERNAL    if attempt to write into the request pipe of a
   *                    Worker failed.
   *     E::ACCESS      The service denied access to this client based on
   *                    credentials presented
   */
  virtual lsn_t getTailLSNSync(logid_t logid) noexcept = 0;

  /**
   * A non-blocking version of getTailLSNSync().
   *
   * @param logid is the ID of the log for which to get the tail LSN
   * @param cb will be called once the tail LSN of the log is determined or an
   *           error occurred. The possible status values are the same as for
   *           getTailLSNSync().
   * @return 0 if the request was successfuly scheduled, -1 otherwise.
   */
  virtual int getTailLSN(logid_t logid,
                         get_tail_lsn_callback_t cb) noexcept = 0;

  /**
   * Return current attributes of the tail of the log by sending request to
   * sequencer.
   *
   * NOTE: Can fail with E::AGAIN error in healthy cluster if sequencer can not
   * determine result at this point.
   *
   * For an empty log getTailAttributesSync(log_id)->valid() may return false
   *
   * @param logid is the ID of the log for which to find the tail LSN;
   * @return            Pointer to current attributes of the tail of the log if
   *                    succeed or nullptr on error and err is set to:
   *     E::TIMEDOUT    We could not get a reply from a sequencer in time;
   *     E::CONNFAILED  Unable to reach a sequencer node;
   *     E::NOSEQUENCER Failed to determine which node runs the sequencer;
   *     E::FAILED      Sequencer activation failed for some other
   *                    reason e.g. due to E::SYSLIMIT, E::NOBUFS,
   *                    E::TOOMANY(too many activations), E::NOTFOUND(log-id not
   *                    found) or attributes values failed to be fetched for
   *                    because of internal failure;
   *     E::NOBUFS      if too many requests are pending to be delivered to
   *                    Workers;
   *     E::SHUTDOWN    Processor is shutting down;
   *     E::INTERNAL    if attempt to write into the request pipe of a
   *                    Worker failed.
   *     E::AGAIN       Sequencer can not determine result at this point.
   *     E::ACCESS      The service denied access to this client based on
   *                    credentials presented
   *
   * LogTailAttributes includes:
   *  last_released_real_lsn    Sequence number of last written and released for
   *                            delivery record of the log.
   *  last_timestamp  Estimated timestamp of record with last_released_real_lsn
   *                  sequence number. It may be slightly larger than real
   *                  timestamp of a record with last_released_real_lsn lsn.
   *  byte_offset     Amount of data in bytes written from the beginning of the
   *                  log up to the end.
   */
  virtual std::unique_ptr<LogTailAttributes>
  getTailAttributesSync(logid_t logid) noexcept = 0;

  /**
   * A non-blocking version of getTailAttributesSync().
   *
   * @param logid is the ID of the log for which to get the tail attributes
   * @param cb    will be called once the tail attributes of the log are
   *              determined or an error occurred. The possible status values
   *              are the same as for getTailAttributesSync().
   * @return 0 if the request was successfully scheduled, -1 otherwise.
   */
  virtual int getTailAttributes(logid_t logid,
                                get_tail_attributes_callback_t cb) noexcept = 0;

  /**
   * Return current attributes of the head of the log.
   * See LogHeadAttributes.h docs about possible head attributes.
   * The timestamp of the next record after trim point may be approximate.
   * It allows to make request efficient. The error of approximation is limited
   * by log append rate and should be negligible for high throughput logs.
   * See GetHeadAttributesRequest.h doc block for more Implementation details.
   *
   * @param logid is the ID of the log for which to get the tail attributes
   * @return      Pointer to log head attributes (see LogHeadAttributes.h docs)
   *              if request executed with status E::OK.
   *              Otherwise returns nullptr and sets logdevice::err to:
   *  E::TIMEDOUT       We could not get enough replies from a nodes in time.
   *  E::ACCESS         permission to access the log was denied
   *  E::INVALID_PARAM  Specified log is metadata log or invalid.
   *  E::SHUTDOWN:      Client was destroyed while the request was processing.
   *  E::FAILED         Request failed.
   */
  virtual std::unique_ptr<LogHeadAttributes>
  getHeadAttributesSync(logid_t logid) noexcept = 0;

  /**
   * A non-blocking version of getHeadAttributesSync().
   *
   * @param logid is the ID of the log for which to get the tail attributes
   * @param cb    will be called once the tail attributes of the log are
   *              determined or an error occurred. The possible status values
   *              are the same as for getHeadAttributesSync().
   * @return 0 if the request was successfully scheduled, -1 otherwise.
   */
  virtual int getHeadAttributes(logid_t logid,
                                get_head_attributes_callback_t cb) noexcept = 0;
  /**
   * Looks up the boundaries of a log range by its name as specified
   * in this Client's configuration.
   *
   * If configuration has a JSON object in the "logs" section with "name"
   * attribute @param name, returns the lowest
   * and highest log ids in the range.
   *
   * Note: This synchronous method may not be called from a callback of an async
   * LogDevice API. This is checked, asserted in debug builds, and causes
   * requests in release builds to fail. The reason is that we would already
   * be on an internal LogDevice thread, the request would need to be processed
   * by an internal LogDevice thread, and having one wait for another could
   * result in deadlock. Use the async version below.
   *
   * @return  If there's a range with name @param name, returns a pair
   *          containing the lowest and  highest log ids in the range
   *          (this may be the same id for log ranges of size 1).
   *          Otherwise returns a pair where both ids are set to LOGID_INVALID,
   *          and sets err to one of:
   *            * E::OK       Range found and set in the result
   *            * E::NOTFOUND Request succeeded, but a range with such name
   *                          was not found
   *            * E::FAILED   Request failed
   */
  virtual logid_range_t getLogRangeByName(const std::string& name) noexcept = 0;

  /**
   * An async variant of getLogRangeByName(). This can be called from async
   * LogDevice callbacks safely.
   *
   * @param cb    callback that will be called on an unspecified thread with
   *              the result. The callback can be called before or after the
   *              call to getLogRangeByName() finishes.
   */
  virtual void
  getLogRangeByName(const std::string& name,
                    get_log_range_by_name_callback_t cb) noexcept = 0;

  /**
   * Returns the character that delimits namespaces when specifying a nested
   * namespace hierarchy (see getLogRangesByNamespace()).
   */
  virtual std::string getLogNamespaceDelimiter() noexcept = 0;

  /**
   * Looks up the boundaries of all log ranges that have a "name" attribute
   * set and belong to the namespace @param ns. You can query nested namespaces
   * by concatenating their names with namespace delimiters as returned by
   * getLogNamespaceDelimiter() in between. E.g. if the namespace delimiter is
   * '/', you can submit 'ns1/ns2/ns3' as the @param ns here.
   *
   * Note: This synchronous method may not be called from a callback of an async
   * LogDevice API. This is checked, asserted in debug builds, and causes
   * requests in release builds to fail. The reason is that we would already
   * be on an internal LogDevice thread, the request would need to be processed
   * by an internal LogDevice thread, and having one wait for another could
   * result in deadlock. Use the async version below.
   *
   * @return  A map from log range name to a pair of the lowest and highest log
   *          ids in the range (this may be the same id for log ranges of size
   *          1). If empty, err contains an error code if the operation failed,
   *          or E::OK if the operation succeeded but there are no log ranges
   *          in the namespace.
   */
  virtual std::map<std::string, logid_range_t>
  getLogRangesByNamespace(const std::string& ns) noexcept = 0;

  /**
   * An async variant of getLogRangesByNamespace(). This can be called from
   * async LogDevice callbacks safely.
   *
   * @param cb    callback that will be called on an unspecified thread with
   *              the result. The callback can be called before or after the
   *              call to getLogRangesByNamespace() finishes.
   */
  virtual void getLogRangesByNamespace(
      const std::string& ns,
      get_log_ranges_by_namespace_callback_t cb) noexcept = 0;

  /**
   * Looks up metadata of a log group by its name as specified in this Client's
   * configuration.
   *
   * Note: This synchronous method may not be called from a callback of an async
   * LogDevice API. This is checked, asserted in debug builds, and causes
   * requests in release builds to fail. The reason is that we would already
   * be on an internal LogDevice thread, the request would need to be processed
   * by an internal LogDevice thread, and having one wait for another could
   * result in deadlock. Use the async version below.
   *
   * @return  If configuration has a log with "name" attribute @param name,
   *          returns the LogGroup object that contains the attributes for
   *          that entry.
   */
  virtual std::unique_ptr<client::LogGroup>
  getLogGroupSync(const std::string& path) noexcept = 0;

  /**
   * An async variant of getLogGroup(). This can be called from async
   * LogDevice callbacks safely.
   *
   * @param cb    callback that will be called on an unspecified thread with
   *              the result. The callback can be called before or after the
   *              call to getLogGroup() finishes.
   * @return 0 if the request was successfuly scheduled, -1 otherwise.
   */
  virtual void getLogGroup(const std::string& path,
                           get_log_group_callback_t cb) noexcept = 0;

  /**
   * Looks up metadata of a log group by its name as specified in this Client's
   * configuration.
   *
   * Note: This synchronous method may not be called from a callback of an async
   * LogDevice API. This is checked, asserted in debug builds, and causes
   * requests in release builds to fail. The reason is that we would already
   * be on an internal LogDevice thread, the request would need to be processed
   * by an internal LogDevice thread, and having one wait for another could
   * result in deadlock. Use the async version below.
   *
   * @return  If configuration has a log with log_id @param logid,
   *          returns the LogGroup object that contains the attributes for
   *          that entry.
   */
  virtual std::unique_ptr<client::LogGroup>
  getLogGroupByIdSync(const logid_t logid) noexcept = 0;

  /**
   * An async variant of getLogGroupByIdSync(). This can be called from async
   * LogDevice callbacks safely.
   *
   * @param cb    callback that will be called on an unspecified thread with
   *              the result. The callback can be called before or after the
   *              call to getLogGroupByIdSync() finishes.
   * @return 0 if the request was successfuly scheduled, -1 otherwise.
   *                           sets err to one of:
   *                                 E::NOTFOUND the log group for this id does
   *                                   not exist
   *                                 E::TIMEDOUT Operation timed out.
   */
  virtual void getLogGroupById(const logid_t logid,
                               get_log_group_callback_t cb) noexcept = 0;

  /**
   * Creates a new directory in LogsConfig.
   *
   * @param path                  The path of the directory you want to create.
   * @param mk_intermediate_dirs  creates the directories in the supplied path
   *                              if they don't exist.
   * @param attrs                 The attributes of the target directory.
   *
   * @return 0 if the request was successfuly scheduled, -1 otherwise.
   *                              sets err to one of:
   *                                 E::ACCESS you don't have permissions to
   *                                           mutate the logs configuration.
   *                                 E::EXISTS Directory already exists.
   *                                 E::NOTFOUND the parent directory does not
   *                                 exit
   *                                 E::TIMEDOUT Operation timed out.
   */
  virtual int makeDirectory(const std::string& path,
                            bool mk_intermediate_dirs,
                            const client::LogAttributes& attrs,
                            make_directory_callback_t cb) noexcept = 0;

  /**
   * blocking version of makeDirectory()
   * If failure_reason is not nullptr, it will be populated with a
   * human-readable error string if the operation failed.
   * @return the newly created Directory or nullptr. In case of nullptr the
   *                              err will be set like the async counterpart.
   */
  virtual std::unique_ptr<client::Directory> makeDirectorySync(
      const std::string& path,
      bool mk_intermediate_dirs = false,
      const client::LogAttributes& attrs = client::LogAttributes(),
      std::string* failure_reason = nullptr) noexcept = 0;

  /**
   * Remove a directory if it's empty:
   *
   *  @param path       The path of the directory you want to remove.
   *  @param recursive  Removes the directory recursively, If the supplied path
   *                    is the root directory, the full tree will be removed.
   * @return 0 if the request was successfuly scheduled, -1 otherwise.
   *                              sets err to one of:
   *                                 E::ACCESS you don't have permissions to
   *                                           mutate the logs configuration.
   *                                 E::TIMEDOUT Operation timed out.
   *                                 E::NOTFOUND the directory was not found
   *                                             and thus couldn't be deleted.
   */
  virtual int removeDirectory(const std::string& path,
                              bool recursive,
                              logsconfig_status_callback_t) noexcept = 0;

  /**
   * blocking version of removeDirectory()
   *
   * @param version: If not nullptr, gets populated with the version of the
   * logsconfig at which the directory got removed.
   *
   * @return true if removed, otherwise the err will be set like its async
   * counterpart.
   */
  virtual bool removeDirectorySync(const std::string& path,
                                   bool recursive = false,
                                   uint64_t* version = nullptr) noexcept = 0;

  /**
   * blocking version of removeLogGroup()
   *
   * @param version: If not nullptr, gets populated with the version of the
   * logsconfig at which the log group got removed.
   *
   * @return true if removed, otherwise err will be set as the async
   * counterpart.
   */
  virtual bool removeLogGroupSync(const std::string& path,
                                  uint64_t* version = nullptr) noexcept = 0;

  /**
   * Removes a logGroup defined at path
   *
   * @return 0 if the request was successfuly scheduled, -1 otherwise.
   *                              sets err to one of:
   *                                E::NOTFOUND it was not found
   *                                E::TIMEDOUT Operation timed out.
   *                                E::ACCESS you don't have permissions to
   *                                          mutate the logs configuration.
   */
  virtual int removeLogGroup(const std::string& path,
                             logsconfig_status_callback_t cb) noexcept = 0;

  /**
   * Rename the leaf of the supplied path. This does not move entities in the
   * tree it only renames the last token in the path supplies.
   *
   * The new path is the full path of the destination, it must not exist,
   * otherwise you will receive status of E::EXISTS
   * @param from_path   The source path to rename
   * @param to_path     The new path you are renaming to
   *
   * @return           0 if the request was successfuly scheduled, -1 otherwise.
   *                      sets err to one of:
   *                        E::INVALID_PARAM if paths are invalid, a common
   *                        example is that source or destination are the root
   *                        path. or that the source and destination are the
   *                        same.
   *                        E::NOTFOUND source path doesn't exist.
   *                        E::EXISTS the destination path already exists!
   *                        E::TIMEDOUT Operation timed out.
   *                        E::ACCESS you don't have permissions to
   *                                  mutate the logs configuration.
   */
  virtual int rename(const std::string& from_path,
                     const std::string& to_path,
                     logsconfig_status_callback_t cb) noexcept = 0;

  /**
   * blocking version of rename()
   * If failure_reason is not nullptr, it will be populated with a
   * human-readable error string if the operation failed.
   *
   * @param version: If not nullptr, gets populated with the version of the
   * logsconfig at which the path got renamed.
   *
   * Return true if rename was successful, otherwise err is set like the async
   * counterpart.
   */
  virtual bool renameSync(const std::string& from_path,
                          const std::string& to_path,
                          uint64_t* version = nullptr,
                          std::string* failure_reason = nullptr) noexcept = 0;

  /**
   * Creates a log group under a specific directory path.
   *
   * @param   mk_intermediate_dirs    creates the directories in the supplied
   *                                  path if they don't exist.
   * @return  0 if the request was successfuly scheduled, -1 otherwise.
   *                      sets err to one of:
   *                        E::ID_CLASH   the ID range clashes with existing
   *                                      log group.
   *                        E::INVALID_ATTRIBUTES After applying the parent
   *                                              attributes and the supplied
   *                                              attributes, the resulting
   *                                              attributes are not valid.
   *                        E::NOTFOUND source path doesn't exist.
   *                        E::NOTDIR if the parent of destination path
   *                                  doesn't exist and mk_intermediate_dirs is
   *                                  false.
   *                        E::EXISTS the destination path already exists!
   *                        E::TIMEDOUT Operation timed out.
   *                        E::ACCESS you don't have permissions to
   *                                  mutate the logs configuration.
   */
  virtual int makeLogGroup(const std::string& path,
                           const logid_range_t& range,
                           const client::LogAttributes& attrs,
                           bool mk_intermediate_dirs,
                           make_log_group_callback_t cb) noexcept = 0;
  /**
   * blocking version of makeLogGroup()
   * If failure_reason is not nullptr, it will be populated with a
   * human-readable error string if the operation failed.
   */
  virtual std::unique_ptr<client::LogGroup>
  makeLogGroupSync(const std::string& path,
                   const logid_range_t& range,
                   const client::LogAttributes& attrs = client::LogAttributes(),
                   bool mk_intermediate_dirs = false,
                   std::string* failure_reason = nullptr) noexcept = 0;

  /**
   * This sets either a LogGroup or LogsDirectory attributes to the supplied
   * attributes object. If the path refers to directory, all child directories
   * and log groups will be updated accordingly.
   *
   * @return  0 if the request was successfuly scheduled, -1 otherwise.
   *                      sets err to one of:
   *                        E::INVALID_ATTRIBUTES After applying the parent
   *                                              attributes and the supplied
   *                                              attributes, the resulting
   *                                              attributes are not valid.
   *                        E::NOTFOUND the path supplied doesn't exist.
   *                        E::TIMEDOUT Operation timed out.
   *                        E::ACCESS you don't have permissions to
   *                                  mutate the logs configuration.
   */
  virtual int setAttributes(const std::string& path,
                            const client::LogAttributes& attrs,
                            logsconfig_status_callback_t cb) noexcept = 0;

  /**
   * blocking version of setAttributes()
   *
   * If version is not nullptr, it will be populated with the version at which
   * the attributes were set.
   *
   * If failure_reason is not nullptr, it will be populated with a
   * human-readable error string if the operation failed.
   */
  virtual bool
  setAttributesSync(const std::string& path,
                    const client::LogAttributes& attrs,
                    uint64_t* version = nullptr,
                    std::string* failure_reason = nullptr) noexcept = 0;

  /**
   * This sets the log group range to the supplied new range.
   * @return 0 on success, -1 otherwise and sets `err` to:
   *                        E::ID_CLASH   the ID range clashes with existing
   *                                      log group.
   *                        E::NOTFOUND if the path doesn't exist or it's
   *                            pointing to a directory
   *                        E::INVALID_ATTRIBUTES the range you supplied is
   *                            invalid or reserved for system-logs.
   *                        E::TIMEDOUT Operation timed out.
   *                        E::ACCESS you don't have permissions to
   *                                  mutate the logs configuration.
   */
  virtual int setLogGroupRange(const std::string& path,
                               const logid_range_t& range,
                               logsconfig_status_callback_t) noexcept = 0;

  /**
   * blocking version of setLogGroupRange()
   *
   * If version is not nullptr, it will be populated with the version at which
   * the log group range was set.
   *
   * If failure_reason is not nullptr, it will be populated with a
   * human-readable error string if the operation failed.
   */
  virtual bool
  setLogGroupRangeSync(const std::string& path,
                       const logid_range_t& range,
                       uint64_t* version = nullptr,
                       std::string* failure_reason = nullptr) noexcept = 0;

  /**
   * Returns all directories and LogGroupNode(s) under this path. Note that this
   * will return the full tree if the dir_path equals the delimiter.
   * e.g, dir_path = "/" will return the root directory with all children,
   * recursively!
   */
  virtual int getDirectory(const std::string& path,
                           get_directory_callback_t) noexcept = 0;

  /**
   * blocking version of getDirectory()
   */
  virtual std::unique_ptr<client::Directory>
  getDirectorySync(const std::string& path) noexcept = 0;

  /**
   * This waits (blocks) until the LogsConfig satisfy the supplied predicate or
   * until the timeout has passed.
   *
   * @param version   The minimum version you need to sync LogsConfig to
   *
   * @return          true if successful false on timeout.
   *                  This  will set the `err` to E::TIMEDOUT.
   */
  virtual bool syncLogsConfigVersion(uint64_t version) noexcept = 0;

  /**
   * The callback will be called when the LogsConfig on this client has at
   * least the version passed to this function (first argument).
   *
   * @return          The returned subscription handle that the client needs to
   *                  hold for as long as the client is still interested in
   *                  this. If the client deleted that handle, the subscription
   *                  will be destroyed and the callback will not be called.
   */
  virtual ConfigSubscriptionHandle
  notifyOnLogsConfigVersion(uint64_t version,
                            std::function<void()>) noexcept = 0;

  /**
   * Exposes configuration attributes.
   *
   * @return ClusterAttributes object that contains attributes coming from the
   *         client's configuration
   */
  virtual std::unique_ptr<ClusterAttributes>
  getClusterAttributes() noexcept = 0;

  /**
   * Subscribes to notifications of configuration file updates.
   *
   * Whenever the LogDevice client library picks up a new config, it will call
   * the supplied callback on an unspecified thread after the new config is
   * loaded.
   *
   * @param cb  the callback to call
   *
   * @return    returns the subscription handle. The subscription will be valid
   *            as long as the handle exists. Subscription will cease and the
   *            callback will not be called after the handle is destroyed.
   */
  virtual ConfigSubscriptionHandle
      subscribeToConfigUpdates(config_update_callback_t) noexcept = 0;

  /**
   * @return  returns the maximum permitted payload size for this client. The
   *          default is 1MB, but this can be increased via changing the
   *          max-payload-size setting.
   */
  virtual size_t getMaxPayloadSize() noexcept = 0;

  /**
   * Exposes a ClientSettings instance that can be used to change settings
   * for the Client.
   */
  virtual ClientSettings& settings() = 0;

  /**
   * @return a string containing the state of all ClientReadStreams
   * running on workers owned by this client. Used for debugging
   */
  virtual std::string getAllReadStreamsDebugInfo() noexcept = 0;

  /**
   * Emit a user defined event to the event logging system.
   *
   * User events are intended to be used to for debugging and to make
   * failures or performance issues visible. User events are aggregated
   * with client library events so that customer visible issues can
   * be easily correlated with internal failures.
   *
   * NOTE: User events are rate limited to 10 events in every 10s.
   *
   * @param sev         Event Severity. Can be one of:
   *                    CRITICAL, ERROR, WARNING, NOTICE, INFO, or DEBUG.
   *
   * @param name_space  The name_space argument ensures overlapping types
   *                    allocated by different customers are not ambiguous.
   *                    Namespace identifiers that start with "LD_" are
   *                    reserved for internal use by the LogDevice library.
   *
   * @param type        String representation of an event type enumeration.
   *
   * @param data        Supporting information for the event type. Optional.
   *
   * @param context     Program context (e.g. stack trace) that may aid in
   *                    understanding the cause of the event. Optional.
   */
  virtual void publishEvent(Severity sev,
                            std::string name_space,
                            std::string type,
                            std::string data = "",
                            std::string context = "") noexcept = 0;

 private:
  Client(const Client&) = delete;            // non-copyable
  Client& operator=(const Client&) = delete; // non-assignable
};

}} // namespace facebook::logdevice
