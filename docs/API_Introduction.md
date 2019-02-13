---
title: LogDevice API
sidebar_label: Introduction
---
## API basics

To communicate with a LogDevice cluster, use the C++ client library.

### `Client` class

To interact with a LogDevice cluster, create an instance of the `logdevice::Client` class.
A `logdevice::Client` object represents a connection to a specific LogDevice cluster.
Some of the actions that you can take with a `Client` object are:
* Appending records to logs.
* Creating `logdevice::Reader` objects that are used to read records from logs.
* Mapping timestamps (which are required record attributes) to the sequence number (LSN) of the
closest record in the log.
* Trimming logs all the way to a specified LSN.

You only need one instance of the class in order to interact with the cluster. It
will spawn several worker threads to enable it to scale. All `Client` methods are thread-safe.

Create a `Client` using a `ClientFactory`.
```c++
#include <logdevice/include/Client.h>

// ...

std::shared_ptr<facebook::logdevice::Client> client =
    facebook::logdevice::ClientFactory().create(
      // path to the config file
      "zk:10.0.0.1:2181,10.0.0.2:2181,10.0.0.3:2181/logdevice_test.conf");
```

### Synchronous vs. asynchronous API

Most `Client` API methods are available in synchronous or asynchronous flavors.
Generally speaking, using the asynchronous methods scales better, as you can schedule many
outstanding requests to LogDevice without blocking your threads of execution.

If you use asynchronous methods, keep in mind that:
* The given callback functions are called on an unspecified LogDevice
client worker thread.
* The callback function has to complete quickly. If it takes a long time to
complete, it will block the LogDevice client worker thread, which may delay the
execution of any other pending requests.

## Writing Records

When writing to the cluster, you can optimize for latency or for throughput.
`Client::append()` sends the record to the cluster immediately, and each record is assigned its own LSN.
Because there is a cost to process each append inside LogDevice,
sending many small records limits throughput.
Unless you definitely do not want LogDevice to perform batching, or if you want
to have the lowest latency possible, use `BufferedWriter::append()`.

### Buffered writes for higher throughput (Preferred)

`BufferedWriter` maintains buffers of unsent writes for each log on the client.
It sends each batch of writes to LogDevice to be stored as one larger record with one LSN.
A batch is sent to the cluster when the time since starting the current batch, or the number of bytes in the batch, exceeds a threshold.
The records are automatically decoded on the read path by the `Reader` or `AsyncReader` object.

When the append of each batch of data is completed, the application is notified via a callback
interface.
Because BufferedWriter is meant for high-throughput writing, the callback interface
does not use `std::function`.
Instead, when you create the BufferedWriter object, you pass it a single subclass of `AppendCallback`.

You can, optionally, pass a pointer to a piece of context with each append call.
This context is returned on the callback in the `ContextSet` vector.
Some applications use this context to include state or tracking information for each record.

```c++
// this class is called when the cluster reports the status of the append
class BufferedWriterCallback : public BufferedWriter::AppendCallback {
 public:
  void onSuccess(logid_t /* unused */,
                 ContextSet contexts,
                 const DataRecordAttributes& /* unused */) override {
    std::cout << "BufferedWriterCallback: a batch of " << contexts.size()
              << " records successfully written ." << '\n';
    }

  // BufferedWriter exhausted all retries it was configured to do.
  void onFailure(logid_t log_id, ContextSet contexts, Status status) override {

    // Handle the error if needed. For example, collect the
    // failed payloads to retry them later.
    /*
    std::lock_guard<std::mutex> guard(mutex);
    for (auto& ctx : contexts) {
      payloadsFailed.push_back(std::move(ctx.second));
      }
      */
    }
    // ...
};

facebook::logdevice::logid_t logid(1);
facebook::logdevice::BufferedWriter::Options options;
BufferedWriterCallback cb;
std::unique_ptr<facebook::logdevice::BufferedWriter> buffered_writer;
buffered_writer = facebook::logdevice::BufferedWriter::create(client, &cb, options);

int buffered = 0;
for (int record_idx = 0; record_idx < 15000; ++record_idx) {
  int error = buffered_writer->append(
      logid,
      std::string("payload " + std::to_string(record_idx)),
      /* context */ nullptr);
  if (error) {
    // Insert error handling. For example, try again at least once.
    // Payload remains in std::string.
    std::cerr << "BufferedWriter->append() failed. "
              << facebook::logdevice::error_description(
                     facebook::logdevice::err)
              << '\n';
  } else {
    ++buffered;
  }
}

```
### Lifetime of payloads
To avoid copying payloads too much, BufferedWriter takes advantage of
std::string's move constructor. BufferedWriter::append() takes the payload as
`std::string&&` and moves it into BufferedWriter.
When it calls the append callback, BufferedWriter doesn't need the payloads anymore.
It passes payloads to the callback, and your application can steal those strings.
If `destroy_payloads` is set to true, BufferedWriter destroys the payloads as soon as they're not needed.

### BufferedWriter options
As you make a series of asynchronous appends, one or more might fail.
If you need the records to be written in order, set the `mode` option so that LogDevice waits for each batch
to be appended successfully before sending the next batch.
```c++
// Only allow one batch at a time to be inflight to LogDevice servers.
options.mode = BufferedWriter::Options::Mode::ONE_AT_A_TIME;
```
Be sure to set a time- or memory-based trigger.
The default time trigger is infinity.
The default size trigger is the maximum allowed record size (max-payload-size setting).
```c++
// Writes for this log are flushed once the oldest has been
// buffered for 1000 ms
options.time_trigger = std::chrono::milliseconds(1000);
```

### BufferedWriter example
See `examples/buffered_writer.cpp` in the source tree for an example of how to
use the `BufferedWriter` API.

### Unbatched writes

To write data with the lowest possible latency, call `append()` on the `Client` instance,
passing a std::function callback.
The callback is invoked when the append completes, whether it is successful or not.
```c++

void append_callback(facebook::logdevice::Status st,
             const facebook::logdevice::DataRecord& r) {
  std::cout << "Append result: ";
  if (st == facebook::logdevice::E::OK){
    std::cout << "succeeded, record appended to log " << r.logid.val()
              << ", LSN " << facebook::logdevice::lsn_to_string(r.attrs.lsn);
  } else {
    std::cout << "failed with error: "
              << facebook::logdevice::error_description(st);
  }
  std::cout << std::endl;
}
// ...

facebook::logdevice::logid_t logid(1);
std::string payload("data");

int rv = client->append(logid, payload, &append_callback);
if (rv != 0) {
  std::cout << "Failed to enqueue append to log " << logid.val() << ": "
      << facebook::logdevice::error_description(facebook::logdevice::err);
}
```

Each record in this writing flow is assigned a unique sequence number (LSN).

See `examples/write.cpp` in the source tree for a complete example of how to use the `append()` API.


## Reading from LogDevice

The read API implemented by the `logdevice::Reader` and `logdevice::AsyncReader` classes follows the non-durable
subscription model. A single `Reader` or `AsyncReader` object can be used to create and manage
multiple read streams. Those are roughly equivalent to POSIX file descriptors —
read points in a specific file.

The difference between the `logdevice`
and POSIX read models is that the `read()` method returns records for
all active read streams on that `Reader` or `AsyncReader` object, which often belong to many
different logs. The records are passed to the application as they arrive from
the LogDevice cluster and become available. In contrast, the POSIX `read()`
call on a file descriptor returns data from that one file only.

In addition to records, the `read()` method may also report a number of
gaps. A gap describes a range of LSNs with some sort of an exceptional
condition. Common gap types include:
* TRIM - records in the range are older than
the trim point.
* BRIDGE - a bridge that completes an epoch. This could be the result of a sequencer failover or log reconfiguration, but no data was lost.
* DATALOSS - records in the range appear to be permanently lost.
* ACCESS - the reader does not have permissions to read the log.

The `Reader::stopReading()` method may be used to indicate that the client is no
longer interested in receiving records for the specified log.

If you prefer to stream records rather than pulling them, define a
callback to be called for each record or gap, and use the
`AsyncReader` class instead.

LogDevice does not offer durable read pointers. Log
readers that want to retain their position in the log, so that they can resume
reading after a crash or restart, should periodically save the LSN of the last
record they read in a persistent key-value store.

### Reading using the `Reader` class

To read from LogDevice using the `Reader` class:
1. Create a `Reader` instance by calling the `createReader()` method on the
`Client` instance.
2. Call `startReading()` on it for each log.
3. Call the `read()` method on the reader whenever your application is ready to
consume records.

Check out `examples/tail.cpp` and `examples/cat.cpp` in the source tree for sample code.

### Reading using the `AsyncReader` class

`AsyncReader` allows you to register callbacks for each record/gap received, as
well as a callback for when the `until_lsn` has been reached.

Example:
```c++
#include <logdevice/include/AsyncReader.h>

// ...

bool recordCallback(std::unique_ptr<facebook::logdevice::DataRecord>& record) {
  std::cout << "Received record for log " << record->logid.val() << ": LSN "
      << facebook::logdevice::lsn_to_string(record->attrs.lsn)
      << ", payload \"" << record->payload.toString() << "\"" << std::endl;
}

bool gapCallback(const facebook::logdevice::GapRecord& gap) {
  using facebook::logdevice::GapType;
  switch (gap.type) {
    case GapType::BRIDGE:
    case GapType::HOLE:
    case GapType::TRIM:
    case GapType::FILTERED_OUT:
      // benign gaps in LSN numbering sequence
      break;
    case GapType::DATALOSS:
      std::cout << "Error! Data has been lost for log " << gap.logid.val()
          << " from LSN " << facebook::logdevice::lsn_to_string(gap.lo)
          << " to LSN " << facebook::logdevice::lsn_to_string(gap.hi)
          << std::endl;
      break;
    case GapType::ACCESS:
      std::cout << "Error! Access denied to log " << gap.logid.val()
          << std::endl;
      break;
    case GapType::NOTINCONFIG:
      std::cout << "Error! Log " << gap.logid.val() << " not found!"
          << std::endl;
      break;
    default:
      std::cout << "Unrecognized gap of type "
          << facebook::logdevice::gapTypeToString(gap.type) << " for log "
          << gap.logid.val() << " from LSN "
          << facebook::logdevice::lsn_to_string(gap.lo) << " to LSN "
          << facebook::logdevice::lsn_to_string(gap.hi) << std::endl;
      break;
  }
}

void doneCallback(facebook::logdevice::logid_t log_id) {
  std::cout << "Finished reading log_id " << log_id.val() << std::endl;
}

std::unique_ptr<facebook::logdevice::AsyncReader> reader =
    client->createAsyncReader();
reader->setRecordCallback(recordCallback);
reader->setGapCallback(gapCallback);
reader->setDoneCallback(doneCallback);

facebook::logdevice::logid_t logid(1);
facebook::logdevice::lsn_t from_lsn = facebook::logdevice::LSN_OLDEST;

int rv = reader->startReading(logid, from_lsn);
if (rv != 0) {
  std::cout << "Error! Couldn't start reading: "
      << facebook::logdevice::error_description(facebook::logdevice::err)
      << std::endl;
}
```

## Discovering positions to read

As LogDevice doesn't store read pointers for clients, the clients have to
specify explicit `start` and `until` LSNs when reading. There are several
ways to discover the LSNs of interest.

### Maximum bound values

Specify `facebook::logdevice::LSN_OLDEST` as the `start_lsn` to start
reading from the first possible LSN.

Specify `facebook::logdevice::LSN_MAX` as the `until_lsn` to read indefinitely.
New records will be read as they are appended to the
tail and released.

Specify both of these values to cover the entire possible LSN space for a log.

### Checkpointed values

If your reader saves checkpoints (LSNs) somewhere based on records it has
already seen, these checkpoints can be used to restart reading from the same
point.

### LSN based on append timestamp

A typical way to use LogDevice is to start reading from a certain timestamp.
To discover which LSN corresponds to that timestamp, use the `findTime()` API call:

```c++
void findTimeCallback(facebook::logdevice::Status st,
                   facebook::logdevice::lsn_t result) {
  if (st == facebook::logdevice::Status::OK) {
    std::cout << "Received findtime result: "
              << facebook::logdevice::lsn_to_string(result) << std::endl;
  } else {
    std::cout << "Error returned by findtime: "
              << facebook::logdevice::error_description(st) << std::endl;
  }
}

facebook::logdevice::logid_t logid(1);
// Find LSN for records written 30 seconds ago
std::chrono::milliseconds timestamp(
 std::chrono::duration_cast<std::chrono::milliseconds>(
   std::chrono::system_clock::now().time_since_epoch() -
       std::chrono::seconds(30)));
int rv = client->findTime(logid, timestamp, findTimeCallback);
if (rv != 0) {
  std::cout << "Couldn't run findtime: "
      << facebook::logdevice::error_description(facebook::logdevice::err)
      << std::endl;
}

```

**Note:** this feature assumes that clocks between all sequencer nodes in a
LogDevice cluster are in sync. If the clocks on machines in your cluster diverge
by a lot, the results of `findTime()` calls may be inconsistent.

### Current tail LSN
To start or stop reading at the tail of the log, determine the LSN of the last record using the `getTailLSN()` method.
```c++
void tailLSNCallback(facebook::logdevice::Status st,
                  facebook::logdevice::lsn_t result) {
  if (st == facebook::logdevice::Status::OK) {
    std::cout << "Received tail LSN: "
              << facebook::logdevice::lsn_to_string(result) << std::endl;
  } else {
    std::cout << "Error fetching tail LSN: "
              << facebook::logdevice::error_description(st) << std::endl;
  }
}

facebook::logdevice::logid_t logid(1);
int rv = client->getTailLSN(logid, tailLSNCallback);
if (rv != 0) {
  std::cout << "Couldn't run getTailLSN(): "
            << facebook::logdevice::error_description(facebook::logdevice::err)
            << std::endl;
}

```

## Trimming data from logs

You can configure LogDevice to trim data automatically:
* Based on time (see `backlog` attribute in [log configuration](log_configuration.md)).
* Based on available storage space (see `rocksdb-free-disk-space-threshold-low` and
`sbr-node-threshold` in [settings](settings.md)).

A client can also trim data explicitly. See the `trim()` and `trimSync()` methods in
the `Client` class.

## Running the samples

To run the samples, follow the instructions in [Running a local cluster](localcluster.md)
to start a local cluster. Next, create a log range using `ldshell logs create`.
Run the sample with `--help` to make it display a list of its options.
