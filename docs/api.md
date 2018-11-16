---
id: API_Intro
title: LogDevice API
sidebar_label: Introduction
---
## API basics

Clients communicate with a LogDevice cluster through a C++ API.

### `Client` class

The core of the LogDevice client API is very simple. In order to interact with a
LogDevice cluster, you need an instance of the `logdevice::Client` class. A
`logdevice::Client` object represents a connection to a specific LogDevice
cluster. It provides facilities for:
* appending records to logs,
* trimming logs all the way to a specified LSN,
* mapping timestamps (which are required record attributes) to the LSN of the
closest record in the log,
* creating `logdevice::Reader` objects that are used to read records from logs,
* other API calls.

You only need one instance of the class in order to interact with the cluster
(it will spawn several worker threads to enable it to scale), and all methods on
it are thread-safe.

`Client`s are created using a `ClientFactory`. A typical instantiation of the 
`Client` would look like this:
```c++
#include <logdevice/include/Client.h>

// ...

std::shared_ptr<facebook::logdevice::Client> client =
    facebook::logdevice::ClientFactory().create(
      // path to the config file
      "zk:10.0.0.1:2181,10.0.0.2:2181,10.0.0.3:2181/logdevice_test.conf");
```

### Synchronous vs. asynchronous API

Most API methods are available in synchronous or asynchronous flavours.
Generally using the asynchronous APIs scales better, as you can schedule many
outstanding requests to LogDevice without blocking your threads of execution.
However, note that if you use asynchronous methods:
1) the given callback functions will be called on an unspecified LogDevice
client worker thread, and
2) the callback function has to complete quickly. If it takes a long time to
complete, it will block the LogDevice client worker thread, which may delay the
execution of any other pending requests.

## Writing to LogDevice

### Vanilla low-latency writes

In order to write data to LogDevice, use the `append()` call on the `Client`
instance, like so:
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

Each record in this writing flow will get a different LSN.

### Buffered writes

The writing flow via the `append()` calls, as described above, is optimized for
lowest latency - whenever you call `append()`, your write will be sent to the
cluster as soon as possible. If instead you want to optimize for throughput, you
may want to buffer writes on the client. Take a look at
`logdevice/include/BufferedWriter.h` in the main source tree if you want to use
the buffered write API.

## Reading from LogDevice

The read API implemented by class `logdevice::Reader` follows the non-durable
subscription model. A single `Reader` object can be used to create and manage
multiple read streams. Those are roughly equivalent to POSIX file descriptors —
read points in a specific file. The difference between the `logdevice::Reader`
and POSIX read models is that the `Reader::read()` method returns records for
all active read streams on that `Reader` object, which often may belong to many
different logs. The records are passed to the application as they arrive from
the LogDevice cluster and become available. In contrast, the POSIX `read()`
call on a file descriptor may return data from that one file only.

In addition to records the `Reader::read()` method may also report a number of
gaps. A gap describes a range of LSNs with some sort of an exceptional
condition. Common gap types include TRIM (records in the range are older than
the trim point), DATALOSS (records in the range appear to be permanently lost),
and ACCESS (the reader does not have permissions to read the log). The
`Reader::stopReading()` method may be used to indicate that the client is no
longer interested in receiving records for the specified log.

If instead of pulling records you prefer them to be streamed to you and a
callback to be called for each record/gap received, you can use the
`AsyncReader` class instead.

It is worth noting that LogDevice does not offer durable read pointers. Log
readers that want to persist their position in the log so that they can resume
reading after a crash or restart should periodically save the LSN of the last
record they read in a persistent key-value store.

### Reading using the `Reader` class

To read from LogDevice using the `Reader` class, you:
1. create a `Reader` instance by calling the `createReader()` method on the
`Client` instance,
2. call `startReading()` on it for each log, and
3. call the `read()` method on the reader whenever your application is ready to
consume records

Check out `examples/tail.cpp` and `examples/cat.cpp` in the source tree to see
how that works.

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

## Discovering positions to read from/to

As LogDevice doesn't store read pointers for clients, the clients have to
specify explicit `start`/`until` LSNs when reading. There are several common
ways how these are discovered.

### Max bound values

Specifying `facebook::logdevice::LSN_OLDEST` as the `start_lsn` will cause
reading to start from the first possible LSN.

Specifying `facebook::logdevice::LSN_MAX` as the `until_lsn` will cause reading
to continue indefinitely (new records will be read as they are appended to the
tail and released).

Specifying both of these values covers the entire possible LSN space for a log.

### Checkpointed values

If your reader saves checkpoints (LSNs) somewhere based on records it has
already seen, these checkpoints can be used to restart reading from the same
point.

### Discover LSN based on append timestamp

A typical way to use LogDevice is to start reading from a certain timestamp. In
order to discover which LSN maps to that timestamp, the `findTime()` API call
can be used:

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

### Get the current tail LSN
Another common position to start/stop reading at is the tail of the log:
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

## Trimming data in LogDevice

There are several ways to configure LogDevice to trim data automatically: based
on time (see `backlog` attribute in [log configuration](log_configuration.md))
or available storage space (see `rocksdb-free-disk-space-threshold-low` and
`sbr-node-threshold` in [settings](settings.md)). Another way to trim data in a
log is for a client to do this explicitly. See `trim()/trimSync()` methods in
the `Client` class.
