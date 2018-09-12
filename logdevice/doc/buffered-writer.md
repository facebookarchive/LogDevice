## BufferedWriter

This file explains the implementation of `BufferedWriter`, a utility class that allows writes to be buffered and batched on the client in order to achieve higher throughput.

### Threading model

`BufferedWriter` methods are called on application threads but quickly hand over work to LogDevice worker threads, which do most of the heavy lifting.

Appends sent to `BufferedWriter` are sharded by log ID and distributed to LogDevice worker threads.  Pinning appends for a log to a thread makes code a lot simpler as all of the following is single-threaded: buffering logic, the sending of APPEND messages over the network, APPEND callbacks.  The main downside is that a thread may limit throughput on a single log.

Why run on LogDevice workers?  We would need an event base anyway for timed flushes.  Using LogDevice workers gives us one and reduces interthread communication for sending APPEND messages, handling replies from the server, retries...

### Class layout

- `BufferedWriter`.  The main client-facing interface.  Methods are called on application threads.  `BufferedWriter::append()` posts `Request`s to hand appends over to appropriate workers (hashing log IDs to workers).
- `BufferedWriterShard`.  Thin layer that contains all `BufferedWriterSingleLog` instances on one worker.
- `BufferedWriterSingleLog`.  Manages all appends for the same log.
- `BufferedWriteDecoder`.  Client-facing class for decoding batched writes into constituent appends.
