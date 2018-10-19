/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>

#include <boost/make_shared.hpp>

#include "logdevice/clients/python/util/util.h"
#include "logdevice/include/Client.h"

using namespace boost::python;
using namespace facebook::logdevice;

// A class that implements the Python iterator interface, and treats the
// LogDevice Reader.read method as an infinite iterator -- returning one
// record at a time, from the internal buffer that the Reader already
// contains, and returns it.
//
// This has some inter-thread communication overhead, so we might need to do a
// little less calling into Reader, and a bit more "serve out of our own
// buffer", to reduce that overhead -- since we can be non-locking in here,
// while they have to be locking.
class ReaderWrapper : boost::noncopyable {
 public:
  explicit ReaderWrapper(std::unique_ptr<Reader> reader)
      : reader_(std::move(reader)), keep_reading_(true) {
    // this determines the minimum delay to shut down reading from outside,
    // once LogDevice have a method to break out (thread-)safely from
    // iteration, this can go away in favour of not emulating their timeout.
    auto timeout = std::chrono::milliseconds(1000);
    reader_->setTimeout(timeout);
  };

  /**
   * Return the next (DataRecord, GapRecord) pair, in which exactly one of the
   * two is not None.
   *
   * Ownership of a DataRecord or GapRecord is handed to Python in a
   * boost::shared_ptr, which will arrange to destroy it when the Python
   * object wrapper goes out of scope and is garbage collected.
   */
  boost::python::tuple next() {
    std::vector<std::unique_ptr<DataRecord>> record;
    GapRecord gap;

    while (keep_reading_ && reader_->isReadingAny()) {
      // check for signals using Python layer, and raise if one was found;
      // the PyErr_CheckSignals function sets the error indicator.
      if (PyErr_CheckSignals() != 0)
        throw_python_exception();

      // read one record, which is a blocking operation, so ensure that we
      // don't hold the Python GIL while we do it.  we reacquire it at the end
      // of the read because we are doing things (like Python exception
      // handling) that require the lock.
      ssize_t n = 0;
      {
        gil_release_and_guard guard;
        n = reader_->read(1, &record, &gap);
      }

      if (n < 0) {
        if (err == E::GAP) {
          return boost::python::make_tuple(object(), // DataRecord is None
                                           boost::make_shared<GapRecord>(gap));
        }

        throw_logdevice_exception();
        throw std::runtime_error("unpossible, the line above always throws!");
      }

      if (n > 0) {
        // Got a data record
        return boost::python::make_tuple(
            // TODO boost::shared_ptr(std::unique_ptr) constructor requires
            // Boost >= 1.57 which not all of our deps are ready for.
            // boost::shared_ptr<DataRecord>(std::move(record[0])),
            boost::shared_ptr<DataRecord>(record[0].release()),
            object() // GapRecord is None
        );
      }

      // no records found in our logs before the timeout, so we exited
      // just to check if we should terminate iteration early, and then
      // go back to waiting.
    }

    // this is how you tell Python we ran out of things to do.
    throw_python_exception(PyExc_StopIteration, "No more data.");
    throw std::runtime_error("unpossible, the line above always throws!");
  }

  bool stop_iteration() {
    keep_reading_ = false;
    return true; // yes, we did stop as you requested
  }

  bool start_reading(logid_t logid, lsn_t from, lsn_t until = LSN_MAX) {
    if (reader_->startReading(logid, from, until) == 0)
      return true;
    throw_logdevice_exception();
    throw std::runtime_error("unpossible, the line above always throws!");
  }

  bool stop_reading(logid_t logid) {
    if (reader_->stopReading(logid) == 0)
      return true;
    throw_logdevice_exception();
    throw std::runtime_error("unpossible, the line above always throws!");
  }

  bool is_connection_healthy(logid_t logid) {
    switch (reader_->isConnectionHealthy(logid)) {
      case 1:
        return true;
      case 0:
        return false;
      default:
        throw_logdevice_exception();
        throw std::runtime_error("unpossible, the line above always throws!");
    };
  }

  bool without_payload() {
    reader_->withoutPayload();
    return true;
  }

 private:
  // our reader
  std::unique_ptr<Reader> reader_;

  // should we break out from reading?
  std::atomic<bool> keep_reading_;
};

// helper function to wrap a reader for return to Python -- helps hide the
// class, which nobody else needs to know about
object wrap_logdevice_reader(std::unique_ptr<Reader> reader) {
  // boost::shared_ptr is used because boost::python understands it, but does
  // not understand std::shared_ptr.
  auto wrapper =
      boost::shared_ptr<ReaderWrapper>(new ReaderWrapper(std::move(reader)));
  return object(wrapper);
}

// The Python iterator protocol requires that we return ourself from a method
// call; this simply returns the existing Python object, which allows that
// to work.
object identity(object const& o) {
  return o;
}

// Generate overloads to handle the optional argument to start_reading()
#pragma GCC diagnostic ignored "-Wunused-local-typedefs"
BOOST_PYTHON_MEMBER_FUNCTION_OVERLOADS(start_reading_overloads,
                                       ReaderWrapper::start_reading,
                                       2,
                                       3)

void register_logdevice_reader() {
  class_<ReaderWrapper, boost::shared_ptr<ReaderWrapper>, boost::noncopyable>(
      "Reader",
      R"DOC(
A LogDevice reader, able to read from one or more logs and return records from
them.  If reading more than one log there is no assurance of sequencing
between logs, but records in any individual log will be strictly ordered.

The reader yields (data, gap) pairs.  In each pair, exactly one of the two is
not None.  If a data record is available, the first element is a DataRecord
instance.  Otherwise, there is a gap in the numbering sequence; the second
element is a GapRecord instance.

This class is NOT thread safe, and you MUST NOT call any method on it from
another thread.  This includes calling any method while iterating on log
entries with the read() method!
)DOC",
      no_init)

      .def("__iter__", &identity)

      .def(NEXT_METHOD,
           &ReaderWrapper::next,
           R"DOC(
Implement the 'next' operation from the Python iterator protocol.

This will read until the 'stop_iteration()' method is called
from Python, or a record (data or gap) can be returned.
)DOC")

      .def("stop_iteration",
           &ReaderWrapper::stop_iteration,
           "Stop iteration immediately, breaking out of any wait.\n"
           "This is thread-safe.")

      .def("start_reading",
           &ReaderWrapper::start_reading,
           start_reading_overloads(args("logid", "from", "until"),
                                   "Start reading log LOGID from LSN FROM "
                                   "through LSN UNTIL (optional)"))

      .def("stop_reading",
           &ReaderWrapper::stop_reading,
           R"DOC(Stop reading log LOGID.

WARNING: This is not suitable for breaking out early from reading
on the log!  This will cause internal corruption and SEVs.
)DOC")

      .def("is_connection_healthy",
           &ReaderWrapper::is_connection_healthy,
           R"DOC(
Checks if the connection to the LogDevice cluster for a log appears
healthy.  When a read() call times out, this can be used to make an
informed guess whether this is because there is no data or because there
a service interruption.

NOTE: this is not 100% accurate but will expose common issues like losing
network connectivity.

This returns True if everything looked healthy when checked, or False if
something went wrong talking to the cluster.

It can also raise an exception if something terrible goes wrong during
the process of checking.
)DOC")

      // TODO: danielp 2015-03-17: disabled until LogDevice has a native "break
      // out
      // of read early" function that is thread-safe, since we use timeout
      // internally.  If this is actually needed in code we can emulate it, but
      // until then lets disable it but keep the code present.
      //
      //     .def("set_timeout", [](ReaderWrapper &self, double seconds) {
      //         auto ts = std::chrono::milliseconds(
      //           // support -1 == unlimited convention
      //           seconds == -1 ? -1 : lround(seconds * 1000)
      //         );
      //         if (self.reader->setTimeout(ts) == 0)
      //           return true;
      //         throw_logdevice_exception(); // TODO: danielp 2015-03-13: I
      //         assume
      //                                      // this is set when this failure
      //                                      // happens, because it is for
      //                                      // everything else.
      //       },
      //       R"DOC(
      // Sets the limit on how long the iterator returned from read() may wait
      // for
      // records to become available.  A timeout of -1 means no limit (infinite
      // timeout).  A timeout of 0 means no waiting (nonblocking reads).
      //
      // When the timeout is hit without new records being returned the iterator
      // will reach the 'end' of the iteration, and the Python for loop will
      // return.
      //
      // If you set an infinite timeout there is no way to break out early from
      // reading, so be careful about your decisions.
      // )DOC"
      //       )

      .def("without_payload",
           &ReaderWrapper::without_payload,
           R"DOC(
If called, data records read by this Reader will not include payloads.

This makes reading more efficient when payloads are not needed (they won't
be transmitted over the network).
)DOC");
}
