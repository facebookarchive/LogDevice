/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>

#include <boost/python.hpp>
#include <boost/python/suite/indexing/vector_indexing_suite.hpp>
#include <boost/shared_ptr.hpp>
#include <folly/Demangle.h>

// PyString has been re-named to PyBytes in Python3.  This is a little
// hack to keep this code compatible with Python 2 and 3
#if PY_MAJOR_VERSION == 3
#define PyString_Check PyBytes_Check
#define PyInt_Check PyLong_Check
#define PyInt_AsLong PyLong_AsLong
#define NEXT_METHOD "__next__"
#else
#define NEXT_METHOD "next"
#endif

// unfortunately, boost::python *still* doesn't provide any native tools to
// handle the Python GIL, so here we are:
class gil_release_and_guard {
 public:
  explicit gil_release_and_guard();
  ~gil_release_and_guard();

 private:
  PyThreadState* state_;
};

// adapter to safely convert boost and std shared pointers, since
// boost::python only handles the boost version well today. :/
//
// based on http://stackoverflow.com/questions/12314967
template <typename T>
boost::shared_ptr<T>
make_boost_shared_ptr_from_std_shared_ptr(std::shared_ptr<T> ptr) {
  return boost::shared_ptr<T>(ptr.get(), [ptr](T*) mutable { ptr.reset(); });
}

// raise a LogDevice error (logdevice::err) as a Python exception
// throws, and never returns, ever.
[[noreturn]] void throw_logdevice_exception(
    boost::python::object extra = boost::python::object());

// If `coerce` is true, converts numeric and boolean types to string.
std::string extract_string(const boost::python::object& from,
                           const char* name,
                           bool coerce = false);
// If `coerce` is true, converts integer and boolean types to double
double extract_double(const boost::python::object& from,
                      const char* name,
                      bool coerce = false);

// convenience wrappers for throwing a python exception
[[noreturn]] void throw_python_exception();
[[noreturn]] void throw_python_exception(PyObject*, const char*);
[[noreturn]] void throw_python_exception(PyObject*, PyObject*);
[[noreturn]] void throw_python_exception(PyObject*, boost::python::object);
[[noreturn]] void throw_python_exception(boost::python::object,
                                         boost::python::object);

template <typename Out>
Out convert_or_throw(boost::python::object input, const std::string& key) {
  boost::python::extract<Out> typed_value(input);
  if (typed_value.check()) {
    return typed_value();
  }
  throw_python_exception(
      PyExc_ValueError,
      boost::python::str(
          "Invalid data type of key '" + key + "', expected type '" +
          folly::demangle(typeid(Out).name()).toStdString() + "'"));
  throw std::runtime_error("unpossible, the line above always throws!");
}

/**
 * Create a new Python Exception derived class, and return the object after
 * registering it in the current namespace.
 */
boost::python::object
createExceptionClass(const char* name,
                     const char* doc = nullptr,
                     PyObject* baseClass = PyExc_Exception);

// helper to wrap a logdevice Reader instance for return to Python, since we
// need to do some clever things to make it work cleanly.
namespace facebook { namespace logdevice {
class Reader;
}}; // namespace facebook::logdevice

boost::python::object
    wrap_logdevice_reader(std::unique_ptr<facebook::logdevice::Reader>);

template <typename T>
inline std::vector<T> to_std_vector(const boost::python::object& iterable) {
  return std::vector<T>(boost::python::stl_input_iterator<T>(iterable),
                        boost::python::stl_input_iterator<T>());
}

// multiple C++ modules in the single end object requires registration
void register_logdevice_reader();
void register_logdevice_record();
