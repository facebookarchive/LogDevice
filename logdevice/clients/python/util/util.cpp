/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/clients/python/util/util.h"

using namespace boost::python;

gil_release_and_guard::gil_release_and_guard() {
  if (PyEval_ThreadsInitialized())
    state_ = PyEval_SaveThread();
  else
    state_ = nullptr;
}

gil_release_and_guard::~gil_release_and_guard() {
  if (state_ and PyEval_ThreadsInitialized()) {
    PyEval_RestoreThread(state_);
    state_ = nullptr;
  }
}

double extract_double(const object& from, const char* name, bool coerce) {
  if (PyFloat_Check(from.ptr())) {
    return PyFloat_AsDouble(from.ptr());
  }

  if (coerce) {
    if (PyInt_Check(from.ptr()) || PyLong_Check(from.ptr())) {
      return PyInt_AsLong(from.ptr());
    }
    if (PyBool_Check(from.ptr())) {
      return from.ptr() == Py_True ? 1 : 0;
    }
  }

  static const str msgend(" is not a float");
  static const str msgend_coerce(" is not a float, int, long, or bool");
  auto msg = str(name) + (coerce ? msgend_coerce : msgend);

  throw_python_exception(PyExc_TypeError, msg);
  throw std::runtime_error("unpossible, the line above always throws!");
}

std::string extract_string(const object& from, const char* name, bool coerce) {
  if (PyString_Check(from.ptr())) {
    return extract<std::string>(from)();
  }

  if (PyUnicode_Check(from.ptr())) {
    return extract<std::string>(from.attr("encode")("utf-8"))();
  }

  if (coerce) {
    if (PyInt_Check(from.ptr()) || PyLong_Check(from.ptr())) {
      return std::to_string(PyInt_AsLong(from.ptr()));
    }
    if (PyBool_Check(from.ptr())) {
      return from.ptr() == Py_True ? "true" : "false";
    }
    if (PyFloat_Check(from.ptr())) {
      return std::to_string(PyFloat_AsDouble(from.ptr()));
    }
  }

  static const str msgend(" is not a string or unicode object");
  static const str msgend_coerce(
      " is not a string, unicode, long, float or bool");
  auto msg = str(name) + (coerce ? msgend_coerce : msgend);

  throw_python_exception(PyExc_TypeError, msg);
  throw std::runtime_error("unpossible, the line above always throws!");
}

object createExceptionClass(const char* name,
                            const char* doc,
                            PyObject* baseClass) {
  std::string scopeName = extract<std::string>(scope().attr("__name__"));
  scopeName += ".";
  scopeName += name;
  char* qualifiedName = const_cast<char*>(scopeName.c_str());

  PyObject* rawClass;
  if (doc)
    rawClass = PyErr_NewExceptionWithDoc(
        qualifiedName, const_cast<char*>(doc), baseClass, 0);
  else
    rawClass = PyErr_NewException(qualifiedName, baseClass, 0);

  if (!rawClass)
    throw_error_already_set();

  auto handleForException = handle<>(borrowed(rawClass));
  auto exception = object(handleForException);

  // publish to the namespace
  scope().attr(name) = exception;

  // and this is a horrible hack that I regret: for some reason we have a
  // persistent crash in production, which I cannot track down in testing,
  // when we hit a LogDevice error under load.  the failure is the GC, used
  // for cyclic reference detection, hits an assertion that it discovered a
  // missing incref on the object.
  //
  // worse, everything *looks* to be correct.  so, lets hit it with the
  // biggest possible hammer, since this is our exception class and, honestly,
  // we don't have any reason to ever destroy it short of the end of the
  // process and all.
  //
  // technically this line is a refcount leak.
  incref(exception.ptr());
  return exception;
}

void throw_python_exception() {
  throw_error_already_set();
  // this will never be reached, because ^^^ that throws, after doing some
  // bookkeeping to ensure that error handling works right over the shared
  // library boundary.
  throw std::runtime_error("unpossible, the line above always throws!");
}

void throw_python_exception(PyObject* ex, const char* msg) {
  PyErr_SetString(ex, msg);
  throw_python_exception();
};

void throw_python_exception(PyObject* ex, PyObject* msg) {
  PyErr_SetObject(ex, msg);
  throw_python_exception();
};

void throw_python_exception(PyObject* ex, object msg) {
  throw_python_exception(ex, msg.ptr());
};

void throw_python_exception(object ex, object msg) {
  throw_python_exception(ex.ptr(), msg.ptr());
};
