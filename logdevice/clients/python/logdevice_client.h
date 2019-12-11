/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once
#include <boost/python.hpp>

// raise a LogDevice error (logdevice::err) as a Python exception
// throws, and never returns, ever.
[[noreturn]] void throw_logdevice_exception(
    boost::python::object extra = boost::python::object());

// helper to wrap a logdevice Reader instance for return to Python, since we
// need to do some clever things to make it work cleanly.
namespace facebook { namespace logdevice {
class Reader;
}}; // namespace facebook::logdevice

boost::python::object
    wrap_logdevice_reader(std::unique_ptr<facebook::logdevice::Reader>);

// multiple C++ modules in the single end object requires registration
void register_logdevice_reader();
void register_logdevice_record();
