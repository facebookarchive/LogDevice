/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <boost/program_options.hpp>

#include "logdevice/include/BufferedWriter.h"

namespace facebook { namespace logdevice {

// Populates `po` with descriptions of options in `opts`. Option names will be
// prefixed with `prefix`, e.g. if prefix is "buffered-writer-", one of the
// options will be called "--buffered-writer-size-trigger".
// Uses the current values in *opts as default values.
void describeBufferedWriterOptions(
    boost::program_options::options_description& po,
    BufferedWriter::Options* opts,
    std::string prefix = "buffered-writer-");

}} // namespace facebook::logdevice
