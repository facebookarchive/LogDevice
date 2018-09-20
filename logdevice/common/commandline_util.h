/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <boost/program_options.hpp>

namespace facebook { namespace logdevice {

/**
 * @file Helper wrappers around boost::program_options.
 */

/**
 * Convenience wrapper for parsing `argv' based on a
 * `boost::program_options::options_description', with positional arguments
 * disallowed.
 *
 * Throws `boost::program_options::error' on error.
 */
boost::program_options::variables_map program_options_parse_no_positional(
    int argc,
    const char* argv[],
    const boost::program_options::options_description& desc) {
  namespace style = boost::program_options::command_line_style;
  // Empty positional_options_description, will throw if any are provided
  boost::program_options::positional_options_description positional;
  boost::program_options::command_line_parser parser(argc, argv);
  boost::program_options::variables_map parsed;
  boost::program_options::store(
      parser.options(desc)
          .positional(positional)
          .style(style::unix_style & ~style::allow_guessing)
          .run(),
      parsed);
  return parsed;
}

}} // namespace facebook::logdevice
