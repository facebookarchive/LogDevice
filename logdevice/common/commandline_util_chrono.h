/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
// override-include-guard
#ifdef LOGDEVICE_COMMANDLINE_UTIL_CHRONO_H
// Because the implementation pollutes the std namespace in a questionable
// way, this file should only be included directly in .cpp files that need it.
#error This file may be included at most once in a translation unit (.cpp).
#endif
#define LOGDEVICE_COMMANDLINE_UTIL_CHRONO_H

#include <boost/program_options.hpp>

#include "logdevice/common/util.h"

namespace facebook { namespace logdevice {

/**
 * Utility that parses time amounts with units.
 *
 * Regardless of the granularity of DurationClass, different units are
 * accepted and converted if necessary.  For example, all these are acceptable
 * on the command line:
 *   --param 100ms
 *   --param '100 ms'
 *   --param 1s
 *   --param 0.1s
 *   --param 1min
 *   --param 0.5hr
 *
 * Recognized suffixes: ns, us, ms, s, min, mins, h, hr, hrs, d, day, days.
 *
 * Parsed values are truncated if storing into a coarser unit, e.g. "1500ms"
 * gets parsed as 1 second.  As a special case, if the value would truncate to
 * zero (e.g. "1ms" as seconds), parsing will fail.
 *
 * min and max values for the DurationClass can be passed:
 *   --param min
 *   --param max
 *
 * @return On success, returns 0.  On failure, returns -1 and sets err to one of
 *           TRUNCATED      result would get truncated to zero
 *           INVALID_PARAM  all other errors
 */
template <class DurationClass>
int parse_chrono_string(const std::string& str, DurationClass* duration_out);

/**
 * Formats duration as number with units. Picks the biggest unit by which
 * duration is divisible. E.g. "1d", "25h", "3600001ms".
 * The returned value is parseable by parse_chrono_string().
 */
template <class DurationClass>
std::string format_chrono_string(DurationClass duration);

/**
 * Has a similar effect as calling
 *
 *   boost::program_options::value<DurationClass>(&value)
 *
 * If `default_value' is true, the pointed-to value is set as the default
 * value (it is tricky for the caller to do this because the value needs to be
 * formatted as a string).
 *
 * Uses parse_chrono_string() to parse the parameter into a
 * std::chrono::duration class.
 */
template <class DurationClass>
boost::program_options::typed_value<DurationClass>*
chrono_value(DurationClass* duration_out, bool default_value = true);

template <class DurationClass>
boost::program_options::typed_value<chrono_interval_t<DurationClass>>*
chrono_interval_value(chrono_interval_t<DurationClass>* interval_out,
                      bool default_value = true);

template <class DurationClass>
boost::program_options::typed_value<chrono_expbackoff_t<DurationClass>>*
chrono_expbackoff_value(chrono_expbackoff_t<DurationClass>* out,
                        bool default_value = true);

/**
 * Helper function that converts a std::chrono::duration argument to string
 * (with a suffix that depends on the type).
 */
template <class DurationClass>
std::string chrono_string(DurationClass duration);

}} // namespace facebook::logdevice

#include "commandline_util_chrono-inl.h"
