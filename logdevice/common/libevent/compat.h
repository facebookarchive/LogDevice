/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

/**
 * @file This file must be included and the LD_EV() macro must be used
 * everywhere we call into libevent.
 */

#ifdef LOGDEVICE_LIBEVENT_SYMBOLS_PREFIXED
// Within FB we have a copy of libevent2 with function names prefixed with
// 'ld_' to allow linking with projects that still require libevent1
#define LD_EV(x) ld_##x
#else
// In the opensource build we just use regular libevent names
#define LD_EV(x) x
#endif
