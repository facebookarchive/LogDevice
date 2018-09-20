/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

// RSM logging that prepends the RSMType in messages

#define rsm_log(l, rsm_type, f, args...)  \
  ld_log_impl(__FILE__,                   \
              __FUNCTION__,               \
              __LINE__,                   \
              l,                          \
              "[%s] " f,                  \
              toString(rsm_type).c_str(), \
              ##args)

#define rsm_critical(rsm_type, f, args...) \
  rsm_log(facebook::logdevice::dbg::Level::CRITICAL, rsm_type, f, ##args)
#define rsm_error(rsm_type, f, args...) \
  rsm_log(facebook::logdevice::dbg::Level::ERROR, rsm_type, f, ##args)
#define rsm_warning(rsm_type, f, args...) \
  rsm_log(facebook::logdevice::dbg::Level::WARNING, rsm_type, f, ##args)
#define rsm_notify(rsm_type, f, args...) \
  rsm_log(facebook::logdevice::dbg::Level::NOTIFY, rsm_type, f, ##args)
#define rsm_info(rsm_type, f, args...) \
  rsm_log(facebook::logdevice::dbg::Level::INFO, rsm_type, f, ##args)
#define rsm_debug(rsm_type, f, args...) \
  rsm_log(facebook::logdevice::dbg::Level::DEBUG, rsm_type, f, ##args)
#define rsm_spew(rsm_type, f, args...) \
  rsm_log(facebook::logdevice::dbg::Level::SPEW, rsm_type, f, ##args)
