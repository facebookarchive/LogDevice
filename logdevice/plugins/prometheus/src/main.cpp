/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

/**
 * @author Mohamed Bassem
 */

#include "PrometheusPublisherFactory.h"

extern "C" __attribute__((__used__)) facebook::logdevice::Plugin*
logdevice_plugin() {
  return new facebook::logdevice::PrometheusStatsPublisherFactory();
}
