#!/usr/bin/env python3
# pyre-strict

# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


from enum import Enum


class MaintenanceOverallStatus(Enum):
    INVALID = 0
    IN_PROGRESS = 1
    BLOCKED = 2
    COMPLETED = 3
