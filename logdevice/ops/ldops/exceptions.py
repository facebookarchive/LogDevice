#!/usr/bin/env python3

# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


"""
ldops.exceptions
~~~~~~~~~~~

Contains LDOps-wide exceptions.
"""


class LDOpsError(Exception):
    """
    Generic error in LDOps
    """

    pass


class NodeNotFoundError(LDOpsError):
    """
    Raised when node not found
    """

    pass
