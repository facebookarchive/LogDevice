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


class NodeIsNotASequencerError(LDOpsError):
    """
    Raised when node which is not a sequencer was used in a context
    expecting that it is a sequencer
    """

    pass


class NodeIsNotAStorageError(LDOpsError):
    """
    Raised when node which is not a storage is used in a context
    expectit that it is a storage
    """

    pass
