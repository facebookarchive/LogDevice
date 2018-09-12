#!/usr/bin/env python3

# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import sys

from ldshell.logdevice_plugin import LogDevicePlugin
from nubia import Nubia


def main():
    plugin = LogDevicePlugin()
    shell = Nubia(name="ldshell", plugin=plugin)
    sys.exit(shell.run())


if __name__ == "__main__":
    main()
