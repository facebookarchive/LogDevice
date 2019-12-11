#!/usr/bin/env python3

# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import sys

# This is the mother of all hacks!
# There is possibly:
#   a) Compiler bug that makes sending exceptions across the boundary of the
#   shared library look weird to Python.
#   b) Boost-Python bug that causes throw_error_already_set() to crash the
#   python process (SIGABRT).
#   c) A Python bug (tested on 3.6.9)
# The effect is that if Client is loaded via our automatic loading, throwing
# Python exceptions from our C++ bindings will cause the process to crash. This
# faux import here is added to ensure the module/extension is loaded before any
# dynamic/automatic loading later.
# TODO: Consider removing when updating Python/Boost/GCC=>Clang.
from logdevice.client import Client

from ldshell.autoload import commands
from ldshell.logdevice_plugin import LogDevicePlugin
from nubia import Nubia


def main():
    plugin = LogDevicePlugin()
    shell = Nubia(name="ldshell", plugin=plugin, command_pkgs=commands)
    sys.exit(shell.run())


if __name__ == "__main__":
    main()
