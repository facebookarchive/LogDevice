#!/usr/bin/env python3

# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


# The ExecutionBase module provides some common function of executing multiple
# processes. Any module that needs to execute multiple processes can inherit
# this module.
#   wait -- wait all running processes finish
#   stop -- stop all running Processes
# We do not provide a start method since each submodule may have different
# start process
class ExecutionBase:
    # store all running subprocesses
    processes = []

    # wait all running subprocesses finish
    def wait(self):
        for exe_proc in self.processes:
            exe_proc.wait()

    # stop all running subprocess
    def stop(self):
        for exe_proc in self.processes:
            exe_proc.kill()
