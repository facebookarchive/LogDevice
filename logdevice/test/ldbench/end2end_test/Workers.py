#!/usr/bin/env python3

# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import logging
import subprocess

from Execution import ExecutionBase


# Worker module is used for manage the write and read workers. Read/write workers
# will be started with the corresponding commands. When the testing ends, read and
# write workers will be stopped using the stop functions.
class Workers(ExecutionBase):
    def __init__(self, worker_file, hosts, execution_inst, benchname, commands):
        self.logger = logging.getLogger("coordinator.run")
        self.worker_file = worker_file
        self.name = benchname
        self.execution_inst = execution_inst
        self.hosts = hosts
        # The backfill workloads uses read workers but different parameters
        if benchname == "backfill":
            benchname = "read"
        self.commands = f" --bench-name={benchname} {commands}"

    def start(self):
        worker_id = 0
        total_worker = len(self.hosts)
        for host in self.hosts:
            remote_inst = self.execution_inst.split()
            remote_inst.append(host)
            remote_inst.append(self.worker_file)
            remote_inst.append(
                "{} --worker-id={}/{}".format(self.commands, worker_id, total_worker)
            )
            self.logger.info(remote_inst)
            try:
                worker_proc = subprocess.Popen(remote_inst)
            except OSError:
                self.logger.error(f"Start {self.name} {worker_id} failed")
                return False
            self.processes.append(worker_proc)
            worker_id = worker_id + 1
        return True
