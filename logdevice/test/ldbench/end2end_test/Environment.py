#!/usr/bin/env python3

# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import logging
import subprocess
from string import Template

from Execution import ExecutionBase


# Environment module is for setting up the testing environment including
#  1. copy worker binaries to testing hosts if needed
#  2. clean up the publish-dir if needed
#  3. setting the total test duration
class Environment(ExecutionBase):
    def __init__(self, publish_dir, env_dict):
        self.logger = logging.getLogger("coordinator.run")
        self.stop_all_runs = env_dict["stop-all-runs"]
        self.publish_dir = publish_dir
        self.update_worker = env_dict["update-worker"]
        self.update_config = env_dict["update-config"]
        self.copy_inst = env_dict["file-copy-instruction"]
        self.clean_old_results = env_dict["clean-old-results"]
        self.worker_local_file = env_dict["worker-binary-local-path"]
        self.worker_remote_file = env_dict["worker-binary-remote-path"]
        self.config_local_file = env_dict["worker-config-local-path"]
        self.config_remote_file = env_dict["worker-config-remote-path"]
        self.total_test_duration = env_dict["test-duration-second"]
        self.execution_inst = env_dict["worker-execution-instruction"]
        self.writer_counts = env_dict["write-host-count"]
        self.reader_counts = env_dict["read-host-count"]
        self.backfill_counts = env_dict["backfill-host-count"]
        self.all_hosts = env_dict["hosts"]
        self.benchmark = env_dict["benchmark"]

    # Copy the worker binaries to the remote servers
    def deploy(self):
        if not self.update_worker and not self.update_config:
            self.logger.info("Will not deploy new workers")
            return True
        remote_dir = Template("${hostname}:${path}")
        self.logger.info("Deploying workers and config files")
        for host in self.all_hosts:
            if self.update_worker:
                # copy the worker binary
                remote_worker = remote_dir.substitute(
                    hostname=host, path=self.worker_remote_file
                )
                deploy_worker_inst = self.copy_inst.split()
                deploy_worker_inst.append(self.worker_local_file)
                deploy_worker_inst.append(remote_worker)
                self.logger.info(deploy_worker_inst)
                try:
                    dep_proc = subprocess.Popen(deploy_worker_inst)
                except OSError:
                    return False
                self.processes.append(dep_proc)
        self.wait()
        self.processes = []
        for host in self.all_hosts:
            if self.update_config:
                # copy the config files
                remote_config = remote_dir.substitute(
                    hostname=host, path=self.config_remote_file
                )
                deploy_config_inst = self.copy_inst.split()
                deploy_config_inst.append(self.config_local_file)
                deploy_config_inst.append(remote_config)
                self.logger.info(deploy_config_inst)
                try:
                    dep_proc = subprocess.Popen(deploy_config_inst)
                except OSError:
                    return False
                self.processes.append(dep_proc)
        self.wait()
        self.processes = []
        self.logger.info("Deploy new workers successfully")
        return True

    # Delete the results on the remote server in the current publish dir
    def clean(self):
        if not self.clean_old_results:
            self.logger.info("Adding to the old results")
            return True
        if self.publish_dir == "":
            self.logger.info("Publish_dir is not set!")
            return False

        clean_command_tmp = Template("rm -rf ${path}/*.csv")
        for host in self.all_hosts:
            clean_inst = self.execution_inst.split()
            clean_inst.append(host)
            clean_inst.append(
                clean_command_tmp.substitute(hostname=host, path=self.publish_dir)
            )
            self.logger.info(f"cleaining {host}")
            self.logger.info(clean_inst)
            try:
                clean_proc = subprocess.Popen(clean_inst)
            except OSError:
                return False
            self.processes.append(clean_proc)
        self.wait()
        self.processes = []
        self.logger.info("Clean old results successfully")
        return True

    # Stop all of the running workers
    def stopAllRun(self):
        for host in self.all_hosts:
            stop_inst = self.execution_inst.split()
            stop_inst.append(host)
            stop_inst.append("pkill worker")
            self.logger.info(f"Killing workers on {host}")
            self.logger.info(stop_inst)
            try:
                kill_proc = subprocess.Popen(stop_inst)
            except OSError:
                return False
            self.processes.append(kill_proc)
        self.wait()
        self.processes = []
        self.logger.info("Kill all running workers successfully")
        return True
