#!/usr/bin/env python3

# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import json
import logging
import sys

from Command import Command
from Environment import Environment
from Results import Results
from Workers import Workers


# Validate the config file and create objects
# If required parameters are missing in the configuration file
# It reports errors and will not create any objects
class ConfigChecker:
    # Check if an item in a dictionary
    # This function wraps the logging information
    def checkItem(self, item, dict, dict_name):
        if item not in dict:
            self.logger.error("%s is not missing in %s", item, dict_name)
            return False
        return True

    # Check if every item in a list is in a directionary or not
    # Return false if any item is not in the dict
    # This function is used for checking if a section includes all required parameters
    def checkItems(self, items, dict, dict_name):
        for item in items:
            self.checkItem(item, dict, dict_name)
        return True

    # Check the section of environment
    def checkEnv(self):
        sec_name = "environment"
        self.logger.info("checking the %s section", sec_name)
        if not self.checkItem(sec_name, self.config_dict, "configuration file"):
            return False
        params = [
            "hosts",
            "write-host-count",
            "read-host-count",
            "backfill-host-count",
            "stop-all-runs",
            "update-worker",
            "update-config",
            "file-copy-instruction",
            "stop-all-runs",
            "clean-old-results",
            "worker-binary-local-path",
            "worker-binary-remote-path",
            "worker-config-remote-path",
            "worker-config-remote-path",
            "worker-execution-instruction",
            "test-duration-second",
        ]
        env_dict = self.config_dict[sec_name]
        if not self.checkItems(params, env_dict, sec_name):
            return False
        return True

    # Check the required-worker-config section
    def checkRequiredSections(self):
        sec_name = "required-worker-config"
        self.logger.info("checking the %s section", sec_name)
        if not self.checkItem(sec_name, self.config_dict, "configuration file"):
            return False
        required_dict = self.config_dict[sec_name]
        sub_sections = [
            "common-config",
            "write-config",
            "read-config",
            "backfill-config",
        ]
        item_lists = [
            [
                "sys-name",
                "config-file",
                "log-range-name",
                "stats-interval-second",
                "event-sample-ratio",
                "publish-dir",
            ],
            [
                "worker-start-delay-second",
                "use-buffered-writer",
                "write-bytes-per-seconds",
                "write-bytes-increase-type",
                "write-bytes-increase-step",
                "write-bytes-increase-factor",
                "write-bytes-increase-interval",
                "payload-size",
            ],
            ["fanout"],
            ["worker-start-delay-second", "fanout", "restart-backlog-depth"],
        ]
        i = 0
        for sub_section in sub_sections:
            if not self.checkItem(sub_section, required_dict, sec_name):
                return False
            if not self.checkItems(
                item_lists[i], required_dict[sub_section], sub_section
            ):
                return False
            i += 1
        return True

    # check the section for result collections
    def checkCollection(self):
        sec_name = "collection-config"
        self.logger.info("checking the %s section", sec_name)
        if not self.checkItem(sec_name, self.config_dict, "configuration file"):
            return False
        collection_dict = self.config_dict[sec_name]
        params = [
            "plot-figure-only",
            "collect-data-only",
            "figure-prefix",
            "collection-dir",
            "latency-percentile",
            "end2end-latency-and-throughput-figure",
            "backfill-and-end2end-latency-figure",
        ]
        if not self.checkItems(params, collection_dict, sec_name):
            return False
        end2end_fig_dict = collection_dict["end2end-latency-and-throughput-figure"]
        end2end_params = [
            "width",
            "height",
            "latency-boxplot-count",
            "y-logscale",
            "min-latency-ms",
            "max-latency-ms",
            "min-timestamp-second",
            "max-timestamp-second",
            "throughput-type",
        ]
        if not self.checkItems(end2end_params, end2end_fig_dict, "end2end figure"):
            return False
        backfill_fig_dict = collection_dict["backfill-and-end2end-latency-figure"]
        backfill_params = [
            "width",
            "height",
            "latency-boxplot-count",
            "y-logscale",
            "min-latency-ms",
            "max-latency-ms",
            "min-timestamp-second",
            "max-timestamp-second",
            "throughput-type",
        ]
        if not self.checkItems(backfill_params, backfill_fig_dict, "backfill figure"):
            return False
        return True

    # Check each section of the configuration file
    # Return false if any section does not pass the check
    def checkConfig(self):
        if self.checkEnv() and self.checkRequiredSections() and self.checkCollection():
            return True
        return False

    # create a command object if command is configured
    def createCommand(self):
        command = Command(self.config_dict)
        return command

    # create an environment obj
    def createEnv(self):
        common_dict = self.config_dict["required-worker-config"]["common-config"]
        publish_dir = common_dict["publish-dir"]
        env = Environment(publish_dir, self.config_dict["environment"])
        return env

    # create workers
    # We only create workers when we assign hosts for them. For example, if
    # write-hosts is empty, we will not create write workers. Besides, workers
    # also need a Commands and an Environment obj.
    def createWorkers(self, type, commands, env):
        worker_hosts = []
        total_host_counts = len(env.all_hosts)
        if type == "write":
            worker_count = min(env.writer_counts, total_host_counts)
            worker_hosts = env.all_hosts[0:worker_count]
            commands = commands.common_command + " " + commands.write_command
        elif type == "read":
            worker_count = -1 * min(env.reader_counts, total_host_counts)
            worker_hosts = env.all_hosts[worker_count:]
            commands = commands.common_command + " " + commands.read_command
        elif type == "backfill":
            worker_count = min(env.backfill_counts, total_host_counts)
            worker_hosts = env.all_hosts[0:worker_count]
            commands = commands.common_command + " " + commands.backfill_command
        else:
            self.logger.error("Illegal worker namne %s", type)
        if len(worker_hosts) > 0 and commands != "":
            workers = Workers(
                env.worker_remote_file, worker_hosts, env.execution_inst, type, commands
            )
            return workers

    # Create an Results object
    def createResults(self, env):
        common_dict = self.config_dict["required-worker-config"]["common-config"]
        throughput_interval = common_dict["stats-interval-second"]
        publish_dir = common_dict["publish-dir"]
        results = Results(
            env.all_hosts,
            publish_dir,
            throughput_interval,
            self.config_dict["collection-config"],
            env.copy_inst,
        )
        return results

    def __init__(self, json_file_name):
        self.logger = logging.getLogger("coordinator.check")
        self.config_file = json_file_name
        self.logger.info("checking the config file %s", self.config_file)
        with open(json_file_name) as json_file:
            self.config_dict = json.load(json_file)
        if not self.checkConfig():
            self.logger.error("Configuration file is not valid.")
            sys.exit()
        self.logger.info("Configuration file is valid")


if __name__ == "__main__":
    logging.basicConfig()
    checker = ConfigChecker(sys.argv[1])
    checker.logger.setLevel(logging.DEBUG)
    command = checker.createCommand()
    if command:
        checker.logger.info(
            "\nCommand:\n\
            common_command: %s\n\
            write_command: %s\n\
            read_command:%s\n\
            backfill_command %s\n",
            command.common_command,
            command.write_command,
            command.read_command,
            command.backfill_command,
        )
    else:
        checker.logger.error("fail to create command")
    env = checker.createEnv()
    if env:
        checker.logger.info(
            "Environment info:\n\
             update_worker:%s\n\
             update_config:%s\n\
             clean_old_results:%s\n\
             publish_dir:%s\n\
             file_copy_inst:%s\n\
             local_worker_binary:%s\n\
             remote_worker_binary:%s\n\
             local_config_binary:%s\n\
             remote_config_binary:%s\n\
             worker_execution_inst:%s\n\
             allhosts:%s\n\
             test_duration:%s\n",
            env.update_worker,
            env.update_config,
            env.clean_old_results,
            env.publish_dir,
            env.copy_inst,
            env.worker_local_file,
            env.worker_remote_file,
            env.config_local_file,
            env.config_remote_file,
            env.execution_inst,
            len(env.all_hosts),
            env.total_test_duration,
        )
    else:
        checker.logger.error("Failed to create environment!")

    host_names = ["write", "read", "backfill"]

    for name in host_names:
        workers = checker.createWorkers(name, command, env)
        if workers:
            checker.logger.info(
                "worker name: %s\n hosts: %d\n start command: %s\n",
                workers.name,
                len(workers.hosts),
                workers.commands,
            )
        else:
            checker.logger.error("Failed to create %s workers!", name)

    results = checker.createResults(env)
    if results:
        checker.logger.info(
            " ".join(
                len(results.hosts),
                results.remote_dir,
                results.throughput_interval,
                results.local_dir,
                results.figure_prefix,
                results.lat_percentile,
                results.plot_only,
            )
        )
