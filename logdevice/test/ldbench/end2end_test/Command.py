#!/usr/bin/env python3

# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import datetime
from string import Template


# Command module is used for parsing commands from the configuration file.
# Commands are classified into four types:
#  read-write-common-command -- will be applied for all workers
#                               it will built with read_write_common_temp
#  read-command -- for read workers only built with read_temp
#  write-command -- for write workers only built with write_temp
#  backfill-command -- for backfill workers only built with backfill_temp
class Command:
    read_write_common_temp = Template(
        "--duration=-1\
         --sys-name=${sys_name}\
         --config-path=${config_file}\
         --log-range-name=${log_range_name}\
         --publish-dir=${publish_dir}\
         --stats-interval=${stats_interval}\
         --event-sample-ratio=${sample_ratio}"
    )
    write_temp = Template(
        "--start-time=${start_time}\
         --record-writer-info=True\
         --use-buffered-writer=${use_buffer_writer}\
         --write-bytes-per-sec=${write_bytes_per_sec}\
         --write-bytes-increase-interval=${rate_inc_interval}\
         --payload-size=${payload_size} "
    )
    read_temp = Template(
        "--fanout=${fanout}\
         --record-writer-info=True"
    )
    backfill_temp = Template(
        "--restart-backlog-probability=1\
         --pct-readers-consider-backlog-on-start=1\
         --start-time=${start_time}\
         --fanout=${fanout}\
         --restart-backlog-depth=${backlog_depth}"
    )

    def getCommonCommand(self, common_dict):
        self.common_command = self.read_write_common_temp.substitute(
            sys_name=common_dict["sys-name"],
            config_file=common_dict["config-file"],
            log_range_name=common_dict["log-range-name"],
            stats_interval=common_dict["stats-interval-second"],
            sample_ratio=common_dict["event-sample-ratio"],
            publish_dir=common_dict["publish-dir"],
        )

    def getWriteCommand(self, write_required_dict, write_extra_dict):
        delay_seconds = write_required_dict["worker-start-delay-second"]
        currentDT = datetime.datetime.now()
        startDT = currentDT + datetime.timedelta(seconds=delay_seconds)
        start_time = startDT.strftime("%Y-%m-%d_%H:%M:%S")
        self.write_command = self.write_temp.substitute(
            use_buffer_writer=write_required_dict["use-buffered-writer"],
            write_bytes_per_sec=write_required_dict["write-bytes-per-seconds"],
            rate_inc_interval=write_required_dict["write-bytes-increase-interval"],
            payload_size=write_required_dict["payload-size"],
            start_time=start_time,
        )
        inc_arg = ""
        if write_required_dict["write-bytes-increase-type"] == "step":
            inc_arg = (
                " --write-bytes-increase-step="
                + write_required_dict["write-bytes-increase-step"]
                + " "
            )
        elif write_required_dict["write-bytes-increase-type"] == "factor":
            inc_arg = (
                " --write-bytes-increase-factor="
                + str(write_required_dict["write-bytes-increase-factor"])
                + " "
            )
        self.write_command += inc_arg
        for key, value in write_extra_dict.items():
            self.write_command += f" --{key}={value} "

    def getReadCommand(self, read_required_dict, read_extra_dict):
        self.read_command = self.read_temp.substitute(
            fanout=read_required_dict["fanout"]
        )
        for key, value in read_extra_dict.items():
            self.read_command += f" --{key}={value} "

    def getBackfillCommand(
        self, test_duration, backfill_required_dict, backfill_extra_dict
    ):
        delay_seconds = backfill_required_dict["worker-start-delay-second"]
        currentDT = datetime.datetime.now()
        startDT = currentDT + datetime.timedelta(seconds=delay_seconds)
        start_time = startDT.strftime("%Y-%m-%d_%H:%M:%S")
        self.backfill_command = self.backfill_temp.substitute(
            start_time=start_time,
            fanout=backfill_required_dict["fanout"],
            backlog_depth=backfill_required_dict["restart-backlog-depth"],
        )
        for key, value in backfill_extra_dict.items():
            self.backfill_command += f" --{key}={value} "

    def __init__(self, config_dict):
        test_duration = config_dict["environment"]["test-duration-second"]
        field = Template("${type}-worker-config")
        sub_field = Template("${name}-config")
        required_dict = config_dict[field.substitute(type="required")]
        common_dict = required_dict[sub_field.substitute(name="common")]
        self.getCommonCommand(common_dict)
        write_required_dict = required_dict[sub_field.substitute(name="write")]
        read_required_dict = required_dict[sub_field.substitute(name="read")]
        backfill_required_dict = required_dict[sub_field.substitute(name="backfill")]
        optional_dict = config_dict[field.substitute(type="extra")]
        write_extra_dict = optional_dict[sub_field.substitute(name="write")]
        read_extra_dict = optional_dict[sub_field.substitute(name="read")]
        backfill_extra_dict = optional_dict[sub_field.substitute(name="backfill")]
        self.getWriteCommand(write_required_dict, write_extra_dict)
        self.getReadCommand(read_required_dict, read_extra_dict)
        self.getBackfillCommand(
            test_duration, backfill_required_dict, backfill_extra_dict
        )
