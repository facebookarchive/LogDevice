#!/usr/bin/env python3

# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import glob
import json
import logging
import math
import subprocess
from string import Template

import matplotlib.pyplot as plt
import numpy as np
from Execution import ExecutionBase
from matplotlib.ticker import ScalarFormatter


# We store the aggregate results in AggregatedStats and AggregatedEvents objs
class AggregatedStats:
    def __init__(self):
        # timestamps, in seconds
        self.ts = []
        # total successful records until the current timestamp
        self.total_records = []
        # total successful bytes until the current timestamp
        self.total_bytes = []
        # total failed records until the current timestamp
        self.failed_records = []


class AggregatedEvents:
    def __init__(self):
        # time stamps, in seconds
        self.ts = []
        # grouped events by timestamp
        self.events_by_ts = []
        # percentil event for each group
        self.percentile_events = []


# Results module is used for results collection including
#   1. collecting results (copy files) from remote server to local directory
#   2. aggregate the results from different files
#   3. produce a figure using the aggregated results
class Results(ExecutionBase):
    remote_path_temp = Template("${host}:${dir}/*.csv")

    def __init__(
        self, hosts, publish_dir, throughput_interval, collection_dict, collect_inst
    ):
        self.hosts = hosts
        self.remote_dir = publish_dir
        self.throughput_interval = throughput_interval
        self.local_dir = collection_dict["collection-dir"]
        self.figure_prefix = collection_dict["figure-prefix"]
        self.lat_percentile = collection_dict["latency-percentile"]
        self.plot_only = collection_dict["plot-figure-only"]
        self.end2end_fig = collection_dict["end2end-latency-and-throughput-figure"]
        self.backfill_fig = collection_dict["backfill-and-end2end-latency-figure"]
        self.collect_inst = collect_inst
        self.logger = logging.getLogger("coordinator.run")
        self.collect_only = collection_dict["collect-data-only"]
        self.end2end_min_time = self.end2end_fig["min-timestamp-second"]
        self.end2end_max_time = self.end2end_fig["max-timestamp-second"]
        self.backfill_min_time = self.backfill_fig["min-timestamp-second"]
        self.backfill_max_time = self.backfill_fig["max-timestamp-second"]

    # copy result files from remote servers to local directory
    def collect(self):
        for host in self.hosts:
            collect_command = self.collect_inst.split()
            collect_command.append(
                self.remote_path_temp.substitute(host=host, dir=self.remote_dir)
            )
            collect_command.append(self.local_dir)
            self.logger.info(collect_command)
            collect_proc = subprocess.Popen(collect_command)
            self.processes.append(collect_proc)

    # Aggregate stats for a list of files
    # Our stats are published with a given time intervals. We start write workers
    # using multiple subprocess at the same time. Therefore, all workers should
    # publish stats at close time points. That means, the stats on the same line
    # from different files are published at close time points. Therefore, when
    # aggregate the stats, we sum the stats with the same line offset from
    # different files.
    def aggregateStats(self, aggregated_stats, type_name):
        stats_file_names = glob.glob(f"{self.local_dir}/stats_{type_name}*")
        if len(stats_file_names) == 0:
            return
        for stats_file_name in stats_file_names:
            # get throughput from each stats file
            with open(stats_file_name, "r") as stats_file:
                # Since the stats files are typically small,
                # we read the whole file out at one time
                stats_lines = stats_file.readlines()
                total_lines = len(stats_lines)
                cur_len = len(aggregated_stats.total_bytes)
                # Pad our lists if they're too short
                if cur_len < total_lines:
                    extend_list = [0] * (total_lines - cur_len)
                    aggregated_stats.ts.extend(extend_list)
                    aggregated_stats.total_bytes.extend(extend_list)
                    aggregated_stats.total_records.extend(extend_list)
                    aggregated_stats.failed_records.extend(extend_list)
                # Accumulate this file into our stats struct
                for line_idx, stats_line in enumerate(stats_lines):
                    stats_obj = json.loads(stats_line)
                    aggregated_stats.total_bytes[line_idx] += int(
                        stats_obj["success_byte"]
                    )
                    aggregated_stats.total_records[line_idx] += int(
                        stats_obj["success"]
                    )
                    aggregated_stats.failed_records[line_idx] += int(stats_obj["fail"])
                    # timestamps in stats files are in nanoseconds
                    timestamp_sec = stats_obj["timestamp"] / 10 ** 9
                    if timestamp_sec > aggregated_stats.ts[line_idx]:
                        aggregated_stats.ts[line_idx] = round(timestamp_sec, 2)
        line_temp = Template(
            '{"ts":${ts}, "total_byte":${bytes}, "total_records":${records}}\n'
        )
        with open(
            self.local_dir + "/total_stats" + type_name + ".json", "w"
        ) as total_f:
            for i in range(len(aggregated_stats.ts)):
                line = line_temp.substitute(
                    ts=aggregated_stats.ts[i],
                    bytes=aggregated_stats.total_bytes[i],
                    records=aggregated_stats.total_records[i],
                )
                total_f.write(line)

    # After aggregating stats, we continue to aggregate events.
    # The events will be aggregated as several groups. Each group has a timestamp
    # and a list of events which happen before the timestamp. The timestamps
    # in each group are detemined by the number of boxes to plot in the figure.
    # Note that in this function, all arguments are in seconds.
    def aggregateEvents(
        self, start_time, end_time, group_count, aggregated_events, type_name
    ):
        test_duration = end_time - start_time
        box_plot_interval = test_duration / group_count
        aggregated_events.ts = [
            round(
                start_time + box_plot_interval * (i + 1), 2
            )  # marks end of time bucket
            for i in range(group_count)
        ]
        aggregated_events.events_by_ts = [[] for _ in range(group_count)]
        event_file_names = glob.glob(f"{self.local_dir}/event_{type_name}*")
        if len(event_file_names) == 0:
            self.logger.info("No event files")
            return False
        for event_file_name in event_file_names:
            # get events from event files
            with open(event_file_name, "r") as event_file:
                ts_idx = 0
                event_lines = event_file.readlines()
                if len(event_lines) == 0:
                    self.logger.error(f"Event file {event_file_name} is empty")
                    continue
                for event_line in event_lines:
                    event_obj = json.loads(event_line)
                    # timestamps in event files are in microseconds
                    timestamp_sec = event_obj["timestamp"] / 10 ** 6
                    # only count the latency within the user-defined ranges
                    if not (start_time < timestamp_sec < end_time):
                        continue
                    # calculate which bucket we're in
                    ts_idx = int((timestamp_sec - start_time) / box_plot_interval)
                    if not (0 <= ts_idx < group_count):
                        continue
                    # In event file, the latency is in us. In our figure,
                    # we plot latency in milliseconds. We convert latency from
                    # us to ms
                    aggregated_events.events_by_ts[ts_idx].append(
                        float(event_obj["value"]) / 1000
                    )
        # update percentile events
        for _ts_idx, event_list in enumerate(aggregated_events.events_by_ts):
            if len(event_list) == 0:
                aggregated_events.percentile_events.append(None)
            else:
                aggregated_events.percentile_events.append(
                    np.percentile(event_list, self.lat_percentile * 100)
                )
        line_temp = Template(
            '{"ts":${ts}, "lats":${lat}, "percentile_lats":${p_lat}}\n'
        )
        with open(
            f"{self.local_dir}/total_events_{type_name}{group_count}.json", "w"
        ) as total_f:
            for i in range(len(aggregated_events.ts)):
                line = line_temp.substitute(
                    ts=aggregated_events.ts[i],
                    lat=aggregated_events.events_by_ts[i],
                    p_lat=aggregated_events.percentile_events[i],
                )
                total_f.write(line)
        return True

    # Calculate relative time and throughput in MB/s
    def getThroughput(self, aggregated_stats, start_ts_second, end_ts_second):
        if len(aggregated_stats.ts) == 0:
            return False
        relative_ts = []
        throughputs_mbs = []
        throughputs_qps = []
        # we are fine to ingore the first entry since they are always 0
        for i in range(1, len(aggregated_stats.total_bytes)):
            relative_ts_second = round(
                aggregated_stats.ts[i] - aggregated_stats.ts[0], 2
            )
            # user will define the time range to plot
            if not (start_ts_second <= relative_ts_second <= end_ts_second):
                continue
            delta_bytes = (
                aggregated_stats.total_bytes[i] - aggregated_stats.total_bytes[i - 1]
            )
            delta_records = (
                aggregated_stats.total_records[i]
                - aggregated_stats.total_records[i - 1]
            )
            interval_second = aggregated_stats.ts[i] - aggregated_stats.ts[i - 1]
            if interval_second == 0:
                continue
            throughput_mbs = round(delta_bytes / 1024 / 1024 / interval_second, 2)
            throughput_qps = round(delta_records / interval_second, 2)
            if throughput_mbs < 0:
                continue
            throughputs_mbs.append(throughput_mbs)
            throughputs_qps.append(throughput_qps)
            relative_ts.append(relative_ts_second)
        return relative_ts, throughputs_mbs, throughputs_qps

    def createFigures(self, aggregated_backfill_stats, aggregated_write_stats):
        write_stats_count = len(aggregated_write_stats.ts)
        backfill_stats_count = len(aggregated_backfill_stats.ts)
        # only plot end2end latency with throughputs
        # If not end2end latency information, we will only plot throuhgput
        if write_stats_count > 0:
            write_ts_e2e, write_tp_e2e, write_qps_e2e = self.getThroughput(
                aggregated_write_stats, self.end2end_min_time, self.end2end_max_time
            )
            self.plotE2eAndThroughput(
                aggregated_write_stats.ts[0], write_ts_e2e, write_tp_e2e, write_qps_e2e
            )
        if backfill_stats_count > 0:
            backfill_ts, backfill_tp, backfill_qps = self.getThroughput(
                aggregated_backfill_stats,
                self.backfill_min_time,
                self.backfill_max_time,
            )
            write_ts_bf, write_tp_bf, write_qps_bf = self.getThroughput(
                aggregated_write_stats, self.backfill_min_time, self.backfill_max_time
            )
            self.plotBackfillAndE2eAndThoughput(
                aggregated_backfill_stats.ts[0],
                backfill_ts,
                backfill_tp,
                backfill_qps,
                write_ts_bf,
                write_tp_bf,
                write_qps_bf,
            )

    # Plot figure of end2end latency with throughput (byte/second)
    def plotE2eAndThroughput(self, start_time, write_ts, write_tp, write_qps):
        plt.clf()
        min_lat_ms = self.end2end_fig["min-latency-ms"]
        max_lat_ms = self.end2end_fig["max-latency-ms"]
        y_log_scale = self.end2end_fig["y-logscale"]
        width = self.end2end_fig["width"]
        height = self.end2end_fig["height"]
        box_count = self.end2end_fig["latency-boxplot-count"]
        tp_type = self.end2end_fig["throughput-type"]
        aggregated_read_events = AggregatedEvents()
        aggregated_append_events = AggregatedEvents()
        has_read_events = self.aggregateEvents(
            start_time + self.end2end_min_time,
            start_time + self.end2end_max_time,
            box_count,
            aggregated_read_events,
            "read",
        )
        has_write_events = self.aggregateEvents(
            start_time + self.end2end_min_time,
            start_time + self.end2end_max_time,
            box_count,
            aggregated_append_events,
            "write",
        )
        plt.figure(figsize=(width, height))
        if y_log_scale:
            plt.yscale("log")
        ay = plt.gca().yaxis
        plt.ylabel("Latency (ms)", fontsize=18)
        ay.set_major_formatter(ScalarFormatter())
        plt.ticklabel_format(axis="y", style="plain", useOffset=False)
        plt.ylim(min_lat_ms, max_lat_ms)
        fig_name = self.local_dir + "/" + self.figure_prefix + "_end2end_lats.png"
        self.logger.info("Plotting %s", fig_name)
        # Calculate the pos for boxplot and throughput
        # They may have different counts
        # We use the pos of throuhput as the baseline
        tp_box_ratio = len(write_tp) / len(aggregated_read_events.ts)
        tp_x = []
        box_x = []
        for pos in range(len(write_tp)):
            tp_x.append(pos)
        for pos in range(len(aggregated_read_events.ts)):
            box_x.append((pos + 1) * tp_box_ratio)
        if has_read_events:
            plt.boxplot(
                aggregated_read_events.events_by_ts,
                positions=box_x,
                widths=tp_box_ratio * 0.8,
            )
            plt.plot(
                box_x,
                aggregated_read_events.percentile_events,
                label="P" + str(100 * self.lat_percentile) + " delivery latency",
            )

        if has_write_events:
            plt.plot(
                box_x,
                aggregated_append_events.percentile_events,
                label="P" + str(100 * self.lat_percentile) + " append latency",
            )
        # If we collect throughput with a short interval_second, the labels on
        # x-axis may be overlapped. Therefore, we only allow at most 10 labels
        # to avoid overlapped number
        x_step = 1
        if len(tp_x) > 10:
            x_step = math.ceil(len(tp_x) / 10.0)
        throughput_pos = tp_x[0::x_step]
        if tp_type == "qps":
            throughput_val = write_qps[0::x_step]
            plt.xlabel("Append Throughput (Records/s)", fontsize=18)
        else:
            throughput_val = write_tp[0::x_step]
            plt.xlabel("Append Throughput (MB/s)", fontsize=18)

        thrput_ts = write_ts[0::x_step]
        x_ticks = [f"{thrput}\n{ts}s" for thrput, ts in zip(throughput_val, thrput_ts)]

        plt.xticks(throughput_pos, x_ticks, fontsize=18)

        plt.legend(fontsize=18)
        plt.savefig(fig_name)

    # Plot figure of end2end latency with throughput (byte/second)
    def plotBackfillAndE2eAndThoughput(
        self,
        start_time,
        backfill_ts,
        backfill_tp,
        backfill_qps,
        write_ts,
        write_tp,
        write_qps,
    ):
        test_duration = self.backfill_max_time - self.backfill_min_time
        min_lat_ms = self.backfill_fig["min-latency-ms"]
        max_lat_ms = self.backfill_fig["max-latency-ms"]
        y_log_scale = self.backfill_fig["y-logscale"]
        width = self.backfill_fig["width"]
        height = self.backfill_fig["height"]
        box_count = self.backfill_fig["latency-boxplot-count"]
        tp_type = self.backfill_fig["throughput-type"]
        aggregated_read_events = AggregatedEvents()
        aggregated_append_events = AggregatedEvents()
        has_read_events = self.aggregateEvents(
            start_time + self.backfill_min_time,
            start_time + self.backfill_max_time,
            box_count,
            aggregated_read_events,
            "read",
        )
        has_write_events = self.aggregateEvents(
            start_time + self.backfill_min_time,
            start_time + self.backfill_max_time,
            box_count,
            aggregated_append_events,
            "write",
        )
        fig_name = self.local_dir + "/" + self.figure_prefix + "_backfill_lats.png"
        self.logger.info("Plotting %s", fig_name)
        plt.clf()
        fig, ax1 = plt.subplots(figsize=(width, height))
        ax1.set_xlabel("Timestamp (s)", fontsize=18)
        if y_log_scale:
            ax1.set_yscale("log")
        ax1.yaxis.set_major_formatter(ScalarFormatter())
        ax1.ticklabel_format(axis="y", style="plain", useOffset=False)
        ax1.set_ylabel("Latency (ms)", fontsize=18)
        ax1.set_ylim(min_lat_ms, max_lat_ms)
        group_interval = test_duration / box_count

        lines = []
        relative_pos = []
        if has_read_events:
            for i in range(len(aggregated_read_events.ts)):
                relative_pos.append(
                    int(aggregated_read_events.ts[i] - aggregated_read_events.ts[0])
                )
            ax1.boxplot(
                [lats for lats in aggregated_read_events.events_by_ts],
                positions=relative_pos,
                widths=group_interval * 0.6,
            )
            lines += ax1.plot(
                relative_pos,
                aggregated_read_events.percentile_events,
                label="P" + str(100 * self.lat_percentile) + " delivery latency",
            )

        relative_pos = []
        if has_write_events:
            for i in range(len(aggregated_read_events.ts)):
                relative_pos.append(
                    int(aggregated_read_events.ts[i] - aggregated_read_events.ts[0])
                )
            lines += ax1.plot(
                relative_pos,
                aggregated_append_events.percentile_events,
                label="P" + str(100 * self.lat_percentile) + " append latency",
            )

        relative_pos = []
        for i in range(len(backfill_ts)):
            relative_pos.append(backfill_ts[i] - backfill_ts[0])

        # If we collect throughput with a short interval_second, the labels on
        # x-axis may be overlapped. Therefore, we only allow at most 10 labels
        # to avoid overlapped number
        x_step = 1
        if len(relative_pos) > 10:
            x_step = math.ceil(len(relative_pos) / 10.0)

        x_pos = relative_pos[0::x_step]
        x_val = backfill_ts[0::x_step]

        plt.xticks(x_pos, x_val, fontsize=18)
        ax2 = ax1.twinx()
        if tp_type == "qps":
            ax2.set_ylabel("Throughput (Records/s)", fontsize=18)
            lines += ax2.plot(
                relative_pos, backfill_qps, label="backfill throughput", c="red"
            )
        else:
            ax2.set_ylabel("Throughput (MB/s)", fontsize=18)
            lines += ax2.plot(
                relative_pos, backfill_tp, label="backfill throughput", c="red"
            )

        # if we has write throughput, draw a line for it
        if len(write_ts) > 0:
            relative_pos = []
            for i in range(len(write_ts)):
                relative_pos.append(write_ts[i] - write_ts[0])
            if tp_type == "qps":
                lines += ax2.plot(
                    relative_pos, write_qps, label="write throughput", c="green"
                )
            else:
                lines += ax2.plot(
                    relative_pos, write_tp, label="write throughput", c="green"
                )

        labs = [l.get_label() for l in lines]
        ax1.legend(lines, labs, fontsize=18)
        plt.savefig(fig_name)

    # open all result files, aggregate results and draw a figure
    def aggregateAndDraw(self):
        self.logger.info("Aggregating from all result files ...")
        aggregated_write_stats = AggregatedStats()
        self.aggregateStats(aggregated_write_stats, "write")
        aggregated_backfill_stats = AggregatedStats()
        self.aggregateStats(aggregated_backfill_stats, "backfill")

        self.logger.info("Plotting figures ...")

        self.createFigures(aggregated_backfill_stats, aggregated_write_stats)
        self.logger.info("Plotting Done")
        self.logger.info("Writing Summary")
        summary_file_name = f"{self.local_dir}/append_summary.txt"
        with open(summary_file_name, "w") as summary_file:
            if len(aggregated_write_stats.ts) > 0:
                summary_file.write("Append Summary:\n")
                summary_file.write(
                    f"--Successful Records:{aggregated_write_stats.total_records[-1]}\n"
                )
                summary_file.write(
                    f"--Successful Bytes:{aggregated_write_stats.total_bytes[-1]}\n"
                )
                summary_file.write(
                    f"--Failed Records:{aggregated_write_stats.failed_records[-1]}\n"
                )
