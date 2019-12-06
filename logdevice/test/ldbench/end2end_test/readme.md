To spin up an end-to-end benchmark, a python script reads the configuration file
and spins up the ldbench workers based on the config.

mpmc_example_config.json is an example. Here we explain the options in the file
one-by-one. The terms labeled by number (e.g. 1, 1.1 etc.) are section names.
The configuration file consists of 4 sections, environment, required-worker-config,
extra-worker-config and collection-config. They may include subsections which are
also labeled by numbers. The parameters to set are marked with "*". Users are
free to update the values of parameters. However, any changes on the structure of
the configuration file including add new parameters or changing the type of a
parameter will lead to a check failure for the configuration file.

1. environment: This section provides configurations for testing environment
  settings including

  * benchmark: string. The names of benchmark separated by ','. Options are write,
  read and backfill.

  * hosts: list of string(user@hostname). A list of all available hosts.

  * write-host-count: int. The number of hosts to run write workers. We will
  select from the beginning of the host list. If the number of is larger than
  that of all hosts, we will use all hosts.

  * read-host-count: int. The number of hosts to run write workers. We will select
  hosts reversely from the last hosts for read workers.

  * backfill-host-count: int. The number of hosts to run backfill workers. Backfill
  hosts will be selected from the beginning of the host list.

  * stop-all-runs: bool. If true, we will kill any running workers on all of the hosts.
  This parameter has the highest priority. If true, we will only kill running workers
  and not run any tests. This parameter is useful when the coordinator exits accidentally
  after starting all workers.

  * update-worker: bool. If true, then the script copies the worker binaries
  to the remote hosts. The copy source file is configured in worker-binary-local-path
  and the destination is set by worker-binary-remote-path. Set to true if the hosts
  don't have worker already, or if the binary has changed.

  * update-config: bool. If true, the script copies the worker configuration file
  from worker-config-local-path to worker-config-remote-path. Note that the config
  file to copy with this option is for client configurations, not for the ldbench.

  * clean-old-results: bool. If true, the script deletes the old result files
  on remote servers.

  * file-copy-instruction: string. The instruction used to copy files from the local
  machine to remote servers. A general option is scp. When using scp, users are
  responsible to configure scp to copy without passwords.

  * worker-binary-local-path: string. The full pathname, including the filename,
  on the local system of the worker binary.

  * worker-binary-remote-path: string. The full pathname, including the filename,
  on the remote servers to store the worker binary.

  * worker-config-local-path: string. The full pathname, including the filename,
  on the local system of the configuration file.

  * worker-config-remote-path: string. The full pathname, including the filename,
  on the remote servers of the configuration file.

  * worker-execution-instruction: string. The instruction used to execute the ldbench
  workers on the remote servers. A general option is ssh. When using ssh, users are
  responsible to configure ssh to execute commands without passwords.

  * test-duration-second: int. How long to run the test in seconds.

2. required-worker-config: This section defines the minimal required configuration
to run benchmarks.

  2.1 common-config: Parameters in this section apply to both writer and
  reader workers.

    * sys-name: string. The name of the logging system. Options are: logdevice

    * config-file: string. The path and filename of the configuration file
    of the logging system

    * log-range-name: string. The name of the log range (LogDevice) or log
    partition (Kafka). The worker will only write to or read from this range.

    * stats-interval-second: int. The number of seconds between two stats collections

    * event-sample-ratio: double. The ratio of sampling latency. For example, 0.1
      means sampling latency with a ratio of 10%.

    * publish-dir: string. An directory on remote server to store results files.
      Leave it empty if you do not want to write the performance results to files.

  2.2 write-config: Parameters in this section will only apply for write workers.

    * worker-start-delay-second: int. The number of seconds before write workers
    start. Workers may need some preparation time. This parameter can be set to
    let all write workers start generating append operation simultaneously.

    * use-buffered-writer: bool. If true, records will be batched before appending.
    (in LogDevice, use BufferedWriter).

    * write-bytes-per-seconds: string. The write rate (byte/second).
    For example, "1M" means to append with a rate of 1MB/s.

    * write-bytes-increase-type: string. Write rate will be increased after a given
    time duration defined by write-bytes-increase-interval. This parameter specifies
    how to increase the write rate. Options are constant, step, and factor. "constant"
    means ldbench will never increase the write rate. It will append with a steady
    write rate. If configured to "constant", the following two parameters
    "write-byte-increase-step" and "write-byte-increase-ratio" will be invalid.
    "step" indicating the write rate increases with a fixed step "byte/second".
    If use "step", the "write-byte-increase-ratio" will be invalid. "factor" means
    the write rate increases with a given multiplier e.g., 1.2.  If use "factor",
    the "write-byte-increase-step" will be invalid.

    * write-bytes-increase-step: string. How many byte/second to increase each time.
    For example, setting to "1K" or "1M" indicates to increase the write rate by
    1KB/s and 1MB/s every time.

    * write-bytes-increase-factor: double. The factor that we want to change the
    write rate. For example, setting to 1.1 means to increase the write rate by1.1x
    during each increase interval.

    * write-rates-increase-interval: string. This parameter defines the interval
    between  e.g. "2s", "1m", between two throughput increases

    * payload-size: int. The average size (in bytes) of payloads

  2.3 read-config: Parameters in this section will only apply to read workers.

    * fanout: int. In our benchmark, a log may be read by multiple readers. This
    parameter defines the number of readers for each log.

  2.4 backfill-config: Parameters in this section will apply for backfill workers.

    * worker-start-delay-second: int. Define how long a backfill workload wants
    to wait after the test starts. Specifically, we hope that a backfill workload
    will not start until the status of the logging system becomes stable. Waiting
    for a duration will be helpful for this purpose.

    * fanout: int. Define the number of readers for each log.

    * restart-backlog-depth: string. A backward time point from now indicates
    where the backfill workload starts to read. For example, if configured as 24h,
    ldbench will seek to the record position of 24h before now and start to read.

3. extra-worker-config: Although required-worker-config provides minimal configures
to run the end-to-end testing, we use extra_worker_config to allow users to set
other parameters. Note that in this section, users can add arbitrary parameters
in "write-config" or "read-config" subsections. However, we will not perform any
parameter name or type conversions as required_worker_config does. Therefore,
the parameters provided in this section must be the same as the worker command
line, including names and types.

  3.1 write-config: {}, list of key-value pairs

  3.2 read-config: {}

4. collection-config: This section provides the required parameters to collect
benchmarking results from remote servers. Besides, it also configures some
attributes for the produced figures.
  * collect-data-only: bool. If true, we will not run any tests. The coordinator
  will not copy the result files from remote servers to the local system.

  * plot-figure-only: bool. After a test, we may only want to adjust the figure
  without re-running the whole test. If true, we will not run the worker and only
  update the produced figure with the related configurations.

  * figure-prefix: string. It provides the prefix of generated figures from testing
  results. It is useful to distinguish figures from different tests.

  * collection-dir: string. The local directory to store the collected result
  files from remote servers.

  * latency-percentile: double. The percentile of latency we want to mark in the
  figure. For example, 0.99 means to mark the p99 latency.

  4.1 end2end-latency-and-throughput-figure: This subsection provides basic
  parameters to plot the figure of end-to-end latency with append throughputs.
  In this figure, the x-axis will be the append throughput. The y-axis is the
  latency. We use boxplot to represent the delivery latency, and also plot the
  percentile latency with lines for both deliveries (reads) and writes.

    * width: int. The width of the figure in inches.

    * height: int. The height of the figure in inches

    * latency-boxplot-count: int. We use boxplot to represent the latency. By changing
    this parameter, you can define the number of boxes in figure.

    * y-logscale: bool. If true, y-axis will use the log scale based on 10.

    * min-latency-ms: int. It defines the lower-bound of the range of y-axis.

    * max-latency-ms: int. The upper-bound of the range of y-axis in the figure.
    This parameter together with min-latency-ms defines the range of y-axis.

    * min-timestamp-second: int. The minimal timestamp to plot. We may do not
    want to plot the whole time range of a test. This parameter together with
    max-timestamp-second defines the time range to plot.

    * min-timestamp-second: int. The maximal timestamp to plot.

    * throughput-type: string. Options are mbs and qps. If configured to mbs, the
    throughput on x-axis will be presented as MB/second. pqs means to present
    throughput on x-axis with records/second.

  4.2 backfill-and-end2end-latency-figure: This subsection provides some basic
    parameters to plot the figure of backfill throughput with the end-to-end latency.
    In this figure, the x-axis will be the relative timestamp since the test starts.
    We have two y-axises. The left y-axis is the latency. We still plot the delivery
    latency (boxplot) and the percentile latency (lines) for both deliveries and
    writes. The right y-axis is the throughput (mbs or qps) for both backfills and
    appends.

    * width: int. The width of the figure in inches.

    * height: int. The height of the figure in inches

    * latency-boxplot-count: int. We use boxplot to represent the latency. By changing
      this parameter, you can define the number of boxes in figure.

    * y-logscale: bool. If true, left y-axis will use the log scale based on 10.

    * min-latency-ms: int. It defines the lower-bound of the range of y-axis.

    * max-latency-ms: int. The upper-bound of the range of y-axis in the figure.
      This parameter together with min-latency-ms defines the range of y-axis.

    * max-latency-ms: int. The upper-bound of the range of y-axis in the figure.
      This parameter together with min-latency-ms defines the range of y-axis.

    * min-timestamp-second: int. The minimal timestamp to plot. We may do not
      want to plot the whole time range of a test. This parameter together with
      max-timestamp-second defines the time range to plot.

    * throughput-type: string. Options are mbs and qps. If configured to mbs, the
    throughput on x-axis will be presented as MB/second. pqs means to present
    throughput on x-axis with records/second.
}
