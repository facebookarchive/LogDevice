#!/usr/bin/env python3

# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# Coordinator of the distributed testing.
# We must specify a configuration file.
# Please refer to Mpmc_example_config.xml in the same directory
# Basically, the coordinator executes the distributed testing with following steps:
#   1. using Command module get related commands from the configuration file
#   2. using Environment module to setting up testing environment
#   3. using Worker module start and manage the read/write worker
#   4. using Results module collect results and generate a figure

import logging
import sys
import time

from ConfigChecker import ConfigChecker


def main(config_file_name):
    config_checker = ConfigChecker(config_file_name)
    logging.basicConfig()
    logger = logging.getLogger("coordinator.run")
    logger.setLevel(logging.INFO)

    env = config_checker.createEnv()
    if not env:
        logger.error("Failed to create env")
        return -1
    if env.stop_all_runs:
        env.stopAllRun()
        return

    results = config_checker.createResults(env)
    if not results:
        logger.error("Failed to create results")

    if results.collect_only:
        results.collect()
        results.wait()
        return

    if results.plot_only:
        results.aggregateAndDraw()
        return

    if env.clean_old_results:
        env.clean()

    if env.update_worker or env.update_config:
        env.deploy()

    commands = config_checker.createCommand()
    if not commands:
        logger.error("Failed to create command")
        sys.exit(-1)
    benchmarks = env.benchmark.split(",")
    workers = []

    for benchmark in benchmarks:
        worker = config_checker.createWorkers(benchmark, commands, env)
        if not worker:
            logger.error("Failed to create workers %d", benchmark)
            return
        else:
            workers.append(worker)

    # start the workers
    # start read workers and then write workers
    # If any failures when starting the workers, stop all started workers
    failed = False
    for worker in workers:
        if not worker.start():
            logger.error("Failed to start workers for %s", worker.name)
            failed = True
            break

    if failed:
        for worker in workers:
            worker.stop()
        logger.error("All workers are killed due to exception")
        return

    # Allow workers to run the defined duration
    time.sleep(env.total_test_duration)

    # stop all workers
    for worker in workers:
        worker.stop()
    logger.info("All workers stopped!")

    # collect results
    results.collect()
    results.wait()

    # aggregate results and produce a figure
    results.aggregateAndDraw()


if __name__ == "__main__":
    main(sys.argv[1])
