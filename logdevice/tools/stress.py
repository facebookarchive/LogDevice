#!/usr/bin/env python3

# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import argparse
import multiprocessing
import os
import signal
import subprocess
import sys
import tempfile
import threading
import time


DESC = """Stress-tests a command by invoking it repeatedly with parallelism.
Particularly useful for stress-testing integration tests, which often end up
flaky if care is not taken when implementing them. By default, runs the
command until it fails once."""


class WorkersSharedState:
    def __init__(self):
        self.ok = 0
        self.fail = 0
        self.aborted = False


class Stats:
    def __init__(self, state):
        self.state = state
        self.thread_stop = False
        self.thread_cv = threading.Condition()
        self.thread = threading.Thread(target=self.run_thread)
        self.thread.start()

    def run_thread(self):
        tstart = time.time()
        with self.thread_cv:
            while not self.thread_stop:
                self.thread_cv.wait(5)
                print(
                    "[{:5.0f}s] {} of {} jobs succeeded".format(
                        time.time() - tstart,
                        self.state.ok,
                        self.state.ok + self.state.fail,
                    ),
                    file=sys.stderr,
                )

    def stop_thread(self):
        with self.thread_cv:
            self.thread_stop = True
            self.thread_cv.notify()
        self.thread.join()


class TaskQueue:
    def __init__(self, ntasks, max_failures, state):
        self.max_id = ntasks
        self.max_failures = max_failures
        self.state = state
        self.mutex = threading.Lock()
        self.next_task_no = 1
        pass

    def reachedMaxFailures(self):
        with self.mutex as mutex:
            return self._reachedMaxFailures(mutex)

    def getTaskNumber(self):
        """Returns a unique task number for a worker to run, or None if the
worker should stop.

        """
        with self.mutex as mutex:
            if self.max_id is not None and self.next_task_no > self.max_id:
                return None
            if self._reachedMaxFailures(mutex):
                return None
            self.next_task_no += 1
            return self.next_task_no - 1

    def _reachedMaxFailures(self, mutex):
        return (
            self.max_failures is not None
            and self.max_failures > 0
            and self.state.fail >= self.max_failures
        )


def worker(root, queue, state):
    while not state.aborted:
        task_no = queue.getTaskNumber()
        if task_no is None:
            return
        global ARGS
        outpath = os.path.join(root, "{}.out".format(str(task_no)))
        with open(outpath, "wt") as outfile, open(os.devnull, "r") as devnull_r:
            proc = subprocess.Popen(
                ARGS.cmd, stdin=devnull_r, stdout=outfile, stderr=subprocess.STDOUT
            )
            while proc.poll() is None:
                time.sleep(0.05)
                if queue.reachedMaxFailures():
                    proc.kill()
                    return
        if proc.returncode == -signal.SIGINT:
            break
        if proc.returncode == 0:
            state.ok += 1
        else:
            print(
                "Job {} (pid {}) failed with code {}, output in {}".format(
                    task_no, proc.pid, proc.returncode, outpath
                ),
                file=sys.stderr,
            )
            state.fail += 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=DESC, formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "-j",
        default=multiprocessing.cpu_count(),
        dest="parallelism",
        type=int,
        help="how many instances to run at once",
    )
    parser.add_argument(
        "-n",
        default="inf",
        dest="instances",
        type=str,
        help="how many total instances to run",
    )
    parser.add_argument(
        "-f",
        default="1",
        dest="max_failures",
        type=str,
        help="stop after this many failures",
    )
    parser.add_argument("cmd", nargs="+", help="command to invoke")
    ARGS = parser.parse_args()
    print(ARGS)
    ARGS.instances = None if ARGS.instances == "inf" else int(ARGS.instances)
    ARGS.max_failures = None if ARGS.max_failures == "inf" else int(ARGS.max_failures)

    state = WorkersSharedState()
    stats = Stats(state)
    root = tempfile.mkdtemp(prefix="stress.")
    print("Saving output to {}/*.out".format(root))

    queue = TaskQueue(
        ntasks=ARGS.instances, max_failures=ARGS.max_failures, state=state
    )
    worker_args = {"root": root, "queue": queue, "state": state}
    workers = []
    for _ in range(ARGS.parallelism):
        w = threading.Thread(target=worker, kwargs=worker_args)
        w.start()
        workers.append(w)

    interrupt_handled = False
    while workers:
        try:
            for w in workers:
                w.join(0.2)
            workers = [w for w in workers if w.is_alive()]
        except KeyboardInterrupt:
            # NOTE: ^C propagates to all processes in the process group so any
            # running binaries are failing or have already failed

            # If this is the second time we see ^C, fail quickly
            if interrupt_handled:
                raise

            state.aborted = True
            interrupt_handled = True

    stats.stop_thread()

    sys.exit(state.fail)
