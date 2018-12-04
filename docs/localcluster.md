---
id: LocalCluster
title: Running a local cluster
sidebar_label: Running a local cluster
---

After you've built LogDevice, you can use the `ld-dev-cluster` utility to
start a local cluster. This utility creates directories for nodes under the
specified path, writes a config file that includes all of these nodes, and starts an
instance of the `logdeviced` daemon for each node.

To run the binary from within the git
repo, enter `cd LogDevice` where `LogDevice` is the root of your local LogDevice source tree.

```text
./_build/bin/ld-dev-cluster
```

The utility creates temporary directories, which it deletes on a clean exit, and starts 5 nodes.

You should see output similar to this:

```text
...
Cluster running. ^C or type "quit" or "q" to stop. Commands:

        replace &lt;nid&gt;   Replace a node (kill the old node, wipe the existing data, start a replacement). Do not wait for rebuilding.
        start &lt;nid&gt;     Start a node if it is not already started.
        stop &lt;nid&gt;      Pause logdeviced by sending SIGSTOP. Waits for the process to stop accepting connections.
        resume &lt;nid&gt;    Resume logdeviced by sending SIGCONT. Waits for the process to start accepting connections again.
        kill &lt;nid&gt;      Kill a node.
        expand &lt;n&gt;      Add new nodes to the cluster.
        shrink &lt;n&gt;      Remove nodes from the cluster.


To create a log range:
        ldshell -c /dev/shm/tmp/logdevice/IntegrationTestUtils.8b62-42b5-1e7a-7cec/logdevice.conf logs create --from 1 --to 100 --replicate-across "node: 2" test_logs

To write to log 1:
         echo hello | _build/bin/ldwrite /dev/shm/tmp/logdevice/IntegrationTestUtils.8b62-42b5-1e7a-7cec/logdevice.conf 1

To start a tailer for log 1:
        _build/bin/ldtail /dev/shm/tmp/logdevice/IntegrationTestUtils.8b62-42b5-1e7a-7cec/logdevice.conf 1 --follow

To tail the error log of a node:
        tail -f /dev/shm/tmp/logdevice/IntegrationTestUtils.8b62-42b5-1e7a-7cec/N0:1/log

To send an admin command to a node:
        echo info | nc -U /dev/shm/tmp/logdevice/IntegrationTestUtils.8b62-42b5-1e7a-7cec/N0:1/socket_command  
cluster&gt;
```

The output suggests several steps. Copy and paste those steps to another terminal to create a log range, write data to
it, and read it using a tailer.

To inspect the config file that `ld-dev-cluster` generated, substitute your own directory:

```bash
 cat /dev/shm/tmp/logdevice/IntegrationTestUtils.8b62-42b5-1e7a-7cec/logdevice.conf
```

Run `ld-dev-cluster --help` to see all of the possible options. These are particularly useful:
```text
--nnodes      The number of nodes in the cluster
--loglevel    Controls verbosity of the output of the `ld-dev-cluster` utility
--root        If specified, the data will be placed here instead of a temporary directory (and will not be cleaned up on exit)
--param       Additional parameters to pass to every logdeviced instance. For example, `--param loglevel=debug`
--use-tcp     Configure nodes to listen on TCP ports instead of Unix sockets
```
