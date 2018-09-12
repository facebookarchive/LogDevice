---
id: LocalCluster
title: Running a local cluster
sidebar_label: Running a local cluster
---

After you've built logdevice, you can use the `ld-dev-cluster` utility to
start a local cluster. The utility will create directories for nodes under the
specified path, write a config that includes all of these nodes and start an
instance of the `logdeviced` daemon for each node.

The simplest way to use it is just run the binary:

```text
# (e.g. from within the git repo)
~/$ ./_build/bin/ld-dev-cluster
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
        ldshell -c /tmp/logdevice/IntegrationTestUtils.8b62-42b5-1e7a-7cec/logdevice.conf logs create --from 1 --to 100 --replicate-across "node: 2" test_logs

To write to log 1:
         echo hello | _build/bin/ldwrite /tmp/logdevice/IntegrationTestUtils.8b62-42b5-1e7a-7cec/logdevice.conf 1

To start a tailer for log 1:
        _build/bin/ldtail /tmp/logdevice/IntegrationTestUtils.8b62-42b5-1e7a-7cec/logdevice.conf 1 --follow

To tail the error log of a node:
        tail -f /tmp/logdevice/IntegrationTestUtils.8b62-42b5-1e7a-7cec/N0:1/log

To send an admin command to a node:
        echo info | nc -U /tmp/logdevice/IntegrationTestUtils.8b62-42b5-1e7a-7cec/N0:1/socket_command

NOTE: Internal LogsConfig Manager is ENABLED.
You will need to use ldshell to create logs and provision before use
cluster&gt;
```

This will create temporary directories (Note: they will be deleted when the
`ld-dev-cluster` utility exits cleanly) and start 5 nodes. You can then
follow the steps suggested by the utility to create a log range, write data to
it and read it back. You can also inspect the config file that it generated to
see what a typical logdevice config looks like in practice

Useful options to `ld-dev-cluster` include:
```text
--nnodes      The number of nodes in the cluster
--loglevel    Controls verbosity of the output of the `ld-dev-cluster` utility
--root        If specified, the data will be placed here instead of a temporary directory (and will not be cleaned up on exit)
--param       Additional params to pass to every logdeviced instance, e.g. `--param loglevel=debug`
--use-tcp     Configure nodes to listen on TCP ports instead of Unix sockets
```
