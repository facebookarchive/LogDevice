---
id: LocalCluster
title: Run a local cluster in Docker
sidebar_label: Quickstart
---
You can quickly create a local cluster, write to it, and read from it.

The easiest way to get started is by retrieving the LogDevice Docker image:

```shell
docker pull facebookincubator/logdevice
```
Once you have downloaded the Docker image, launch a container named mycluster to start a local cluster.
```shell
docker run --rm -ti --name dev-cluster facebookincubator/logdevice ld-dev-cluster --root /data/logdevice
```
You'll see the output of the ld-dev-cluster utility, which the container starts if no command is specified.

Or, if you did a full LogDevice build, from the root directory of the repo, run the ld-dev-cluster utility:

```shell
./_build/bin/ld-dev-cluster --root /data/logdevice
```

`ld-dev-cluster` creates temporary directories, which it deletes on a clean exit, and starts 5 nodes. It also writes a config file that includes all of these nodes, and starts an instance of the `logdeviced` daemon for each node.

You should see output similar to this:

```text
LogDevice Cluster running. ^C or type "quit" or "q" to stop. Commands:

        replace <nid>   Replace a node (kill the old node, wipe the existing data, start a replacement). Do not wait for rebuilding.
        start <nid>     Start a node if it is not already started.
        stop <nid>      Pause logdeviced by sending SIGSTOP. Waits for the process to stop accepting connections.
        resume <nid>    Resume logdeviced by sending SIGCONT. Waits for the process to start accepting connections again.
        kill <nid>      Kill a node.
        expand <nid>    Add new nodes to the cluster.
        shrink <nid>    Remove nodes from the cluster.

To connect to the cluster via ldshell:
        ldshell --admin-server-unix-path=/data/logdevice/N0:1/socket_admin
To create a log range:
        ldshell --admin-server-unix-path=/data/logdevice/N0:1/socket_admin logs create --from 1 --to 100 --replicate-across "node: 2" test_logs

To view existing log ranges: (We've already created one for you)
        ldshell --admin-server-unix-path=/data/logdevice/N0:1/socket_admin logs show

To write to log 1:
         echo hello | usr/local/bin/ldwrite /data/logdevice/logdevice.conf 1

To start a tailer for log 1:
        usr/local/bin/ldtail /data/logdevice/logdevice.conf 1 --follow

To tail the error log of a node:
        tail -f /data/logdevice/N0:1/log

To send an admin command to a node:
        echo info | nc -U /data/logdevice/N0:1/socket_command
cluster>
```

To inspect the config file that `ld-dev-cluster` generated, start a new terminal session and run bash in the Docker container. Substitute your own directory in the cat command.

```bash
docker exec -it dev-cluster /bin/bash

$ cat /data/logdevice/logdevice.conf
```

You can connect ldshell to this cluster from within the container:
```bash
ldshell --admin-server-unix-path=/data/logdevice/N0:1/socket_admin
```


The `ld-dev-cluster` output suggests several steps. Copy and paste those steps to another terminal to create a log range, write data to
it, and read it using a tailer.

Run `ld-dev-cluster --help` to see all of the possible options. These are particularly useful:
```text
--nnodes      The number of nodes in the cluster
--loglevel    Controls verbosity of the output of the `ld-dev-cluster` utility
--root        If specified, the data will be placed here instead of a temporary directory (and will not be cleaned up on exit)
--param       Additional parameters to pass to every logdeviced instance. For example, `--param loglevel=debug`
--use-tcp     Configure nodes to listen on TCP ports instead of Unix sockets
```
