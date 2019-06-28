---
id: FirstCluster
title: Creating your first cluster
sidebar_label: Creating your first cluster
---

## 1. Provision the servers
The recommended minimum for running a fully functional LogDevice cluster is 4 servers. If you want to try out LogDevice on a single machine for development or experimental purposes, you can follow a much simpler process to [run a local cluster](localcluster.md) instead.

Each LogDevice server should have at least 4GB of RAM. For best results the network latency between the servers in the cluster should be low.

The only platform currently supported is Ubuntu 18 LTS "Bionic Beaver".

Make sure that network ports specified in the config (`16111` and `16112` for the config file listed below) and the command port (`5440` by default, it's also the only value currently supported by `ldshell`) are open. Also, the nodes have to be able to communicate with ZooKeeper.

**Note:** This simple tutorial sets up a cluster without any security controls. You should restrict network access to these ports using other means (like a firewall or NAT) to prevent your cluster being open to the world.

## 2. Set up a ZooKeeper ensemble
LogDevice requires ZooKeeper in order to store minimal per-log metadata accessed during sequencer activation and failover.  ZooKeeper can also be used as a mechanism for distributing the config file (see the [Configuration section](#5-create-a-configuration-file) below).

You can re-use an existing ZooKeeper ensemble, or create a new one. Different LogDevice clusters can use the same ZooKeeper ensemble as long as they have different values in the `cluster` attribute in the LogDevice configuration file.

You can run ZooKeeper on the same system as LogDevice or on separate hardware. For best results, latency from the LogDevice cluster to the ZooKeeper ensemble should be minimal. However, LogDevice doesn't communicate with ZooKeeper outside of config loading / sequencer activation / failover scenarios, so having higher latency to ZooKeeper shouldn't affect performance in steady state.

You can find a tutorial online on how to set up a proper Zookeeper ensemble. Note that if the metadata in the ZooKeeper ensemble is lost, the data in the cluster will be corrupted and unreadable.

#### Unsafe single-node ZooKeeper setup on Ubuntu
If you are just setting up a test LogDevice cluster and are OK with it becoming unusable and losing all your data if you lose your ZooKeeper node, you can set up a single-node Zookeeper ensemble easily on Ubuntu by running this:
```sh
sudo apt-get update && sudo apt-get install zookeeper zookeeperd
```

## 3. Distribute LogDevice to the servers
You can use the Docker image or build LogDevice yourself.
### Option 1. Use Docker image
Get the LogDevice Docker image.
```sh
docker pull facebookincubator/logdevice
```

### Option 2. Build LogDevice
Follow the steps at [Build LogDevice](installation.md) to build and install LogDevice.

If you are building binaries on one machine and distributing them to the others, make sure every server has dependencies installed. On Ubuntu, you can run this command from the root of the git checkout to install them:

```sh
sudo apt-get update && sudo apt-get install -y $(cat logdevice/build_tools/ubuntu.deps)
```


## 4. Create data folders on storage nodes
Storage nodes store data in shards. Typically each shard maps to a different physical disk. The example commands below create a user and a typical folder structure for LogDevice, and should be run on each storage node. They assume that your data disk is mounted on `/mnt/data0`. The recommended filesystem to be used for data storage in LogDevice is XFS.
```sh
# creates the root folder for data
sudo mkdir -p /data/logdevice/

# writes the number of shards that this box will have
echo 1 | sudo tee /data/logdevice/NSHARDS

# creates symlink for shard 0
sudo ln -s /mnt/data0 /data/logdevice/shard0

# Adds the user for the logdevice daemon
sudo useradd logdevice

# changes ownership for the data directory and the disk
sudo chown -R logdevice /data/logdevice/
sudo chown -R logdevice /mnt/data0/
```

## 5. Create a configuration file
An example of a minimal configuration file for 4 nodes and a 3-node ZooKeeper ensemble hosted on the same machine can be found below (and is also included in the source tree under `logdevice/examples/logdevice.conf`).

Modify it to adapt it to your situation and save it to a file somewhere (e.g. `~/logdevice_test.conf`). The parts that you need to modify are:
1. `host` attribute for every node. It should be an `IPv4:port` or `[IPv6]:port` pair. Hostnames are not supported.
2. `quorum` in the `zookeeper` section - list of ZooKeeper ensemble nodes and ports.
3. (optional) The `cluster` attribute if you are creating several clusters - it has to be unique for each cluster sharing a ZooKeeper ensemble!
5. (optional) `gossip_port` for every node if you intend to use a different one.
6. (optional) `num_shards` for every node if you intend to use more than one disk for data on that machine.

If you are adding more nodes to the cluster, make sure they all have unique `node_id` values and consider expanding the `nodeset` under the `metadata_logs` section to include them as well.

Detailed explanations of all the attributes can be found in the [Cluster configuration](configuration.md) docs.

```js
{
	"cluster": "test",
	"nodes": [
		{
			"node_id": 0,
			"host": "10.0.0.1:16111",
			"gossip_port": 16112,
			"generation": 1,
			"roles": [ "sequencer", "storage" ],
			"sequencer": true,
			"storage": "read-write",
			"num_shards": 1
		},
		{
			"node_id": 1,
			"host": "10.0.0.2:16111",
			"gossip_port": 16112,
			"generation": 1,
			"roles": [ "sequencer", "storage" ],
			"sequencer": true,
			"storage": "read-write",
			"num_shards": 1
		},
		{
			"node_id": 2,
			"host": "10.0.0.3:16111",
			"gossip_port": 16112,
			"generation": 1,
			"roles": [ "sequencer", "storage" ],
			"sequencer": true,
			"storage": "read-write",
			"num_shards": 1
		},
		{
			"node_id": 3,
			"host": "10.0.0.4:16111",
			"gossip_port": 16112,
			"generation": 1,
			"roles": [ "sequencer", "storage" ],
			"sequencer": true,
			"storage": "read-write",
			"num_shards": 1
		}
	],
	"internal_logs": {
		"config_log_deltas": {
			"replicate_across": {
				"node": 3
			}
		},
		"config_log_snapshots": {
			"replicate_across": {
				"node": 3
			}
		},
		"event_log_deltas": {
			"replicate_across": {
				"node": 3
			}
		},
		"event_log_snapshots": {
			"replicate_across": {
				"node": 3
			}
		}
	},
	"metadata_logs": {
		"nodeset": [0,1,2,3],
		"replicate_across": { "node": 3 }
	},
	"zookeeper": {
		"quorum": [
			"10.0.0.1:2181",
			"10.0.0.2:2181",
			"10.0.0.3:2181"
		],
		"timeout": "30s"
	}
}
```

## 6. Store the configuration file

All LogDevice nodes have to have access to the same configuration file when starting. They monitor the file for changes and pick up (most) changes without being restarted, so ideally they should have access to it while running as well. There are different ways to ensure this. One option is to store the file on a shared network filesystem. The simplest way is to store the configuration file in ZooKeeper.

You can save the config file you created to ZooKeeper by running the following (paths assumed from previous steps):
```sh
/usr/share/zookeeper/bin/zkCli.sh create /logdevice_test.conf "`cat ~/logdevice_test.conf`"
```

If you need to modify the config in ZooKeeper after you've created it, use the `set` command instead of `create`:
```sh
/usr/share/zookeeper/bin/zkCli.sh set /logdevice_test.conf "`cat ~/logdevice_test.conf`"
```

## 7. (optional) Install LogDevice as a service
**Note:** This step is not needed if you are using the Docker image.

Under Ubuntu Bionic, you can install the service by populating `/etc/systemd/system/logdevice.service` on every node.
Change the Zookeeper quorum, the port, and data storage paths to your values as needed.
```text
[Unit]
Description=LogDevice

[Service]
ExecStart=/usr/local/bin/logdeviced --config-path zk:10.0.0.1:2181,10.0.0.2:2181,10.0.0.3:2181/logdevice_test.conf --port=16111 --local-log-store-path=/data/logdevice/
StandardError=journal
Restart=always
TimeoutStopSec=120
Type=simple
KillMode=process
User=logdevice

[Install]
WantedBy=multi-user.target
```

Next, enable the service by running:
```sh
sudo systemctl daemon-reload
sudo systemctl enable logdevice
```

## 8. Start logdeviced on every node (3 options)

**If you are using the Docker image:**
```sh
docker run -d -p 16111:16111 -p 16112:16112 -p 5440:5440 -v /data/logdevice:/data/logdevice facebookincubator/logdevice /usr/local/bin/logdeviced --config-path zk:10.0.0.1:2181,10.0.0.2:2181,10.0.0.3:2181/logdevice_test.conf --port=16111 --local-log-store-path=/data/logdevice/
```
* ```-p``` exports the ports that LogDevice uses outside the container
* ```-v``` mounts the host data volume inside the container.

Change the Zookeeper quorum, the port, and data storage paths to your values as needed.

**If you installed LogDevice as a service,** you can run
```sh
sudo systemctl start logdevice
```

**If neither of the above,** you can start LogDevice like this:
```sh
cd /data/logdevice/
sudo -u logdevice nohup /usr/local/bin/logdeviced --config-path zk:10.0.0.1:2181,10.0.0.2:2181,10.0.0.3:2181/logdevice_test.conf --port=16111 --local-log-store-path=/data/logdevice/ &
```


## 9. See if it's running
`ldshell` is a utility to introspect and mutate the state of LogDevice. One of its most powerful commands is `query`, which allows you to run SQL `SELECT` queries against [various virtual SQL tables](ldquery.md) that expose the state of the cluster and some of its internals. The following command queries the `info` table. The output displays the start time of the nodes that are up, their pids and a couple more fields:
```sh
# export the config path (change the Zookeeper quorum to yours here) so it's available for subsequent commands
export LOGDEVICE_CONFIG=zk:10.0.0.1:2181,10.0.0.2:2181,10.0.0.3:2181/logdevice_test.conf
ldshell -c $LOGDEVICE_CONFIG query 'SELECT * FROM info'
```

## 10. Create log ranges using ldshell
Before you can write any data to LogDevice logs, you need to create them. By default, the cluster starts with an empty log list. To add a log range called `test_logs` with 100 logs where the data would be replicated across 2 nodes and trimmed after 3 days, run this command:
```sh
ldshell -c $LOGDEVICE_CONFIG logs create --replicate-across "node: 2" --backlog "259200s" --from 1 --to 100 /test_logs
```
More information about managing your log configuration is available [here](log_configuration.md).

## 11. Write data into a log
All ready to go! You can now write your first payload into log 1:
```sh
echo payload | ./LogDevice/_build/bin/ldwrite $LOGDEVICE_CONFIG -l 1
```

## 12. Read data from a log
This command runs the tailer for log 1. If you did the write above, it should print out "payload".
```sh
./LogDevice/_build/bin/ldtail $LOGDEVICE_CONFIG -l 1 --follow
```

## All done!
Congratulations! You have a running LogDevice cluster! Read the [Introduction to the API](API_Introduction.md) to learn how to use it.
