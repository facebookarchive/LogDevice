---
id: FirstCluster
title: Creating your first cluster
sidebar_label: Creating your first cluster
---

## 1. Provision the servers
The recommended minimum for running a fully functional LogDevice cluster is 4 servers. Each LogDevice server should have at least 4GB of RAM. For best results the network latency between the servers in the cluster should be low.

The only platform currently supported is Ubuntu 18 LTS "Bionic Beaver".

Make sure that network ports specified in the config (`16111` and `16112` for the config file listed below) and the command port (`5440` by default, it's also the only value currently supported by `ldshell`) are open. Also, the nodes have to be able to communicate with ZooKeeper (see below).

**Note:** the setup steps in this simple tutorial will setup a cluster without any security controls. You should restrict network access to these ports using other means (like a firewall or NAT), otherwise your cluster will be open for the world to use.

## 2. Set up a ZooKeeper ensemble
LogDevice requires ZooKeeper in order to store minimal per-log metadata accessed during sequencer activation and failover.  ZooKeeper can also be used as a mechanism for distributing the config file (see the [Configuration section](#5-create-a-configuration-file) below).

You can re-use an existing ZooKeeper ensemble, or create a new one. Different LogDevice clusters can use the same ZooKeeper ensemble as long as they have a different value of the `cluster` attribute.

You can run ZooKeeper on the same machines as LogDevice or on separate hardware. For best results, latency from the LogDevice cluster to the ZooKeeper ensemble should be minimal. However, LogDevice doesn't communicate with ZooKeeper outside of config loading / sequencer activation / failover scenarios, so this shouldn't affect performance in steady state.

You can find a tutorial online on how to set up a proper Zookeeper ensemble. Note that if the metadata in the ZooKeeper ensemble is lost, the data in the cluster will be corrupted and unreadable.

#### Unsafe single-node ZooKeeper setup on Ubuntu
If you are just setting up a test cluster and are OK with it becoming unusable and losing all your data if you lose your ZooKeeper node, you can set up a single-node Zookeeper ensemble easily on Ubuntu by running this:
```sh
sudo apt-get update && sudo apt-get install zookeeper zookeeperd
```

## 3. Build LogDevice and distribute it to the machines
You can follow the steps on the [Installation page](installation.md) to see how LogDevice should be built and installed.

If you are building binaries on one machine and distributing them to the others, make sure every server has dependencies installed. On Ubuntu, you can run this from the root of the git checkout to install them:

```sh
sudo apt-get update && sudo apt-get install -y $(cat logdevice/build_tools/ubuntu.deps)
```

## 4. Create data folders on storage nodes
Storage nodes store data in shards. Typically each shard maps to a different physical disk. The example commands below create a user and a typical folder structure for logdevice, and should be run on each storage node. They assume that your data disk is mounted on `/mnt/data0`. The recommended filesystem to be used for data storage in LogDevice is XFS.
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
An example of a minimal configuration file for 4 nodes and a 3-node ZooKeeper ensemble hosted on the same machine can be found below (and is also included in the source tree under `examples/logdevice.conf`).

Modify it to adapt it to your situation and save it to a file somewhere (e.g. `~/logdevice_test.conf`). The parts that you need to modify are:
1. `host` attribute for every node. It should be an IP:port pair, hostnames are not supported.
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

All LogDevice nodes have to have access to the same configuration file when starting. They will monitor the file for changes and pick up (most) changes without being restarted, so ideally they should have access to it while running as well. There can be different ways to ensure this - e.g. one option is to store the file on a shared network filesystem. However, the simplest way is to store the configuration file in ZooKeeper.

You can save the config file you created to ZooKeeper by running the following (paths assumed from previous steps):
```sh
/usr/share/zookeeper/bin/zkCli.sh create /logdevice_test.conf "`cat ~/logdevice_test.conf`"
```

If you need to modify the config in ZooKeeper after you've created it, use the `set` command instead of `create`, like so:
```sh
/usr/share/zookeeper/bin/zkCli.sh set /logdevice_test.conf "`cat ~/logdevice_test.conf`"
```

## 7. (optional) Install LogDevice as a service
Under Ubuntu Bionic, you can install the service by populating `/etc/systemd/system/logdevice.service` on every node with this (note that you should change the zookeeper quorum to the one that holds your config, as well as the port and data storage path if they are different in your deployment):
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

You can then enable the service by running this:
```sh
sudo systemctl daemon-reload
sudo systemctl enable logdevice
```

## 8. Start logdeviced on every node
If you used the instructions above to install a service, you can just run
```sh
sudo systemctl start logdevice
```

If you didn't, you can start logdevice like this (substitute zookeeper quorum for the one that holds the config, and the port and data storage paths if you used different ones):
```sh
cd /data/logdevice/
sudo -u logdevice nohup /usr/local/bin/logdeviced --config-path zk:10.0.0.1:2181,10.0.0.2:2181,10.0.0.3:2181/logdevice_test.conf --port=16111 --local-log-store-path=/data/logdevice/ &
```

## 9. Check it's running
`ldshell` is a utility to introspect and mutate the state of LogDevice. One of its most powerful commands is `query`, which allows you to run SQL `SELECT` queries against [various virtual SQL tables](ldquery.md) that expose the state of the cluster and some of its internals. The following command queries the `info` table which will show you the start time of the nodes that are up, their pids and a couple more fields: 
```sh
# export the config path (change the Zookeeper quorum to yours here) so it's available for subsequent commands
export LOGDEVICE_CONFIG=zk:10.0.0.1:2181,10.0.0.2:2181,10.0.0.3:2181/logdevice_test.conf
ldshell -c $LOGDEVICE_CONFIG query 'SELECT * FROM info'
```

## 10. Create log ranges using ldshell
Before you can write any data to LogDevice logs, you need to create them. By default the cluster starts with an empty log list. To add a log range called `test_logs` with 100 logs where the data would be replicated across 2 nodes and trimmed after 3 days, run this command:
```sh
ldshell -c $LOGDEVICE_CONFIG logs create --replicate-across "node: 2" --backlog "259200s" --from 1 --to 100 /test_logs
```
More information about managing your log configuration is available [here](log_configuration.md)

## 11. Write data into a log
All ready to go! You can now write your first payload into log 1:
```sh
echo payload | ./LogDevice/_build/bin/ldwrite $LOGDEVICE_CONFIG -l 1
```

## 12. Read data from a log
This command will run the tailer for log 1. If you did the write above, it should print out "payload":
```sh
./LogDevice/_build/bin/ldtail $LOGDEVICE_CONFIG -l 1 --follow
```

## All done!
Congratulations! Now you have a running LogDevice cluster! You can now read the [Introduction to the API](api.md) to learn how to use it.
