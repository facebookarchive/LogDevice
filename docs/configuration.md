---
id: Config
title: Cluster configuration
sidebar_label: Cluster configuration
---

The main config file passed to LogDevice server through the `--config-path` command line argument.
The same file can be used to create a client, for which it is passed in the constructor.

A simple config file structure looks as follows:
```
{
  "client_settings": {},
  "server_settings": {},
  "cluster": "your_cluster_name",
  "internal_logs": {},
  "metadata_logs": {},
  "nodes": [],
  "traffic_shaping": {},
  "version": 123,
  "zookeeper": {}
}
```

## Cluster (`cluster`)
The cluster name is used for handshaking between clients and servers. This will enforce that clients and servers use a config file with the same cluster identifier. It is also used as part of the path of znodes in ZooKeeper, so make sure this is different for every tier.

## Internal logs (`internal_logs`)
Internal logs are used for:
- Logs configuration
- Event log

Both of these have a delta and and snapshot log. Their log ranges are hard-coded, but their replication can be [configured dynamically](log_configuration.md).
Since a lot of LogDevice internals depend on these logs, you want higher fault-tolerance than normal data logs.

```
"internal_logs": {
    "config_log_deltas": {
        "extra_copies": 0,
        "max_writes_in_flight": 2000,
        "nodeset_size": 20,
        "replicate_across": {
            "node": 6,
            "rack": 3
        },
        "scd_enabled": false,
        "synced_copies": 0
    },
    "config_log_snapshots": {
        "extra_copies": 0,
        "max_writes_in_flight": 2000,
        "nodeset_size": 20,
        "replicate_across": {
            "node": 6,
            "rack": 3
        },
        "scd_enabled": false,
        "synced_copies": 0
    },
    "event_log_deltas": {
        "extra_copies": 0,
        "max_writes_in_flight": 2000,
        "nodeset_size": 20,
        "replicate_across": {
            "node": 6,
            "rack": 3
        },
        "scd_enabled": false,
        "synced_copies": 0
    },
    "event_log_snapshots": {
        "extra_copies": 0,
        "max_writes_in_flight": 2000,
        "nodeset_size": 20,
        "replicate_across": {
        "replicate_across": {
            "node": 6,
            "rack": 3
        },
        "scd_enabled": false,
        "synced_copies": 0
    }
},
```

## Metadata logs (`metadata logs`)
Metadata log configuration requires an explicit nodeset. This nodeset is crucial to the system and should always be read and write available. Metadata nodeset can be changed as long as a subset of the nodes is still maintained and data is overreplicated manually, but this is generally a dangerous operation.

Replication constraints should **not** be changed without proper caution once the cluster is up and running.

The metadata nodeset should contain as many failure domains as possible to ensure availability.

```
"metadata_logs": {
    "nodeset": [0, 5, 10, 15, 20, 25, 30, 35, 40],
    "replicate_across": {
      "node": 6,
      "rack": 3
    }
  },
```

## Nodes (`nodes`)
Nodes is an array of node objects. An example node object would be:
```
{
    "generation": 1,
    "gossip_port": 4441,
    "host": "127.0.0.1:4440",
    "roles": [
        "sequencer",
        "storage"
    ],
    "sequencer": true,
    "storage": "read-write",
    "storage_weight": 2,
    "location": "abc.def.gh.ij.kl",
    "node_id": 0,
    "num_shards": 15,
    "ssl_port": 4443,
},
```

### Node ID (`node_id`)
A unique identifier for a node. This should not be changed during the lifetime of a host.

### Host (`host`)
Host is an IP:port pair. The port is the data port (non-ssl) to which both nodes and clients connect when SSL is disabled.
For IPv6, the `[::1]:4440` format can be used.

### Location (`location`)
Location is used to encode the failure domain of a the host. This is used for data placement, based on the [log configuration](log_configuration.md)

The format of the string is: `region.datacenter.cluster.row.rack`.
As an example, a log configuration using:
```
"replicate_across": {
      "rack": 3
}
```
can be placed on machines with 3 different rack identifiers (but could share same `region.datacenter.cluster.row`):
- `00.00.00.00.01`
- `00.00.00.00.02`
- `00.00.00.00.03`

Each section can be expressed as a string.

### Roles and state (`roles`, `sequencer`, `sequencer_weight`, `storage` and `storage_weight`)
A distinction should be made between roles and state:
- Role: this remains the same during the lifetime of the node.
- State: could change (for e.g. maintenance operations)

#### Roles / permanent properties
`roles` is defined as a list, and can combine the following options:
- `sequencer`
- `storage`

`storage_weight` defines a proportional value for the amount of data to be stored compared to other machines. When e.g. total disk size is used as weight for machines with variable disk sizes, the storage will be used proportionally.
`sequencer_weight` can similarly define a proportional value for the number of sequencers to be placed on the machine.

#### State
The following variables can be used to perform maintenances / temporarily change the state of a node:

`storage` can have the following values:
- `disabled`: This node does not serve reads nor writes
- `read-only`: This node still serves reads, but not writes
- `read-write`: This node takes both reads and writes

`sequencer` is a boolean value that can be toggled to temporarily disable or enable the sequencer (if the node has the `sequencer` role).

### Gossip port (`gossip_port`)
The (TCP) port LogDevice uses to gossip node state internally. This is required when `gossip-enabled` is `true`.
Gossip is only used between nodes and is not required to be exposed to clients.

### Generation (`generation`)
Generation should be increased when node ids are reused, but IP addresses are changed. Generation is used by LogDevice to infer whether the host is a physically new machine and forces clients to reconnect when bumped.

## Version (`version`)
`version` should be bumped whenever the `node` section has been changed. This property is used to determine whether configuration needs to be synchronized to clients or not (when `enable-config-synchronization` is `true`).
Generally, using current unix epoch timestamp is an easy way to provide incremental version numbers.

## Zookeeper settings (`zookeeper`)
```
"zookeeper": {
    "quorum": [
        "192.168.0.5:2181",
        "192.168.0.6:2181",
        "192.168.0.7:2181",
        "192.168.0.8:2181",
        "192.168.0.9:2181"
    ],
    "timeout": "30s"
}
```

- `quorum` defines the list of IP+port pairs of all the Zookeeper nodes. For IPv6, square brackets can be used (e.g.`[::1]:2183`)
- `timeout` defines the connection timeout.

## Settings (`client_settings` and `server_settings`)
Please check the [settings documentation](settings.md) for a detailed explanation on all the settings.

Settings are expressed as key-value pairs in both `client_settings` and `server_settings`.
For boolean-like values, any of the following can be used:
- `1`, `true`, `True`
- `0`, `false`, `False`

## Traffic shaping (`traffic_shaping`)
TODO
