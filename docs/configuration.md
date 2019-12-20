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
  "zookeeper": {}
}
```

## Cluster (`cluster`)
The cluster name is used for handshaking between clients and servers. This will enforce that clients and servers use a config file with the same cluster identifier. It is also used as part of the path of znodes in ZooKeeper, so make sure this is different for every tier.

## Internal logs (`internal_logs`)
Internal logs are used for:
- Logs configuration
- Event log
- Maintenances

Both of these have a delta and and snapshot log. Their log ranges are hard-coded.
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
            "node": 6,
            "rack": 3
        },
        "scd_enabled": false,
        "synced_copies": 0
    }
    "maintenance_log_deltas": {
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
    "maintenance_log_snapshots": {
        "extra_copies": 0,
        "max_writes_in_flight": 2000,
        "nodeset_size": 20,
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
Metadata log configuration requires an explicit nodeset. This nodeset is crucial
to the system and should always be read and write available. Metadata nodeset is
dynamically reconfigured by the [Maintenance
Manager](administration/admin_server.md#the-maintenance-manager) and the
replication property cannot be modified after the initial bootstrapping of the
server via ldshell.

```shell-session
ldshell -s localhost nodes-config bootstrap --metadata-replicate-across="node:3"
```

The metadata nodeset should contain as many failure domains as possible to ensure availability.


## Nodes (`nodes`)
The nodes configuration is dynamically stored in Zookeeper should not be
modified directly, however, `ldshell nodes-config edit` command is available in
case of emergency.

LogDevice nodes register themselves dynamically into the cluster on startup
using a feature that's enabled by the server setting 
`--enable-node-self-registration`. In general, we strongly advise that you
include it always in your configuration file.
```
"server_settings": {
  "enable-node-self-registration": "true",
  "enable-nodes-configuration-manager": "true",
  "enable-cluster-maintenance-state-machine": "true",
  "use-nodes-configuration-manager-nodes-configuration": "true"
},
"client_settings": {
  "enable-nodes-configuration-manager": "true",
  "use-nodes-configuration-manager-nodes-configuration": "true",
  "admin-client-capabilities": "true"
}
```

The following is a comprehensive example on `logdeviced` arguments that can be
used to configure the node on startup

```shell-session
logdeviced \
    # required arguments
    --config-path <path> \
    --name <node-name> \ # must be unique in the cluster
    --address <IP Address> \ # Used by others to connect to this node
    --local-log-store-path /data/logdevice \ # Where should we store data
    # optional arguments
    --roles storage,sequencer
    --port 4440
    --gossip-port 4441
    --admin-port 6440
    --location us1.west1
    --storage-capacity 1
    --sequencer-weight 1
    --num-shards 1 # MUST be the same value for all nodes in the cluster
```

The self-registration works by first looking up the nodes configuration by
node `name`. If the node exists, then the rest of the properties of the node
will be updated from the supplied command-line arguments. If the node `name`
is new, a new [Node ID](#node-id-node_id) is automatically assigned to this node
and the cluster will automatically expand to include this node.

> Note that the newly added node will by default be added with the state
> `PROVISIONING` until the daemon finishes its self-test on startup, then the
> state will be `NONE`. The [Maintenance
> Manager](administration/admin_server.md#the-maintenance-manager) will notice
> that we have a newly added node to the cluster and since there are no
> maintenances applied, it will drive its operational state to `ENABLED`. Only
> then it will start receiving reads and writes.

Nodes can be removed from the cluster **only** if the have a `COMPLETED`
maintenance that sets all the shards of that node to `DRAINED`. See
[Cluster Maintenance](administration/maintenances.md) for reference.

### Node ID (`node_id`)
A unique identifier for a node. This should not be changed during the lifetime 
of a host. The nodes ID is sometime referred to as the `node-index`, this is
particularly true in ldshell. You may also see this is written in the format
`N0` where `0` is the `node-index`/`node_id`. We also use this notation in cases
where referencing a certain shard is necessary. In that case you will see
something like this `N0:S0`. This is shard `0` of node `0`.

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

`storage-capacity` defines a proportional value for the amount of data to be stored compared to other machines. When e.g. total disk size is used as weight for machines with variable disk sizes, the storage will be used proportionally.
`sequencer-weight` can similarly define a proportional value for the number of sequencers to be placed on the machine.

### Gossip port (`gossip_port`)
The (TCP) port LogDevice uses to gossip node state internally. This is required when `gossip-enabled` is `true`.
Gossip is only used between nodes and is not required to be exposed to clients.

## Zookeeper settings (`zookeeper`)
```
"zookeeper": {
    "zookeeper_uri": "ip://192.168.0.5:2181,192.168.0.6:2181",
    "timeout": "30s"
}
```

- `zookeeper_uri` defines the list of IP/Name+port pairs of the Zookeeper nodes. For IPv6, square brackets can be used (e.g.`[::1]:2183`)
- `timeout` defines the connection timeout.

## Settings (`client_settings` and `server_settings`)
Please check the [settings documentation](settings.md) for a detailed explanation on all the settings.

Settings are expressed as key-value pairs in both `client_settings` and `server_settings`.
For boolean-like values, any of the following can be used:
- `1`, `true`, `True`
- `0`, `false`, `False`

## Traffic shaping (`traffic_shaping`)
See [Traffic Shaping](TrafficShaping.md) detailed documentation on how to configure this.
