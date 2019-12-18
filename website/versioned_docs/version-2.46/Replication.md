---
id: version-2.46-Replication
title: Log replication configuration
sidebar_label: Log replication configuration
original_id: Replication
---

The sequencer gets an epoch number and log replication configuration from the epoch store when it is activated. Each log has a metadata log associated with it that stores the epoch and replication configuration history.

##  Replication configuration

A simplified view of the replication configuration looks like this:
![replication property](assets/writepath/replication_property.png)

## Epoch store

The epoch store is a consistent metadata store that provides 2 functions:

* Provide monotonically increasing epoch numbers. During sequencer activation, the epoch store provides epoch numbers through read-modify-write transactions. The sequencer retrieves the current number and bumps the number.
* Store the current replication configuration for each log. It also stores the last *clean* (fully recovered) epoch (LCE) of the log.

The LogDevice epoch store is currently implemented using Zookeeper.

## Log epoch segments

Log reconfiguration prompts a new sequencer to be activated (or an existing one to be reactivated). When a sequencer activates, it gets the new epoch number. It also creates a new epoch segment with an immutable configuration for the log.

![epoch segments of a log](assets/replication/epoch_segments_of_a_log.png)

<center>**Epoch segments of a log**</center>

## Metadata log

Each log is paired with a short metadata log that contains the current and historical replication configuration of the (data) log. Epoch numbers are used as an index into this historical replication configuration. The metadata log is immutable.

![historical epoch segments](assets/replication/historical_epoch_segments.png)

<center>**A simplified replication configuration of a log**</center>

Sequencers usually have a cached view of the log's historical metadata content.

## Cluster membership

So far we have talked about per-log configuration. But this doesn’t allow for changes to the effective nodeset that may happen later, such as removing nodes from the cluster due to draining or rebuilding. These changes cause the node storage state in the historical epoch configuration to be updated.

Cluster membership describes the storage state for storage nodes that are in the effective nodeset. We use the term storage node, but this could be also “storage device” or “storage shard”.
![replication configuration](assets/replication/replication_configuration.png)

<center>**The replication configuration of a log, including storage state**</center>

Unlike the replication properties, cluster membership is mutable and dynamic. It is driven by maintenance activities such as draining and rebuilding. Storage states are, for example, read only, data migration, and read write.

The source of truth for cluster membership is the cluster membership state machine. LogDevice components, including clients and storage nodes, subscribe to membership updates via the state machine. They use the cluster membership and the nodeset for a (log, epoch) to calculate the effective nodeset.
![log replication configuration](assets/replication/log_replication_configuration.png)
