---
id: version-2.46.5-FailureDetection
title: Failure detection
sidebar_label: Failure detection
original_id: FailureDetection
---

The nodes in a LogDevice cluster exchange health messages, using a gossip protocol, in order to maintain a list of ALIVE and DEAD nodes. If one node crashes, or if a rack loses power and 10 nodes are suddenly unavailable, the other nodes detect this quickly and automatically.

Gossip protocol refers to a way of periodically exchanging messages and sharing information among peers. All sequencer and storage nodes in the cluster send and receive messages that include the sender's view of the state of every other node in the cluster.

All nodes run a failure detection algorithm to update their view of all the other nodes in the cluster and so arrive at a consensus view of the cluster. FailureDetector can be run on a dedicated thread so that its high priority messages can be unaffected by head-of-line blocking on other threads.

## How failure detection is used

The view of the cluster that each node generates locally is used in a number of situations. Some important use cases are:

* Sequencer routing. The client gets the list of nodes in the cluster from the configuration file, and it asks any one of the nodes in the cluster for its list of ALIVE and DEAD nodes. The client calls a consistent hash function with the list of live nodes and the log id to determine the primary sequencer for the log. When a server node receives an APPEND from the client, it runs the same consistent hash function, with its own view of live nodes, to confirm its role as sequencer for the log.
* Copyset selection. The sequencer uses its view of the storage nodes, along with the log replication property, when determining the copyset for a record.
* Rebuilding. If a storage node in the cluster doesn't respond via gossip for some time (20 minutes by default), rebuilding needs to start. Rebuilding is triggered when one node in a cluster writes a record to the event log. Each node determines whether it should write the record by looking at its view of live nodes; the node with the lowest node ID writes the record.

## Gossip protocol

Every `gossip-interval` (100ms by default), each node in the cluster sends its view of the cluster to a randomly-chosen node in the same cluster. This is an indirect update. The receiving node runs the failure detector algorithm locally using the reported cluster view, the time of the message, and the sender as inputs, and updates its own view.

The receiving node doesn't always change its view based on indirect updates. It's possible that the received status for any one node is older than the existing entry in the recipient. For example, if N20 gets an indirect report that N3 was alive 5 heartbeats ago, and it last heard from N3 15 heartbeats ago, then it updates its view. But if N20 has a report that N3 was alive 16 heartbeats ago, then it ignores the update since it knows N3 was also alive more recently (15 heartbeats ago).

The heartbeat is incremented every 100 ms.

A `GOSSIP` message includes:

* The number of nodes in the cluster.
* The sender's node ID.
* Flags including BringUp (node is starting up) and Failover (node is shutting down).
* gossip list G[]: G[i] is the number of heartbeats since some node in the cluster received a gossip from node ‘i’.
* sending time: To detect delays on receiving side.
* gossip_ts[]: Monotonically increasing instance-ids. This helps detect new instances of LogDevice, for example, after a crash or restart. The id is currently implemented as the timestamp when logdeviced started.
* failover_list: Nodes that are going down.

## Marking a node as dead or alive

A node Nx is marked as dead in the view of node Ny:

* If Ny's view has not seen a gossip from Nx for `gossip-threshold` intervals (default is 30). By default, this translates to 3 seconds (assuming  `gossip-interval` is 100ms).
* If Nx is marked as failing over in any incoming gossip message's failover_list.

At the start of shutdown, the node populates itself as failing over in the failover_list[]. This means that other nodes will know about it as soon as they receive a gossip message directly (from the node itself) or indirectly (from another node) with this information.

If a node is marked dead, the other nodes attempt to contact it but less frequently.

A node Nx is marked alive in the view of node Ny:

* If Ny receives a "BringUp" message from Nx. On startup, a node sends an initial broadcast declaring it as coming up.
* If Nx sends a regular gossip message.

## Viewing gossip state

To see the gossip state of one node in the cluster:

``` bash
%echo info gossip | nc -U /dev/shm/my_cluster/N0:1/socket_command
GOSSIP N0 ALIVE (gossip: 0, instance-id: 1556044004841, failover: 0, starting: 0, state: ALIVE) -
GOSSIP N1 ALIVE (gossip: 2, instance-id: 1556044004629, failover: 0, starting: 0, state: ALIVE) -
GOSSIP N2 ALIVE (gossip: 0, instance-id: 1556044004859, failover: 0, starting: 0, state: ALIVE) -
GOSSIP N3 ALIVE (gossip: 1, instance-id: 1556044004728, failover: 0, starting: 0, state: ALIVE) -
GOSSIP N4 ALIVE (gossip: 2, instance-id: 1556044004831, failover: 0, starting: 0, state: ALIVE) -
DOMAIN_ISOLATION enabled true
DOMAIN_ISOLATION NODE NOT_ISOLATED
DOMAIN_ISOLATION RACK NOT_ISOLATED
DOMAIN_ISOLATION ROW NOT_ISOLATED
DOMAIN_ISOLATION CLUSTER NOT_ISOLATED
DOMAIN_ISOLATION DATA_CENTER NOT_ISOLATED
DOMAIN_ISOLATION REGION NOT_ISOLATED
ISOLATED false
END
```

## Settings

[Settings for failure detection and gossip.](settings.md#failure-detector)
