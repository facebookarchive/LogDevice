---
id: ClusterMaintenance
title: Cluster Maintenance Operations
sidebar_label: Cluster Maintenance
---
## Bootstrapping the cluster
This is an important first step to do once you create a cluster. Once you start
the minimum number of nodes that you require to operate a cluster based on your
configuration. You will need to run this command:
<!--DOCUSAURUS_CODE_TABS-->
<!--CLI-->
```shell-session
ldshell -s <admin-server-host> nodes-config bootstrap --metadata-replicate-across="node:3"
```
<!--Interactive-->
```shell-session
root@logdevice> nodes-config bootstrap metadata-replicate-across={node:3}
```
<!--END_DOCUSAURUS_CODE_TABS-->
This will allow the nodes to finish provisioning (finish transitioning to
`ENABLED`) and will tell maintenance manager about your required metadata
replication.

> Note that metadata logs replication property cannot be changed later. This is
> a critical decision that you need to carefully think about. A `node:3`
> metadata replication on a 3-node cluster means that you cannot lose **any**
> nodes, ever!

## Cluster Status
You can use [ldshell](ldshell.md) to get the cluster status via the `status`
command.

```shell-session
ldshell -s <admin-server-host> status
```

![LDShell Screenshot](assets/ldshell-status-1.png "LDShell status example")

The status command will show the current and target states for the operational
state of shards and sequencers. This is especially useful if you want to know if
we have active maintenances and what are their status.

![LDShell Screenshot](assets/ldshell-status-2.png "LDShell status example")

As you can see under the `SHARD OP.` (Shard Operational State) our current state
is `ENABLED(1)` which means that we have 1 shard in `ENABLED` state and we have
a target to become `MAY_DISAPPEAR(1)` for the same shard but we are
`BLOCKED_UNTIL_SAFE`. 

If we run the command `maintenance show` in ldshell, we will be able to see the
reason.

![LDShell Screenshot](assets/ldshell-maintenance-capacity-1.png "LDShell maintenance example")

Oh, our maintenance is blocked because we will cause `SEQUENCING_CAPACITY_LOSS`
and `STORAGE_CAPACITY_LOSS`. Check out [Safety Checker](safety_checker.md) to
figure out how to configure the cluster to allow more capacity loss.

> Hint: By default, we cannot lose more than 25% of the capacity unless we
> configure the cluster to allow more. Since this is a 3-node cluster, losing a
> single node means losing ~33.3% of the capacity which is not allowed.

## Expanding the cluster
Expanding a logdevice cluster is as simple as running more `logdeviced`
instances, nodes will automatically register themselves into the cluster and
[Maintenance Manager](admin_server.md#the-maintenance-manager) will make sure
that these nodes are `ENABLED` once they become alive.

Check out an [example `logdeviced` invocation with
self-registration](../configuration.md#nodes-nodes).

## Applying maintenances
> Make sure that you carefully read the [Maintenance
> Manager](admin_server.md#the-maintenance-manager) for detailed explanation on
> what the different states and arguments mean.

### Applying an unsafe MAY_DISAPPEAR maintenance on a single node
<!--DOCUSAURUS_CODE_TABS-->
<!--CLI-->
```shell-session
ldshell -s <admin-server-host> maintenance apply --node-indexes=1
--reason="Testing MM"
```
<!--Interactive-->
```shell-session
root@logdevice> maintenance apply node-indexes=1 reason="Testing MM"
```
<!--END_DOCUSAURUS_CODE_TABS-->

> Note: In this example cluster we configured the
> `max-unavailable-storage-capacity` and `max-unavailable-sequencing-capacity`
> to `50` to allow losing half the cluster so that capacity checking doesn't get
> in our way.

The output in our example case (assuming our k8s sample cluster):
![LDShell Screenshot](assets/ldshell-maintenance-rebuilding-stall-1.png "LDShell maintenance example")
As we can see, the `Impact Result: REBUILDING_STALL`. 
Now, why is that happening? You probably assumed that by default it's safe to
take one node down in a 3-node cluster, right? This is why we need [Maintenance
Manager](admin_server.md#the-maintenance-manager). Checkout [Why do we need a
safety checker](safety_checker.md#why-do-we-need-a-safety-checker) to learn
more.

Luckily, there is a way to understand why. Let's run this command:
<!--DOCUSAURUS_CODE_TABS-->
<!--CLI-->
```shell-session
ldshell -s <admin-server-host> maintenance show --show-safety-check-results
--reason="Testing MM"
```
<!--Interactive-->
```shell-session
root@logdevice> maintenance show show-safety-check-results=True
```
<!--END_DOCUSAURUS_CODE_TABS-->
Here is a snippet of the output

```text
Safety Check Impact:

CRITICAL: Internal Logs are affected negatively!

Log: 4611686018427387898 (maintenance_log_snapshots)
Epoch: 1
Storage-set Size: 3
Replication: {node: 3}
Write/Rebuilding availability: 3 → 2  (we need at least 3 nodes that are healthy, writable, and alive.)
Read availability: We can't afford losing more than . Nodes must be healthy, readable, and alive.
Impact: REBUILDING_STALL
+------------------------+--------------------------------------+------------------------+
| global.uk.k8s.nw.rk1.0 | global.uk.k8s.nw.rk1.1               | global.uk.k8s.nw.rk1.2 |
+------------------------+--------------------------------------+------------------------+
| N0:S0    READ_WRITE    | N1:S0    READ_WRITE → MAY_DISAPPEAR  | N2:S0    READ_WRITE    |
+------------------------+--------------------------------------+------------------------+
```
It turns out that the [internal logs](../configuration.md#internal-logs-internal_logs)
are configured with `Replication: {node: 3}` which means that we cannot
rebuild historical data if we don't have 3 writeable nodes in that nodeset.
Since the entire cluster is 3 nodes we can't really stop any node or we won't
be able to re-replicate the under-replicated data!

We can update the configuration file to `"node": 2` instead but this will not
fix the historical data. We will have to wait for automatic snapshotting and
trimming of internal logs before our maintenance is good to go.

### Applying an MAY_DISAPPEAR maintenance on a single node
Assuming the scenario where our cluster is correctly configured to allow
losing 1 node.
<!--DOCUSAURUS_CODE_TABS-->
<!--CLI-->
```shell-session
ldshell -s <admin-server-host> maintenance apply --node-indexes=3
--reason="will restart"
```
<!--Interactive-->
```shell-session
root@logdevice> maintenance apply node-indexes=3 reason="will restart"
```
<!--END_DOCUSAURUS_CODE_TABS-->

![LDShell Screenshot](assets/ldshell-maintenance-safe-1.png "LDShell maintenance example")

After a couple of minutes, if we run the _exact same command_ again, or if we
used `maintenance show`. We should see the maintenance status similar to this:

![LDShell Screenshot](assets/ldshell-maintenance-completed-1.png "LDShell maintenance example")

And in `ldshell status` we will see the current state of the sequencer to be
`DISABLED` and the shard operational state is set to `MAY_DISAPPEAR`.

![LDShell Screenshot](assets/ldshell-status-completed-maintenance-1.png "LDShell maintenance example")
Only now we can safely restart this node!
### Removing a maintenance
We can easily remove a maintenance by using the `maintenance remove` which can
remove multiple maintenance at the same time if you didn't supply filters to it.

For our maintenance, this is how we will remove it:
<!--DOCUSAURUS_CODE_TABS-->
<!--CLI-->
```shell-session
ldshell -s <admin-server-host> maintenance remove --node-indexes=3
--reason="restart completed"
```
<!--Interactive-->
```shell-session
root@logdevice> maintenance remove node-indexes=3 reason="restart completed"
```
<!--END_DOCUSAURUS_CODE_TABS-->
After confirmation, the maintenance will be removed and the node will go back to
`ENABLED` after a couple of seconds.

### Draining a couple of nodes
<!--DOCUSAURUS_CODE_TABS-->
<!--CLI-->
```shell-session
ldshell -s <admin-server-host> maintenance apply \
         --node-indexes 3 4 \
         --shard-target-state=drained \
         --reason "will shrink"
```
<!--Interactive-->
```shell-session
root@logdevice> maintenance apply node-indexes=[3, 4] shard-target-state=drained reason="will shrink"
```
<!--END_DOCUSAURUS_CODE_TABS-->

![LDShell Screenshot](assets/ldshell-maintenance-drain-two-nodes-1.png "LDShell maintenance example")
This will trigger data rebuilding, and the nodes will move into `MIGRATING_DATA`
and as shards finish you will see one by one the shards go to `DRAINED`. But the
maintenance `Overall Status` will transition to `COMPLETED` **only** when all
requested shards finish rebuilding and transition to `DRAINED`.

![LDShell Screenshot](assets/ldshell-maintenance-drain-two-nodes-2.png "LDShell maintenance example")

## Shrinking the cluster
In order to shrink the cluster, you need first to apply `DRAINED` maintenances
on the storage nodes that you want to remove along with disabling any sequencers
running on these nodes. See [Applying maintenances](#applying-maintenances) for
reference.

> Note that draining the nodes might take quite sometime since we need to
finish data rebuilding before setting the state to `DRAINED`.

Once these maintenances are `COMPLETED`, we know that it's safe to remove the
nodes. The nodes *must* be stopped (by making sure that `logdeviced` is not
running on these nodes). Only then, you can run the shrink command:

This assumes that the node ID for the node you want to remove is `5`
```shell-session
ldshell -s <admin-server-host> nodes-config shrink --node-indexes=5
Successfully removed the nodes
```
