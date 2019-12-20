---
id: SafetyChecker
title: Operations Safety Checker
sidebar_label: Operations Safety Checker
---
The safety checker is an internal component that runs as part of the [Admin
Server](administration/admin_server.md) and is used extensively by the [Maintenance
Manager](administration/admin_server.md#the-maintenance-manager) to decide whether maintenance
requests should proceed or not.

## Why do we need a safety checker?
There are several reasons that make it non-intuitive to decide on
which nodes can safely be taken down in a cluster in LogDevice:
1. [Non-deterministic record
   placement](concepts.md#non-deterministic-record-placement) means that for a
   given log, there can be a certain historical epoch where there are no
   enough readable or writeable nodes after a given operation is applied. This
   means that we will lose read-availability, or we won't be able to rebuild
   the historical data since we **require** that we rebuild the data on the
   same historical nodeset. As of now we don't support amending historical
   nodesets (Immutable historical nodesets).
2. [Log attributes can change](log_configuration.md#updating-attributes) at any
   time: This means that you might think that the current log attributes show
   that your logs are replicated across multiple racks but this doesn't tell
   you anything about the historical/old data that was written before you
   perform that change. As part of the LogDevice design, we only apply log
   attribute changes on the newly written data. This means that you need to
   perform a check on the historical epochs to be truly sure.
3. There might be a temporary under-replication on the cluster at the moment:
   LogDevice will perform what we refer to as [mini
   rebuilding](../Rebuilding.md#mini-rebuilding) in case we
   detect that `logdeviced` has crashed and _might_ have not performed a
   _flush_ on the last partition (rocksdb's memtable). In this case we
   need to run a time-range quick rebuilding to re-replicate that data to
   ensure data durability. During that time we want to make sure that we don't
   lose availability that would put this data rebuilding operation at
   risk.

### The Metadata Cache
As mentioned above, we need to perform a comprehensive check to
verify that for every log, every historical epoch's nodeset will have enough
nodes so that we maintain the read/write/rebuilding availability
also without losing nodes that we would lose data. Luckily, we don't need to
actually read the records in order to determine that, we only need to read the
metadata for these epochs (typically, few epochs per log).

Safety checker will perform a full scan of the epoch metadata for all logs
periodically on the admin server (controlled by
`enable-safety-check-periodic-metadata-update` setting). Even if this setting is
not set to `true`, safety checker will consider its metadata stale and will
perform a re-scan after `safety-check-metadata-update-period` (defaults to 10
minutes). We recommend enabling `enable-safety-check-periodic-metadata-update`
on the standalone [Admin Server](administration/admin_server.md) to amortise the cost of this
operation.

When asked for a safety check (e.g., ldshell's `check-impact`), the safety
checker will use the cache to speed up this comprehensive check or it will have
to wait for the metadata cache to be populated if empty or stale.

### Capacity Checks
Safety checker also supports validating that after a given operation we maintain
minimum storage node and sequencing capacity available. The way this is measured
follows the following strategy:
- The total storage capacity is the sum of all configured storage capacity
  values of storage nodes. Note that by default all storage nodes
  have `--storage-capacity=1` unless configured otherwise.
- Similarly, the total sequencing weight is the sum of all configured
sequencer `--sequencer-weight` (also defaults to `1`)
- If any storage node or sequencer node are down (`DEAD` in the failure
  detector) then we consider that this is a lost capacity.
- Nodes that we want to check the impact of taking down will be deducted from
the remaining available capacity given their configured weights/capacity.

The capacity thresholds are configured via the configuration file in
`server_settings` section as following:
- `max-unavailable-storage-capacity-pct`: _(defaults to `25`)_ This means that
we will reject operations that will result in taking more than 25% of the
storage capacity of the cluster down.
- `max-unavailable-sequencing-capacity-pct`: _(defaults to `25`)_ This means that
we will reject operations that will result in taking more than 25% of the
sequencing capacity of the cluster down.


> If you are using ldshell's `check-impact` command, note that these take
> explicit arguments for these thresholds, `check-impact` will **not** respect
> the values configured in the configuration file.


## Understanding Safety Checker Impact Result
Safety checker returns enough information on why it thinks that an operation is
unsafe, that information will be rendered via ldshell in `check-impact` command
or `maintenance show` for `BLOCKED_UNTIL_SAFE` maintenances if you pass
the `show-safety-check-results` argument. In this section we will explain how to
interpret the output.

**Impact Result:**
- `REBUILDING_STALL`: This means that data rebuilding will not be able to
  re-replicate the lost data (after the operation is done, or now if no new nodes
  are asked to be taken down). Rebuilding will stall waiting for the shards to
  come back online. Rebuilding will unstall if we **explicitly declared that these
  shards have permanently lost their data** via ldshell's command
  `maintenance mark-data-unrecoverable`.
- `WRITE_AVAILABILITY_LOSS`: We will lose write availability for specific logs
  (or all logs). It means that we cannot form a new valid copyset given the
  remaining nodes.
- `READ_AVAILABILITY_LOSS`: We will lose too many shards that readers will stall
  since they will not be able to perform data loss detection
  [f-majority check](ReadPath.md#gap-detection-algorithm).
- `STORAGE_CAPACITY_LOSS`: We will lose enough storage capacity below the
  acceptable threshold.
- `SEQUENCING_CAPACITY_LOSS`: We will lose enough sequencing capacity below the
  acceptable threshold.

In ldshell, we will render this like following:
![Check Impact Example](assets/ldshell-check-impact-example.png "Check Impact
    Example")
The output of ldshell shows a set of example epochs that will be affected by the
operation (or are already affected if no arguments are passed to `check-impact`)

> Note that the output highlights an **example** of the affected logs or epochs,
> if you are interested in testing a specific log-id, you will need to pass
> the `logs` argument to `check-impact` to specify the log-ids you are
> interested in.

Let's break this down line by line:
- `Log: 9`: Is the log-id
- `Epoch: 95`: Is the epoch number for this log that we are testing
- `Storage-set Size: 13`: The number of nodes/shards in the nodeset for this
epoch
- `Replication: {rack: 3}`: The replication property configured for **this
epoch**
- `Write/Rebuilding availability: 13 → 10  (we need at least 3 nodes in 3 racks
  that are healthy, writable, and alive.)`: This tells you that our write or
  rebuilding availability requirement. We are reducing the number of nodes from
  `13` to `10` but this doesn't violate our write availability requirement.
  In order for us to form a correct copyset in this epoch, we need 3 nodes to
  be write-available in 3 different racks. We are good.
- `Read availability: We can't afford losing more than 2 nodes across more than
2 (actual 3) racks. Nodes must be healthy, readable, and alive.`: This is our
  read-availability requirement, as you can see as a result of the operation
  we will lose more nodes across racks than we can afford. The result of the
  test is that we will lose nodes across **3** racks but we can't afford losing
  more than **2**.
- `Impact: READ_AVAILABILITY_LOSS`: This is the summary of the impact. We will
lose read availability.

Afer this, we show a table of the nodes/shards in this particular epoch's
nodeset that we are checking. The nodes are grouped according to the biggest
replication domain as configured in this epoch. In this case, we are grouping
nodes per rack identifier. Note that we split the output on multiple
tables/lines for readability.

> Note that if you don't specify a [`location`](../configuration.md#location-location)
> string for your storage nodes, the shards in this nodeset table will be
> grouped under `UNKNOWN` header.

This shows that our nodeset has one shard (e.g., `N5:S10`) per rack. And we
highlight the nodes such that:
- Shards that we are changing their state appear as `READ_WRITE → DISABLED`,
  they also appear in **bold**
- Shards that are already disabled/dead also show their current status
(`READ_ONLY` and/or `DEAD`). These usually will appear with different colors.

This gives you a visual representation of the epoch and this is usually enough
to convince you that an operation is unsafe.
