---
id: Logs
title: Log configuration
sidebar_label: Log configuration
---
In order to use LogDevice, you will need to create logs groups. Each log-group
in LogDevice defines a range of log IDs, and log-groups can be further grouped into directories forming a file-system-like structure. Attributes of directories propagate to sub-directories and log-groups. Attributes of log-groups give you the control over things like the replication property, some sequencer settings, and more. Log attributes are defined below in this document for your reference.

This tree structure is stored internally in a durable replicated state-machine (logdevice internal logs). This is the default storage backend that we use in production use-cases of LogDevice. This allows dynamic provisioning and decommissioning of logs using either ldshell or the LogDevice client API.

Let's start by looking into the current tree of the logs config:

```text
$ ldshell -c <config-path> logs show

Logging to /tmp/ldshell-yb0bq1b_
Logging Level: WARNING
▼ /
  Version: 244813135873
  Attributes:
  - extra-copies: 0
  - synced-copies: 0
  - max-writes-in-flight: 1000
  - single-writer: False
  - sequencer-affinity: None
  - backlog: None
  ...
```
The tree is currently empty, we can only see the root directory `/` with the default attributes.

To create log-groups or directories, you will need to use `ldshell` for this. Here is an example of creating a single log-group with a range of IDs from 1 to 10 (inclusive). Setting the replication property to be 3 nodes replicated.

```text
$ ldshell -c <config-path> logs create --from 1 --to 100 --replicate-across "node:3" /my-logs

Logging to /tmp/ldshell-x673_hgj
Logging Level: WARNING
◎ /my-logs (1..100)
  Version: 244813135878
  Attributes:
  - extra-copies: 0
  - synced-copies: 0
  - max-writes-in-flight: 1000
  - single-writer: False
  - sequencer-affinity: None
  - replicate-across: {'node': 3}    (Overridden)
  - nodeset-size: 9
  - backlog: None
  - delivery-latency: None
  - scd-enabled: False
  - local-scd-enabled: False
  - write-token: None
  - sticky-copysets: False
```

Now as you have created the log-group `/my-logs`, you can use either the LogDevice client API or any of the ld* utilities to test appending and tailing any of the logs in the log-range 1..10.

## Removing Logs
If you would like to remove a log-group or a directory, you can use ldshell:

```text
$ ldshell -c <config-path> logs remove /my-logs
```

NOTE: Deleting the logs from the logs config means that the data stored for these logs will eventually get trimmed. LogDevice has a safety mechanism that prevents data from getting deleted immediately. Only after a configurable grace period (see setting ....[TODO].) the data will deleted. This is 48 hours by default. During that period if you created the same log-range again, the old data will be accessible.

## Directories
You can also create directories to organize your log-groups. These directories can have sub-directories or log-groups. You can always assign attributes to directories and these attributes will be inherited by all sub-directories and log-groups as long as they are not overridden further down the tree.

For instance, you can create a directory for all log-groups that are replicated 4 times on at least two racks.

```text
$ ldshell -c <config-path> logs create --directory --replicate-across "rack:2; node:4" /important

Logging to /tmp/ldshell-7ibt7c5z
Logging Level: WARNING
▼ /important/
  Version: 244813135879
  Attributes:
  - extra-copies: 0
  - synced-copies: 0
  - max-writes-in-flight: 1000
  - single-writer: False
  - sequencer-affinity: None
  - replicate-across: {'node': 4, 'rack': 2}    (Overridden)
  - nodeset-size: 9
  - backlog: None
  - delivery-latency: None
  - scd-enabled: False
  - local-scd-enabled: False
  - write-token: None
  - sticky-copysets: False

```

Tip: did you notice the `(Overridden)` tag there? This is to tell you which attributes are inherited from parents and which ones are explicitly overridden for this log-group or directory.

# Updating Attributes
You can update the log/directory attributes at any time while the cluster is running. Note that some of these changes might cause a sequencer reactivation, there should be no noticeable impact in production.

Let's update the log-group `/my-logs` to set up a data retention duration. Currently `backlog` is set to `None` which means that LogDevice will not trim records automatically. Let's set the backlog to 36 hours. Records older than 36 hours will automatically get trimmed.

```text
$ ldshell -c <config-path> logs update --backlog 36hr /my-logs

Logging to /tmp/ldshell-8ufxii7u
Logging Level: WARNING
Original Attributes:
  - extra-copies: 0
  - synced-copies: 0
  - max-writes-in-flight: 1000
  - single-writer: False
  - sequencer-affinity: None
  - replicate-across: {'node': 3}    (Overridden)
  - nodeset-size: 9
  - backlog: None
  - delivery-latency: None
  - scd-enabled: False
  - local-scd-enabled: False
  - write-token: None
  - sticky-copysets: False

New Attributes will be:
  - extra-copies: 0
  - synced-copies: 0
  - max-writes-in-flight: 1000
  - single-writer: False
  - sequencer-affinity: None
  - replicate-across: {'node': 3}    (Overridden)
  - nodeset-size: 9
  - backlog: 36hr (Edited)
  - delivery-latency: None
  - scd-enabled: False
  - local-scd-enabled: False
  - write-token: None
  - sticky-copysets: False
Are you sure you want to update the attributes at '/my-logs'? (y/n)y

Attributes for '/my-logs' has been updated!
```
The `Edited` tag here shows which attributes will get updated and what will be the resulting log attributes for your review before accepting them.

# Rename
You can also rename a log-group, note that you cannot arbitrarily move log-groups in the logs config tree. The only allowed operation is to change a log-group or a directory name in-place.

```text
$ ldshell -c <config-path> logs rename /my-logs /new-logs
Logging to /tmp/ldshell-77bemjub
Logging Level: WARNING
Are you sure you want to rename "/my-logs" to "/new-logs"? (y/n)y

Path '/my-logs' has been renamed to '/new-logs'
```

# Edit Range
You can also extend, shrink, or even change the entire range of IDs for the log-group. **Note** that log IDs that will be removed from the range will have its data trimmed after the safety grace period as described above.

```text
$ ldshell -c <config-path> logs set-range --from 10 --to 100 /my-logs
Logging to /tmp/ldshell-vpote0p4
Logging Level: WARNING
Are you sure you want to set the log range at "/my-logs" to be (10..100) instead of(1..100)? (y/n)y

Log group '/my-logs' has been updated!
```

# Log Attributes
This is a list of the log attributes that you can configure and what they mean.
## `replicate-across`
Following options are available for replication constraints (similar to the `location` sections in the node configuration).:
- `node`
- `rack`
- `row`
- `cluster`
- `region`

For each log, they can be defined as a combination of any of the above, e.g.:

```text
--replicate-across "region:2; node:4"
```
This will in total place 4 copies, for which at least two are in two different regions.

You can get a bit more creative with this if you like, something like:

```text
--replicate-across "region:2; rack:2; node:4"
```
This means that we would like to place 4 copies over 2 regions and in 2 racks in each region. So each copy ends up in a different rack across the 2 regions. Nifty, right?

## `backlog`
The data in the log range can be trimmed by the node after the time period specified in this attribute. Note that there is no guarantee that the data will be trimmed soon after `backlog` expires and sometimes it can be trimmed much later.
## `nodeset-size`
This is used by LogDevice as a _hint_ on how big the storage set for these logs. This is not the exact size of the nodeset and LogDevice will round this up to the closest possible value that can still satisfy the replication property. Note that changing the `nodeset-size` attribute only affects newly created epochs and does not expand the historical epochs. These ones are immutable once created.
## `scd-enabled`
SCD stands for "Single Copy Delivery". This enables the bandwidth/IO saving copy delivery technique for readers. This optimization targets lowering the read amplification (records read by servers vs. records delivered). In that mode, not all storage nodes that have a copy will attempt to deliver it to clients.
## `max-writes-in-flight`
This attribute controls how many records the sequencer will keep in-flight before receiving all acknowledgements from storage nodes. We commonly refer to this as the `sequencer sliding window`. If the sequencer has its sliding window full (maybe the storage nodes are slow or the client is writing too fast), the client will start seeing append errors in the form of the error code `E::SEQNOBUFS`.
## `sticky-copysets`
This enables a feature that will make the sequencer pick the same copyset for a block of records stored consecutively. It will start a new block by generating a new copyset whenever a threshold for the total size of processed appends is hit, or the block's maximum lifespan expires.
## `extras`
This is a dictionary-like field that is left for the operators of LogDevice to store metadata about this log-group into. One common use-case for this is to store who is the _oncall_ team for a particular log-group.

```text
$ ldshell -c <config-path> logs update --extras "oncall: security-team" /my-logs

Logging to /tmp/ldshell-p8ombp_v
Logging Level: WARNING
Original Attributes:
  - extra-copies: 0
  ...
New Attributes will be:
  ...
  - extra-attributes: {'oncall': 'security-team'} (Edited)
Are you sure you want to update the attributes at '/my-logs'? (y/n)
```
