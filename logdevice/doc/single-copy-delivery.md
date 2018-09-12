# Single Copy Delivery

Single Copy Delivery (or SCD) is an optimization that consists in having only one copy of each record be sent to readers
when in steady state. Without it, storage nodes send everything they have and readers take care of de-duplicating. This
is good for read availability if a storage node goes down but we waste a lot of network bandwidth and cpu on the client.
When SCD is enabled, only one storage node sends each record. When a storage node is down or does not have a record it
is supposed to send, some failover mechanisms are put in place by the client.

## Primary recipient

When a read stream is in SCD mode, each record in the log will be sent by only one storage node, the primary recipient.
In steady state (when there is no failover in place), the primary recipient is the first node in the record's copyset.

DB content for node N0:
lsn: 42, copyset: [1, 0, 2, 3]
lsn: 43, copyset: [3, 5, 0, 1]
lsn: 44, copyset: [0, 1, 2, 3]
lsn: 45, copyset: [4, 0, 5, 2]
lsn: 46, copyset: [0, 3, 2, 1]
lsn: 47, copyset: [4, 3, 2, 5]
lsn: 48, copyset: [1, 4, 0, 5]
Here N0 will only send {44, 46}.

## Failover mechanisms on clients

### Known down list

Clients start reading from a storage node by sending a START message. In this message, the flag SINGLE_COPY_DELIVERY
defines if the created read stream will use SCD. If the flag is set, the START message also contains a list of node
indices — the "known down list" — that are considered down by the client.

The storage nodes take this list into account when deciding if a record should be shipped. To say it differently: the
primary recipient of a record is in fact the first recipient in that record's copyset that's not in the known down list
of the read stream.

Using the example above:
- If known_down = {N1}, N0 will send {42, 44, 46}.
- If known_down = {N1, N4}, N0 will send {42, 44, 45, 46, 48}.

Note that if a node sees itself in the known down list, it will act as if it was not in the list so it can ship what it
is supposed to ship. This is important for the mechanism that detects that a storage node is back up.

### Rewinding the stream

We say a stream is "rewound" if the client decides to send another START with a start_lsn smaller or equal to the read
pointer of that stream. The client will rewind the stream when it modifies the known_down list or when it switches
between SCD and non-SCD mode (also called all send all mode).

### When does the client add nodes in the known down list and rewinds?

This happens when the client knowns for sure that a node is currently unable to send some records it is suppose to send,
because:

* A storage node sent a record having a bad checksum;
* We could not establish a healthy connection to a storage node because we could not send START to it or timed out
  waiting for it to send STARTED or it sent STARTED with an error code.
* The socket of a storage node was closed;
* ClientReadStream has not been able to make any progress for some time (configured with the option --scd-timeout) and a
  storage node has not sent any record beyond next_lsn_to_deliver_. This particular case ensures that we do not get
  stuck forever on the client if a storage node suddenly starts hanging, which can happen if the box OOMs or there is a
  bug causing it to stop sending data.

### When does the client remove nodes from the known down list and rewinds?

This happens when the client decides that a storage node should not be considered down anymore. Storage nodes that are
in the known down list still receive START messages and can send RECORDs even though they see themselves in the list.
Each time ClientReadStream slides the window, it checks each storage node in the known down list to see if it has been
sending RECORDs. If that's the case for a node, it is removed from the list and the stream is rewound.

### When does the client do failover to All Send All mode?

All Send All mode is the mode where SCD is not enabled, ie each storage node sends everything it has, ie each copy of a
data record will be sent. Failing over to that mode is expensive, the client tries really hard to avoid it, but
sometimes it is necessary in order to make progress. The client will decide to failover to that mode when:

* The sum of the number of nodes in the known down list plus the number of nodes we know can't send
  next_lsn_to_deliver_ is equal to the read set size. Each time we receive a gap or a record, we check for the above
  condition in order to do failover immediately if required;
* ClientReadStream has not been able to make any progress for some time (configured with the option
  --scd-all-send-all-timeout) and *all* storage nodes do not seem to have sent anything beyond next_lsn_to_deliver_, or
  the known_down list has not changed during that time.

It is important to notice that ClientReadStream will *never* deliver a gap to the client while in SCD mode. Indeed,
each time there is an epoch bump or data loss, we will first switch to all send all mode.

### When does the client switches back to SCD mode?

Switching back to SCD mode is done the next time we slide the senders' window.

## Appender stages

This mechanism relies on the fact that the first node indices in a record's copyset must store a copy of the record for
sure. If extras (X) is configured to be 0, this is not an issue. However, if X > 0, there are X node indices in the
record's copyset that will — in most cases — not end up storing the record. In order for SCD to work, we need to try
hard to ensure that these X node indices are the right-most indices in the copyset. This is the responsibility of
Appender.

Appender's lifetime is divided into 3 stages:
1. Replication stage;
2. Sync-leader stage;
3. Leader redundancy stage.

Example candidate copyset: [3, 2, 1, 0, 4]. R = 3, X = 2

### Replication stage

The replication stage consists in waiting for any R recipients in the copyset to reply that they successfully stored
their copy. When this stage is complete, there are X node indices in the copyset for storage nodes that do not actually
store the record, and they can be scattered in any order in the copyset. This stage is sufficient for being able to
reply back to the client with an APPENDED message.

Example: at the end of this stage, N2, N0 and N4 replied positively.
    x     x  x
[3, 2, 1, 0, 4]

### Sync-leader stage

The log group of the log is configured with a parameter called "num-sync-leaders", which defaults to 1. This is the
number of left-most recipients in the copyset that *must* have their copy before the record can be released for delivery
(ie the Appender retires).

If num-sync-leaders = 1, we must wait for N3 to store its copy before releasing the record for delivery.

Example: during this stage, N3 responded positively. As a result, a DELETE will be sent to N4. It is now possible for
readers to start reading this record.
 x  x     x
[3, 2, 1, 0, 4]

### Leader redundancy stage

The log group also has a configuration parameter "num-leaders" which defines the number of left-most recipients that the
Appender should ensure have their copy before it completes its state machne. This parameter will default to R.

If num-leaders = 3, we must wait for N3, N2, N1 to store their copy before Appender completes its state machine.

Example: during this stage, N1 responded positively. As a result, a DELETE will be sent to N0.
 x  x  x
[3, 2, 1, 0, 4]

N3 will always ship the record in steady state. If N3 is down, N2 will take over. If both N3 and N2 are down, N1 will
take over.

Appender tries really hard to complete these 3 stages successfully so that SCD can function correctly. However, it will
always prioritize write availability over this. For more information, see Appender.h.

## Local SCD

Local SCD is a further optimization where readers attempt to read from local storage nodes (those in the same region,
datacenter, cluster, etc.), which helps in reducing read latency.

When clients connect to storage nodes, they can send their location as part of the HELLO message. They then send the
START message with the LOCAL_SCD_ENABLED flag to read in local SCD mode. Each storage node computes the priority of
each node within copysets of the records being read. Nodes are given priority (0-5) equal to the number of location
scopes they share with the client location.
* 0 = nodes are in different regions
* 5 = nodes are in the same rack
The node that is the left-most node in the highest alive priority group of the copyset sends that record to the client.

Example:
Nodes N0, N1, and N2 are local

DB content of N1:
  lsn: 42, copyset: [1, 0, 2, 3], will be sent by N1
  lsn: 43, copyset: [3, 5, 0, 1], will be sent by N0
  lsn: 44, copyset: [0, 1, 2, 3], will be sent by N0
  lsn: 45, copyset: [4, 0, 5, 2], will be sent by N0
  lsn: 46, copyset: [0, 3, 2, 1], will be sent by N0
  lsn: 47, copyset: [4, 3, 2, 5], will be sent by N2
  lsn: 48, copyset: [1, 4, 0, 5], will be sent by N1

- If known_down = {N0}, N1 will send {42, 43, 44, 48}.
- If known_down = {N0, N2}, N0 will send {42, 43, 44, 46, 48}.

### Current implementation

The current implementation of local SCD does not employ all priority levels. Local SCD is only supported at the REGION
granularity. What this means it that all the storage nodes that share the same region as the client have the same
priority of 1, where everything else gets a priority of 0. This is because restricting messages within the same
datacenter, cluster, etc. does not save much latency compared to region. I am planning on adding more scope
functionality, which could help with testing.
