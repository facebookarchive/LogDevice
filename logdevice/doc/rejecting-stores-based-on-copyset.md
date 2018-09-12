We reject STORE messages with copyset intersecting rebuilding set. Why?

Consider the scenario:
 1. we started rebuilding node N (wrote SHARD_NEEDS_REBUILD to event log), but N is still alive and taking writes, then
 2. some of the donor nodes quickly finished rebuilding some log, then
 3. a sequencer for this log appended a new record X and stored a copy on N and other copies on donors that have already rebuilt this log,
 4. rebuilding finished; now N has authoritative status AUTHORITATIVE_EMPTY, which means that (a) readers expect records to be replicated outside N, which is not true for record X; readers may ship data loss in place of X, (b) from operations point of view it looks safe to remove node N from the cluster since it was just rebuilt; but if you do so, record X will stay underreplicated.

How to prevent that? Some obvious measures:
 - make node N reject STOREs if it sees that it's in rebuilding set, according to event log,
 - make sequencer not store copies on nodes in rebuilding set.
Both of these options leave a race condition: if node N or sequencer node fall behind on reading the event log, they may let the bad STOREs through. A solution that doesn't have this race condition is to make _all_ storage nodes reject STOREs that have N in copyset. Then our scenraio can go two ways:
 - sequencer sent a wave of STOREs, and some of them were rejected; it'll send more waves and eventually store a wave that doesn't have N in copyset; or, possibly, it will hit the next case:
 - sequencer sent a wave of STOREs, and none of them were rejected; this means that all the copies were stored on nodes that haven't yet seen the SHARD_NEEDS_REBUILD event, i.e. they haven't started rebuilding yet; when they finally see the event and start rebuilding, one of them will rebuild our record, so the record won't be underreplicated.

Disaster averted.
